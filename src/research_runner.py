"""Research Cycle Runner — безопасный автоматический исследовательский цикл.

Оркестратор над существующим ResearchController (контроллер не меняется):

  A. DATA BOUNDARY: живой gate -> stop marketdata -> стабилизация ФС ->
     повторный scan klines_max -> frozen_boundary.json.
  B. OB: при устаревших OB metrics кратко поднять reconstructor -> дождаться
     валидных -> остановить ДО research. Недостаток OB не fatal.
  C. RESEARCH: цикл subprocess-запусков controller --once --limit 40.
     Frozen boundary держит cycle_id стабильным: controller продолжает
     существующий next_selection, BASELINE внутри цикла не выбирается
     (guard). Остановка: PASS_PENDING_PAPER / STOPPED / бюджет 12 / family 4
     (семейные лимиты — внутри controller).
  D. RESOURCE SAFETY: check MemAvailable/swap перед каждым тяжёлым run;
     OOM/exit!=0 -> цикл останавливается с диагнозом, без бесконечных
     повторов; --limit не уменьшается автоматически.
  E. RECOVERY: после падения процесса runner видит frozen_boundary.json ->
     цикл тот же; controller._recover() закрывает висящий RUNNING как
     FAILED(interrupted); использованные config_hash дедуплицируются,
     BASELINE не повторяется.
  F. FINISH: clear_frozen_boundary() + restore marketdata; paper/timers не
     трогаются; краткий итог в лог.

Все внешние интеграции (systemctl, scan_*, subprocess controller) —
модульные функции: подменяются в тестах через unittest.mock.patch.
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config.settings import LOGS_DIR, RESEARCH_DIR, load_toml
from src.data_ready import (
    Lock,
    check_ready,
    clear_frozen_boundary,
    config_hash,
    git_head,
    load_frozen_boundary,
    now_utc,
    save_frozen_boundary,
    scan_klines,
    scan_ob,
)

logger = logging.getLogger("research_runner")

RUNNER_LOCK = RESEARCH_DIR / "runner.lock"
RUNNER_STATE = RESEARCH_DIR / "runner_state.json"
STATE_PATH = RESEARCH_DIR / "controller_state.json"
JOURNAL_PATH = RESEARCH_DIR / "experiments.jsonl"
ARTIFACTS_DIR = RESEARCH_DIR / "experiments"

MARKETDATA_SERVICE = "bybit_marketdata.service"
RECONSTRUCTOR_SERVICE = "bybit_ob_reconstructor.service"

_GATE = load_toml("auto_research.toml")["gate"]
_CTL_CFG = load_toml("research_controller.toml")

LIMIT_DEFAULT = 40              # рабочий лимит; не уменьшается автоматически
MIN_FREE_GIB = 30.0             # порог MemAvailable перед тяжёлым run
MIN_SWAP_FREE_GIB = 2.0         # порог SwapFree перед тяжёлым run
OB_POLL_SEC = 5
OB_TIMEOUT_SEC = 180            # терпимый таймаут: OB не fatal
FREEZE_POLL_SEC = 5
STABLE_READS = 3                # сколько раз подряд klines_max не меняется
FREEZE_TIMEOUT_SEC = 120


# --- systemctl (интеграция) ---------------------------------------------

def systemctl(action: str, unit: str) -> subprocess.CompletedProcess:
    return subprocess.run(["systemctl", "--user", action, unit],
                          capture_output=True, text=True)


def service_active(unit: str) -> bool:
    return systemctl("is-active", unit).stdout.strip() == "active"


def service_start(unit: str) -> None:
    r = systemctl("start", unit)
    if r.returncode != 0:
        raise RuntimeError(f"systemctl start {unit}: {r.stderr.strip()}")


def service_stop(unit: str) -> None:
    r = systemctl("stop", unit)
    if r.returncode != 0:
        raise RuntimeError(f"systemctl stop {unit}: {r.stderr.strip()}")


# --- controller subprocess -----------------------------------------------

def run_controller(limit: int | None, category: str | None) -> tuple[int, str]:
    cmd = [sys.executable, "-m", "src.research_controller", "--once"]
    if limit:
        cmd += ["--limit", str(limit)]
    if category:
        cmd += ["--category", category]
    r = subprocess.run(cmd, capture_output=True, text=True,
                       cwd=Path(__file__).resolve().parent.parent)
    return r.returncode, (r.stdout + r.stderr)[-2000:]


# --- state I/O -----------------------------------------------------------

def _read_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text())
    except (OSError, ValueError):
        return {}


def _write_state(state: dict) -> None:
    """Атомарная запись controller_state.json (tmp + replace)."""
    tmp = STATE_PATH.parent / (STATE_PATH.name + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str))
    tmp.replace(STATE_PATH)


def _read_journal() -> list[dict]:
    if not JOURNAL_PATH.exists():
        return []
    out = []
    for line in JOURNAL_PATH.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def _cycle_experiments(frozen_cycle_id: str) -> list[dict]:
    return [e for e in _read_journal() if e.get("cycle_id") == frozen_cycle_id]


# --- resource safety (D) -------------------------------------------------

def _mem_info() -> dict[str, float]:
    out: dict[str, float] = {}
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                key, _, rest = line.partition(":")
                if key in ("MemAvailable", "SwapFree", "SwapTotal"):
                    out[key] = int(rest.strip().split()[0]) / 1024 / 1024
    except OSError:
        pass
    return out


def _mem_ok() -> tuple[bool, str]:
    m = _mem_info()
    avail = m.get("MemAvailable")
    if avail is not None and avail < MIN_FREE_GIB:
        return False, f"MemAvailable {avail:.1f}GiB < {MIN_FREE_GIB:.0f}GiB"
    swap_free = m.get("SwapFree")
    if swap_free is not None and swap_free < MIN_SWAP_FREE_GIB:
        return False, f"SwapFree {swap_free:.1f}GiB < {MIN_SWAP_FREE_GIB:.0f}GiB"
    return True, ""


# --- A. DATA BOUNDARY ----------------------------------------------------

def _wait_stable_klines_max() -> datetime | None:
    """Повторный scan klines_max до стабилизации (3 одинаковых замера)."""
    last: datetime | None = None
    stable = 0
    deadline = time.monotonic() + FREEZE_TIMEOUT_SEC
    while time.monotonic() < deadline:
        km = scan_klines().get("klines_max")
        if km is None:
            time.sleep(FREEZE_POLL_SEC)
            continue
        if km.tzinfo is not None:
            km = km.astimezone(timezone.utc).replace(tzinfo=None)
        if km == last:
            stable += 1
            if stable >= STABLE_READS:
                return km
        else:
            last = km
            stable = 0
        time.sleep(FREEZE_POLL_SEC)
    return last


def _restore_marketdata_if_was_active(marketdata_was_active: bool) -> None:
    if marketdata_was_active:
        try:
            service_start(MARKETDATA_SERVICE)
            logger.info("marketdata restored")
        except RuntimeError as e:
            logger.warning("marketdata restore failed: %s", e)


def _active_cycle_boundary() -> dict | None:
    st = _read_state()
    if not st.get("cycle_id") or st.get("state") in ("STOPPED", "PASS_PENDING_PAPER"):
        return None
    b = st.get("boundary") or {}
    if not b.get("klines_max") or not b.get("gate_config_hash"):
        return None
    return b


def _phase_data_boundary() -> tuple[dict | None, dict]:
    ready, reasons, metrics = check_ready()
    if not ready:
        logger.info("Runner SKIP: %s", "; ".join(reasons))
        return None, {"verdict": "SKIP", "reasons": reasons,
                      "metrics": {"klines_max": metrics["klines"]["klines_max"]}}

    marketdata_was_active = service_active(MARKETDATA_SERVICE)
    if marketdata_was_active:
        # Записываем до остановки: если упадём до сохранения снапшота,
        # _recover_interrupted_freeze() вернёт marketdata на место.
        _runner_state_write(marketdata_was_active)
        try:
            service_stop(MARKETDATA_SERVICE)
            logger.info("marketdata stopped")
        except RuntimeError as e:
            logger.warning("marketdata stop failed, continuing: %s", e)
    else:
        logger.info("marketdata already stopped")

    # Активный незавершённый цикл -> продолжаем его boundary, а не создаём
    # новый из текущего klines_max (иначе cycle_id дрейфует и controller
    # сбрасывает цикл обратно к BASELINE).
    active_b = _active_cycle_boundary()
    if active_b is not None:
        klines_max = datetime.fromisoformat(active_b["klines_max"])
        config_hash = active_b["gate_config_hash"]
        logger.info("Continuing active cycle boundary: klines_max=%s "
                    "config_hash=%s", klines_max.isoformat(), config_hash)
    else:
        klines_max = _wait_stable_klines_max()
        if klines_max is None:
            logger.error("klines_max never stabilised; aborting cycle")
            _restore_marketdata_if_was_active(marketdata_was_active)
            return None, {"verdict": "ABORT", "reason": "fs_stabilize_timeout"}
        config_hash = metrics.get("config_hash")
        logger.info("New cycle boundary from data: klines_max=%s config_hash=%s",
                    klines_max.isoformat(), config_hash)

    frozen = {
        "klines_max": klines_max.isoformat(),
        "config_hash": config_hash,
        "git_head": metrics.get("git_head"),
        "data_version": metrics.get("data_version", "1.0"),
        "n_files": metrics["klines"]["n_files"],
        "fresh_symbols": metrics["klines"]["fresh_symbols"],
        "ob_valid_symbols": metrics["ob"]["ob_valid_symbols"],
        "ob_unique_symbols": metrics["ob"]["ob_unique_symbols"],
        "ob_rows": metrics["ob"]["ob_rows"],
        "marketdata_was_active": marketdata_was_active,
        "source": "research_runner",
    }
    save_frozen_boundary(frozen)
    _runner_state_clear()
    logger.info("Frozen boundary saved: klines_max=%s cycle_id=%s:%s",
                klines_max.isoformat(), frozen["config_hash"], klines_max.isoformat())
    return frozen, {"verdict": "FROZEN"}


# --- B. OB ---------------------------------------------------------------

def _refresh_ob(frozen: dict) -> None:
    ob = scan_ob()
    if ob["ob_valid_symbols"] >= _GATE["min_ob_symbols"]:
        logger.info("OB fresh: %d valid symbols", ob["ob_valid_symbols"])
        _sync_frozen_ob(frozen, ob)
        return

    logger.info("OB stale (%d valid < %d): refreshing via reconstructor",
                ob["ob_valid_symbols"], _GATE["min_ob_symbols"])
    try:
        service_start(RECONSTRUCTOR_SERVICE)
    except RuntimeError as e:
        logger.warning("reconstructor start failed (OB non-fatal): %s", e)
        _sync_frozen_ob(frozen, ob)
        return

    deadline = time.monotonic() + OB_TIMEOUT_SEC
    last_ob = ob
    while time.monotonic() < deadline:
        time.sleep(OB_POLL_SEC)
        last_ob = scan_ob()
        if last_ob["ob_valid_symbols"] >= _GATE["min_ob_symbols"]:
            logger.info("OB refreshed: %d valid symbols", last_ob["ob_valid_symbols"])
            break
    else:
        logger.warning("OB not refreshed within %ss (non-fatal, continuing)",
                       OB_TIMEOUT_SEC)
    try:
        service_stop(RECONSTRUCTOR_SERVICE)
        logger.info("reconstructor stopped before research")
    except RuntimeError as e:
        logger.warning("reconstructor stop failed: %s", e)
    _sync_frozen_ob(frozen, last_ob)


def _sync_frozen_ob(frozen: dict, ob: dict) -> None:
    frozen["ob_valid_symbols"] = ob.get("ob_valid_symbols", 0)
    frozen["ob_unique_symbols"] = ob.get("ob_unique_symbols", 0)
    frozen["ob_rows"] = ob.get("ob_rows", 0)
    save_frozen_boundary(frozen)


# --- C. RESEARCH ---------------------------------------------------------

def _frozen_cycle_id(frozen: dict) -> str:
    return f"{frozen['config_hash']}:{frozen['klines_max']}"


def _max_experiments() -> int:
    return int(_CTL_CFG.get("budget", {}).get("max_experiments_per_boundary", 12))


def _abort_cycle(frozen: dict, reason: str) -> dict:
    logger.error("Cycle ABORT: %s", reason)
    return {"verdict": "ABORT", "reason": reason, "finished": True}


def _mark_dangling_interrupted(frozen_cycle_id: str) -> int:
    """Помечает висящий RUNNING эксперимента цикла как FAILED(interrupted)."""
    n = 0
    with open(JOURNAL_PATH, "a", encoding="utf-8") as fh:
        for e in _read_journal():
            if (e.get("cycle_id") == frozen_cycle_id
                    and e.get("status") == "RUNNING"):
                fh.write(json.dumps({
                    "experiment_id": e.get("experiment_id"),
                    "cycle_id": frozen_cycle_id,
                    "config_hash": e.get("config_hash"),
                    "mode": e.get("mode"),
                    "status": "FAILED",
                    "final_verdict": "ERROR",
                    "reason": "interrupted",
                    "finished_at_utc": now_utc().isoformat(),
                }, ensure_ascii=False) + "\n")
                n += 1
    return n


# --- C2. RESEARCH -> PAPER ------------------------------------------------

def _launch_paper(state: dict) -> dict:
    """Принять PASS/CANDIDATE-результат в MODE A paper.

    Идемпотентно: повторный вызов для того же experiment_id не перезапускает
    paper; после первого запуска параметры paper заморожены (provenance).
    """
    eid = state.get("pass_pending")
    if not eid:
        return {"verdict": "SKIP", "reason": "no_pass_pending"}

    art_path = ARTIFACTS_DIR / f"{eid}.json"
    try:
        art = json.loads(art_path.read_text())
    except (OSError, ValueError):
        return {"verdict": "SKIP", "reason": "artifact_missing", "experiment_id": eid}

    report_path = art.get("acceptance_report")
    finalist = None
    if report_path:
        try:
            finalist = json.loads(Path(report_path).read_text()).get("finalist")
        except (OSError, ValueError):
            finalist = None
    if not finalist or not finalist.get("hypothesis_id"):
        return {"verdict": "SKIP", "reason": "no_finalist", "experiment_id": eid}

    from src.paper import load_state, reset_state, save_state

    pstate = load_state()
    prov = pstate.get("provenance") or {}
    if prov.get("experiment_id") == eid:
        return {"verdict": "ALREADY_LAUNCHED", "experiment_id": eid,
                "hypothesis_id": pstate.get("hypothesis_id")}
    if prov.get("experiment_id"):
        # paper уже привязан к другому результату — не перезаписываем
        return {"verdict": "ALREADY_LAUNCHED", "reason": f"bound:{prov['experiment_id']}",
                "experiment_id": prov["experiment_id"],
                "hypothesis_id": pstate.get("hypothesis_id")}

    launched_at = now_utc().isoformat()
    pstate = reset_state()
    pstate["mode"] = "VALIDATED"
    pstate["hypothesis_id"] = finalist["hypothesis_id"]
    pstate["started_at"] = launched_at
    pstate["provenance"] = {
        "experiment_id": eid,
        "cycle_id": art.get("cycle_id") or state.get("cycle_id"),
        "config_hash": art.get("config_hash"),
        "horizon_min": finalist.get("horizon_min"),
        "entry_side": finalist.get("entry_side"),
        "condition": finalist.get("condition"),
        "description": finalist.get("description"),
        "launched_at_utc": launched_at,
    }
    save_state(pstate)
    logger.info("Paper LAUNCHED: experiment=%s hypothesis=%s equity=%s",
                eid, finalist["hypothesis_id"], pstate["equity_start"])
    return {"verdict": "LAUNCHED", "experiment_id": eid,
            "hypothesis_id": finalist["hypothesis_id"]}


def _resume_research_after_paper(state: dict, handoff: dict) -> None:
    """PASS_PENDING_PAPER -> SELECT_NEXT: research продолжается, paper заморожен."""
    state = dict(state)
    state["state"] = "SELECT_NEXT"
    state["next_selection"] = None
    state["paper_handoff"] = {
        "verdict": handoff.get("verdict"),
        "experiment_id": handoff.get("experiment_id"),
        "hypothesis_id": handoff.get("hypothesis_id"),
        "at_utc": now_utc().isoformat(),
    }
    _write_state(state)


def _phase_research_loop(frozen: dict, limit: int | None,
                         category: str | None, once: bool) -> dict:
    frozen_cycle_id = _frozen_cycle_id(frozen)
    budget = _max_experiments()
    iterations = 0

    while True:
        state = _read_state()
        st = state.get("state")
        if st == "PASS_PENDING_PAPER":
            handoff = _launch_paper(state)
            if handoff.get("verdict") in ("LAUNCHED", "ALREADY_LAUNCHED"):
                _resume_research_after_paper(state, handoff)
                logger.info("Paper handoff %s -> research resumes",
                            handoff["verdict"])
                if once:
                    return {"verdict": "STEP", "paper_handoff": handoff,
                            "experiments": len(_cycle_experiments(frozen_cycle_id)),
                            "finished": False}
                continue
            logger.warning("Paper handoff skipped: %s", handoff.get("reason"))
            return {"verdict": "PASS_PENDING_PAPER", "paper_handoff": handoff,
                    "experiments": len(_cycle_experiments(frozen_cycle_id)),
                    "finished": True}
        if st == "STOPPED":
            logger.info("Cycle terminal: state=%s", st)
            return {"verdict": "DONE",
                    "experiments": len(_cycle_experiments(frozen_cycle_id)),
                    "finished": True}
        if int(state.get("budget_used", 0)) >= budget:
            logger.info("Budget exhausted (%s/%s)", state.get("budget_used"), budget)
            return {"verdict": "DONE", "stop_reason": "budget_per_boundary",
                    "experiments": len(_cycle_experiments(frozen_cycle_id)),
                    "finished": True}
        if iterations >= budget:
            logger.error("Runner loop exceeded %s iterations", budget)
            return _abort_cycle(frozen, "loop_bound")

        ok, why = _mem_ok()
        if not ok:
            logger.error("Resource pressure: %s", why)
            return _abort_cycle(frozen, f"resource_pressure: {why}")

        logger.info("Launching controller (iteration %s/%s)",
                    iterations + 1, budget)
        code, output = run_controller(limit=limit, category=category)
        if code != 0:
            n = _mark_dangling_interrupted(frozen_cycle_id)
            reason = f"controller_exit_{code}"
            if n:
                reason += f"; {n} experiment(s) marked FAILED(interrupted)"
            logger.error("Controller failed (exit=%s): %s", code, output)
            return _abort_cycle(frozen, reason)
        iterations += 1

        # guard: BASELINE внутри цикла допустим только как первый эксперимент.
        # Дедуплицируем по experiment_id (RUNNING + DONE = один эксперимент).
        by_id: dict[str, dict] = {}
        for e in _cycle_experiments(frozen_cycle_id):
            by_id[e.get("experiment_id")] = e
        cycle_exps = list(by_id.values())
        last = cycle_exps[-1] if cycle_exps else None
        if last and last.get("mode") == "BASELINE" and len(cycle_exps) > 1:
            logger.error("Unexpected BASELINE inside frozen cycle "
                         "(experiment %s)", last.get("experiment_id"))
            return _abort_cycle(frozen, "unexpected_baseline")

        if once:
            st2 = _read_state().get("state")
            if st2 == "PASS_PENDING_PAPER":
                # хендовер Research->Paper обрабатывается началом цикла
                continue
            if st2 == "STOPPED":
                logger.info("Cycle terminal after step: state=%s", st2)
                return {"verdict": "DONE",
                        "experiments": len(_cycle_experiments(frozen_cycle_id)),
                        "finished": True}
            return {"verdict": "STEP", "finished": False,
                    "experiment_id": (last or {}).get("experiment_id")}


# --- F. FINISH -----------------------------------------------------------

def _phase_finish(frozen: dict) -> None:
    clear_frozen_boundary()
    logger.info("frozen boundary cleared")
    _restore_marketdata_if_was_active(frozen.get("marketdata_was_active", False))


# --- E. RECOVERY + основной цикл ----------------------------------------

def _runner_state_write(marketdata_was_active: bool) -> None:
    RUNNER_STATE.write_text(json.dumps({
        "marketdata_was_active": marketdata_was_active,
        "created_at_utc": now_utc().isoformat(),
    }))


def _runner_state_clear() -> None:
    try:
        RUNNER_STATE.unlink()
    except FileNotFoundError:
        pass


def _recover_interrupted_freeze() -> None:
    """Падение между stop marketdata и сохранением frozen_boundary.json:
    runner_state.json без frozen -> restore marketdata и начать заново."""
    try:
        rs = json.loads(RUNNER_STATE.read_text())
    except (OSError, ValueError):
        return
    if not load_frozen_boundary():
        logger.warning("Crash leftover: marketdata stopped, no frozen boundary")
        _restore_marketdata_if_was_active(rs.get("marketdata_was_active", False))
    _runner_state_clear()


def run_cycle(limit: int | None = LIMIT_DEFAULT,
              category: str | None = None, once: bool = False) -> dict:
    lock = Lock(RUNNER_LOCK)
    if not lock.acquire():
        logger.warning("Runner SKIP: another cycle in progress (%s)",
                       RUNNER_LOCK.name)
        return {"verdict": "SKIP", "reason": "lock"}

    try:
        _recover_interrupted_freeze()

        frozen = load_frozen_boundary()
        if frozen is None:
            frozen, result = _phase_data_boundary()
            if frozen is None:
                return result
        else:
            logger.info("Recovery: resuming cycle %s", _frozen_cycle_id(frozen))

        _refresh_ob(frozen)
        result = _phase_research_loop(frozen, limit=limit,
                                      category=category, once=once)

        if result.get("finished"):
            _phase_finish(frozen)
            _runner_state_clear()
        return result
    finally:
        lock.release()


def status() -> dict:
    return {
        "frozen_boundary": load_frozen_boundary(),
        "runner_state": json.loads(RUNNER_STATE.read_text())
        if RUNNER_STATE.exists() else None,
        "controller_state": _read_state(),
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(module)s %(message)s")
    fh = logging.FileHandler(LOGS_DIR / "research_cycle.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(module)s %(message)s"))
    logging.getLogger().addHandler(fh)

    ap = argparse.ArgumentParser(description="Bybit Research Cycle Runner")
    ap.add_argument("--limit", type=int, default=LIMIT_DEFAULT,
                    help=f"лимит символов (default {LIMIT_DEFAULT}, не авто-уменьшается)")
    ap.add_argument("--category", type=str, default=None,
                    choices=["linear", "spot"])
    ap.add_argument("--once", action="store_true",
                    help="одна итерация research-цикла и выход")
    ap.add_argument("--status", action="store_true",
                    help="показать состояние цикла")
    args = ap.parse_args(argv)

    if args.status:
        print(json.dumps(status(), ensure_ascii=False, indent=2, default=str))
        return 0

    result = run_cycle(limit=args.limit, category=args.category, once=args.once)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())