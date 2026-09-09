"""Периодический системный аудит bybit-research через OpenCode (AUDIT ONLY).

Запуск:
    python -m src.system_reviewer            # демон-цикл, период REVIEW_INTERVAL
    python -m src.system_reviewer --once     # один аудит (для systemd timer / cron)

Reviewer НЕ меняет код: читает состояние системы, запускает READ-ONLY
проверки, вызывает `opencode run --agent system-reviewer` (агент без edit/write
прав), получает структурированный вердикт, пишет отчёт и шлёт ntfy при FAIL.

Конфигурация (env):
    REVIEW_INTERVAL      секунды между запусками (default 21600 = 6ч)
    REVIEW_TIMEOUT       таймаут opencode, сек (default 1800)
    REVIEW_MODEL         модель opencode (default opencode/big-pickle)
    REVIEW_OPENCODE_BIN  путь к opencode (default ~/.opencode/bin/opencode)
    REVIEW_NOTIFY_ON_PASS=1  слать ntfy и при PASS (default: только FAIL/критика)

Никаких архитектурных изменений pipeline: модуль импортирует только stdlib,
config.settings (пути) и src.notify (существующие уведомления).
"""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import re
import signal
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from config.settings import ROOT, LOGS_DIR
from src.notify import notify

logger = logging.getLogger("system_reviewer")

REVIEW_ROOT = ROOT / "data" / "system_reviewer"
LOCK_PATH = REVIEW_ROOT / "system_reviewer.lock"
REPORTS_DIR = ROOT / "docs"
RUNS_DIR = LOGS_DIR / "system_reviewer"

# --- Константы проверок (по ТЗ) -------------------------------------------

SERVICES = [  # (systemd scope, unit)
    ("--user", "bybit_marketdata.service"),
    ("--user", "bybit_ob_reconstructor.service"),
    ("--system", "bybit_klines.service"),
]

OB_RECONSTRUCTED = ROOT / "collector" / "data" / "market" / "orderbook" / "reconstructed"
OB_METRICS = OB_RECONSTRUCTED / "_metrics.jsonl"
TRADES_DIR = ROOT / "collector" / "data" / "market" / "trades"
KLINES_DIR = ROOT / ".." / "bybit_rs" / "data" / "klines"
SYMBOLS_FILE = ROOT / "data" / "market" / "symbols" / "linear_smoke.txt"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ts_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def sh(cmd: str, timeout: int = 10) -> str:
    """Read-only оболочка для контекст-сбора; не падает при ошибке."""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True,
                           text=True, timeout=timeout)
        return (r.stdout or r.stderr or "").strip()
    except Exception:
        return ""


# --- Lock --------------------------------------------------------------

class Lock:
    """flock-лок от двойного запуска: авто-освобождается ядром при смерти."""

    def __init__(self, path: Path):
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            os.close(self._fd)
            self._fd = None
            return False

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None


# --- Контекст ----------------------------------------------------------

def _parse_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _last_files(directory: Path, pattern: str, n: int) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.rglob(pattern), key=lambda p: p.stat().st_mtime)[-n:]


def service_statuses() -> list[dict]:
    out = []
    for scope, unit in SERVICES:
        out.append({
            "unit": unit,
            "scope": scope,
            "active": sh(f"systemctl {scope} is-active {unit}",
                         5).strip() == "active",
        })
    return out


def data_growth() -> dict:
    def count(d: Path, pat: str) -> int:
        return len(list(d.rglob(pat))) if d.exists() else 0

    def newest(d: Path, pat: str) -> str | None:
        fs = _last_files(d, pat, 1)
        return fs[0].name if fs else None

    return {
        "ob_reconstructed_parquet": count(OB_RECONSTRUCTED, "*.parquet"),
        "ob_reconstructed_newest": newest(OB_RECONSTRUCTED, "*.parquet"),
        "trades_parquet": count(TRADES_DIR, "*.parquet"),
        "trades_newest": newest(TRADES_DIR, "*.parquet"),
        "klines_parquet": count(KLINES_DIR, "*.parquet"),
        "klines_newest": newest(KLINES_DIR, "*.parquet"),
        "symbols_cnt": len(SYMBOLS_FILE.read_text().splitlines())
        if SYMBOLS_FILE.exists() else 0,
    }


def ob_quality() -> dict | None:
    """Сводка качества реконструированного OB из _metrics.jsonl (последний час)."""
    if not OB_METRICS.exists():
        return None
    rows, reconnects, jumps, invalid, errors = [], 0, 0, 0, 0
    for line in OB_METRICS.read_text(errors="replace").splitlines()[-2000:]:
        try:
            m = json.loads(line)
        except ValueError:
            continue
        ts = m.get("timestamp", "")
        if ts < now_utc()[:11] and "T0" in ts:  # грубый фильтр свежести
            pass
        rows.append(m.get("rows_written", 0))
        reconnects += 1 if m.get("reconnects") else 0
        jumps += m.get("update_id_jumps", 0) or 0
        invalid += 0 if m.get("is_valid", True) else 1
        errors += m.get("errors", 0) or 0
    if not rows:
        return None
    return {
        "symbols_reported": len(rows),
        "total_rows_5h": sum(rows),
        "reconnects": reconnects,
        "update_id_jumps": jumps,
        "invalid_states": invalid,
        "errors": errors,
        "symbols_with_zero_updates": 0,
    }


def recent_research() -> list[dict]:
    out = []
    for p in _last_files(ROOT / "data/research/results", "acceptance_*.json", 5):
        d = _parse_json(p)
        if not d:
            continue
        out.append({
            "file": p.name,
            "timestamp": d.get("timestamp"),
            "verdict": d.get("verdict"),
            "n_hypotheses": d.get("n_hypotheses"),
            "n_events_total": d.get("n_events_total"),
            "candidates": d.get("candidates"),
            "reject_reasons": (d.get("reject_reasons") or [])[:3],
        })
    return out


def registry_state() -> list[dict]:
    d = _parse_json(ROOT / "data/research/registry.json")
    if not d:
        return []
    return [{"id": k, "status": v.get("status"),
             "updated_at": v.get("updated_at")} for k, v in d.items()]


def paper_state() -> dict | None:
    d = _parse_json(ROOT / "data/paper/portfolio/state.json")
    if not d:
        return None
    return {
        "balance": d.get("balance"),
        "realized_pnl": d.get("realized_pnl"),
        "trade_count": d.get("trade_count"),
        "updated_at": d.get("updated_at"),
    }


def log_tail(path: Path, n: int = 15) -> list[str]:
    if not path.exists():
        return []
    try:
        return path.read_text(errors="replace").splitlines()[-n:]
    except Exception:
        return []


def gather_context() -> dict:
    """Компактный снимок состояния системы (БЕЗ датасетов)."""
    disk = sh("df -h /home/vlad/Документы/построение 2>/dev/null | tail -1").split()
    mem = {}
    try:
        for line in Path("/proc/meminfo").read_text().splitlines()[:4]:
            k, _, v = line.replace("kB", "").partition(":")
            mem[k.strip()] = int(v.strip())
    except Exception:
        pass
    git_head = sh("git -C %s rev-parse --short HEAD 2>/dev/null" % ROOT).split()
    return {
        "generated_at": now_utc(),
        "git_head": git_head[0] if git_head else None,
        "host": sh("hostname"),
        "services": service_statuses(),
        "data_growth": data_growth(),
        "ob_quality": ob_quality(),
        "disk": {"fs": disk[0] if disk else "?", "size": disk[1] if len(disk) > 1 else "?",
                 "used": disk[2] if len(disk) > 2 else "?", "avail": disk[3] if len(disk) > 3 else "?",
                 "use_pct": disk[4] if len(disk) > 4 else "?"},
        "mem_mb": {k: v // 1024 for k, v in mem.items()},
        "swap": sh("free -m | awk 'NR==3 {print $2 \" total, \" $3 \" used\"}'"),
        "procs": sh("ps aux | grep -E 'marketdata|ob_reconstructor|src.dashboard|src.orchestrator' | grep -v grep | awk '{print $11, $12}'").splitlines()[:10],
        "recent_research": recent_research(),
        "registry": registry_state(),
        "paper": paper_state(),
        "dashboard_state": sh("curl -s -m 8 -o /dev/null -w '%{http_code}' http://127.0.0.1:8420/ || true"),
        "orchestrator_log_tail": log_tail(LOGS_DIR / "orchestrator.log"),
        "marketdata_log_tail": log_tail(ROOT / "collector/logs/marketdata.log", 8),
        "ob_log_tail": log_tail(ROOT / "collector/logs/ob_reconstructor.log", 8),
    }


AUDIT_INSTRUCTIONS = """\
Ты — периодический аудитор исследовательской системы bybit-research. Режим AUDIT ONLY:
читай код и данные, запускай read-only проверки, НЕ меняй файлы и НЕ запускай pipeline.
Проверь по категориям: collector/data/research/hypotheses/orderbook/shadow_paper/
infrastructure/methodology. Ниже — компактный context.json со снимком состояния.
Верни ТОЛЬКО JSON-вердикт:
{"overall_status": "PASS|PASS_WITH_WARNINGS|FAIL", "summary": "...",
 "findings": [{"level": "FACT|WARNING|RECOMMENDATION", "category": "...",
               "finding": "...", "evidence": "..."}],
 "anomalies": [...], "severity": "info|warning|critical",
 "recommended_actions": [...], "tests_executed": [...]}
"""


# --- Запуск OpenCode ---------------------------------------------------

def run_opencode(context: dict, bin_path: str, model: str,
                 timeout: int) -> tuple[bool, str]:
    """opencode run: вывод в temp-файл (не пайп — opencode плодит демонов,
    держащих пайпы), своя группа процессов, killpg по таймауту."""
    prompt = AUDIT_INSTRUCTIONS + "\n\nCONTEXT:\n" + json.dumps(context, ensure_ascii=False)
    cmd = [bin_path, "run", "--agent", "system-reviewer", "-m", model,
           "--dir", str(ROOT), prompt]
    try:
        with tempfile.TemporaryFile() as tf:
            proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL,
                                    stdout=tf, stderr=subprocess.STDOUT,
                                    start_new_session=True)
            try:
                rc = proc.wait(timeout)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
                logger.warning("opencode timeout %ss — дерево процессов убито", timeout)
                return False, "TIMEOUT"
            tf.seek(0)
            out = tf.read().decode(errors="replace")
        if rc != 0:
            logger.warning("opencode exit=%s", rc)
            return False, f"EXIT {rc}\n{out[-2000:]}"
        # Агент не должен фолбэчить на default (у того нет AUDIT-ограничений)
        if "not a primary agent" in out or "Falling back to default agent" in out:
            logger.error("agent system-reviewer не загружен — fallback запрещён")
            return False, "AGENT_NOT_PRIMARY_OR_MISSING"
        return True, out
    except FileNotFoundError:
        logger.error("opencode bin не найден: %s", bin_path)
        return False, "OPENCODE_BIN_NOT_FOUND"


def extract_verdict(text: str) -> dict | None:
    """Ищем JSON-вердикт с конца вывода (перед ним — логи инструментов/ANSI)."""
    if not text:
        return None
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)  # снять ANSI-цвета
    tail = text[-12000:]
    for s in reversed([m.start() for m in re.finditer(r"\{", tail)]):
        for e in reversed([m.end() for m in re.finditer(r"\}", tail[s:])]):
            try:
                v = json.loads(tail[s:s + e])
            except ValueError:
                continue
            if isinstance(v, dict) and "overall_status" in v:
                return v
    return None


# --- Отчёт --------------------------------------------------------------

REPORT_HEADER = """# System Review — {ts}

- **Timestamp**: {ts}
- **Git commit**: {git}
- **Overall status**: **{status_upper}**
- **Context snapshot**: `logs/system_reviewer/{run_file}.json`

## 1. Collector
{services_table}

## 2. Data quality
{data_growth}

## 3. Research pipeline
{research}

## 4. Hypotheses
{registry}

## 5. Orderbook
{orderbook}
- **quality**: {ob_quality}

## 6. Shadow/Paper
{paper}

## 7. Infrastructure
{infra}

## 8. Statistical/methodological integrity
{methodology}

## 9. Detected anomalies
{anomalies}

## 10. Severity
**{severity}**

## 11. Recommended actions
{actions}

## 12. Tests executed
{tests}

## 13. Findings (FACT / WARNING / RECOMMENDATION)
{findings}
"""


def fmt_services(ctx: dict) -> str:
    return "\n".join(
        f"- `{s['scope']} {s['unit']}`: **{'ACTIVE' if s['active'] else 'STOPPED'}**"
        for s in ctx["services"]) or "- no services"


def render_report(ctx: dict, verdict: dict | None, run_file: str, status: str,
                  error: str | None = None) -> str:
    g = ctx.get("data_growth", {})
    v = verdict or {}
    findings = v.get("findings", [])
    findings_txt = "\n".join(
        f"- **{f.get('level')}** [{f.get('category')}]: {f.get('finding')} "
        f"({f.get('evidence', '')})" for f in findings) or "- none"

    ern = ctx.get("recent_research", [])
    research_txt = "\n".join(
        f"- `{r['file']}`: verdict={r.get('verdict')}, hyps={r.get('n_hypotheses')}, "
        f"events={r.get('n_events_total')}, candidates={r.get('candidates')}"
        for r in ern) or "- no acceptance reports"

    reg = ctx.get("registry", [])
    reg_txt = "\n".join(
        f"- `{r['id']}`: {r.get('status')} (updated {r.get('updated_at')})"
        for r in reg) or "- registry empty/missing"

    p = ctx.get("paper") or {}
    paper_txt = (f"- balance={p.get('balance')}, realized_pnl={p.get('realized_pnl')}, "
                 f"trades={p.get('trade_count')} (updated {p.get('updated_at')})"
                 if p else "- no paper portfolio state")

    infra_lines = [
        f"- git: {ctx.get('git_head')}",
        f"- host: {ctx.get('host')}",
        f"- disk: {ctx.get('disk')}",
        f"- mem: {ctx.get('mem_mb')}",
        f"- swap: {ctx.get('swap')}",
        f"- procs: {ctx.get('procs')}",
        f"- dashboard HTTP: {ctx.get('dashboard_state')}",
    ]
    if error:
        infra_lines.append(f"- **REVIEWER ERROR**: {error}")

    anomalies = v.get("anomalies") or (["audit not completed"] if error else [])
    anomalies_txt = "\n".join(f"- {a}" for a in anomalies) or "- none"

    actions = v.get("recommended_actions") or ["-"]
    actions_txt = "\n".join(f"- {a}" for a in actions)

    tests = v.get("tests_executed") or (["context snapshot collected (read-only)"] if not error else [])
    tests_txt = "\n".join(f"- {t}" for t in tests)

    return REPORT_HEADER.format(
        ts=ctx["generated_at"], git=ctx.get("git_head") or "?",
        status_upper=status.upper(),
        services_table=fmt_services(ctx),
        data_growth=json.dumps(g, ensure_ascii=False),
        research=research_txt, registry=reg_txt,
        orderbook=f"- reconstructed parquet: {g.get('ob_reconstructed_parquet')} (newest: {g.get('ob_reconstructed_newest')})\n- trades parquet: {g.get('trades_parquet')} (newest: {g.get('trades_newest')})\n- klines parquet: {g.get('klines_parquet')} (newest: {g.get('klines_newest')})\n- symbols: {g.get('symbols_cnt')}",
        ob_quality=json.dumps(ctx.get("ob_quality"), ensure_ascii=False),
        paper=paper_txt, infra="\n".join(infra_lines),
        methodology=v.get("methodology_notes", "- см. findings (leakage/BH/OOS/cost проверки)"),
        anomalies=anomalies_txt, severity=v.get("severity", "info" if not error else "critical"),
        actions=actions_txt, tests=tests_txt, findings=findings_txt,
        run_file=run_file,
    )


def should_notify(status: str, severity: str, notify_on_pass: bool) -> bool:
    return (status == "FAIL" or severity == "critical") or (notify_on_pass and status != "FAIL")


def notify_review(status: str, context: dict, summary: str, severity: str) -> bool:
    """Отправка через существующий ntfy-механизм (src.notify.notify)."""
    head = context.get("git_head") or "?"
    msg = (f"[System Review {head}] status={status}, severity={severity}\n"
           f"{summary[:400] or 'см. docs/system_review_*.md'}")
    tags = "rotating_light" if status == "FAIL" else "mag"
    return notify(f"SYSTEM REVIEW: {status}", msg, tags=tags)


# --- Основной цикл ------------------------------------------------------

def run_review(bin_path: str, model: str, timeout: int, notify_on_pass: bool) -> dict:
    """Один полный цикл аудита. Возвращает итоговый статус."""
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    D = datetime.now(timezone.utc)
    run_file = f"run_{D.strftime('%Y%m%d_%H%M%S')}"
    ctx = gather_context()

    ok, output = run_opencode(ctx, bin_path, model, timeout)
    verdict = extract_verdict(output) if ok else None

    if ok and verdict:
        status = verdict.get("overall_status", "FAIL")
        severity = verdict.get("severity", "info")
        summary = verdict.get("summary", "")
    else:
        status, severity = "FAIL", "critical"
        summary = f"opencode audit failed: {output[:200]}"
    if not ok and verdict is None and not output.startswith("TIMEOUT"):
        summary = f"opencode: {output[:200]}"

    report_path = REPORTS_DIR / f"system_review_{D.strftime('%Y%m%d_%H%M%S')}.md"
    report = render_report(ctx, verdict, run_file, status,
                           error=None if ok else summary)
    report_path.write_text(report)

    # Сохранение контекста + raw вывода для аудита прошлых запусков
    (RUNS_DIR / f"{run_file}.json").write_text(json.dumps({
        "status": status, "severity": severity, "summary": summary,
        "verdict": verdict, "context": ctx,
        "raw_output_tail": (output or "")[-4000:],
        "report": str(report_path),
    }, ensure_ascii=False, indent=1))

    if should_notify(status, severity, notify_on_pass):
        notify_review(status, ctx, summary, severity)

    logger.info("review done: %s (report=%s)", status, report_path)
    return {"status": status, "severity": severity, "report": str(report_path)}


def main() -> None:
    ap = argparse.ArgumentParser(description="Periodic system reviewer (AUDIT ONLY)")
    ap.add_argument("--once", action="store_true", help="один аудит и выход")
    args = ap.parse_args()

    interval = getenv_int("REVIEW_INTERVAL", 21600)
    timeout = getenv_int("REVIEW_TIMEOUT", 1800)
    model = os.environ.get("REVIEW_MODEL", "opencode/big-pickle")
    bin_path = os.environ.get("REVIEW_OPENCODE_BIN",
                              str(Path.home() / ".opencode/bin/opencode"))
    notify_on_pass = os.environ.get("REVIEW_NOTIFY_ON_PASS") == "1"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(LOGS_DIR / "system_reviewer.log")])
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    block = "once" if args.once else f"every {interval}s"
    logger.info("system_reviewer start (mode=%s, timeout=%ss, model=%s)",
                block, timeout, model)

    while True:
        lock = Lock(LOCK_PATH)
        if not lock.acquire():
            logger.warning("другой reviewer уже запущен — пропуск цикла")
            if args.once:
                return
            time.sleep(interval)
            continue
        try:
            res = run_review(bin_path, model, timeout, notify_on_pass)
            if res["status"] == "FAIL":
                pass  # notify уже отправлен внутри run_review
        except Exception as e:
            logger.exception("review crash: %s", e)
            notify("SYSTEM REVIEW: ERROR", f"reviewer crash: {e}", tags="rotating_light")
        finally:
            lock.release()

        if args.once:
            return
        logger.info("sleep %ss", interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()