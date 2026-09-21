"""Research Controller — state machine над существующим pipeline.

Цикл: data-ready gate -> SELECT_NEXT -> run_pipeline(hypothesis_specs, auto_paper=False)
-> acceptance report -> REJECT_DIAGNOSIS -> SELECT_NEXT -> ... до PASS или бюджета.
PAPER контроллером НИКОГДА не запускается: только `PASS_PENDING_PAPER` + ручная
авторизация вовне. См. docs/research_controller_design.md.

Принцип (минимизация): контроллер не считает статистику сам — он конструирует
`Hypothesis` и вызывает `run_pipeline`; гейты/Critic/registry не меняются.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from config.settings import RESEARCH_DIR, RESULTS_DIR, load_toml
from src.data_ready import Lock
from src.experiment import (
    Experiment,
    build_hypothesis_specs,
    compute_experiment_hash,
    make_experiment_id,
    spec_record,
    utc_now_iso,
)

logger = logging.getLogger("research_controller")

LOCK_NAME = "controller.lock"
JOURNAL_NAME = "experiments.jsonl"
ARTIFACTS_NAME = "experiments"
STATE_NAME = "controller_state.json"

# Порядок fallback-режимов, если диагноз не дал предпочтений.
FALLBACK_MODES = ["THRESHOLD_SWEEP", "REGIME_SWEEP", "HORIZON_SWEEP",
                  "CONDITIONAL", "FAMILY_SWITCH"]

TERMINAL_VERDICTS = ("ERROR", "STOP")


def default_state() -> dict:
    return {
        "state": "READY",
        "cycle_id": None,
        "boundary": {},
        "budget_used": 0,
        "n_tests_cumulative": 0,
        "family_usage": {},
        "used_config_hashes": [],
        "last_experiment_id": None,
        "last_diagnosis": None,
        "last_mode": None,
        "next_selection": None,
        "pass_pending": None,
        "stop_reason": None,
        "updated_at_utc": utc_now_iso(),
    }


class ResearchController:
    """Тонкий контроллер исследований. Всё внешнее инжектируется для тестов."""

    def __init__(self, base_dir: Path | None = None, config: dict | None = None,
                 gate=None, runner=None, now=None):
        self.base = Path(base_dir) if base_dir else RESEARCH_DIR
        self.base.mkdir(parents=True, exist_ok=True)
        self.cfg = config if config is not None else load_toml("research_controller.toml")
        self._gate = gate or self._default_gate
        self._runner = runner or self._default_runner
        self._now = now or (lambda: datetime.now(tz=timezone.utc))
        self._lock = Lock(self.base / LOCK_NAME)
        self._min_t_stat = float(load_toml("research.toml").get("min_t_stat", 2.0))

    # --- paths -----------------------------------------------------------
    @property
    def _state_path(self) -> Path:
        return self.base / STATE_NAME

    @property
    def _journal_path(self) -> Path:
        return self.base / JOURNAL_NAME

    @property
    def _artifacts_dir(self) -> Path:
        d = self.base / ARTIFACTS_NAME
        d.mkdir(parents=True, exist_ok=True)
        return d

    # --- default integrations -------------------------------------------
    def _default_gate(self):
        from src.data_ready import check_ready
        return check_ready()

    def _default_runner(self, **kwargs):
        from src.orchestrator import run_pipeline
        return run_pipeline(**kwargs)

    # --- state I/O -------------------------------------------------------
    def _load_state(self) -> dict:
        try:
            st = json.loads(self._state_path.read_text())
        except Exception:
            return default_state()
        base = default_state()
        base.update(st)
        return base

    def _save_state(self, state: dict) -> None:
        state["updated_at_utc"] = utc_now_iso(self._now())
        tmp = self._state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str))
        tmp.replace(self._state_path)

    def _journal_append(self, record: dict) -> None:
        self._journal_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._journal_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def _read_journal(self) -> list[dict]:
        if not self._journal_path.exists():
            return []
        out = []
        for line in self._journal_path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                logger.warning("controller journal: bad line skipped")
        return out

    def _artifact_path(self, experiment_id: str) -> Path:
        return self._artifacts_dir / f"{experiment_id}.json"

    def _write_artifact(self, exp: Experiment) -> None:
        self._artifact_path(exp.experiment_id).write_text(
            json.dumps(exp.to_dict(), ensure_ascii=False, indent=2, default=str))

    def _read_artifact(self, experiment_id: str) -> dict | None:
        p = self._artifact_path(experiment_id)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text())
        except Exception:
            return None

    # --- recovery (§6) ---------------------------------------------------
    def _recover(self) -> None:
        """Помечает «висящий» RUNNING как FAILED(interrupted); ledger не теряется."""
        entries = self._read_journal()
        last: dict[str, dict] = {}
        for e in entries:
            eid = e.get("experiment_id")
            if eid:
                last[eid] = e
        for eid, e in last.items():
            if e.get("status") != "RUNNING":
                continue
            logger.warning("Recover: dangling RUNNING %s -> FAILED(interrupted)", eid)
            self._journal_append({
                "experiment_id": eid,
                "status": "FAILED",
                "final_verdict": "ERROR",
                "reason": "interrupted",
                "finished_at_utc": utc_now_iso(self._now()),
            })
            art = self._read_artifact(eid)
            if art:
                art["status"] = "FAILED"
                art["final_verdict"] = "ERROR"
                art["reject_diagnosis"] = {"category": "infra",
                                           "reject_reasons": ["interrupted"]}
                self._write_artifact(Experiment.from_dict(art))

        # Собрать ledger конфигураций из журнала (включая прерванные).
        entries = self._read_journal()
        hashes: list[str] = []
        for e in entries:
            h = e.get("config_hash")
            if h and h not in hashes:
                hashes.append(h)

        if self._state_path.exists():
            state = self._load_state()
            for h in hashes:
                if h not in state["used_config_hashes"]:
                    state["used_config_hashes"].append(h)
            if state["state"] == "RUNNING":
                state["state"] = "SELECT_NEXT"
                state["next_selection"] = None
        else:
            state = default_state()
            state["used_config_hashes"] = list(hashes)
            if entries:
                last_entry = entries[-1]
                state["cycle_id"] = last_entry.get("cycle_id")
                state["last_experiment_id"] = last_entry.get("experiment_id")
                state["last_diagnosis"] = last_entry.get("diagnosis")
                state["last_mode"] = last_entry.get("mode")
                state["budget_used"] = sum(
                    1 for e in entries if e.get("status") in ("DONE", "FAILED"))
        self._save_state(state)

    # --- boundary / cycle (§4) ------------------------------------------
    def _boundary(self, metrics: dict) -> dict:
        km = (metrics.get("klines") or {}).get("klines_max")
        if hasattr(km, "isoformat"):
            ts = km.isoformat()
        else:
            ts = str(km) if km else "none"
        return {
            "data_version": str(metrics.get("data_version", "1.0")),
            "klines_max": ts,
            "gate_config_hash": str(metrics.get("config_hash", "")),
            "git_head": str(metrics.get("git_head", "")),
        }

    @staticmethod
    def _cycle_id(boundary: dict) -> str:
        return (f"{boundary['gate_config_hash']}:{boundary['klines_max']}")

    def _reset_cycle(self, state: dict, cycle_id: str, boundary: dict,
                     metrics: dict) -> dict:
        logger.info("New dataset boundary -> cycle %s", cycle_id)
        state["cycle_id"] = cycle_id
        state["boundary"] = boundary
        state["budget_used"] = 0
        state["n_tests_cumulative"] = 0
        state["family_usage"] = {}
        state["used_config_hashes"] = []   # дедуп в пределах boundary; история — в журнале
        state["next_selection"] = None
        state["last_diagnosis"] = None
        state["stop_reason"] = None
        state["state"] = "READY"
        return state

    # --- budget ----------------------------------------------------------
    def _max_experiments(self) -> int:
        return int(self.cfg.get("budget", {}).get("max_experiments_per_boundary", 12))

    def _max_per_family(self) -> int:
        return int(self.cfg.get("budget", {}).get("max_experiments_per_family", 4))

    def _max_tests(self) -> int:
        return int(self.cfg.get("budget", {}).get("max_tests_cumulative", 600))

    # --- diagnosis (§3) --------------------------------------------------
    def _diagnose(self, report: dict) -> str:
        verdict = report.get("verdict")
        if verdict in ("ERROR", "STOP"):
            return "infra"
        stages = {s.get("stage"): s for s in report.get("stages", [])}

        critic = stages.get("critic")
        if critic and critic.get("status") == "REJECT":
            reason = " ".join(critic.get("errors", []))
            if "temporal_stability" in reason:
                return "unstable"
            if "concentration" in reason or "dependency" in reason:
                return "concentrated"
            if "costs" in reason:
                return "cost_sensitive"

        if stages.get("validation_gate", {}).get("status") == "REJECT":
            return "validation_fail"
        if stages.get("oos_gate", {}).get("status") == "REJECT":
            return "oos_fail"
        if verdict == "NO_CANDIDATE":
            return "no_candidates"

        ds = report.get("discovery_summary") or {}
        max_t = ds.get("max_t_stat")
        if (max_t is not None and max_t <= self._min_t_stat) or ds.get("n_positive_t", 0) == 0:
            return "no_signal"
        return "weak_signal"

    # --- selection (§3, §4) ---------------------------------------------
    def _mode_candidates(self, diagnosis: str | None) -> list[str]:
        if diagnosis == "infra":
            return []
        modes = list(self.cfg.get("diagnosis", {}).get(diagnosis or "", []))
        for m in FALLBACK_MODES:
            if m not in modes:
                modes.append(m)
        return modes

    def _family_used(self, state: dict, family: str) -> int:
        return int(state.get("family_usage", {}).get(family, 0))

    def _base_params(self, key: str, meta: dict, horizons: list[int],
                     threshold: float) -> dict:
        return {
            "feature": key,
            "column": meta["column"],
            "operator": meta["operator"],
            "entry_side": meta["entry_side"],
            "threshold": threshold,
            "horizons": list(horizons),
        }

    def _variants(self, mode: str, parent: Experiment | None):
        feats = self.cfg.get("features", {})
        horizons = list(self.cfg.get("horizons", {}).get("default", [5, 10, 30]))

        if mode == "BASELINE":
            yield {"parameters": {"family": "standard"}}
            return

        if mode == "FAMILY_SWITCH":
            order = self.cfg.get("families", {}).get(
                "order", ["baseline", "generator", "mr", "ob"])
            for fam in order:
                p = {"family": fam}
                if fam == "mr":
                    p["thresholds"] = list(
                        self.cfg.get("mr", {}).get("thresholds", []))
                    p["horizons"] = horizons
                yield {"parameters": p}
            return

        if mode == "THRESHOLD_SWEEP":
            for key, meta in feats.items():
                for thr in meta["thresholds"]:
                    yield {"parameters": self._base_params(key, meta, horizons, thr)}
            return

        if mode == "HORIZON_SWEEP":
            for key, meta in feats.items():
                for h in horizons:
                    p = self._base_params(key, meta, horizons,
                                          meta["thresholds"][0])
                    p["horizon"] = h
                    yield {"parameters": p}
            return

        if mode == "REGIME_SWEEP":
            for key, meta in feats.items():
                for rk, rc in self.cfg.get("regimes", {}).items():
                    p = self._base_params(key, meta, horizons,
                                          meta["thresholds"][0])
                    p["regime_key"] = rk
                    p["regime_condition"] = rc
                    yield {"parameters": p}
            return

        if mode == "CONDITIONAL":
            for key, meta in feats.items():
                for ck, cc in self.cfg.get("combinations", {}).items():
                    p = self._base_params(key, meta, horizons,
                                          meta["thresholds"][0])
                    p["second_key"] = ck
                    p["second_condition"] = cc
                    yield {"parameters": p}
            return

    def _select_next(self, state: dict, parent: Experiment | None,
                     diagnosis: str | None, allow_baseline: bool) -> dict | None:
        if diagnosis == "infra":
            return None
        modes = self._mode_candidates(diagnosis)
        if allow_baseline and state.get("budget_used", 0) == 0:
            modes = ["BASELINE"] + [m for m in modes if m != "BASELINE"]

        for mode in modes:
            for variant in self._variants(mode, parent):
                params = variant["parameters"]
                fam = params.get("family")
                if fam and self._family_used(state, fam) >= self._max_per_family():
                    continue
                specs = build_hypothesis_specs(mode, params)
                spec_records = [spec_record(h) for h in specs] if specs is not None else []
                chash = compute_experiment_hash(mode, params, spec_records)
                if chash in state.get("used_config_hashes", []):
                    continue
                variant.update({
                    "mode": mode,
                    "config_hash": chash,
                    "hypothesis_specs": spec_records,
                })
                return variant
        return None

    def _parent_experiment(self, state: dict) -> Experiment | None:
        eid = state.get("last_experiment_id")
        if not eid:
            return None
        art = self._read_artifact(eid)
        return Experiment.from_dict(art) if art else None

    # --- one experiment --------------------------------------------------
    def run_once(self, limit: int | None = None,
                 category: str | None = None) -> dict:
        if not self._lock.acquire():
            logger.info("Controller SKIP: another controller run holds lock")
            return {"state": "SKIP", "reason": "lock"}
        try:
            self._recover()
            state = self._load_state()

            if state["state"] == "STOPPED":
                return {"state": "STOPPED", "stop_reason": state.get("stop_reason")}
            if state["state"] == "PASS_PENDING_PAPER":
                return {"state": "PASS_PENDING_PAPER",
                        "experiment_id": state.get("pass_pending")}

            ready, reasons, metrics = self._gate()
            if not ready:
                state["state"] = "WAIT_DATA"
                self._save_state(state)
                return {"state": "WAIT_DATA", "reasons": reasons}

            boundary = self._boundary(metrics)
            cycle_id = self._cycle_id(boundary)
            if state.get("cycle_id") != cycle_id:
                state = self._reset_cycle(state, cycle_id, boundary, metrics)

            if state["budget_used"] >= self._max_experiments():
                state["state"] = "STOPPED"
                state["stop_reason"] = "budget_per_boundary"
                self._save_state(state)
                return {"state": "STOPPED", "stop_reason": state["stop_reason"]}
            if state["n_tests_cumulative"] >= self._max_tests():
                state["state"] = "STOPPED"
                state["stop_reason"] = "tests_budget"
                self._save_state(state)
                return {"state": "STOPPED", "stop_reason": state["stop_reason"]}

            parent = self._parent_experiment(state)
            selection = state.get("next_selection")
            if not selection:
                selection = self._select_next(state, parent,
                                              state.get("last_diagnosis"),
                                              allow_baseline=True)
            if selection is None:
                state["state"] = "STOPPED"
                state["stop_reason"] = "search_space_exhausted"
                self._save_state(state)
                return {"state": "STOPPED", "stop_reason": state["stop_reason"]}

            return self._execute(state, selection, cycle_id, boundary,
                                 parent, limit, category)
        finally:
            self._lock.release()

    def _execute(self, state: dict, selection: dict, cycle_id: str,
                 boundary: dict, parent: Experiment | None,
                 limit: int | None, category: str | None) -> dict:
        mode = selection["mode"]
        params = selection["parameters"]

        existing = {e.get("experiment_id") for e in self._read_journal()}
        existing |= {p.stem for p in self._artifacts_dir.glob("*.json")}
        eid = make_experiment_id(existing, now=self._now())

        exp = Experiment(
            experiment_id=eid,
            parent_experiment_id=parent.experiment_id if parent else None,
            cycle_id=cycle_id,
            config_hash=selection["config_hash"],
            mode=mode,
            parameters=dict(params),
            hypothesis_specs=selection["hypothesis_specs"],
            created_at_utc=utc_now_iso(self._now()),
            dataset_boundary=boundary,
            status="RUNNING",
        )
        exp.parameters["_selection"] = {
            "diagnosis": state.get("last_diagnosis"),
            "config_hash": selection["config_hash"],
            "changed_vs_parent": self._param_diff(parent, params),
        }

        self._journal_append({
            "experiment_id": eid,
            "parent_experiment_id": exp.parent_experiment_id,
            "cycle_id": cycle_id,
            "config_hash": exp.config_hash,
            "mode": mode,
            "parameters": params,
            "status": "RUNNING",
            "created_at_utc": exp.created_at_utc,
        })
        self._write_artifact(exp)
        state["state"] = "RUNNING"
        self._save_state(state)

        specs = build_hypothesis_specs(mode, params)
        report: dict | None = None
        error: str | None = None
        try:
            report = self._runner(
                limit=limit, category=category, hypothesis_specs=specs,
                auto_paper=False, experiment_id=eid, cycle_id=cycle_id)
        except Exception as e:  # infra-failure: эксперимент не считается сигналом
            error = str(e)
            logger.exception("Experiment %s failed: %s", eid, e)

        if report is None:
            exp.status = "FAILED"
            exp.final_verdict = "ERROR"
            exp.reject_diagnosis = {"category": "infra", "reject_reasons": [error or "runner error"]}
            diagnosis = "infra"
            n_tests = 0
        else:
            verdict = report.get("verdict", "ERROR")
            exp.status = "DONE"
            exp.final_verdict = verdict
            exp.discovery_result = report.get("discovery_summary")
            exp.validation_result = self._stage_metrics(report, "validation_gate")
            exp.oos_result = self._stage_metrics(report, "oos_gate")
            exp.critic_result = self._stage_metrics(report, "critic")
            run_id = report.get("run_id")
            if run_id:
                exp.acceptance_report = str(RESULTS_DIR / f"acceptance_{run_id}.json")
            diagnosis = self._diagnose(report)
            exp.reject_diagnosis = {
                "category": diagnosis,
                "reject_reasons": report.get("reject_reasons", []),
            }
            ds = report.get("discovery_summary") or {}
            n_tests = ds.get("n_evaluated") or report.get("n_hypotheses", 0)

        self._journal_append({
            "experiment_id": eid,
            "cycle_id": cycle_id,
            "config_hash": exp.config_hash,
            "mode": mode,
            "status": exp.status,
            "final_verdict": exp.final_verdict,
            "diagnosis": diagnosis,
            "finished_at_utc": utc_now_iso(self._now()),
        })
        self._write_artifact(exp)

        # --- budget accounting ---
        state["budget_used"] += 1
        state["n_tests_cumulative"] += int(n_tests or 0)
        if exp.config_hash not in state["used_config_hashes"]:
            state["used_config_hashes"].append(exp.config_hash)
        fam = params.get("family")
        if fam:
            state["family_usage"][fam] = self._family_used(state, fam) + 1
        state["last_experiment_id"] = eid
        state["last_diagnosis"] = diagnosis
        state["last_mode"] = mode
        state["next_selection"] = None

        # --- terminal state ---
        verdict = exp.final_verdict
        if verdict in ("PASS", "CANDIDATE") and (report or {}).get("finalist"):
            state["state"] = "PASS_PENDING_PAPER"
            state["pass_pending"] = eid
            logger.info("Experiment %s PASS -> PASS_PENDING_PAPER (paper is manual)", eid)
        elif verdict in TERMINAL_VERDICTS or diagnosis == "infra":
            state["state"] = "STOPPED"
            state["stop_reason"] = f"experiment_{str(verdict).lower()}"
        else:
            nxt = self._select_next(state, exp, diagnosis, allow_baseline=False)
            if nxt is None:
                state["state"] = "STOPPED"
                state["stop_reason"] = "search_space_exhausted"
            else:
                state["state"] = "SELECT_NEXT"
                state["next_selection"] = nxt
        self._save_state(state)

        return {
            "state": state["state"],
            "experiment_id": eid,
            "mode": mode,
            "verdict": exp.final_verdict,
            "diagnosis": diagnosis,
            "budget_used": state["budget_used"],
            "next_mode": (state.get("next_selection") or {}).get("mode"),
            "stop_reason": state.get("stop_reason"),
        }

    @staticmethod
    def _param_diff(parent: Experiment | None, params: dict) -> dict:
        if parent is None:
            return {}
        diff = {}
        for k, v in params.items():
            old = (parent.parameters or {}).get(k)
            if old != v:
                diff[k] = {"from": old, "to": v}
        return diff

    @staticmethod
    def _stage_metrics(report: dict, stage: str) -> dict | None:
        for s in report.get("stages", []):
            if s.get("stage") == stage:
                return s.get("metrics")
        return None

    # --- status ----------------------------------------------------------
    def status(self) -> dict:
        state = self._load_state()
        return {
            "state": state["state"],
            "cycle_id": state.get("cycle_id"),
            "boundary": state.get("boundary"),
            "budget_used": state.get("budget_used"),
            "budget_limit": self._max_experiments(),
            "n_tests_cumulative": state.get("n_tests_cumulative"),
            "tests_limit": self._max_tests(),
            "family_usage": state.get("family_usage"),
            "last_experiment_id": state.get("last_experiment_id"),
            "last_diagnosis": state.get("last_diagnosis"),
            "pass_pending": state.get("pass_pending"),
            "next_mode": (state.get("next_selection") or {}).get("mode"),
            "stop_reason": state.get("stop_reason"),
        }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(module)s %(message)s")
    ap = argparse.ArgumentParser(description="Research Controller")
    ap.add_argument("--once", action="store_true", help="Один эксперимент и выход")
    ap.add_argument("--status", action="store_true", help="Показать состояние")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--category", type=str, default=None,
                    choices=["linear", "spot"])
    args = ap.parse_args(argv)

    ctl = ResearchController()
    if args.status:
        print(json.dumps(ctl.status(), ensure_ascii=False, indent=2, default=str))
        return 0
    result = ctl.run_once(limit=args.limit, category=args.category)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
