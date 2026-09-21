"""Research Cycle Runner tests: freeze, OB, loop, RAM/OOM, recovery, finish."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import data_ready, research_runner as rr

FROZEN_CYCLE = "9be56a1fe207:2026-09-20T19:55:00"


def _frozen(**over):
    snap = {
        "klines_max": "2026-09-20T19:55:00",
        "config_hash": "9be56a1fe207",
        "git_head": "test",
        "data_version": "1.0",
        "n_files": 10,
        "fresh_symbols": 93,
        "ob_valid_symbols": 744,
        "ob_unique_symbols": 100,
        "ob_rows": 5000,
        "marketdata_was_active": True,
        "source": "research_runner",
    }
    snap.update(over)
    return snap


def _km(iso: str = "2026-09-20T19:55:00") -> datetime:
    return datetime.fromisoformat(iso)


class RunnerBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.td = Path(self._tmp.name)
        patchers = [
            patch.object(data_ready, "RESEARCH_DIR", self.td),
            patch.object(rr, "RUNNER_LOCK", self.td / "runner.lock"),
            patch.object(rr, "RUNNER_STATE", self.td / "runner_state.json"),
            patch.object(rr, "STATE_PATH", self.td / "controller_state.json"),
            patch.object(rr, "JOURNAL_PATH", self.td / "experiments.jsonl"),
            patch.object(rr, "ARTIFACTS_DIR", self.td / "experiments"),
            patch.object(rr, "service_active", return_value=False),
            patch.object(rr, "service_start"),
            patch.object(rr, "service_stop"),
            patch.object(rr, "scan_klines",
                   return_value={"klines_max": _km()}),
            patch.object(rr, "scan_ob",
                   return_value={"ob_valid_symbols": 744, "ob_unique_symbols": 100,
                                 "ob_rows": 5000}),
            patch.object(rr, "run_controller", return_value=(0, "{}")),
            patch.object(rr, "_mem_info",
                   return_value={"MemAvailable": 50.0, "SwapFree": 20.0}),
            patch.object(rr, "FREEZE_POLL_SEC", 0.01),
            patch.object(rr, "OB_POLL_SEC", 0.01),
            patch.object(rr, "OB_TIMEOUT_SEC", 0.05),
            patch.object(rr, "FREEZE_TIMEOUT_SEC", 0.05),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        self._check_ready = patch.object(rr, "check_ready",
            return_value=(True, [], {
                "klines": {"klines_max": _km(), "n_files": 10, "fresh_symbols": 93},
                "ob": {"ob_valid_symbols": 744, "ob_unique_symbols": 100,
                       "ob_rows": 5000},
                "config_hash": "9be56a1fe207", "git_head": "test",
                "data_version": "1.0",
            }))
        self._check_ready.start()
        self.addCleanup(self._check_ready.stop)

    def tearDown(self):
        self._tmp.cleanup()

    def _write_state(self, **over):
        state = {"state": "SELECT_NEXT", "budget_used": 1,
                 "cycle_id": FROZEN_CYCLE, "next_selection": {"mode": "REGIME_SWEEP"}}
        state.update(over)
        (self.td / "controller_state.json").write_text(json.dumps(state))

    def _journal_line(self, eid, mode="REGIME_SWEEP", status="DONE",
                      cycle_id=FROZEN_CYCLE, config_hash="abc"):
        return json.dumps({"experiment_id": eid, "cycle_id": cycle_id,
                           "config_hash": config_hash, "mode": mode,
                           "status": status, "parameters": {}}) + "\n"


class TestPhaseDataBoundary(RunnerBase):
    def test_skip_when_gate_not_ready(self):
        rr.check_ready.return_value = (False, ["cooldown"], {"klines": {"klines_max": None}})
        frozen, result = rr._phase_data_boundary()
        self.assertIsNone(frozen)
        self.assertEqual(result["verdict"], "SKIP")

    def test_freezes_boundary_and_stops_marketdata(self):
        rr.service_active.return_value = True
        frozen, result = rr._phase_data_boundary()
        self.assertEqual(result["verdict"], "FROZEN")
        rr.service_stop.assert_called_once_with(rr.MARKETDATA_SERVICE)
        self.assertEqual(frozen["klines_max"], "2026-09-20T19:55:00")
        self.assertTrue(data_ready.frozen_boundary_path().exists())
        rr.service_start.assert_not_called()

    def test_ob_refresh_starts_and_stops_reconstructor(self):
        rr.scan_ob.return_value = {"ob_valid_symbols": 3, "ob_unique_symbols": 5,
                                   "ob_rows": 10}
        rr._refresh_ob(_frozen())
        rr.service_start.assert_called_once_with(rr.RECONSTRUCTOR_SERVICE)
        rr.service_stop.assert_called_once_with(rr.RECONSTRUCTOR_SERVICE)
        frozen = data_ready.load_frozen_boundary()
        self.assertEqual(frozen["ob_valid_symbols"], 3)

    def test_ob_fresh_no_reconstructor(self):
        rr._refresh_ob(_frozen())
        rr.service_start.assert_not_called()


class TestResearchLoop(RunnerBase):
    def test_continues_selected_experiment(self):
        self._write_state(budget_used=1, state="SELECT_NEXT")
        result = rr._phase_research_loop(_frozen(), limit=40, category=None,
                                         once=True)
        self.assertEqual(result["verdict"], "STEP")
        rr.run_controller.assert_called_once_with(limit=40, category=None)

    def test_baseline_guard_aborts_cycle(self):
        journal = self.td / "experiments.jsonl"
        journal.write_text(self._journal_line("EXP_A", mode="REGIME_SWEEP"))
        self._write_state(budget_used=1, state="SELECT_NEXT")
        def _fake_controller(limit, category):
            with open(journal, "a") as fh:
                fh.write(self._journal_line("EXP_B", mode="BASELINE"))
            return 0, "{}"
        rr.run_controller.side_effect = _fake_controller
        result = rr._phase_research_loop(_frozen(), limit=40, category=None,
                                         once=False)
        self.assertEqual(result["verdict"], "ABORT")
        self.assertIn("unexpected_baseline", result["reason"])

    def test_baseline_allowed_as_first_experiment(self):
        journal = self.td / "experiments.jsonl"
        journal.write_text(self._journal_line("EXP_A", mode="BASELINE"))
        self._write_state(budget_used=0, state="SELECT_NEXT")
        result = rr._phase_research_loop(_frozen(), limit=40, category=None,
                                         once=True)
        self.assertEqual(result["verdict"], "STEP")

    def test_budget_exhausted_stops_cycle(self):
        self._write_state(budget_used=12, state="SELECT_NEXT")
        result = rr._phase_research_loop(_frozen(), limit=40, category=None,
                                         once=False)
        self.assertEqual(result["verdict"], "DONE")
        self.assertEqual(result["stop_reason"], "budget_per_boundary")
        rr.run_controller.assert_not_called()

    def test_terminal_state_stops_cycle(self):
        self._write_state(state="STOPPED", budget_used=2)
        result = rr._phase_research_loop(_frozen(), limit=40, category=None,
                                         once=False)
        self.assertEqual(result["verdict"], "DONE")
        rr.run_controller.assert_not_called()


class TestResourceSafety(RunnerBase):
    def test_ram_guard_aborts(self):
        rr._mem_info.return_value = {"MemAvailable": 5.0, "SwapFree": 20.0}
        self._write_state(state="SELECT_NEXT")
        result = rr._phase_research_loop(_frozen(), limit=40, category=None,
                                         once=False)
        self.assertEqual(result["verdict"], "ABORT")
        self.assertIn("resource_pressure", result["reason"])
        rr.run_controller.assert_not_called()

    def test_oom_marks_interrupted_and_aborts(self):
        journal = self.td / "experiments.jsonl"
        journal.write_text(self._journal_line("EXP_A", status="RUNNING"))
        rr.run_controller.return_value = (137, "Killed")
        self._write_state(state="SELECT_NEXT")
        result = rr._phase_research_loop(_frozen(), limit=40, category=None,
                                         once=False)
        self.assertEqual(result["verdict"], "ABORT")
        self.assertIn("controller_exit_137", result["reason"])
        lines = journal.read_text().splitlines()
        last = json.loads(lines[-1])
        self.assertEqual(last["status"], "FAILED")
        self.assertEqual(last["reason"], "interrupted")


class TestRecoveryAndFinish(RunnerBase):
    def test_recovery_resumes_same_cycle_without_recheck(self):
        data_ready.save_frozen_boundary(_frozen())
        journal = self.td / "experiments.jsonl"
        journal.write_text(self._journal_line("EXP_A", status="RUNNING"))
        self._write_state(budget_used=1, state="SELECT_NEXT")
        rr.check_ready.return_value = (False, ["never called"], {})
        result = rr.run_cycle(limit=40, once=True)
        self.assertEqual(result["verdict"], "STEP")
        # gate не перепроверялся: цикл тот же, frozen уже есть
        rr.run_controller.assert_called_once()
        self.assertTrue(data_ready.frozen_boundary_path().exists())
        data_ready.clear_frozen_boundary()

    def test_finish_restores_marketdata_and_clears_frozen(self):
        data_ready.save_frozen_boundary(_frozen(marketdata_was_active=True))
        self._write_state(state="PASS_PENDING_PAPER", budget_used=2)
        result = rr.run_cycle(limit=40)
        self.assertEqual(result["verdict"], "PASS_PENDING_PAPER")
        rr.service_start.assert_called_once_with(rr.MARKETDATA_SERVICE)
        self.assertFalse(data_ready.frozen_boundary_path().exists())
        self.assertFalse((self.td / "runner_state.json").exists())

    def test_finish_restores_frozen_after_terminal_step(self):
        data_ready.save_frozen_boundary(_frozen(marketdata_was_active=True))
        self._write_state(state="SELECT_NEXT", budget_used=2)
        result = rr.run_cycle(limit=40, once=True)
        # одна итерация привела к SELECT_NEXT — цикл не завершён, frozen жив
        self.assertEqual(result["verdict"], "STEP")
        self.assertTrue(data_ready.frozen_boundary_path().exists())

    def test_crash_between_stop_and_snapshot_restores_marketdata(self):
        (self.td / "runner_state.json").write_text(json.dumps(
            {"marketdata_was_active": True,
             "created_at_utc": "2026-09-20T20:00:00"}))
        result = rr.run_cycle(limit=40, once=True)
        self.assertEqual(result["verdict"], "STEP")
        # recovery вернул marketdata после краха в окне stop→snapshot
        rr.service_start.assert_any_call(rr.MARKETDATA_SERVICE)
        self.assertFalse((self.td / "runner_state.json").exists())
        self.assertTrue(data_ready.frozen_boundary_path().exists())


class TestPaperHandoff(RunnerBase):
    def setUp(self):
        super().setUp()
        from src import paper
        self.paper = paper
        p = patch.object(paper, "STATE_FILE", self.td / "paper_state.json")
        p.start()
        self.addCleanup(p.stop)

    def _write_artifact(self, eid, finalist, report_verdict="PASS"):
        report = self.td / f"acceptance_{eid}.json"
        report.write_text(json.dumps({"verdict": report_verdict, "finalist": finalist}))
        (self.td / "experiments").mkdir(exist_ok=True)
        (self.td / "experiments" / f"{eid}.json").write_text(json.dumps({
            "experiment_id": eid,
            "cycle_id": FROZEN_CYCLE,
            "config_hash": "abc123",
            "mode": "REGIME_SWEEP",
            "acceptance_report": str(report),
        }))

    def test_launch_paper_sets_mode_and_provenance(self):
        eid = "EXP_1"
        self._write_artifact(eid, {"hypothesis_id": "HG123", "horizon_min": 30,
                                   "entry_side": "long", "condition": "pl.col('x')>0",
                                   "description": "test"})
        res = rr._launch_paper({"pass_pending": eid, "cycle_id": FROZEN_CYCLE})
        self.assertEqual(res["verdict"], "LAUNCHED")
        ps = self.paper.load_state()
        self.assertEqual(ps["mode"], "VALIDATED")
        self.assertEqual(ps["hypothesis_id"], "HG123")
        self.assertEqual(ps["equity_start"], 1000.0)
        self.assertEqual(ps["balance"], 1000.0)
        self.assertEqual(ps["provenance"]["experiment_id"], eid)
        self.assertEqual(ps["provenance"]["horizon_min"], 30)

    def test_launch_paper_dedup_same_experiment(self):
        eid = "EXP_1"
        self._write_artifact(eid, {"hypothesis_id": "HG123"})
        rr._launch_paper({"pass_pending": eid, "cycle_id": FROZEN_CYCLE})
        res = rr._launch_paper({"pass_pending": eid, "cycle_id": FROZEN_CYCLE})
        self.assertEqual(res["verdict"], "ALREADY_LAUNCHED")

    def test_launch_paper_does_not_overwrite_bound(self):
        self._write_artifact("EXP_1", {"hypothesis_id": "HG111"})
        rr._launch_paper({"pass_pending": "EXP_1", "cycle_id": FROZEN_CYCLE})
        self._write_artifact("EXP_2", {"hypothesis_id": "HG222"})
        res = rr._launch_paper({"pass_pending": "EXP_2", "cycle_id": FROZEN_CYCLE})
        self.assertEqual(res["verdict"], "ALREADY_LAUNCHED")
        self.assertIn("bound", res["reason"])
        self.assertEqual(self.paper.load_state()["hypothesis_id"], "HG111")

    def test_launch_paper_skips_without_finalist(self):
        eid = "EXP_1"
        self._write_artifact(eid, None, report_verdict="REJECT")
        res = rr._launch_paper({"pass_pending": eid, "cycle_id": FROZEN_CYCLE})
        self.assertEqual(res["verdict"], "SKIP")
        self.assertEqual(res["reason"], "no_finalist")
        self.assertFalse((self.td / "paper_state.json").exists())

    def test_pass_pending_paper_resumes_research(self):
        eid = "EXP_1"
        self._write_artifact(eid, {"hypothesis_id": "HG123"})
        self._write_state(state="PASS_PENDING_PAPER", pass_pending=eid, budget_used=2)
        result = rr._phase_research_loop(_frozen(), limit=40, category=None, once=True)
        self.assertEqual(result["verdict"], "STEP")
        self.assertEqual(result["paper_handoff"]["verdict"], "LAUNCHED")
        st = json.loads((self.td / "controller_state.json").read_text())
        self.assertEqual(st["state"], "SELECT_NEXT")
        self.assertIsNone(st["next_selection"])

    def test_pass_pending_paper_skips_without_finalist_terminal(self):
        eid = "EXP_1"
        self._write_artifact(eid, None, report_verdict="REJECT")
        self._write_state(state="PASS_PENDING_PAPER", pass_pending=eid, budget_used=2)
        result = rr._phase_research_loop(_frozen(), limit=40, category=None, once=False)
        self.assertEqual(result["verdict"], "PASS_PENDING_PAPER")
        self.assertTrue(result["finished"])


class TestLock(RunnerBase):
    def test_runner_lock_prevents_parallel(self):
        lock = data_ready.Lock(self.td / "runner.lock")
        self.assertTrue(lock.acquire())
        try:
            result = rr.run_cycle(limit=40)
            self.assertEqual(result["verdict"], "SKIP")
            self.assertEqual(result["reason"], "lock")
        finally:
            lock.release()


if __name__ == "__main__":
    unittest.main()