"""Tests for automatic shadow→paper pipeline (PHASE 13).

15 deterministic tests covering:
- Shadow paper execution
- Auto research loop
- Registry lifecycle
- Auto paper trigger
- Premature paper guard
- Notification events
- Dashboard shadow endpoint
"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import polars as pl


class TestShadowRun(unittest.TestCase):
    """Tests for shadow_run() in paper.py."""

    def test_shadow_run_basic(self):
        from src.paper import shadow_run, _initial_state

        events = pl.DataFrame({
            "open_time": [datetime(2026, 1, 1, tzinfo=timezone.utc)] * 3,
            "symbol": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "entry_price": [50000.0, 3000.0, 100.0],
            "relative_volume": [5.0, 4.0, 6.0],
            "return_5m": [0.001, -0.002, 0.003],
            "mfe_5m": [0.002, 0.001, 0.004],
            "mae_5m": [-0.001, -0.003, -0.001],
            "vol_gk_30d": [0.02, 0.03, 0.04],
        })

        from src.research import Hypothesis
        hyp = Hypothesis(
            hypothesis_id="TEST001",
            description="test",
            condition="pl.col('relative_volume') > 3.0",
            entry_side="long",
            horizon_min=5,
        )

        state = _initial_state()
        state["balance"] = 1000.0
        state["equity_start"] = 1000.0

        with patch("src.paper.SHADOW_STATE_FILE", Path(tempfile.mktemp(suffix=".json"))):
            result = shadow_run(events, [hyp], state)

        self.assertEqual(result["trades_executed"], 3)
        self.assertIn("TEST001", result["trades_by_hypothesis"])
        self.assertEqual(result["trades_by_hypothesis"]["TEST001"], 3)

    def test_shadow_run_no_matching_events(self):
        from src.paper import shadow_run, _initial_state

        events = pl.DataFrame({
            "open_time": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            "symbol": ["BTCUSDT"],
            "entry_price": [50000.0],
            "relative_volume": [1.0],
            "return_5m": [0.001],
            "mfe_5m": [0.002],
            "mae_5m": [-0.001],
            "vol_gk_30d": [0.02],
        })

        from src.research import Hypothesis
        hyp = Hypothesis(
            hypothesis_id="TEST002",
            description="test",
            condition="pl.col('relative_volume') > 10.0",
            entry_side="long",
            horizon_min=5,
        )

        state = _initial_state()
        with patch("src.paper.SHADOW_STATE_FILE", Path(tempfile.mktemp(suffix=".json"))):
            result = shadow_run(events, [hyp], state)

        self.assertEqual(result["trades_executed"], 0)

    def test_shadow_run_mode_b(self):
        from src.paper import shadow_run, _initial_state, PaperMode

        events = pl.DataFrame({
            "open_time": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            "symbol": ["BTCUSDT"],
            "entry_price": [50000.0],
            "relative_volume": [5.0],
            "return_5m": [0.001],
            "mfe_5m": [0.002],
            "mae_5m": [-0.001],
            "vol_gk_30d": [0.02],
        })

        from src.research import Hypothesis
        hyp = Hypothesis(
            hypothesis_id="TEST003",
            description="test",
            condition="pl.col('relative_volume') > 3.0",
            entry_side="long",
            horizon_min=5,
        )

        state = _initial_state()
        with patch("src.paper.SHADOW_STATE_FILE", Path(tempfile.mktemp(suffix=".json"))):
            result = shadow_run(events, [hyp], state)

        self.assertEqual(result["trades_executed"], 1)
        self.assertEqual(state["trades"][0]["mode"], "SHADOW")


class TestAutoResearchLoop(unittest.TestCase):
    """Tests for auto_research_loop() in orchestrator.py."""

    def test_auto_research_loop_exists(self):
        from src.orchestrator import auto_research_loop
        self.assertTrue(callable(auto_research_loop))


class TestRegistryLifecycle(unittest.TestCase):
    """Tests for registry lifecycle updates."""

    def test_registry_cannot_skip_to_validated(self):
        from src.registry import HypothesisLifecycle, HypothesisStatus

        registry = HypothesisLifecycle(registry_path=Path(tempfile.mktemp(suffix=".json")))
        registry.register("SKIP_TEST", HypothesisStatus.CANDIDATE)

        ok = registry.transition("SKIP_TEST", HypothesisStatus.ACTIVE)
        self.assertFalse(ok)

    def test_registry_validated_requires_all_gates(self):
        from src.registry import update_from_research, HypothesisLifecycle

        registry = HypothesisLifecycle(registry_path=Path(tempfile.mktemp(suffix=".json")))
        result = {
            "candidates": ["GATE_TEST"],
            "validation": {"GATE_TEST": {"mean_net": 0.001, "n": 150, "t_stat": 2.5}},
            "oos": {"GATE_TEST": {"mean_net": 0.001, "n": 120, "t_stat": 2.2}},
            "discovery_results": [{"hypothesis_id": "GATE_TEST", "description": "test"}],
        }

        transitions = update_from_research(registry, result)
        self.assertEqual(len(transitions), 1)
        self.assertEqual(transitions[0]["to"], "VALIDATED")

    def test_registry_no_transition_on_failed_oos(self):
        from src.registry import update_from_research, HypothesisLifecycle, HypothesisStatus

        registry = HypothesisLifecycle(registry_path=Path(tempfile.mktemp(suffix=".json")))
        result = {
            "candidates": ["FAIL_OOS"],
            "validation": {"FAIL_OOS": {"mean_net": 0.001, "n": 150, "t_stat": 2.5}},
            "oos": {"FAIL_OOS": {"mean_net": -0.001, "n": 120, "t_stat": -1.0}},
            "discovery_results": [{"hypothesis_id": "FAIL_OOS", "description": "test"}],
        }

        transitions = update_from_research(registry, result)
        self.assertEqual(len(transitions), 0)
        self.assertEqual(registry.get_status("FAIL_OOS"), HypothesisStatus.CANDIDATE)


class TestAutoPaperTrigger(unittest.TestCase):
    """Tests for auto_paper_trigger() in orchestrator.py."""

    def test_auto_paper_trigger_exists(self):
        from src.orchestrator import auto_paper_trigger
        self.assertTrue(callable(auto_paper_trigger))


class TestPrematurePaperGuard(unittest.TestCase):
    """Tests for premature_paper_guard() in orchestrator.py."""

    def test_guard_blocks_on_stop(self):
        from src.orchestrator import premature_paper_guard

        report = {"verdict": "STOP", "candidates": [], "finalist": None}
        allowed, reason = premature_paper_guard(report)
        self.assertFalse(allowed)
        self.assertIn("stopped", reason.lower())

    def test_guard_blocks_on_no_candidate(self):
        from src.orchestrator import premature_paper_guard

        report = {"verdict": "NO_CANDIDATE", "candidates": [], "finalist": None}
        allowed, reason = premature_paper_guard(report)
        self.assertFalse(allowed)

    def test_guard_allows_on_pass(self):
        from src.orchestrator import premature_paper_guard

        report = {
            "verdict": "PASS",
            "candidates": ["H001"],
            "finalist": {"hypothesis_id": "H001"},
            "stages": [
                {"stage": "data_validation"},
                {"stage": "feature_validation"},
                {"stage": "critic"},
                {"stage": "parameter_freeze"},
            ],
        }
        allowed, reason = premature_paper_guard(report)
        self.assertTrue(allowed)

    def test_guard_blocks_missing_stages(self):
        from src.orchestrator import premature_paper_guard

        report = {
            "verdict": "PASS",
            "candidates": ["H001"],
            "finalist": {"hypothesis_id": "H001"},
            "stages": [{"stage": "data_validation"}],
        }
        allowed, reason = premature_paper_guard(report)
        self.assertFalse(allowed)
        self.assertIn("Missing", reason)


class TestNotificationEvents(unittest.TestCase):
    """Tests for notification events in notify.py."""

    def test_notify_functions_exist(self):
        from src.notify import (
            notify_hypothesis_candidate,
            notify_hypothesis_validated,
            notify_paper_started,
            notify_paper_stopped,
            notify_shadow_summary,
        )
        self.assertTrue(callable(notify_hypothesis_candidate))
        self.assertTrue(callable(notify_hypothesis_validated))
        self.assertTrue(callable(notify_paper_started))
        self.assertTrue(callable(notify_paper_stopped))
        self.assertTrue(callable(notify_shadow_summary))


class TestDashboardShadowEndpoint(unittest.TestCase):
    """Tests for dashboard shadow paper endpoint."""

    def test_get_shadow_paper_exists(self):
        from src.dashboard.collectors import get_shadow_paper
        self.assertTrue(callable(get_shadow_paper))

    def test_get_shadow_paper_no_data(self):
        from src.dashboard.collectors import get_shadow_paper
        with patch("src.dashboard.collectors.PAPER_PORTFOLIO") as mock_dir:
            mock_dir.__truediv__ = MagicMock(return_value=Path("/nonexistent/state.json"))
            result = get_shadow_paper()
            self.assertIn("status", result)


if __name__ == "__main__":
    unittest.main()
