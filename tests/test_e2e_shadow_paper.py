"""End-to-end synthetic test for CANDIDATE→VALIDATED→PAPER flow.

Tests the full lifecycle using synthetic data (allowed in tests only).
Verifies both success path and failure path.
"""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.registry import (
    HypothesisLifecycle,
    HypothesisStatus,
    update_from_research,
    get_eligible_for_paper,
    get_shadow_eligible,
)
from src.paper import (
    shadow_run,
    execute_signal,
    PaperSignal,
    PaperMode,
    _initial_state,
    save_state,
    load_state,
)


class TestEndToEndSuccessPath(unittest.TestCase):
    """Test CANDIDATE → VALIDATED → MODE A PAPER (success path)."""

    def test_full_lifecycle_success(self):
        reg_path = Path(tempfile.mktemp(suffix=".json"))
        registry = HypothesisLifecycle(registry_path=reg_path)

        # Step 1: Register as CANDIDATE
        registry.register("E2E_SUCCESS", HypothesisStatus.CANDIDATE,
                          {"source": "test", "description": "e2e test"})
        self.assertEqual(registry.get_status("E2E_SUCCESS"), HypothesisStatus.CANDIDATE)

        # Step 2: Simulate research result with all gates passed
        result = {
            "candidates": ["E2E_SUCCESS"],
            "validation": {"E2E_SUCCESS": {"mean_net": 0.002, "n": 200, "t_stat": 3.0}},
            "oos": {"E2E_SUCCESS": {"mean_net": 0.001, "n": 150, "t_stat": 2.5}},
            "discovery_results": [
                {"hypothesis_id": "E2E_SUCCESS", "description": "e2e test"}
            ],
        }

        # Step 3: Update lifecycle
        transitions = update_from_research(registry, result)
        self.assertEqual(len(transitions), 1)
        self.assertEqual(transitions[0]["to"], "VALIDATED")
        self.assertEqual(registry.get_status("E2E_SUCCESS"), HypothesisStatus.VALIDATED)

        # Step 4: Check eligible for paper
        eligible = get_eligible_for_paper(registry)
        self.assertIn("E2E_SUCCESS", eligible)

        # Step 5: Execute paper trade
        state = _initial_state()
        state["balance"] = 1000.0
        state["equity_start"] = 1000.0

        signal = PaperSignal(
            hypothesis_id="E2E_SUCCESS",
            symbol="BTCUSDT",
            side="long",
            entry_price=50000.0,
            stop_loss=49900.0,
            take_profit=50150.0,
            signal_timestamp="2026-01-01T00:00:00Z",
            horizon_min=5,
            mode=PaperMode.VALIDATED,
        )

        candles = pl.DataFrame({
            "open_time": [datetime(2026, 1, 1, 0, i, tzinfo=timezone.utc) for i in range(1, 6)],
            "open": [50000.0] * 5,
            "high": [50050.0, 50100.0, 50120.0, 50080.0, 50060.0],
            "low": [49980.0, 49960.0, 49990.0, 50010.0, 50020.0],
            "close": [50020.0, 50080.0, 50100.0, 50060.0, 50040.0],
        })

        with patch("src.paper.STATE_FILE", Path(tempfile.mktemp(suffix=".json"))):
            result = execute_signal(signal, candles, state)

        self.assertTrue(result["executed"])
        self.assertEqual(result["trade"]["hypothesis_id"], "E2E_SUCCESS")
        self.assertEqual(result["trade"]["mode"], "VALIDATED")


class TestEndToEndFailurePath(unittest.TestCase):
    """Test CANDIDATE → failed gates → remains CANDIDATE."""

    def test_full_lifecycle_failure(self):
        reg_path = Path(tempfile.mktemp(suffix=".json"))
        registry = HypothesisLifecycle(registry_path=reg_path)

        # Step 1: Register as CANDIDATE
        registry.register("E2E_FAIL", HypothesisStatus.CANDIDATE)

        # Step 2: Simulate research result with failed OOS
        result = {
            "candidates": ["E2E_FAIL"],
            "validation": {"E2E_FAIL": {"mean_net": 0.001, "n": 150, "t_stat": 2.5}},
            "oos": {"E2E_FAIL": {"mean_net": -0.001, "n": 120, "t_stat": -1.0}},
            "discovery_results": [
                {"hypothesis_id": "E2E_FAIL", "description": "test"}
            ],
        }

        # Step 3: Update lifecycle
        transitions = update_from_research(registry, result)
        self.assertEqual(len(transitions), 0)
        self.assertEqual(registry.get_status("E2E_FAIL"), HypothesisStatus.CANDIDATE)

        # Step 4: Not eligible for MODE A paper
        eligible_a = get_eligible_for_paper(registry)
        self.assertNotIn("E2E_FAIL", eligible_a)

        # Step 5: Still eligible for MODE B shadow
        eligible_b = get_shadow_eligible(registry)
        self.assertIn("E2E_FAIL", eligible_b)


class TestEndToEndShadowFlow(unittest.TestCase):
    """Test CANDIDATE → shadow paper observation."""

    def test_shadow_observation_flow(self):
        reg_path = Path(tempfile.mktemp(suffix=".json"))
        registry = HypothesisLifecycle(registry_path=reg_path)

        registry.register("E2E_SHADOW", HypothesisStatus.CANDIDATE)

        events = pl.DataFrame({
            "open_time": [datetime(2026, 1, 1, tzinfo=timezone.utc)] * 2,
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "entry_price": [50000.0, 3000.0],
            "relative_volume": [5.0, 4.0],
            "return_5m": [0.001, -0.002],
            "mfe_5m": [0.002, 0.001],
            "mae_5m": [-0.001, -0.003],
            "vol_gk_30d": [0.02, 0.03],
        })

        from src.research import Hypothesis
        hyp = Hypothesis(
            hypothesis_id="E2E_SHADOW",
            description="shadow test",
            condition="pl.col('relative_volume') > 3.0",
            entry_side="long",
            horizon_min=5,
        )

        state = _initial_state()
        state["balance"] = 1000.0
        state["equity_start"] = 1000.0
        state["mode"] = "SHADOW"

        with patch("src.paper.SHADOW_STATE_FILE", Path(tempfile.mktemp(suffix=".json"))):
            result = shadow_run(events, [hyp], state)

        self.assertEqual(result["trades_executed"], 2)
        self.assertEqual(state["trades"][0]["mode"], "SHADOW")
        self.assertEqual(state["trades"][1]["mode"], "SHADOW")


if __name__ == "__main__":
    unittest.main()
