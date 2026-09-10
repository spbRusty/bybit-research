"""State isolation invariants (P0): tests and MODE B must never touch production state.

Guarantees (checked on the REAL production paths, not tmp):
1. shadow_run (MODE B) writes ONLY the shadow state file, never MODE A paper state.
2. execute_signal (MODE A) with patched STATE_FILE never touches production.
3. Registry with registry_path=tmp never touches production data/research/registry.json.
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.paper import (
    PaperMode, PaperSignal,
    execute_signal, load_shadow_state, shadow_run,
)
from src.registry import (
    HypothesisLifecycle, HypothesisStatus,
)

PROJ = Path(__file__).resolve().parent.parent
PROD_STATE = PROJ / "data" / "paper" / "portfolio" / "state.json"
PROD_REGISTRY = PROJ / "data" / "research" / "registry.json"


def _snapshot(path: Path):
    """Bytes of the production file, or None if absent."""
    return path.read_bytes() if path.exists() else None


def _shadow_events(hyp_id: str = "HX_ISO") -> pl.DataFrame:
    t0 = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)
    return pl.DataFrame({
        "open_time": [t0] * 10,
        "symbol": ["BTCUSDT"] * 10,
        "entry_price": [50000.0] * 10,
        "relative_volume": [5.0] * 10,
        "return_5m": [0.001] * 10,
        "mfe_5m": [0.002] * 10,
        "mae_5m": [-0.001] * 10,
        "vol_gk_30d": [0.002] * 10,
    })


class _Hyp:
    hypothesis_id = "HX_ISO"
    condition = "pl.col('relative_volume') > 3.0"
    entry_side = "long"
    horizon_min = 5
    description = "isolation test"


class TestShadowDoesNotTouchPaperState(unittest.TestCase):
    def test_shadow_run_moves_only_shadow_state(self):
        before_state = _snapshot(PROD_STATE)
        before_registry = _snapshot(PROD_REGISTRY)

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            shadow_file = td / "shadow_state.json"
            paper_file = td / "paper_state.json"
            with patch("src.paper.SHADOW_STATE_FILE", shadow_file):
                with patch("src.paper.STATE_FILE", paper_file):
                    result = shadow_run(_shadow_events(), [_Hyp()])

            self.assertGreaterEqual(result["trades_executed"], 1)
            # MODE B writes its own file...
            self.assertTrue(shadow_file.exists())
            # ...and must NOT create or touch the MODE A file.
            self.assertFalse(paper_file.exists())

        # Production files untouched by MODE B.
        self.assertEqual(_snapshot(PROD_STATE), before_state)
        self.assertEqual(_snapshot(PROD_REGISTRY), before_registry)

    def test_shadow_run_same_lockstep_with_leaked_patch(self):
        """Regression guard: even if caller forgets the STATE_FILE patch,
        shadow_run must not touch production state (it never calls save_state)."""
        before_state = _snapshot(PROD_STATE)
        with tempfile.TemporaryDirectory() as td:
            with patch("src.paper.SHADOW_STATE_FILE", Path(td) / "shadow.json"):
                shadow_run(_shadow_events(), [_Hyp()], state=load_shadow_state())
        self.assertEqual(_snapshot(PROD_STATE), before_state)


class TestModeASeparation(unittest.TestCase):
    def test_execute_signal_writes_only_patched_state(self):
        before_state = _snapshot(PROD_STATE)
        with tempfile.TemporaryDirectory() as td:
            paper_file = Path(td) / "paper_state.json"
            with patch("src.paper.STATE_FILE", paper_file):
                sig = PaperSignal(
                    hypothesis_id="H001", symbol="BTCUSDT", side="long",
                    entry_price=50000.0, stop_loss=49500.0, take_profit=50750.0,
                    signal_timestamp="2026-09-07T12:00:00Z", horizon_min=5,
                    mode=PaperMode.VALIDATED,
                )
                candles = pl.DataFrame({
                    "open_time": [datetime(2026, 9, 7, 12, i + 1, tzinfo=timezone.utc)
                                  for i in range(5)],
                    "open": [50010.0] * 5, "high": [50050.0] * 5,
                    "low": [49990.0] * 5, "close": [50020.0] * 5,
                    "volume": [100.0] * 5,
                })
                execute_signal(sig, candles)
            self.assertTrue(paper_file.exists())
        self.assertEqual(_snapshot(PROD_STATE), before_state)


class TestRegistryIsolation(unittest.TestCase):
    def test_tmp_registry_does_not_pollute_production(self):
        before = _snapshot(PROD_REGISTRY)
        with tempfile.TemporaryDirectory() as td:
            reg = HypothesisLifecycle(registry_path=Path(td) / "registry.json")
            reg.register("HB_ISO", HypothesisStatus.CANDIDATE,
                         {"reason": "isolation test"})
            reg.transition("HB_ISO", HypothesisStatus.VALIDATED,
                           reason="isolation test")
            self.assertEqual(reg.get_status("HB_ISO"), HypothesisStatus.VALIDATED)
        self.assertEqual(_snapshot(PROD_REGISTRY), before)


if __name__ == "__main__":
    unittest.main()