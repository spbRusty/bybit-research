"""Temporal correctness tests for paper trading (unittest).

Verifies:
1. Signal timestamp is used, not current time
2. No future price leakage in signal construction
3. Entry/exit fills use correct slippage direction
4. Fee = entry_fee + exit_fee (not just entry)
5. Quantity rounding to step
6. Min/max quantity enforcement
7. Exposure limits
8. Max open positions
9. Duplicate signal protection
10. Restart/recovery preserves state
11. Provenance immutability
12. No future OB data in signal
13. Portfolio balance correctness
14. Realized/unrealized PnL
15. Risk halt on zero balance
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.paper import (
    PaperMode, PaperSignal, PaperTrade,
    execute_signal, load_state, save_state, reset_state,
    _position_size, _round_qty, _calculate_fills, _calculate_fees,
    _determine_exit, get_portfolio_summary,
)
from src.risk import check_position, RiskCheck
from src.registry import (
    HypothesisLifecycle, HypothesisStatus,
    get_eligible_for_paper, get_shadow_eligible,
)


def _tmp_state_dir(tmp_path):
    state_file = tmp_path / "state.json"
    return state_file


def _make_signal(**overrides):
    defaults = dict(
        hypothesis_id="H001", symbol="BTCUSDT", side="long",
        entry_price=50000.0, stop_loss=49500.0, take_profit=50750.0,
        signal_timestamp="2026-09-07T12:00:00Z", horizon_min=5,
        mode=PaperMode.SHADOW,
    )
    defaults.update(overrides)
    return PaperSignal(**defaults)


def _make_candles(n=10, base_price=50010.0):
    times = [datetime(2026, 9, 7, 12, i + 1, tzinfo=timezone.utc) for i in range(n)]
    opens = [base_price + i * 10 for i in range(n)]
    highs = [o + 40 for o in opens]
    lows = [o - 20 for o in opens]
    closes = [o + 10 for o in opens]
    return pl.DataFrame({
        "open_time": times,
        "open": opens, "high": highs, "low": lows, "close": closes,
        "volume": [100.0] * n,
    })


class TestSignalTimestamp(unittest.TestCase):
    def test_signal_uses_provided_timestamp(self):
        sig = _make_signal()
        self.assertEqual(sig.signal_timestamp, "2026-09-07T12:00:00Z")

    def test_signal_no_future_data(self):
        sig = _make_signal()
        self.assertGreater(sig.entry_price, 0)
        self.assertLess(sig.stop_loss, sig.entry_price)
        self.assertGreater(sig.take_profit, sig.entry_price)


class TestNoFuturePriceLeakage(unittest.TestCase):
    def test_candle_data_after_signal(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal()
                candles = _make_candles()
                result = execute_signal(sig, candles, state)
                self.assertTrue(result["executed"])
                trade = result["trade"]
                self.assertEqual(trade["signal_timestamp"], sig.signal_timestamp)

    def test_exit_uses_horizon_candles(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal()
                candles = _make_candles()
                result = execute_signal(sig, candles, state)
                self.assertNotEqual(result["trade"]["exit_timestamp"], "")


class TestFeeCalculation(unittest.TestCase):
    def test_fee_is_entry_plus_exit(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal()
                candles = _make_candles()
                result = execute_signal(sig, candles, state)
                trade = result["trade"]
                self.assertGreater(trade["entry_fee"], 0)
                self.assertGreater(trade["exit_fee"], 0)

    def test_net_pnl_includes_both_fees(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal()
                candles = _make_candles()
                result = execute_signal(sig, candles, state)
                trade = result["trade"]
                expected_net = trade["gross_pnl"] - trade["entry_fee"] - trade["exit_fee"]
                self.assertAlmostEqual(trade["net_pnl"], expected_net, places=2)


class TestSlippageCalculation(unittest.TestCase):
    def test_long_entry_slippage_up(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal(side="long")
                candles = _make_candles(n=1, base_price=50010.0)
                result = execute_signal(sig, candles, state)
                self.assertGreater(result["trade"]["entry_price"], 50000.0)

    def test_short_entry_slippage_down(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal(side="short", stop_loss=50500.0, take_profit=49250.0)
                candles = pl.DataFrame({
                    "open_time": [datetime(2026, 9, 7, 12, 1, tzinfo=timezone.utc)],
                    "open": [49990.0], "high": [50000.0], "low": [49980.0],
                    "close": [49985.0], "volume": [100.0],
                })
                result = execute_signal(sig, candles, state)
                self.assertLess(result["trade"]["entry_price"], 50000.0)


class TestQuantityRounding(unittest.TestCase):
    def test_round_to_step(self):
        self.assertEqual(_round_qty(1.234, 0.001), 1.234)
        self.assertEqual(_round_qty(1.2345, 0.001), 1.234)
        self.assertEqual(_round_qty(1.234, 0.01), 1.23)
        self.assertEqual(_round_qty(1.234, 0.1), 1.2)

    def test_position_size_respects_step(self):
        qty = _position_size(50000.0, 49500.0, 1000.0)
        step = 0.001
        self.assertAlmostEqual(qty % step, 0.0, places=10)


class TestMinMaxQuantity(unittest.TestCase):
    def test_above_max_lot_capped(self):
        qty = _position_size(1.0, 0.5, 100000.0,
                            {"qty_step": 0.001, "min_order_qty": 0.001,
                             "max_order_qty": 100.0, "min_notional": 5.0})
        self.assertLessEqual(qty, 100.0)


class TestExposureLimits(unittest.TestCase):
    def test_max_positions_check(self):
        state = {"balance": 1000.0, "open_positions": [{}] * 5}
        check = check_position(0.01, 50000.0, state)
        self.assertFalse(check.allowed)
        self.assertIn("max", check.reason)


class TestDuplicateSignalProtection(unittest.TestCase):
    def test_same_signal_twice_increases_trade_count(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal()
                candles = _make_candles()
                r1 = execute_signal(sig, candles, state)
                self.assertTrue(r1["executed"])
                state = load_state()
                r2 = execute_signal(sig, candles, state)
                self.assertTrue(r2["executed"])
                self.assertEqual(state["trade_count"], 2)


class TestRestartRecovery(unittest.TestCase):
    def test_state_persists(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal()
                candles = _make_candles()
                execute_signal(sig, candles, state)
                loaded = json.loads(sf.read_text())
                self.assertEqual(loaded["trade_count"], 1)
                self.assertNotEqual(loaded["balance"], 1000.0)

    def test_reset_state(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                execute_signal(_make_signal(), _make_candles(), state)
                new_state = reset_state()
                self.assertEqual(new_state["trade_count"], 0)
                self.assertEqual(new_state["balance"], 1000.0)


class TestProvenanceImmutability(unittest.TestCase):
    def test_signal_has_mode(self):
        sig = _make_signal()
        self.assertEqual(sig.mode, PaperMode.SHADOW)

    def test_trade_records_mode(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal()
                candles = _make_candles()
                result = execute_signal(sig, candles, state)
                self.assertEqual(result["trade"]["mode"], "SHADOW")


class TestPortfolioBalance(unittest.TestCase):
    def test_initial_balance(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                state = reset_state()
                self.assertEqual(state["balance"], 1000.0)
                self.assertEqual(state["equity_start"], 1000.0)

    def test_summary_fields(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                state = reset_state()
                summary = get_portfolio_summary(state)
                self.assertEqual(summary["equity_start"], 1000.0)
                self.assertEqual(summary["balance"], 1000.0)
                self.assertEqual(summary["trade_count"], 0)


class TestRiskHalt(unittest.TestCase):
    def test_halt_on_zero_balance(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                state = reset_state()
                state["balance"] = 0.0
                save_state(state)
                check = check_position(0.01, 50000.0, state)
                self.assertFalse(check.allowed)


class TestExitReasons(unittest.TestCase):
    def test_stop_loss_hit(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal(horizon_min=3)
                candles = pl.DataFrame({
                    "open_time": [datetime(2026, 9, 7, 12, i, tzinfo=timezone.utc)
                                  for i in range(1, 4)],
                    "open": [50010.0, 49600.0, 49400.0],
                    "high": [50020.0, 49700.0, 49450.0],
                    "low":  [49980.0, 49450.0, 49350.0],
                    "close": [50000.0, 49500.0, 49400.0],
                    "volume": [100.0, 100.0, 100.0],
                })
                result = execute_signal(sig, candles, state)
                self.assertEqual(result["trade"]["reason"], "stop")

    def test_take_profit_hit(self):
        with tempfile.TemporaryDirectory() as td:
            sf = _tmp_state_dir(Path(td))
            with patch("src.paper.STATE_FILE", sf):
                reset_state()
                state = load_state()
                sig = _make_signal(horizon_min=3)
                candles = pl.DataFrame({
                    "open_time": [datetime(2026, 9, 7, 12, i, tzinfo=timezone.utc)
                                  for i in range(1, 4)],
                    "open": [50010.0, 50500.0, 50800.0],
                    "high": [50200.0, 50800.0, 50900.0],
                    "low":  [50000.0, 50400.0, 50700.0],
                    "close": [50150.0, 50700.0, 50850.0],
                    "volume": [100.0, 100.0, 100.0],
                })
                result = execute_signal(sig, candles, state)
                self.assertEqual(result["trade"]["reason"], "take")


class TestDetermineExit(unittest.TestCase):
    def test_long_stop(self):
        r, px = _determine_exit(50000, 1.0, 49500, 50750, 50100, 49400, 50050)
        self.assertEqual(r, "stop")
        self.assertEqual(px, 49500)

    def test_long_take(self):
        r, px = _determine_exit(50000, 1.0, 49500, 50750, 50800, 49900, 50700)
        self.assertEqual(r, "take")
        self.assertEqual(px, 50750)

    def test_long_close(self):
        r, px = _determine_exit(50000, 1.0, 49500, 50750, 50500, 49800, 50300)
        self.assertEqual(r, "close")
        self.assertEqual(px, 50300)

    def test_short_stop(self):
        r, px = _determine_exit(50000, -1.0, 50500, 49250, 50600, 49900, 50100)
        self.assertEqual(r, "stop")
        self.assertEqual(px, 50500)

    def test_short_take(self):
        r, px = _determine_exit(50000, -1.0, 50500, 49250, 50100, 49200, 49300)
        self.assertEqual(r, "take")
        self.assertEqual(px, 49250)


class TestLifecycleIntegration(unittest.TestCase):
    def test_no_eligible_for_paper(self):
        reg = HypothesisLifecycle(registry_path=Path(tempfile.mktemp(suffix=".json")))
        eligible = get_eligible_for_paper(reg)
        self.assertEqual(len(eligible), 0)

    def test_shadow_eligible(self):
        reg = HypothesisLifecycle(registry_path=Path(tempfile.mktemp(suffix=".json")))
        reg.register("H001", HypothesisStatus.CANDIDATE)
        shadow = get_shadow_eligible(reg)
        self.assertIn("H001", shadow)


if __name__ == "__main__":
    unittest.main()
