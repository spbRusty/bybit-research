"""Tests for candle trigger + orderbook capture lifecycle."""
from __future__ import annotations

import json
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.candle_trigger import (
    CooldownTracker,
    TriggerFile,
    TriggerRule,
    _make_event_id,
    evaluate_trigger,
    evaluate_all_triggers,
    cleanup_processed_trigger,
    list_pending_triggers,
    CONFIG_HASH,
    CONFIG_VERSION,
    RULES,
    _TRIGGERS_DIR,
)
from config.settings import MARKET_DATA_DIR


class TestTriggerRule(unittest.TestCase):
    def test_gt_above(self):
        r = TriggerRule(feature="rv", operator="gt", threshold=3.0)
        self.assertTrue(r.evaluate(5.0))

    def test_gt_below(self):
        r = TriggerRule(feature="rv", operator="gt", threshold=3.0)
        self.assertFalse(r.evaluate(2.0))

    def test_gt_equal(self):
        r = TriggerRule(feature="rv", operator="gt", threshold=3.0)
        self.assertFalse(r.evaluate(3.0))

    def test_lt(self):
        r = TriggerRule(feature="x", operator="lt", threshold=5.0)
        self.assertTrue(r.evaluate(3.0))
        self.assertFalse(r.evaluate(7.0))

    def test_ge(self):
        r = TriggerRule(feature="x", operator="ge", threshold=5.0)
        self.assertTrue(r.evaluate(5.0))
        self.assertFalse(r.evaluate(4.9))

    def test_le(self):
        r = TriggerRule(feature="x", operator="le", threshold=5.0)
        self.assertTrue(r.evaluate(5.0))
        self.assertTrue(r.evaluate(3.0))
        self.assertFalse(r.evaluate(5.1))

    def test_eq(self):
        r = TriggerRule(feature="x", operator="eq", threshold=5.0)
        self.assertTrue(r.evaluate(5.0))
        self.assertFalse(r.evaluate(5.1))


class TestCooldownTracker(unittest.TestCase):
    def test_can_trigger_first_time(self):
        cd = CooldownTracker(cooldown_sec=10)
        self.assertTrue(cd.can_trigger("BTCUSDT"))

    def test_cooldown_blocks(self):
        cd = CooldownTracker(cooldown_sec=300)
        cd.record("BTCUSDT")
        self.assertFalse(cd.can_trigger("BTCUSDT"))

    def test_different_symbols_independent(self):
        cd = CooldownTracker(cooldown_sec=300)
        cd.record("BTCUSDT")
        self.assertTrue(cd.can_trigger("ETHUSDT"))
        self.assertFalse(cd.can_trigger("BTCUSDT"))


class TestEventId(unittest.TestCase):
    def test_format(self):
        ts = datetime(2026, 9, 5, 12, 0, 0, tzinfo=timezone.utc)
        eid = _make_event_id("BTCUSDT", ts)
        self.assertIn("20260905T120000Z", eid)
        self.assertIn("BTCUSDT", eid)
        self.assertIn(CONFIG_HASH[:6], eid)


class TestTriggerFile(unittest.TestCase):
    def test_write_and_read(self):
        trigger = TriggerFile(
            event_id="test_20260905T120000Z_BTCUSDT",
            symbol="BTCUSDT",
            category="linear",
            trigger_params={"relative_volume": 5.0},
            horizons=[5, 10],
            capture_duration_sec=1200,
        )
        try:
            path = trigger.write(_TRIGGERS_DIR)
            self.assertTrue(path.exists())
            data = json.loads(path.read_text())
            self.assertEqual(data["event_id"], "test_20260905T120000Z_BTCUSDT")
            self.assertEqual(data["symbol"], "BTCUSDT")
            self.assertEqual(data["capture_duration_sec"], 1200)
            self.assertEqual(data["trigger_version"], CONFIG_VERSION)
            self.assertEqual(data["trigger_config_hash"], CONFIG_HASH)
        finally:
            path.unlink(missing_ok=True)


class TestEvaluateTrigger(unittest.TestCase):
    def _make_df(self, rvol=5.0, rrange=4.0):
        return pl.DataFrame({
            "symbol": ["BTCUSDT"],
            "category": ["linear"],
            "relative_volume": [rvol],
            "relative_range": [rrange],
            "close": [50000.0],
            "high": [51000.0],
            "low": [49000.0],
            "open": [49500.0],
            "volume": [1000.0],
        })

    def test_trigger_fires(self):
        df = self._make_df(rvol=5.0, rrange=4.0)
        cd = CooldownTracker(cooldown_sec=0)
        trigger = evaluate_trigger(df, "BTCUSDT", "linear", cooldown=cd)
        self.assertIsNotNone(trigger)
        self.assertEqual(trigger.symbol, "BTCUSDT")

    def test_no_trigger_below_threshold(self):
        df = self._make_df(rvol=1.0, rrange=1.0)
        cd = CooldownTracker(cooldown_sec=0)
        trigger = evaluate_trigger(df, "BTCUSDT", "linear", cooldown=cd)
        self.assertIsNone(trigger)

    def test_empty_df(self):
        df = pl.DataFrame({
            "symbol": pl.Series(dtype=pl.Utf8),
            "category": pl.Series(dtype=pl.Utf8),
            "relative_volume": pl.Series(dtype=pl.Float64),
            "relative_range": pl.Series(dtype=pl.Float64),
        })
        trigger = evaluate_trigger(df, "BTCUSDT", "linear")
        self.assertIsNone(trigger)

    def test_cooldown_prevents(self):
        df = self._make_df(rvol=5.0, rrange=4.0)
        cd = CooldownTracker(cooldown_sec=300)
        t1 = evaluate_trigger(df, "BTCUSDT", "linear", cooldown=cd)
        self.assertIsNotNone(t1)
        t2 = evaluate_trigger(df, "BTCUSDT", "linear", cooldown=cd)
        self.assertIsNone(t2)

    def test_trigger_params_recorded(self):
        df = self._make_df(rvol=5.5, rrange=4.2)
        cd = CooldownTracker(cooldown_sec=0)
        trigger = evaluate_trigger(df, "BTCUSDT", "linear", cooldown=cd)
        self.assertIsNotNone(trigger)
        self.assertIn("relative_volume", trigger.trigger_params)
        self.assertAlmostEqual(trigger.trigger_params["relative_volume"]["value"], 5.5)
        self.assertIn("relative_range", trigger.trigger_params)
        self.assertAlmostEqual(trigger.trigger_params["relative_range"]["value"], 4.2)

    def test_horizons_merged(self):
        df = self._make_df(rvol=5.0, rrange=4.0)
        cd = CooldownTracker(cooldown_sec=0)
        trigger = evaluate_trigger(df, "BTCUSDT", "linear", cooldown=cd)
        self.assertIn(5, trigger.horizons)
        self.assertIn(10, trigger.horizons)

    def test_capture_duration_from_config(self):
        df = self._make_df(rvol=5.0, rrange=4.0)
        cd = CooldownTracker(cooldown_sec=0)
        trigger = evaluate_trigger(df, "BTCUSDT", "linear", cooldown=cd)
        self.assertEqual(trigger.capture_duration_sec, 1200)

    def test_config_hash_in_trigger(self):
        df = self._make_df(rvol=5.0, rrange=4.0)
        cd = CooldownTracker(cooldown_sec=0)
        trigger = evaluate_trigger(df, "BTCUSDT", "linear", cooldown=cd)
        self.assertEqual(trigger.trigger_config_hash, CONFIG_HASH)
        self.assertEqual(trigger.trigger_version, CONFIG_VERSION)


class TestEvaluateAllTriggers(unittest.TestCase):
    def test_multiple_symbols(self):
        events = pl.DataFrame({
            "symbol": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "category": ["linear", "linear", "linear"],
            "relative_volume": [5.0, 6.0, 1.0],
            "relative_range": [4.0, 3.5, 0.5],
            "close": [50000.0, 3000.0, 150.0],
            "high": [51000.0, 3100.0, 155.0],
            "low": [49000.0, 2900.0, 145.0],
            "open": [49500.0, 2950.0, 148.0],
            "volume": [1000.0, 5000.0, 10000.0],
        })
        triggers = evaluate_all_triggers(events)
        self.assertEqual(len(triggers), 2)
        symbols = {t.symbol for t in triggers}
        self.assertIn("BTCUSDT", symbols)
        self.assertIn("ETHUSDT", symbols)
        self.assertNotIn("SOLUSDT", symbols)


class TestPendingTriggers(unittest.TestCase):
    def test_list_empty(self):
        result = list_pending_triggers(_TRIGGERS_DIR)
        self.assertIsInstance(result, list)

    def test_cleanup(self):
        trigger = TriggerFile(
            event_id="test_cleanup_xyz",
            symbol="BTCUSDT",
            category="linear",
        )
        path = trigger.write(_TRIGGERS_DIR)
        self.assertTrue(path.exists())
        cleanup_processed_trigger("test_cleanup_xyz", _TRIGGERS_DIR)
        self.assertFalse(path.exists())


class TestRulesLoaded(unittest.TestCase):
    def test_rules_not_empty(self):
        self.assertGreater(len(RULES), 0)

    def test_rules_have_required_fields(self):
        for rule in RULES:
            self.assertIsInstance(rule.feature, str)
            self.assertIsInstance(rule.operator, str)
            self.assertIsInstance(rule.threshold, (int, float))
            self.assertIsInstance(rule.horizons, list)


if __name__ == "__main__":
    unittest.main()
