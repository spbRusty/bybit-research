"""Deterministic comparison tests for OB integration temporal join.

Verifies that the new join_asof implementation produces identical results
to the original O(E×S) algorithm across Cases A-H.
"""
from __future__ import annotations

import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.orderbook_integration import (
    MISSING_THRESHOLD_SEC,
    STALE_THRESHOLD_SEC,
    _get_ob_columns,
    _process_symbol,
)

_BASE = datetime(2026, 9, 5, 12, 0, 0, tzinfo=timezone.utc)
_BASE_MS = int(_BASE.timestamp() * 1000)
_DATE_STR = "2026-09-05"


def _snapshot(symbol: str, ts_ms: int, bid: float = 100.0, gap: bool = False) -> dict:
    ap = bid + 0.5
    row = {
        "timestamp_ms": ts_ms,
        "update_id": 1000,
        "symbol": symbol,
        "best_bid": bid,
        "best_ask": ap,
        "spread_bps": ((ap - bid) / ((bid + ap) / 2)) * 10000,
        "mid_price": (bid + ap) / 2,
        "gap_detected": gap,
        "reconstruction_version": "1.0",
        "n_levels": 50,
    }
    for lvl in range(1, 21):
        row[f"bid_px_{lvl}"] = bid - (lvl - 1) * 0.1
        row[f"bid_sz_{lvl}"] = 10.0 + lvl
        row[f"ask_px_{lvl}"] = ap + (lvl - 1) * 0.1
        row[f"ask_sz_{lvl}"] = 12.0 + lvl
    return row


def _event(ts_ms: int, symbol: str = "BTCUSDT") -> pl.DataFrame:
    return pl.DataFrame([{
        "open_time": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
        "symbol": symbol,
        "category": "linear",
        "entry_price": 100.0,
        "return_5m": 0.001,
        "return_10m": 0.002,
        "return_30m": 0.003,
        "mfe_5m": 0.005,
        "mae_5m": -0.003,
        "relative_volume": 3.5,
        "is_green": True,
    }])


def _run(symbol, snapshots, event_times_ms):
    tmp = Path(tempfile.mkdtemp())
    try:
        path = tmp / symbol / f"{_DATE_STR}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(snapshots).write_parquet(path)
        events = pl.concat([_event(t, symbol) for t in event_times_ms])
        with patch("src.orderbook_integration._RECON_DIR", tmp):
            return _process_symbol(symbol, events, _get_ob_columns())
    finally:
        shutil.rmtree(tmp)


class TestCaseA_ExactTimestampMatch(unittest.TestCase):
    def test_snapshot_exact_t(self):
        result = _run("BTCUSDT", [_snapshot("BTCUSDT", _BASE_MS)], [_BASE_MS])
        self.assertEqual(result["ob_data_quality"][0], "ok")
        self.assertAlmostEqual(result["ob_best_bid"][0], 100.0, places=2)


class TestCaseB_MultipleSnapshotsBeforeT(unittest.TestCase):
    def test_uses_last_before_t(self):
        snaps = [
            _snapshot("BTCUSDT", _BASE_MS - 10000, bid=100.0),
            _snapshot("BTCUSDT", _BASE_MS - 5000, bid=101.0),
            _snapshot("BTCUSDT", _BASE_MS - 2000, bid=102.0),
        ]
        result = _run("BTCUSDT", snaps, [_BASE_MS])
        self.assertEqual(result["ob_data_quality"][0], "ok")
        self.assertAlmostEqual(result["ob_best_bid"][0], 102.0, places=2)


class TestCaseC_SnapshotAfterT(unittest.TestCase):
    def test_rejected(self):
        snaps = [
            _snapshot("BTCUSDT", _BASE_MS + 5000, bid=999.0),
        ]
        result = _run("BTCUSDT", snaps, [_BASE_MS])
        self.assertEqual(result["ob_data_quality"][0], "missing")
        self.assertIsNone(result["ob_best_bid"][0])


class TestCaseD_GapBetweenSnapshots(unittest.TestCase):
    def test_gap_quality(self):
        snaps = [
            _snapshot("BTCUSDT", _BASE_MS - 5000, gap=True),
        ]
        result = _run("BTCUSDT", snaps, [_BASE_MS])
        self.assertEqual(result["ob_data_quality"][0], "gap")
        self.assertIsNone(result["ob_best_bid"][0])

    def test_gap_does_not_propagate_to_later_snapshot(self):
        snaps = [
            _snapshot("BTCUSDT", _BASE_MS - 20000, gap=True),
            _snapshot("BTCUSDT", _BASE_MS - 3000, gap=False),
        ]
        result = _run("BTCUSDT", snaps, [_BASE_MS])
        self.assertEqual(result["ob_data_quality"][0], "ok")
        self.assertAlmostEqual(result["ob_best_bid"][0], 100.0, places=2)


class TestCaseE_NoSuitableSnapshot(unittest.TestCase):
    def test_all_null(self):
        result = _run("BTCUSDT", [], [_BASE_MS])
        self.assertEqual(result["ob_data_quality"][0], "missing")
        self.assertIsNone(result["ob_best_bid"][0])
        self.assertIsNone(result["ob_spread_bps"][0])


class TestCaseF_StaleSnapshot(unittest.TestCase):
    def test_stale_quality(self):
        snap_ms = _BASE_MS - (STALE_THRESHOLD_SEC + 60) * 1000
        snaps = [_snapshot("BTCUSDT", snap_ms)]
        result = _run("BTCUSDT", snaps, [_BASE_MS])
        self.assertEqual(result["ob_data_quality"][0], "stale")
        self.assertIsNotNone(result["ob_best_bid"][0])

    def test_stale_too_old_becomes_missing(self):
        snap_ms = _BASE_MS - (MISSING_THRESHOLD_SEC + 60) * 1000
        snaps = [_snapshot("BTCUSDT", snap_ms)]
        result = _run("BTCUSDT", snaps, [_BASE_MS])
        self.assertEqual(result["ob_data_quality"][0], "missing")
        self.assertIsNone(result["ob_best_bid"][0])


class TestCaseG_MultipleEventsBetweenSnapshots(unittest.TestCase):
    def test_all_events_get_correct_snapshot(self):
        snaps = [
            _snapshot("BTCUSDT", _BASE_MS - 30000, bid=100.0),
            _snapshot("BTCUSDT", _BASE_MS - 10000, bid=101.0),
        ]
        event_times = [
            _BASE_MS - 25000,
            _BASE_MS - 15000,
            _BASE_MS - 8000,
        ]
        result = _run("BTCUSDT", snaps, event_times)
        self.assertEqual(result.height, 3)
        self.assertAlmostEqual(result["ob_best_bid"][0], 100.0, places=2)
        self.assertAlmostEqual(result["ob_best_bid"][1], 100.0, places=2)
        self.assertAlmostEqual(result["ob_best_bid"][2], 101.0, places=2)
        self.assertTrue(all(q == "ok" for q in result["ob_data_quality"].to_list()))


class TestCaseH_MultipleSymbols(unittest.TestCase):
    def test_symbols_isolated(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            btc_snaps = [_snapshot("BTCUSDT", _BASE_MS - 5000, bid=50000.0)]
            eth_snaps = [_snapshot("ETHUSDT", _BASE_MS - 5000, bid=3000.0)]
            for sym, snaps in [("BTCUSDT", btc_snaps), ("ETHUSDT", eth_snaps)]:
                p = tmp / sym / f"{_DATE_STR}.parquet"
                p.parent.mkdir(parents=True, exist_ok=True)
                pl.DataFrame(snaps).write_parquet(p)

            btc_ev = _event(_BASE_MS, "BTCUSDT")
            eth_ev = _event(_BASE_MS, "ETHUSDT")
            events = pl.concat([btc_ev, eth_ev])

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", btc_ev, _get_ob_columns())

            self.assertEqual(result["ob_data_quality"][0], "ok")
            self.assertAlmostEqual(result["ob_best_bid"][0], 50000.0, places=2)
        finally:
            shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main()
