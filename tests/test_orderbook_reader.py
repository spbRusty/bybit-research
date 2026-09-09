"""Tests for orderbook_reader — reconstructed orderbook data reader."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import polars as pl

from src.orderbook_reader import (
    _date_range,
    read_reconstructed,
    get_window,
    get_pre_event,
    get_post_event,
    list_symbols,
    list_dates,
    storage_stats,
)


class TestDateRange(unittest.TestCase):
    def test_same_day(self):
        ms = int(datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
        result = _date_range(ms, ms)
        self.assertEqual(result, ["2026-01-15"])

    def test_cross_midnight(self):
        t1 = int(datetime(2026, 1, 15, 23, 0, tzinfo=timezone.utc).timestamp() * 1000)
        t2 = int(datetime(2026, 1, 16, 1, 0, tzinfo=timezone.utc).timestamp() * 1000)
        result = _date_range(t1, t2)
        self.assertEqual(result, ["2026-01-15", "2026-01-16"])

    def test_week_span(self):
        t1 = int(datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        t2 = int(datetime(2026, 1, 11, 23, 59, tzinfo=timezone.utc).timestamp() * 1000)
        result = _date_range(t1, t2)
        self.assertEqual(len(result), 7)


class TestReadReconstructed(unittest.TestCase):
    def test_returns_none_when_missing(self):
        result = read_reconstructed("NOSYMBOL_NONEXISTENT_42", "2099-01-01")
        self.assertIsNone(result)

    @patch("src.orderbook_reader.pl.read_parquet")
    @patch("src.orderbook_reader._RECON_DIR")
    def test_reads_parquet(self, mock_dir, mock_read):
        real_path = MagicMock(spec=Path)
        real_path.exists.return_value = True
        mock_dir.__truediv__ = MagicMock(return_value=real_path)
        mock_read.return_value = pl.DataFrame({"timestamp_ms": [1000], "best_bid": [100.0]})
        result = read_reconstructed("BTCUSDT", "2026-01-15")
        self.assertIsNotNone(result)
        self.assertEqual(result.height, 1)


class TestGetWindow(unittest.TestCase):
    @patch("src.orderbook_reader.read_reconstructed")
    @patch("src.orderbook_reader._date_range")
    def test_filters_by_timestamp(self, mock_dates, mock_read):
        mock_dates.return_value = ["2026-01-15"]
        t_ms = int(datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
        mock_read.return_value = pl.DataFrame({
            "timestamp_ms": [t_ms - 90_000, t_ms - 30_000, t_ms, t_ms + 30_000, t_ms + 90_000],
            "best_bid": [100.0, 101.0, 102.0, 103.0, 104.0],
        })
        result = get_window("BTCUSDT", t_ms, pre_minutes=1, post_minutes=1)
        self.assertEqual(result.height, 3)

    @patch("src.orderbook_reader.read_reconstructed")
    @patch("src.orderbook_reader._date_range")
    def test_empty_when_no_data(self, mock_dates, mock_read):
        mock_dates.return_value = ["2026-01-15"]
        mock_read.return_value = None
        t_ms = int(datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
        result = get_window("BTCUSDT", t_ms)
        self.assertEqual(result.height, 0)


class TestGetPreEvent(unittest.TestCase):
    @patch("src.orderbook_reader.read_reconstructed")
    @patch("src.orderbook_reader._date_range")
    def test_includes_only_pre_event(self, mock_dates, mock_read):
        mock_dates.return_value = ["2026-01-15"]
        t_ms = int(datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
        mock_read.return_value = pl.DataFrame({
            "timestamp_ms": [t_ms - 300_000, t_ms, t_ms + 300_000],
            "best_bid": [100.0, 101.0, 102.0],
        })
        result = get_pre_event("BTCUSDT", t_ms, pre_minutes=10)
        self.assertEqual(result.height, 2)
        self.assertTrue((result["timestamp_ms"] <= t_ms).all())


class TestGetPostEvent(unittest.TestCase):
    @patch("src.orderbook_reader.read_reconstructed")
    @patch("src.orderbook_reader._date_range")
    def test_includes_only_post_event(self, mock_dates, mock_read):
        mock_dates.return_value = ["2026-01-15"]
        t_ms = int(datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
        mock_read.return_value = pl.DataFrame({
            "timestamp_ms": [t_ms - 300_000, t_ms + 1, t_ms + 300_000],
            "best_bid": [100.0, 101.0, 102.0],
        })
        result = get_post_event("BTCUSDT", t_ms, post_minutes=10)
        self.assertEqual(result.height, 2)
        self.assertTrue((result["timestamp_ms"] > t_ms).all())


class TestStorageStats(unittest.TestCase):
    @patch("src.orderbook_reader._RECON_DIR")
    def test_empty_when_no_dir(self, mock_dir):
        mock_dir.exists.return_value = False
        result = storage_stats()
        self.assertEqual(result["total_files"], 0)
        self.assertEqual(result["symbols"], 0)


if __name__ == "__main__":
    unittest.main()
