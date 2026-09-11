"""OOS auto-shift (variant 2) tests: only oos.end moves to min(klines_max, today)."""
from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import load_toml
from src.research import split_periods

_R = load_toml("research.toml")


def _events(until="2026-09-10 06:00:00"):
    dates = pl.datetime_range(
        pl.datetime(2025, 12, 25), datetime.strptime(until, "%Y-%m-%d %H:%M:%S"),
        interval="1h", eager=True)
    return pl.DataFrame({
        "open_time": dates,
        "symbol": "BTCUSDT",
        "category": "linear",
        "entry_price": 1.0,
    })


class TestSplitPeriodsOosEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ev = _events()
        cls.base = split_periods(cls.ev)  # без oos_end — поведение как раньше

    def test_default_no_oos_end(self):
        # Без oos_end конфиг-граница oos не сдвигается;
        # окно фильтруется строго open_time < end (полночь 2026-09-02)
        self.assertEqual(
            str(self.base["oos"]["open_time"].max()),
            "2026-09-01 23:00:00")

    def test_oos_end_extends_only_oos(self):
        p = split_periods(self.ev, oos_end="2026-09-10T07:49:00")
        self.assertGreater(p["oos"].height, self.base["oos"].height)
        # discovery/validation окна не тронуты
        self.assertEqual(p["discovery"]["open_time"].min(),
                         self.base["discovery"]["open_time"].min())
        self.assertEqual(p["discovery"]["open_time"].max(),
                         self.base["discovery"]["open_time"].max())
        self.assertEqual(p["validation"]["open_time"].max(),
                         self.base["validation"]["open_time"].max())

    def test_stale_oos_end_does_not_shrink(self):
        # oos_end раньше конфиг-конца -> окно НЕ сжимается
        p = split_periods(self.ev, oos_end="2026-08-01T00:00:00")
        self.assertEqual(p["oos"].height, self.base["oos"].height)
        self.assertEqual(str(p["oos"]["open_time"].max()),
                         str(self.base["oos"]["open_time"].max()))

    def test_oos_end_capped_at_today(self):
        # oos_end позже "сегодня" -> min(klines_max, today) урезает
        p = split_periods(self.ev, oos_end="2030-01-01T00:00:00")
        self.assertLessEqual(
            p["oos"]["open_time"].max(), self.ev["open_time"].max())


if __name__ == "__main__":
    unittest.main()