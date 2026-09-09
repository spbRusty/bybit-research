"""Tests for orderbook_features — feature extraction from reconstructed snapshots."""
from __future__ import annotations

import hashlib
import unittest
from datetime import datetime, timezone

import polars as pl

from src.orderbook_features import (
    DEPTH_LEVELS,
    LOOKBACKS,
    CHANGE_INTERVALS,
    FEATURE_VERSION,
    RECONSTRUCTION_VERSION,
    SNAPSHOT_FREQ_SEC,
    _config_hash,
    compute_snapshot_features,
    compute_change_features,
    compute_volatility_features,
    compute_all_features,
)


def _make_snapshot_df(n: int = 20, *, bid_pxs: list[float] | None = None, ask_pxs: list[float] | None = None, bid_szs: list[float] | None = None, ask_szs: list[float] | None = None) -> pl.DataFrame:
    base_ts = 1788598913128
    rows = []
    for i in range(n):
        bp = bid_pxs[i] if bid_pxs else 100.0 + i * 0.1
        ap = ask_pxs[i] if ask_pxs else 100.5 + i * 0.1
        bs = bid_szs[i] if bid_szs else 10.0
        as_ = ask_szs[i] if ask_szs else 12.0
        row = {
            "timestamp_ms": base_ts + i * 5000,
            "update_id": 1000 + i,
            "symbol": "BTCUSDT",
            "best_bid": bp,
            "best_ask": ap,
            "spread_bps": ((ap - bp) / ((bp + ap) / 2)) * 10000,
            "mid_price": (bp + ap) / 2,
            "gap_detected": False,
            "reconstruction_version": "1.0",
            "n_levels": 50,
        }
        for lvl in range(1, 21):
            row[f"bid_px_{lvl}"] = bp - (lvl - 1) * 0.1
            row[f"bid_sz_{lvl}"] = bs + lvl
            row[f"ask_px_{lvl}"] = ap + (lvl - 1) * 0.1
            row[f"ask_sz_{lvl}"] = as_ + lvl
        rows.append(row)
    return pl.DataFrame(rows)


class TestDepthCalculations(unittest.TestCase):
    def test_depth_bid_1(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        self.assertIn("ob_depth_bid_1", result.columns)
        self.assertAlmostEqual(result["ob_depth_bid_1"][0], 11.0)

    def test_depth_bid_3(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        self.assertAlmostEqual(result["ob_depth_bid_3"][0], 11.0 + 12.0 + 13.0)

    def test_depth_ask_5(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        expected = sum(12.0 + lvl for lvl in range(1, 6))
        self.assertAlmostEqual(result["ob_depth_ask_5"][0], expected)

    def test_depth_total(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        self.assertIn("ob_depth_total_5", result.columns)
        bid = result["ob_depth_bid_5"][0]
        ask = result["ob_depth_ask_5"][0]
        self.assertAlmostEqual(result["ob_depth_total_5"][0], bid + ask)

    def test_depth_total_zero(self):
        df = _make_snapshot_df(1)
        for lvl in range(1, 21):
            df = df.with_columns([
                pl.lit(0.0).alias(f"bid_sz_{lvl}"),
                pl.lit(0.0).alias(f"ask_sz_{lvl}"),
            ])
        result = compute_snapshot_features(df)
        self.assertEqual(result["ob_depth_total_1"][0], 0.0)


class TestImbalance(unittest.TestCase):
    def test_imbalance_symmetric(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        self.assertIn("ob_imbalance_5", result.columns)
        bid = result["ob_depth_bid_5"][0]
        ask = result["ob_depth_ask_5"][0]
        expected = (bid - ask) / (bid + ask)
        self.assertAlmostEqual(result["ob_imbalance_5"][0], expected)

    def test_imbalance_zero_depth(self):
        df = _make_snapshot_df(1)
        for lvl in range(1, 21):
            df = df.with_columns([
                pl.lit(0.0).alias(f"bid_sz_{lvl}"),
                pl.lit(0.0).alias(f"ask_sz_{lvl}"),
            ])
        result = compute_snapshot_features(df)
        self.assertIsNone(result["ob_imbalance_1"][0])


class TestSpread(unittest.TestCase):
    def test_spread_bps(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        self.assertIn("ob_spread_bps", result.columns)
        self.assertIn("ob_spread_abs", result.columns)
        bp = result["ob_best_bid"][0]
        ap = result["ob_best_ask"][0]
        self.assertAlmostEqual(result["ob_spread_abs"][0], ap - bp)

    def test_spread_zero_when_equal(self):
        df = _make_snapshot_df(1)
        df = df.with_columns([
            pl.lit(100.0).alias("best_bid"),
            pl.lit(100.0).alias("best_ask"),
            pl.lit(0.0).alias("spread_bps"),
        ])
        result = compute_snapshot_features(df)
        self.assertAlmostEqual(result["ob_spread_abs"][0], 0.0)


class TestVWAP(unittest.TestCase):
    def test_vwap_bid(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        self.assertIn("ob_vwap_bid_5", result.columns)
        num = sum((100.0 - (lvl - 1) * 0.1) * (10.0 + lvl) for lvl in range(1, 6))
        den = sum(10.0 + lvl for lvl in range(1, 6))
        self.assertAlmostEqual(result["ob_vwap_bid_5"][0], num / den, places=4)

    def test_vwap_zero_size(self):
        df = _make_snapshot_df(1)
        for lvl in range(1, 21):
            df = df.with_columns(pl.lit(0.0).alias(f"bid_sz_{lvl}"))
        result = compute_snapshot_features(df)
        self.assertIsNone(result["ob_vwap_bid_1"][0])


class TestConcentration(unittest.TestCase):
    def test_top1_share(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        self.assertIn("ob_top1_share", result.columns)
        bid1 = result["ob_depth_bid_1"][0]
        ask1 = result["ob_depth_ask_1"][0]
        self.assertAlmostEqual(result["ob_top1_share"][0], bid1 / (bid1 + ask1))

    def test_top5_share(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        self.assertIn("ob_top5_share", result.columns)
        bid5 = result["ob_depth_bid_5"][0]
        ask5 = result["ob_depth_ask_5"][0]
        self.assertAlmostEqual(result["ob_top5_share"][0], bid5 / (bid5 + ask5))


class TestChangeFeatures(unittest.TestCase):
    def test_spread_change(self):
        df = _make_snapshot_df(5)
        result = compute_all_features(df)
        self.assertIn("ob_spread_change_1", result.columns)
        self.assertIn("ob_spread_change_6", result.columns)
        self.assertIsNone(result["ob_spread_change_1"][0])
        self.assertIsNotNone(result["ob_spread_change_1"][1])

    def test_depth_change(self):
        df = _make_snapshot_df(10)
        result = compute_all_features(df)
        for iv in CHANGE_INTERVALS:
            col = f"ob_depth_change_5_{iv}"
            self.assertIn(col, result.columns)

    def test_imbalance_change(self):
        df = _make_snapshot_df(10)
        result = compute_all_features(df)
        for iv in CHANGE_INTERVALS:
            col = f"ob_imbalance_change_3_{iv}"
            self.assertIn(col, result.columns)


class TestVolatilityFeatures(unittest.TestCase):
    def test_spread_volatility(self):
        df = _make_snapshot_df(10)
        result = compute_all_features(df)
        for lb in LOOKBACKS:
            col = f"ob_spread_volatility_{lb}"
            self.assertIn(col, result.columns)

    def test_depth_volatility(self):
        df = _make_snapshot_df(10)
        result = compute_all_features(df)
        for lb in LOOKBACKS:
            col = f"ob_depth_volatility_5_{lb}"
            self.assertIn(col, result.columns)

    def test_volatility_insufficient_data(self):
        df = _make_snapshot_df(3)
        result = compute_all_features(df)
        self.assertIsNone(result["ob_spread_volatility_5"][0])
        self.assertIsNone(result["ob_spread_volatility_12"][0])


class TestMissingInvalidSnapshots(unittest.TestCase):
    def test_empty_df(self):
        df = pl.DataFrame()
        result = compute_all_features(df)
        self.assertEqual(result.height, 0)

    def test_single_row(self):
        df = _make_snapshot_df(1)
        result = compute_all_features(df)
        self.assertEqual(result.height, 1)
        self.assertIsNotNone(result["ob_best_bid"][0])

    def test_gap_detected_rows_preserved(self):
        df = _make_snapshot_df(5)
        df = df.with_columns(
            pl.when(pl.col("timestamp_ms") == df["timestamp_ms"][2])
            .then(pl.lit(True))
            .otherwise(pl.col("gap_detected"))
            .alias("gap_detected")
        )
        result = compute_all_features(df)
        self.assertEqual(result.height, 5)


class TestZeroDepthHandling(unittest.TestCase):
    def test_zero_depth_imbalance_is_null(self):
        df = _make_snapshot_df(1)
        for lvl in range(1, 21):
            df = df.with_columns([
                pl.lit(0.0).alias(f"bid_sz_{lvl}"),
                pl.lit(0.0).alias(f"ask_sz_{lvl}"),
            ])
        result = compute_snapshot_features(df)
        self.assertIsNone(result["ob_imbalance_1"][0])
        self.assertIsNone(result["ob_top1_share"][0])

    def test_zero_depth_vwap_is_null(self):
        df = _make_snapshot_df(1)
        for lvl in range(1, 21):
            df = df.with_columns(pl.lit(0.0).alias(f"bid_sz_{lvl}"))
        result = compute_snapshot_features(df)
        self.assertIsNone(result["ob_vwap_bid_1"][0])

    def test_zero_depth_total_is_zero(self):
        df = _make_snapshot_df(1)
        for lvl in range(1, 21):
            df = df.with_columns([
                pl.lit(0.0).alias(f"bid_sz_{lvl}"),
                pl.lit(0.0).alias(f"ask_sz_{lvl}"),
            ])
        result = compute_snapshot_features(df)
        self.assertEqual(result["ob_depth_total_1"][0], 0.0)


class TestTimestampBoundary(unittest.TestCase):
    def test_predictor_no_future_leakage(self):
        event_ts = 1788598913128 + 3 * 5000
        df = _make_snapshot_df(10)
        pre_df = df.filter(pl.col("timestamp_ms") <= event_ts)
        result = compute_all_features(pre_df)
        self.assertTrue((result["timestamp_ms"] <= event_ts).all())
        self.assertGreater(result.height, 0)

    def test_change_features_need_previous_data(self):
        df = _make_snapshot_df(10)
        result = compute_all_features(df)
        self.assertIsNone(result["ob_spread_change_1"][0])
        self.assertIsNotNone(result["ob_spread_change_1"][1])


class TestProvenance(unittest.TestCase):
    def test_feature_version(self):
        df = _make_snapshot_df(1)
        result = compute_all_features(df)
        self.assertEqual(result["feature_version"][0], FEATURE_VERSION)

    def test_reconstruction_version(self):
        df = _make_snapshot_df(1)
        result = compute_all_features(df)
        self.assertEqual(result["reconstruction_version"][0], RECONSTRUCTION_VERSION)

    def test_snapshot_freq(self):
        df = _make_snapshot_df(1)
        result = compute_all_features(df)
        self.assertEqual(result["snapshot_freq_sec"][0], SNAPSHOT_FREQ_SEC)

    def test_config_hash(self):
        df = _make_snapshot_df(1)
        result = compute_all_features(df)
        self.assertEqual(result["config_hash"][0], _config_hash())

    def test_config_hash_deterministic(self):
        h1 = _config_hash()
        h2 = _config_hash()
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 12)


class TestFeatureSchema(unittest.TestCase):
    def test_all_snapshot_feature_columns(self):
        df = _make_snapshot_df(1)
        result = compute_snapshot_features(df)
        for prefix in ("ob_best_bid", "ob_best_ask", "ob_mid", "ob_spread_bps", "ob_spread_abs"):
            self.assertIn(prefix, result.columns)
        for n in DEPTH_LEVELS:
            self.assertIn(f"ob_depth_bid_{n}", result.columns)
            self.assertIn(f"ob_depth_ask_{n}", result.columns)
            self.assertIn(f"ob_depth_total_{n}", result.columns)
            self.assertIn(f"ob_imbalance_{n}", result.columns)
            self.assertIn(f"ob_vwap_bid_{n}", result.columns)
            self.assertIn(f"ob_vwap_ask_{n}", result.columns)
        self.assertIn("ob_top1_share", result.columns)
        self.assertIn("ob_top5_share", result.columns)

    def test_all_change_feature_columns(self):
        df = _make_snapshot_df(5)
        result = compute_all_features(df)
        for n in DEPTH_LEVELS:
            for iv in CHANGE_INTERVALS:
                self.assertIn(f"ob_depth_change_{n}_{iv}", result.columns)
        for iv in CHANGE_INTERVALS:
            self.assertIn(f"ob_spread_change_{iv}", result.columns)
        for n in DEPTH_LEVELS:
            for iv in CHANGE_INTERVALS:
                self.assertIn(f"ob_imbalance_change_{n}_{iv}", result.columns)

    def test_all_volatility_feature_columns(self):
        df = _make_snapshot_df(10)
        result = compute_all_features(df)
        for n in DEPTH_LEVELS:
            for lb in LOOKBACKS:
                self.assertIn(f"ob_depth_volatility_{n}_{lb}", result.columns)
        for lb in LOOKBACKS:
            self.assertIn(f"ob_spread_volatility_{lb}", result.columns)
        for n in DEPTH_LEVELS:
            for lb in LOOKBACKS:
                self.assertIn(f"ob_imbalance_volatility_{n}_{lb}", result.columns)


if __name__ == "__main__":
    unittest.main()
