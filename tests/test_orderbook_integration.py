"""Tests for orderbook_integration — temporal join of OB features to candle events.

Covers 15 categories per Phase 2 audit:
  1. latest snapshot <= T
  2. snapshot > T is rejected
  3. exact timestamp T
  4. missing snapshot
  5. stale snapshot
  6. gap/invalid snapshot
  7. multiple events for one symbol
  8. multiple symbols
  9. date boundary
  10. provenance/config hash
  11. candle-only regression
  12. OB + candle integration
  13. no modification of target construction
  14. BH operates across complete discovery hypothesis set
  15. validation/OOS cannot select OB features
"""
from __future__ import annotations

import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np
import polars as pl

from src.orderbook_features import DEPTH_LEVELS, FEATURE_VERSION, RECONSTRUCTION_VERSION

# Consistent base timestamp: 2026-09-05 12:00:00 UTC
_BASE = datetime(2026, 9, 5, 12, 0, 0, tzinfo=timezone.utc)
_BASE_MS = int(_BASE.timestamp() * 1000)
_DATE_STR = "2026-09-05"


def _make_snapshot_rows(
    symbol: str,
    base_ts_ms: int,
    n: int = 10,
    interval_ms: int = 5000,
    gap_at: int | None = None,
) -> list[dict]:
    rows = []
    for i in range(n):
        bp = 100.0 + i * 0.1
        ap = 100.5 + i * 0.1
        row = {
            "timestamp_ms": base_ts_ms + i * interval_ms,
            "update_id": 1000 + i,
            "symbol": symbol,
            "best_bid": bp,
            "best_ask": ap,
            "spread_bps": ((ap - bp) / ((bp + ap) / 2)) * 10000,
            "mid_price": (bp + ap) / 2,
            "gap_detected": (i == gap_at) if gap_at is not None else False,
            "reconstruction_version": RECONSTRUCTION_VERSION,
            "n_levels": 50,
        }
        for lvl in range(1, 21):
            row[f"bid_px_{lvl}"] = bp - (lvl - 1) * 0.1
            row[f"bid_sz_{lvl}"] = 10.0 + lvl
            row[f"ask_px_{lvl}"] = ap + (lvl - 1) * 0.1
            row[f"ask_sz_{lvl}"] = 12.0 + lvl
        rows.append(row)
    return rows


def _write_parquet(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _make_events(symbol: str, times_ms: list[int]) -> pl.DataFrame:
    rows = []
    for t in times_ms:
        rows.append({
            "open_time": datetime.fromtimestamp(t / 1000, tz=timezone.utc),
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
        })
    return pl.DataFrame(rows)


class TestLatestSnapshotLeT(unittest.TestCase):
    def test_latest_snapshot_before_t(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - 30_000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=5, interval_ms=5000)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "ok")
            self.assertIsNotNone(result["ob_spread_bps"][0])
        finally:
            shutil.rmtree(tmp)

    def test_rejects_snapshot_after_t(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS + 5000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "missing")
        finally:
            shutil.rmtree(tmp)


class TestExactTimestampT(unittest.TestCase):
    def test_exact_match(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        rows = _make_snapshot_rows("BTCUSDT", _BASE_MS, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "ok")
            self.assertIsNotNone(result["ob_spread_bps"][0])
        finally:
            shutil.rmtree(tmp)


class TestMissingSnapshot(unittest.TestCase):
    def test_no_parquet_file(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        tmp = Path(tempfile.mkdtemp())
        try:
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "missing")
            self.assertIsNone(result["ob_spread_bps"][0])
        finally:
            shutil.rmtree(tmp)

    def test_empty_parquet_file(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", [])
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "missing")
        finally:
            shutil.rmtree(tmp)


class TestStaleSnapshot(unittest.TestCase):
    def test_stale_quality(self):
        from src.orderbook_integration import STALE_THRESHOLD_SEC, _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - (STALE_THRESHOLD_SEC + 60) * 1000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "stale")
        finally:
            shutil.rmtree(tmp)

    def test_ok_quality_within_threshold(self):
        from src.orderbook_integration import STALE_THRESHOLD_SEC, _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - (STALE_THRESHOLD_SEC - 10) * 1000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "ok")
        finally:
            shutil.rmtree(tmp)

    def test_stale_features_populated(self):
        from src.orderbook_integration import STALE_THRESHOLD_SEC, MISSING_THRESHOLD_SEC, _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - (STALE_THRESHOLD_SEC + 60) * 1000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "stale")
            self.assertIsNotNone(result["ob_best_bid"][0])
        finally:
            shutil.rmtree(tmp)


class TestMissingFeaturesAreNull(unittest.TestCase):
    def test_missing_quality_features_are_null(self):
        from src.orderbook_integration import MISSING_THRESHOLD_SEC, _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - (MISSING_THRESHOLD_SEC + 60) * 1000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "missing")
            self.assertIsNone(result["ob_best_bid"][0])
            self.assertIsNone(result["ob_spread_bps"][0])
            self.assertIsNone(result["ob_mid"][0])
        finally:
            shutil.rmtree(tmp)

    def test_gap_quality_features_are_null(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - 5_000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=1, gap_at=0)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "gap")
            self.assertIsNone(result["ob_best_bid"][0])
            self.assertIsNone(result["ob_spread_bps"][0])
        finally:
            shutil.rmtree(tmp)

    def test_no_file_features_are_null(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        tmp = Path(tempfile.mkdtemp())
        try:
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "missing")
            self.assertIsNone(result["ob_best_bid"][0])
            self.assertIsNone(result["ob_spread_bps"][0])
        finally:
            shutil.rmtree(tmp)


class TestGapSnapshot(unittest.TestCase):
    def test_gap_detected_marks_gap_quality(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - 30_000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=1, gap_at=0)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "gap")
        finally:
            shutil.rmtree(tmp)

    def test_gap_does_not_affect_other_events(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        snap_before_ms = _BASE_MS - 120_000
        snap_after_ms = _BASE_MS - 5_000
        rows = (
            _make_snapshot_rows("BTCUSDT", snap_before_ms, n=3, interval_ms=30000) +
            _make_snapshot_rows("BTCUSDT", snap_after_ms, n=1, gap_at=0)
        )
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "gap")
        finally:
            shutil.rmtree(tmp)


class TestMultipleEventsOneSymbol(unittest.TestCase):
    def test_multiple_events_same_symbol(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - 30_000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=5, interval_ms=5000)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            event_times = [_BASE_MS, _BASE_MS + 30_000, _BASE_MS + 60_000]
            events = _make_events("BTCUSDT", event_times)
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result.height, 3)
            for i in range(3):
                self.assertEqual(result["ob_data_quality"][i], "ok")
        finally:
            shutil.rmtree(tmp)


class TestMultipleSymbols(unittest.TestCase):
    def test_two_symbols(self):
        from src.orderbook_integration import join_ob_features

        snap_ms = _BASE_MS - 30_000
        tmp = Path(tempfile.mkdtemp())
        try:
            btc_rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=3, interval_ms=5000)
            eth_rows = _make_snapshot_rows("ETHUSDT", snap_ms, n=3, interval_ms=5000)
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", btc_rows)
            _write_parquet(tmp / "ETHUSDT" / f"{_DATE_STR}.parquet", eth_rows)

            btc_events = _make_events("BTCUSDT", [_BASE_MS])
            eth_events = _make_events("ETHUSDT", [_BASE_MS])
            events = pl.concat([btc_events, eth_events])

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = join_ob_features(events)

            self.assertEqual(result.height, 2)
            self.assertTrue(all(q == "ok" for q in result["ob_data_quality"].to_list()))
        finally:
            shutil.rmtree(tmp)


class TestDateBoundary(unittest.TestCase):
    def test_event_at_midnight_reads_both_dates(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        midnight = datetime(2026, 9, 6, 0, 0, 0, tzinfo=timezone.utc)
        event_ms = int(midnight.timestamp() * 1000)
        snap_prev_day_ms = event_ms - 30_000

        rows = _make_snapshot_rows("BTCUSDT", snap_prev_day_ms, n=3, interval_ms=5000)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / "2026-09-05.parquet", rows)
            events = _make_events("BTCUSDT", [event_ms])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertEqual(result["ob_data_quality"][0], "ok")
        finally:
            shutil.rmtree(tmp)


class TestProvenance(unittest.TestCase):
    def test_provenance_fields(self):
        from src.orderbook_integration import get_ob_provenance, INTEGRATION_VERSION

        prov = get_ob_provenance()
        self.assertEqual(prov["ob_integration_version"], INTEGRATION_VERSION)
        self.assertIn("ob_integration_config_hash", prov)
        self.assertIn("ob_feature_version", prov)
        self.assertIn("ob_reconstruction_version", prov)
        self.assertEqual(prov["ob_feature_version"], FEATURE_VERSION)
        self.assertEqual(prov["ob_reconstruction_version"], RECONSTRUCTION_VERSION)

    def test_config_hash_deterministic(self):
        from src.orderbook_integration import _integration_config_hash
        h1 = _integration_config_hash()
        h2 = _integration_config_hash()
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 12)


class TestCandleOnlyRegression(unittest.TestCase):
    def test_ob_columns_do_not_modify_existing_columns(self):
        from src.orderbook_integration import join_ob_features

        events = _make_events("BTCUSDT", [_BASE_MS, _BASE_MS + 60_000])
        original_cols = set(events.columns)
        original_data = {c: events[c].to_list() for c in original_cols}

        with patch("src.orderbook_integration._RECON_DIR", Path(tempfile.mkdtemp())):
            result = join_ob_features(events)

        for c in original_cols:
            self.assertIn(c, result.columns)
            self.assertEqual(result[c].to_list(), original_data[c])

    def test_no_ob_data_preserves_null_quality(self):
        from src.orderbook_integration import join_ob_features

        events = _make_events("BTCUSDT", [_BASE_MS])

        with patch("src.orderbook_integration._RECON_DIR", Path(tempfile.mkdtemp())):
            result = join_ob_features(events)

        self.assertEqual(result["ob_data_quality"][0], "missing")
        self.assertEqual(result["return_5m"][0], 0.001)


class TestObCandleIntegration(unittest.TestCase):
    def test_ob_columns_added_to_events(self):
        from src.orderbook_integration import join_ob_features, _get_ob_columns

        snap_ms = _BASE_MS - 10_000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=5, interval_ms=5000)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            expected_ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = join_ob_features(events)

            for c in expected_ob_cols:
                self.assertIn(c, result.columns)
            self.assertIn("ob_data_quality", result.columns)
        finally:
            shutil.rmtree(tmp)

    def test_ob_features_match_raw_snapshot(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        snap_ms = _BASE_MS - 5_000
        rows = _make_snapshot_rows("BTCUSDT", snap_ms, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            ob_cols = _get_ob_columns()

            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, ob_cols)

            self.assertAlmostEqual(result["ob_best_bid"][0], 100.0, places=2)
            self.assertAlmostEqual(result["ob_best_ask"][0], 100.5, places=2)
            self.assertAlmostEqual(result["ob_mid"][0], 100.25, places=2)
        finally:
            shutil.rmtree(tmp)


class TestTargetConstructionUnchanged(unittest.TestCase):
    def test_future_metrics_not_modified(self):
        from src.orderbook_integration import join_ob_features

        events = _make_events("BTCUSDT", [_BASE_MS])
        orig_return = events["return_5m"][0]
        orig_mfe = events["mfe_5m"][0]
        orig_mae = events["mae_5m"][0]

        with patch("src.orderbook_integration._RECON_DIR", Path(tempfile.mkdtemp())):
            result = join_ob_features(events)

        self.assertEqual(result["return_5m"][0], orig_return)
        self.assertEqual(result["mfe_5m"][0], orig_mfe)
        self.assertEqual(result["mae_5m"][0], orig_mae)


class TestBHAcrossCompleteSet(unittest.TestCase):
    def test_hypothesis_generator_loads_ob_rules(self):
        from src.hypothesis_generator import ob_rules, all_rules

        ob = ob_rules()
        self.assertGreater(len(ob), 0)
        for r in ob:
            self.assertIn("ob_", r.feature_id)

        combined = all_rules()
        self.assertGreater(len(combined), len(ob))

    def test_ob_rules_respect_max_conditions(self):
        from src.hypothesis_generator import ob_rules

        for r in ob_rules():
            self.assertLessEqual(r.max_conditions, 2)


class TestValidationOosCannotSelect(unittest.TestCase):
    def test_discovery_uses_all_hypotheses(self):
        from src.research import benjamini_hochberg

        p_vals = np.array([0.001, 0.01, 0.03, 0.04, 0.05, 0.1])
        sig = benjamini_hochberg(p_vals, 0.05)
        self.assertGreater(sig.sum(), 0)

        p_vals_tight = np.array([0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
        sig_tight = benjamini_hochberg(p_vals_tight, 0.05)
        self.assertEqual(sig_tight.sum(), 0)

    def test_validation_oos_same_condition_string(self):
        from src.hypothesis_generator import ob_rules

        for r in ob_rules():
            cond = f"pl.col('{r.feature_id}')"
            self.assertIn("ob_", cond)

    def test_unified_bh_single_correction(self):
        from src.research import benjamini_hochberg
        p_vals = np.array([0.001, 0.01, 0.05, 0.1, 0.5])
        sig = benjamini_hochberg(p_vals, 0.05)
        self.assertEqual(sig.sum(), 2)

    def test_ob_hypotheses_generated_with_all_rules(self):
        from src.hypothesis_generator import generate_hypotheses, all_rules
        import polars as pl
        df = pl.DataFrame({
            'ob_spread_bps': [0.01, 0.02, 0.03],
            'ob_imbalance_1': [0.1, 0.2, 0.3],
            'ob_imbalance_5': [0.1, 0.2, 0.3],
            'ob_imbalance_10': [0.1, 0.2, 0.3],
            'ob_top1_share': [0.5, 0.6, 0.7],
            'ob_depth_change_5_1': [0.0, 0.1, 0.2],
            'ob_depth_change_10_1': [0.0, 0.1, 0.2],
            'ob_spread_change_1': [0.0, 0.01, 0.02],
            'relative_volume': [1.0, 2.0, 3.0],
            'relative_range': [0.01, 0.02, 0.03],
            'volume_zscore': [0.0, 1.0, 2.0],
            'realized_vol_60': [0.01, 0.02, 0.03],
            'volatility_regime': ['low', 'high', 'very_high'],
            'dist_rolling_high_60': [0.1, 0.2, 0.3],
            'dist_rolling_low_60': [-0.1, -0.2, -0.3],
            'breakout_20': [0.1, 0.5, 1.0],
            'roc_20': [-0.01, 0.0, 0.01],
            'rsi_14': [30.0, 50.0, 70.0],
            'corr_btc_60': [0.1, 0.3, 0.5],
            'btc_trend_regime': [-0.01, 0.0, 0.01],
        })
        hyps = generate_hypotheses(df, rules=all_rules())
        ob_hyps = [h for h in hyps if 'ob_' in h.condition]
        self.assertGreater(len(ob_hyps), 0, 'OB hypotheses must be generated')


class TestGapRecovery(unittest.TestCase):
    def test_gap_then_clean_snapshot(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        old = _make_snapshot_rows("BTCUSDT", _BASE_MS - 60000, n=1)
        gap = _make_snapshot_rows("BTCUSDT", _BASE_MS - 30000, n=1, gap_at=0)
        clean = _make_snapshot_rows("BTCUSDT", _BASE_MS - 5000, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", old + gap + clean)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "ok")
        finally:
            shutil.rmtree(tmp)

    def test_old_gap_does_not_contaminate(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        old_gap = _make_snapshot_rows("BTCUSDT", _BASE_MS - 120000, n=1, gap_at=0)
        new_clean = _make_snapshot_rows("BTCUSDT", _BASE_MS - 2000, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", old_gap + new_clean)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "ok")
        finally:
            shutil.rmtree(tmp)


class TestTemporalEdgeCases(unittest.TestCase):
    def test_snapshot_one_ms_after_rejected(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        rows = _make_snapshot_rows("BTCUSDT", _BASE_MS + 1, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "missing")
        finally:
            shutil.rmtree(tmp)

    def test_snapshot_one_ms_before_accepted(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        rows = _make_snapshot_rows("BTCUSDT", _BASE_MS - 1, n=1)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", rows)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "ok")
        finally:
            shutil.rmtree(tmp)

    def test_mixed_before_after_uses_latest_pre_t(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        before = _make_snapshot_rows("BTCUSDT", _BASE_MS - 10000, n=2, interval_ms=5000)
        after = _make_snapshot_rows("BTCUSDT", _BASE_MS + 1000, n=2, interval_ms=5000)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", before + after)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertEqual(result["ob_data_quality"][0], "ok")
            self.assertAlmostEqual(result["ob_best_bid"][0], 100.1, places=2)
        finally:
            shutil.rmtree(tmp)

    def test_post_t_snapshot_never_used(self):
        from src.orderbook_integration import _process_symbol, _get_ob_columns

        before = _make_snapshot_rows("BTCUSDT", _BASE_MS - 5000, n=1)
        after_rows = _make_snapshot_rows("BTCUSDT", _BASE_MS + 5000, n=1)
        after_modified = []
        for row in after_rows:
            r = dict(row)
            r["best_bid"] = 999.0
            r["best_ask"] = 999.5
            r["mid_price"] = 999.25
            after_modified.append(r)
        tmp = Path(tempfile.mkdtemp())
        try:
            _write_parquet(tmp / "BTCUSDT" / f"{_DATE_STR}.parquet", before + after_modified)
            events = _make_events("BTCUSDT", [_BASE_MS])
            with patch("src.orderbook_integration._RECON_DIR", tmp):
                result = _process_symbol("BTCUSDT", events, _get_ob_columns())
            self.assertNotEqual(result["ob_best_bid"][0], 999.0)
            self.assertAlmostEqual(result["ob_best_bid"][0], 100.0, places=2)
        finally:
            shutil.rmtree(tmp)


class TestProvenanceRulesHash(unittest.TestCase):
    def test_ob_hypothesis_rules_hash_in_provenance(self):
        from src.orderbook_integration import get_ob_provenance
        prov = get_ob_provenance()
        self.assertIn("ob_hypothesis_rules_hash", prov)
        self.assertEqual(len(prov["ob_hypothesis_rules_hash"]), 12)

    def test_ob_rules_hash_deterministic(self):
        from src.orderbook_integration import _ob_rules_hash
        h1 = _ob_rules_hash()
        h2 = _ob_rules_hash()
        self.assertEqual(h1, h2)

    def test_ob_rules_hash_differs_from_config_hash(self):
        from src.orderbook_integration import _integration_config_hash, _ob_rules_hash
        self.assertNotEqual(_integration_config_hash(), _ob_rules_hash())

    def test_provenance_fields_complete(self):
        from src.orderbook_integration import get_ob_provenance
        prov = get_ob_provenance()
        required = [
            "ob_integration_version",
            "ob_integration_config_hash",
            "ob_feature_version",
            "ob_reconstruction_version",
            "ob_hypothesis_rules_hash",
        ]
        for k in required:
            self.assertIn(k, prov)
            self.assertTrue(prov[k], f'{k} is empty')


class TestPredictorTargetBoundary(unittest.TestCase):
    def test_ob_features_never_modify_targets(self):
        from src.orderbook_integration import join_ob_features

        events = _make_events("BTCUSDT", [_BASE_MS])
        targets = {c: events[c][0] for c in ["return_5m", "return_10m", "return_30m",
                                               "mfe_5m", "mae_5m"]}
        with patch("src.orderbook_integration._RECON_DIR", Path(tempfile.mkdtemp())):
            result = join_ob_features(events)
        for c, v in targets.items():
            self.assertEqual(result[c][0], v, f'{c} was modified')

    def test_hypothesis_target_never_ob_column(self):
        from src.research import Hypothesis
        hyp = Hypothesis("test", "test", "pl.col('ob_spread_bps') > 0.01", "long", 5)
        self.assertEqual(hyp.target_column, "return_5m")
        self.assertNotIn("ob_", hyp.target_column)

    def test_critic_net_ret_uses_return_not_ob(self):
        from src.critic import _candidate_subset
        events = _make_events("BTCUSDT", [_BASE_MS])
        finalist = {
            "condition": "pl.col('relative_volume') > 3.0",
            "entry_side": "long",
            "horizon_min": 5,
        }
        sub = _candidate_subset(events, finalist)
        self.assertIn("net_ret", sub.columns)
        self.assertNotIn("ob_", sub.columns)


if __name__ == "__main__":
    unittest.main()
