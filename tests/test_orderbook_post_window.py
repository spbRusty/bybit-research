"""Tests for post-signal orderbook dataset (build_post_signal_dataset + guard).

Covers R1-a post-window semantics:
  1. missing capture file -> quality "missing"
  2. ok capture (full span, complete depth) -> quality "ok", features computed
  3. partial by span (short capture) -> "partial"
  4. partial by depth (top-10 legacy captures, depth_incomplete) -> "partial"
  5. status != ok -> "partial"
  6. exact event_id join (no asof); unrelated event -> missing, not neighbor
  7. build_post_signal_dataset never modifies input events
  8. guard assert_no_post_signal_features rejects ob_post_* / post_signal
"""
from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.orderbook_features import RECONSTRUCTION_VERSION

_BASE = datetime(2026, 9, 5, 12, 0, 0, tzinfo=timezone.utc)
_BASE_MS = int(_BASE.timestamp() * 1000)


def _wide_rows(base_ts_ms: int, n: int = 3, interval_ms: int = 5000) -> list[dict]:
    """Wide snapshot rows: формат ob_capture_reconstruct (20 уровней)."""
    rows = []
    for i in range(n):
        bp = 100.0 + i * 0.1
        ap = 100.5 + i * 0.1
        row = {
            "timestamp_ms": base_ts_ms + i * interval_ms,
            "update_id": 2000 + i,
            "symbol": "BTCUSDT",
            "best_bid": bp,
            "best_ask": ap,
            "spread_bps": ((ap - bp) / ((bp + ap) / 2)) * 10000,
            "mid_price": (bp + ap) / 2,
            "gap_detected": False,
            "reconstruction_version": RECONSTRUCTION_VERSION,
            "n_levels": 20,
        }
        for lvl in range(1, 21):
            row[f"bid_px_{lvl}"] = bp - (lvl - 1) * 0.1
            row[f"bid_sz_{lvl}"] = 10.0 + lvl
            row[f"ask_px_{lvl}"] = ap + (lvl - 1) * 0.1
            row[f"ask_sz_{lvl}"] = 12.0 + lvl
        rows.append(row)
    return rows


def _write_capture(dirpath: Path, event_id: str, rows: list[dict],
                   meta: dict) -> Path:
    dirpath.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(dirpath / f"{event_id}.parquet")
    (dirpath / f"{event_id}.meta.json").write_text(
        json.dumps(meta, ensure_ascii=False))
    return dirpath / f"{event_id}.parquet"


def _ok_meta(span_sec: int = 1200, max_level: int = 20) -> dict:
    return {
        "status": "ok",
        "n_messages": 1000,
        "n_snapshot_msgs": 2,
        "n_rows": len(_wide_rows(0)) + 1,
        "first_ts_ms": _BASE_MS,
        "last_ts_ms": _BASE_MS + 1200_000,
        "span_sec": span_sec,
        "max_level_seen": max_level,
        "depth_incomplete": max_level < 20,
        "reconstruction_version": RECONSTRUCTION_VERSION,
        "freq_sec": 5,
    }


_EID_OK = "20260905T120000Z_BTCUSDT"
_EID_MISSING = "20260904T000000Z_ETHUSDT"


def _events_with(eids: list[str]) -> pl.DataFrame:
    return pl.DataFrame({
        "event_id": eids,
        "symbol": [e.rsplit("_", 1)[1] for e in eids],
        "category": ["linear"] * len(eids),
    })


class TestPostWindowMissing(unittest.TestCase):
    def test_missing_capture(self):
        from src.orderbook_integration import build_post_signal_dataset

        tmp = Path(tempfile.mkdtemp())
        try:
            with patch("src.orderbook_integration._CAPTURE_RECON_DIR", tmp):
                with patch("src.orderbook_integration._POST_SIGNAL_DIR",
                           tmp / "out"):
                    ds = build_post_signal_dataset(
                        _events_with([_EID_MISSING]))
            self.assertEqual(ds.height, 1)
            self.assertEqual(ds["ob_data_quality_post"][0], "missing")
            self.assertTrue(ds["post_signal"][0])
            self.assertIsNone(ds["ob_post_best_bid"][0])
        finally:
            shutil.rmtree(tmp)


class TestPostWindowOk(unittest.TestCase):
    def test_ok_capture_features_computed(self):
        from src.orderbook_integration import build_post_signal_dataset

        tmp = Path(tempfile.mkdtemp())
        try:
            _write_capture(tmp, _EID_OK, _wide_rows(_BASE_MS), _ok_meta(1200, 20))
            with patch("src.orderbook_integration._CAPTURE_RECON_DIR", tmp):
                with patch("src.orderbook_integration._POST_SIGNAL_DIR",
                           tmp / "out"):
                    ds = build_post_signal_dataset(_events_with([_EID_OK]))
            self.assertEqual(ds["ob_data_quality_post"][0], "ok")
            # Фичи из последнего снапшота окна: bid = 100.0 + 2*0.1
            self.assertAlmostEqual(ds["ob_post_best_bid"][0], 100.2, places=2)
            self.assertIn("ob_post_imbalance_20", ds.columns)
        finally:
            shutil.rmtree(tmp)

    def test_artifact_written(self):
        from src.orderbook_integration import build_post_signal_dataset

        tmp = Path(tempfile.mkdtemp())
        try:
            _write_capture(tmp, _EID_OK, _wide_rows(_BASE_MS), _ok_meta(1200, 20))
            out = tmp / "out"
            with patch("src.orderbook_integration._CAPTURE_RECON_DIR", tmp):
                with patch("src.orderbook_integration._POST_SIGNAL_DIR", out):
                    ds = build_post_signal_dataset(_events_with([_EID_OK]))
            artifact = out / "post_signal_events.parquet"
            self.assertTrue(artifact.exists())
            self.assertEqual(pl.read_parquet(artifact).height, ds.height)
        finally:
            shutil.rmtree(tmp)


class TestPostWindowPartial(unittest.TestCase):
    def test_partial_short_span(self):
        from src.orderbook_integration import build_post_signal_dataset

        tmp = Path(tempfile.mkdtemp())
        try:
            # Выброс 640с: span < 900 (75% от 1200)
            _write_capture(tmp, _EID_OK, _wide_rows(_BASE_MS), _ok_meta(640, 20))
            with patch("src.orderbook_integration._CAPTURE_RECON_DIR", tmp):
                with patch("src.orderbook_integration._POST_SIGNAL_DIR",
                           tmp / "out"):
                    ds = build_post_signal_dataset(_events_with([_EID_OK]))
            self.assertEqual(ds["ob_data_quality_post"][0], "partial")
            self.assertIn("span=640s", ds["ob_data_quality_reason"][0])
        finally:
            shutil.rmtree(tmp)

    def test_partial_depth_incomplete(self):
        from src.orderbook_integration import build_post_signal_dataset

        tmp = Path(tempfile.mkdtemp())
        try:
            # Легаси top-10 captures: depth_incomplete=True
            _write_capture(tmp, _EID_OK, _wide_rows(_BASE_MS), _ok_meta(1200, 10))
            with patch("src.orderbook_integration._CAPTURE_RECON_DIR", tmp):
                with patch("src.orderbook_integration._POST_SIGNAL_DIR",
                           tmp / "out"):
                    ds = build_post_signal_dataset(_events_with([_EID_OK]))
            self.assertEqual(ds["ob_data_quality_post"][0], "partial")
            self.assertIn("depth=10", ds["ob_data_quality_reason"][0])
        finally:
            shutil.rmtree(tmp)

    def test_status_not_ok(self):
        from src.orderbook_integration import build_post_signal_dataset

        tmp = Path(tempfile.mkdtemp())
        try:
            meta = _ok_meta(1200, 20)
            meta["status"] = "no_snapshot"
            _write_capture(tmp, _EID_OK, _wide_rows(_BASE_MS), meta)
            with patch("src.orderbook_integration._CAPTURE_RECON_DIR", tmp):
                with patch("src.orderbook_integration._POST_SIGNAL_DIR",
                           tmp / "out"):
                    ds = build_post_signal_dataset(_events_with([_EID_OK]))
            self.assertEqual(ds["ob_data_quality_post"][0], "partial")
            self.assertIn("no_snapshot", ds["ob_data_quality_reason"][0])
        finally:
            shutil.rmtree(tmp)


class TestPostWindowExactJoin(unittest.TestCase):
    def test_unrelated_event_missing_not_neighbor(self):
        """Точный join по event_id: отсутствующий capture не берёт соседнюю строку."""
        from src.orderbook_integration import build_post_signal_dataset

        tmp = Path(tempfile.mkdtemp())
        try:
            _write_capture(tmp, _EID_OK, _wide_rows(_BASE_MS), _ok_meta(1200, 20))
            events = _events_with([_EID_MISSING])
            with patch("src.orderbook_integration._CAPTURE_RECON_DIR", tmp):
                with patch("src.orderbook_integration._POST_SIGNAL_DIR",
                           tmp / "out"):
                    ds = build_post_signal_dataset(events)
            self.assertEqual(ds["event_id"][0], _EID_MISSING)
            self.assertEqual(ds["ob_data_quality_post"][0], "missing")
            self.assertIsNone(ds["ob_post_best_bid"][0])
            self.assertEqual(events.height, 1)
        finally:
            shutil.rmtree(tmp)

    def test_events_not_modified(self):
        from src.orderbook_integration import build_post_signal_dataset

        tmp = Path(tempfile.mkdtemp())
        try:
            events = _events_with([_EID_OK])
            with patch("src.orderbook_integration._CAPTURE_RECON_DIR", tmp):
                with patch("src.orderbook_integration._POST_SIGNAL_DIR",
                           tmp / "out"):
                    build_post_signal_dataset(events)
            self.assertEqual(events.columns, ["event_id", "symbol", "category"])
            self.assertEqual(events.height, 1)
        finally:
            shutil.rmtree(tmp)

    def test_empty_events(self):
        from src.orderbook_integration import build_post_signal_dataset

        ds = build_post_signal_dataset(pl.DataFrame())
        self.assertEqual(ds.height, 0)


class TestPostSignalGuard(unittest.TestCase):
    def test_clean_events_pass(self):
        from src.orderbook_integration import assert_no_post_signal_features

        events = _events_with([_EID_OK])
        assert_no_post_signal_features(events)  # не должно бросать

    def test_ob_post_columns_rejected(self):
        from src.orderbook_integration import assert_no_post_signal_features

        bad = _events_with([_EID_OK]).with_columns(
            pl.lit(100.0).alias("ob_post_best_bid"))
        with self.assertRaises(RuntimeError) as ctx:
            assert_no_post_signal_features(bad)
        self.assertIn("ob_post_best_bid", str(ctx.exception))

    def test_post_signal_column_rejected(self):
        from src.orderbook_integration import assert_no_post_signal_features

        bad = _events_with([_EID_OK]).with_columns(pl.lit(True).alias("post_signal"))
        with self.assertRaises(RuntimeError):
            assert_no_post_signal_features(bad)


class TestPostColumnNaming(unittest.TestCase):
    def test_post_prefix_mapping(self):
        from src.orderbook_integration import _post_col_name, _get_post_ob_columns

        self.assertEqual(_post_col_name("ob_best_bid"), "ob_post_best_bid")
        cols = _get_post_ob_columns()
        self.assertIn("ob_post_imbalance_20", cols)
        self.assertNotIn("ob_imbalance_20", cols)
        self.assertTrue(all(c.startswith("ob_post_") for c in cols))


if __name__ == "__main__":
    unittest.main()