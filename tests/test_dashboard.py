"""Unit tests for the 8 dashboard fixes (collectors.py + server.py + index.html).

Each test patches the module-level path constants with a tmp dir so no real
pipeline data is touched. Tests are unittest.TestCase so they run under both
`pytest` and `python -m unittest discover`.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.dashboard import collectors

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDEX_HTML = PROJECT_ROOT / "src" / "dashboard" / "static" / "index.html"


def _write_klines(cat_dir: Path, symbol: str, ts: datetime) -> Path:
    """Write a single-symbol 1m kline parquet with one open_time row."""
    cat_dir.mkdir(parents=True, exist_ok=True)
    path = cat_dir / f"{symbol}_1m.parquet"
    pl.DataFrame({"open_time": [ts]}).write_parquet(path)
    return path


def _set_mtime(path: Path, ts: datetime) -> None:
    os.utime(path, (ts.timestamp(), ts.timestamp()))


class TestKlinesTzParsing(unittest.TestCase):
    """Fix 1: naive open_time must not crash lag calc (status LIVE, not UNKNOWN)."""

    def _make_recent_klines(self, tmp: Path) -> Path:
        recent = datetime.now(timezone.utc) - timedelta(minutes=5)
        return _write_klines(tmp / "linear", "BTCUSDT", recent.replace(tzinfo=None))

    def test_get_data_status_lag_not_none(self):
        with patch("src.dashboard.collectors.RAW_KLINES_DIR", self._tmp):
            d = collectors.get_data_status()
        lin = d["klines"]["linear"]
        self.assertIsNotNone(lin["lag_min"])
        self.assertEqual(lin["status"], "LIVE")

    def test_get_candle_collector_lag_not_none(self):
        with patch("src.dashboard.collectors.RAW_KLINES_DIR", self._tmp):
            d = collectors.get_candle_collector()
        lin = d["categories"]["linear"]
        self.assertIsNotNone(lin["lag_min"])
        self.assertEqual(lin["status"], "LIVE")

    def setUp(self):
        import tempfile
        collectors._cache.clear()  # _cached() keys by name; stale dir contents leak across tests
        self._tmp = Path(tempfile.mkdtemp())
        self._make_recent_klines(self._tmp)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)


class TestKlinesNewestSampling(unittest.TestCase):
    """Fix 2: newest must come from mtime-sampled files, not alphabetical last."""

    def setUp(self):
        import tempfile
        collectors._cache.clear()
        self._tmp = Path(tempfile.mkdtemp())
        now = datetime.now(timezone.utc)
        # Alphabetically last file is OLD (the bug: files[-1] picked it).
        old = _write_klines(self._tmp / "spot", "ZZZUSDT", (now - timedelta(days=240)).replace(tzinfo=None))
        _set_mtime(old, now - timedelta(days=240))
        # Alphabetically first file is NEW and recently written.
        new = _write_klines(self._tmp / "spot", "AAAUSDT", (now - timedelta(minutes=5)).replace(tzinfo=None))
        _set_mtime(new, now - timedelta(minutes=5))

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_newest_is_recent_file_not_alphabetical_last(self):
        with patch("src.dashboard.collectors.RAW_KLINES_DIR", self._tmp):
            d = collectors.get_data_status()
        spot = d["klines"]["spot"]
        self.assertIn("2026", spot["newest"])
        self.assertLess(spot["lag_min"], 60)
        self.assertEqual(spot["status"], "LIVE")

    def test_candle_collector_newest_is_recent_file(self):
        with patch("src.dashboard.collectors.RAW_KLINES_DIR", self._tmp):
            d = collectors.get_candle_collector()
        spot = d["categories"]["spot"]
        self.assertIn("2026", spot["newest"])
        self.assertLess(spot["lag_min"], 60)
        self.assertEqual(spot["status"], "LIVE")


class TestMarketDataRecords(unittest.TestCase):
    """Fix 3: records must be non-zero (columns=[] returned 0-height frames)."""

    def setUp(self):
        import tempfile
        collectors._cache.clear()
        self._tmp = Path(tempfile.mkdtemp())
        stream_dir = self._tmp / "trades" / "linear"
        stream_dir.mkdir(parents=True)
        pl.DataFrame({"price": [1.0, 2.0, 3.0], "qty": [0.1, 0.2, 0.3]}).write_parquet(stream_dir / "BTCUSDT.parquet")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_records_nonzero(self):
        with patch("src.dashboard.collectors.MARKET_DATA_DIR", self._tmp):
            d = collectors.get_market_data()
        self.assertEqual(d["streams"]["trades"]["records"], 3)


class TestSystemStatusLogs(unittest.TestCase):
    """Fix 4: system status must watch research_timer.log, not pipeline.log."""

    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        log = self._tmp / "research_timer.log"
        log.write_text("Gated run SKIP (cooldown until 2099-01-01 00:00 UTC)\n")
        _set_mtime(log, datetime.now(timezone.utc))

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_research_log_tracked_not_pipeline(self):
        with patch("src.dashboard.collectors.LOGS_DIR", self._tmp), \
             patch("src.dashboard.collectors.ROOT", self._tmp):
            d = collectors.get_system_status()
        self.assertIn("research", d["logs"])
        self.assertNotIn("pipeline", d["logs"])
        self.assertFalse(d["logs"]["research"]["stale"])


class TestResearchRunId(unittest.TestCase):
    """Fix 5: run_id falls back to filename stem when research_*.json lacks it."""

    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        results = self._tmp / "results"
        results.mkdir(parents=True)
        (results / "research_20260911T120000Z.json").write_text(json.dumps({
            "created_at": "2026-09-11T12:00:00Z",
            "n_hypotheses": 3,
            "candidates": [],
            "verdict": "NO_CANDIDATE",
        }))

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_run_id_from_filename_stem(self):
        with patch("src.dashboard.collectors.RESULTS_DIR", self._tmp / "results"):
            d = collectors.get_hypotheses()
        self.assertEqual(d["research_runs"][0]["run_id"], "20260911T120000Z")


class TestResearchGate(unittest.TestCase):
    """Fix 6: /api/research-gate status from last_run.json + research_timer.log."""

    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        self.research = self._tmp / "research"
        self.logs = self._tmp / "logs"
        self.research.mkdir()
        self.logs.mkdir()

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _write_last_run(self, verdict="REJECT"):
        (self.research / "last_run.json").write_text(json.dumps({
            "run_id": "20260911T120000Z",
            "verdict": verdict,
            "finished_at_utc": "2026-09-11T12:00:00Z",
        }))

    def _write_skip_log(self, cooldown: str):
        (self.logs / "research_timer.log").write_text(
            f"Gated run SKIP (cooldown until {cooldown} UTC; new data span too small: 472 min)\n")

    def test_skip_with_future_cooldown(self):
        future = (datetime.now(timezone.utc) + timedelta(hours=12)).strftime("%Y-%m-%d %H:%M")
        self._write_last_run()
        self._write_skip_log(future)
        with patch("src.dashboard.collectors.RESEARCH_DIR", self.research), \
             patch("src.dashboard.collectors.LOGS_DIR", self.logs):
            d = collectors.get_research_gate()
        self.assertEqual(d["status"], "SKIP")
        self.assertIn("cooldown until", d["reason"])
        self.assertEqual(d["next_run_utc"], future + " UTC")
        self.assertEqual(d["last_run_verdict"], "REJECT")

    def test_ready_when_cooldown_expired(self):
        past = (datetime.now(timezone.utc) - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M")
        self._write_last_run()
        self._write_skip_log(past)
        with patch("src.dashboard.collectors.RESEARCH_DIR", self.research), \
             patch("src.dashboard.collectors.LOGS_DIR", self.logs):
            d = collectors.get_research_gate()
        self.assertEqual(d["status"], "READY")
        self.assertEqual(d["reason"], "Cooldown expired")
        self.assertIsNone(d["next_run_utc"])

    def test_ready_from_last_run_verdict(self):
        self._write_last_run()
        with patch("src.dashboard.collectors.RESEARCH_DIR", self.research), \
             patch("src.dashboard.collectors.LOGS_DIR", self.logs):
            d = collectors.get_research_gate()
        self.assertEqual(d["status"], "READY")
        self.assertEqual(d["reason"], "Last run verdict: REJECT")

    def test_ready_no_previous_runs(self):
        with patch("src.dashboard.collectors.RESEARCH_DIR", self.research), \
             patch("src.dashboard.collectors.LOGS_DIR", self.logs):
            d = collectors.get_research_gate()
        self.assertEqual(d["status"], "READY")
        self.assertEqual(d["reason"], "No previous runs")


class TestHypothesesDisplayStatus(unittest.TestCase):
    """Fix 7: CANDIDATE hypotheses tagged REJECTED_BY_PIPELINE after REJECT verdict."""

    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        self.hyp = self._tmp / "hypotheses"
        self.results = self._tmp / "results"
        self.hyp.mkdir()
        self.results.mkdir()
        (self.hyp / "hypotheses_v1.json").write_text(json.dumps([
            {"hypothesis_id": "H1", "status": "CANDIDATE"},
            {"hypothesis_id": "H2", "status": "CANDIDATE"},
            {"hypothesis_id": "H3", "status": "DRAFT"},
        ]))
        (self.results / "acceptance_20260911.json").write_text(json.dumps({
            "verdict": "REJECT",
            "candidates": [{"hypothesis_id": "H1"}],
        }))

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_candidates_tagged_rejected(self):
        with patch("src.dashboard.collectors.HYPOTHESES_DIR", self.hyp), \
             patch("src.dashboard.collectors.RESULTS_DIR", self.results):
            d = collectors.get_hypotheses()
        by_id = {h["hypothesis_id"]: h for h in d["hypotheses"]}
        self.assertEqual(by_id["H1"]["display_status"], "REJECTED_BY_PIPELINE")
        self.assertEqual(by_id["H2"]["display_status"], "REJECTED_BY_PIPELINE")
        self.assertEqual(by_id["H3"]["display_status"], "DRAFT")


class TestOrderbookNaming(unittest.TestCase):
    """Fix 8: HTML headers disambiguate reconstructed OB vs legacy streams."""

    def test_headers_renamed(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn("<h2>Order Book Collector (Reconstructed)</h2>", html)
        self.assertIn("<h2>Legacy Market Data Streams</h2>", html)
        self.assertIn("<h2>Event-driven OB Captures</h2>", html)


class TestResearchGateRoute(unittest.TestCase):
    """Fix 6: server.py exposes /api/research-gate."""

    def test_route_registered(self):
        from src.dashboard.server import app
        paths = {r.path for r in app.routes}
        self.assertIn("/api/research-gate", paths)


class TestObGapMetric(unittest.TestCase):
    """OB gaps metric: counts current per-symbol state, delisted symbols -> missing."""

    def _write_metrics(self, tmp: Path, lines: list[dict]) -> Path:
        path = tmp / "orderbook" / "reconstructed" / "_metrics.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(json.dumps(e) for e in lines) + "\n")
        return path

    def test_gap_counts_current_state_not_history(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            path = self._write_metrics(tmp, [
                {"symbol": "BTCUSDT", "is_valid": True, "updates_received": 100, "invalid_state_duration_secs": 0},
                {"symbol": "MYROUSDT", "is_valid": False, "updates_received": 0, "invalid_state_duration_secs": 300},
                {"symbol": "RIFUSDT", "is_valid": False, "updates_received": 0, "invalid_state_duration_secs": 300},
                {"symbol": "ETHUSDT", "is_valid": False, "updates_received": 50, "invalid_state_duration_secs": 120},
            ])
            with patch("src.dashboard.collectors.MARKET_DATA_DIR", tmp), \
                 patch("src.dashboard.collectors.RAW_KLINES_DIR", tmp / "klines"):
                d = collectors.get_data_quality()
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
        ob = d["orderbook"]
        self.assertEqual(ob["ok"], 1)        # BTCUSDT
        self.assertEqual(ob["gap"], 1)       # ETHUSDT (has updates, invalid state)
        self.assertEqual(ob["missing"], 2)   # MYROUSDT, RIFUSDT (never received data)
        self.assertEqual(ob["stale"], 3)     # MYROUSDT, RIFUSDT, ETHUSDT (invalid > 60s)

    def test_latest_entry_wins_per_symbol(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            self._write_metrics(tmp, [
                {"symbol": "BTCUSDT", "is_valid": False, "updates_received": 100, "invalid_state_duration_secs": 0},
                {"symbol": "BTCUSDT", "is_valid": True, "updates_received": 150, "invalid_state_duration_secs": 0},
            ])
            with patch("src.dashboard.collectors.MARKET_DATA_DIR", tmp), \
                 patch("src.dashboard.collectors.RAW_KLINES_DIR", tmp / "klines"):
                d = collectors.get_data_quality()
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
        self.assertEqual(d["orderbook"]["ok"], 1)
        self.assertEqual(d["orderbook"]["gap"], 0)


class TestStatusBlocks(unittest.TestCase):
    """Dashboard UX blocks: display-only System Status / Research Progress / Live Data."""

    def test_system_status_block_present(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn('<div class="grid g3" id="row-sys">', html)
        for hid in ("sys-data", "sys-research", "sys-paper"):
            self.assertIn(f'id="{hid}"', html)

    def test_research_progress_block_present(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        for hid in ("rp-flow", "rp-gate", "rp-state", "rp-reason"):
            self.assertIn(f'id="{hid}"', html)

    def test_live_data_block_present(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn('id="ld-rows"', html)

    def test_render_functions_registered_in_refresh_all(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn("renderSysStatus(), renderResearchProgress(), renderResearchCycle(), renderLiveData()", html)

    def test_research_cycle_block_present(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn('id="rc-content"', html)
        self.assertIn("renderResearchCycle()", html)

    def test_stage_lookup_uses_api_field_name(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn("d.stages.find(s => s.stage === stage)", html)


if __name__ == "__main__":
    unittest.main()