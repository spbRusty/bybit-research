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
    """Fix 4: system status must watch research_cycle.log, not pipeline.log."""

    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        log = self._tmp / "research_cycle.log"
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
        self.assertEqual(d["reason"], "Пауза истекла")
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
        self.assertEqual(d["reason"], "Предыдущих запусков нет")


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
        self.assertIn("<h2>Коллектор стакана (реконструкция)</h2>", html)
        self.assertIn("<h2>Захваты стакана по событиям</h2>", html)


class TestResearchGateRoute(unittest.TestCase):
    """Fix 6: server.py exposes /api/research-gate."""

    def test_route_registered(self):
        from src.dashboard.server import app
        paths = {r.path for r in app.routes}
        self.assertIn("/api/research-gate", paths)


class TestObGapMetric(unittest.TestCase):
    """OB gaps metric: counts current per-symbol state, delisted symbols -> missing."""

    def setUp(self):
        collectors._cache.clear()

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
    """Dashboard UX blocks: display-only System Status / Live Data."""

    def test_system_status_block_present(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn('<div class="grid g3" id="row-sys">', html)
        for hid in ("sys-data", "sys-research", "sys-paper"):
            self.assertIn(f'id="{hid}"', html)

    def test_live_data_block_present(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn('id="ld-rows"', html)

    def test_render_functions_registered_in_refresh_all(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn(
            "renderSysStatus(), renderResearchController(), renderLiveData()",
            html,
        )


class TestNewRoutes(unittest.TestCase):
    """Redesign: 5 new read-only endpoints are registered."""

    def test_routes_registered(self):
        from src.dashboard.server import app
        paths = {r.path for r in app.routes}
        for p in ("/api/overview", "/api/coverage", "/api/collectors",
                  "/api/equity", "/api/milestones"):
            self.assertIn(p, paths)


class _FakeProc:
    def __init__(self, stdout: str, returncode: int = 0):
        self.stdout = stdout
        self.stderr = ""
        self.returncode = returncode


class _FakeRoot:
    """Поддельный корень проекта: config/, results/, collector/logs/."""

    def __init__(self, tmp: Path):
        self.root = tmp / "root"
        (self.root / "config").mkdir(parents=True)
        (self.root / "config" / "l2_research.toml").write_text(
            '[periods]\n'
            'discovery = ["2026-09-06", "2026-11-12"]\n'
            'validation = ["2026-11-12", "2026-11-26"]\n'
            'oos = ["2026-11-26", "2026-12-10"]\n'
        )
        self.results = self.root / "results"
        self.results.mkdir()
        self.paper = self.root / "paper" / "portfolio"
        self.paper.mkdir(parents=True)
        self.shadow = self.root / "paper" / "shadow"
        self.shadow.mkdir(parents=True)
        self.trades = self.root / "paper" / "trades"
        self.trades.mkdir(parents=True)
        self.events = self.root / "events"
        self.events.mkdir()

    def write_screen(self, **fields):
        payload = {"verdict": "PENDING_DATA", "reason": "нет кандидатов",
                   "n_events": {"discovery": 100, "validation": 0, "oos": 0},
                   "n_bh_significant": 0, "n_hypotheses": 7,
                   "coverage_ok_share": 0.965}
        payload.update(fields)
        (self.results / "l2_screen_20260911T120000Z.json").write_text(json.dumps(payload))

    def write_log(self, rel: str, text: str, age_min: float = 0.0):
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        ts = datetime.now(timezone.utc) - timedelta(minutes=age_min)
        _set_mtime(path, ts)
        return path


class TestOverview(unittest.TestCase):
    """Redesign: /api/overview = сервисы + вердикт + приёмка + блокеры."""

    def setUp(self):
        collectors._cache.clear()
        self.tmp = Path(tempfile.mkdtemp())
        self.fr = _FakeRoot(self.tmp)
        self.fr.write_screen()
        self.fr.write_log("collector/logs/bybit_klines.log", "ok\n")
        self.fr.write_log("collector/logs/marketdata.log", "ok\n")
        self.fr.write_log("collector/logs/ob_reconstructor.log", "ok\n", age_min=120)
        self.fr.paper.joinpath("state.json").write_text(json.dumps(
            {"balance": 1000.0, "equity_start": 1000.0, "trade_count": 0,
             "realized_pnl": 0.0}))

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _overview(self):
        with patch("src.dashboard.collectors.ROOT", self.fr.root), \
             patch("src.dashboard.collectors.RESULTS_DIR", self.fr.results), \
             patch("src.dashboard.collectors.PAPER_PORTFOLIO", self.fr.paper), \
             patch("src.dashboard.collectors.PAPER_SHADOW", self.fr.shadow), \
             patch("src.dashboard.collectors._next_timer_run", lambda *a, **k: "18:36"):
            return collectors.get_overview()

    def test_shape_and_verdict(self):
        d = self._overview()
        for k in ("services", "research", "paper", "next_run", "alerts"):
            self.assertIn(k, d)
        self.assertEqual(d["research"]["verdict"], "PENDING_DATA")
        self.assertEqual(d["research"]["discovery"], ["2026-09-06", "2026-11-12"])
        self.assertEqual(d["next_run"], "18:36")

    def test_acceptance_is_oos_end_plus_day(self):
        d = self._overview()
        self.assertEqual(d["research"]["acceptance_date"], "2026-12-11")
        self.assertIsInstance(d["research"]["days_to_acceptance"], int)

    def test_stale_service_flagged_and_alert(self):
        d = self._overview()
        self.assertTrue(d["services"]["klines"]["active"])
        self.assertFalse(d["services"]["ob_reconstructor"]["active"])
        srcs = {a["source"] for a in d["alerts"]}
        self.assertIn("Стакан", srcs)

    def test_alerts_include_candidate_blocker(self):
        d = self._overview()
        msgs = " ".join(a["message"] for a in d["alerts"])
        self.assertIn("CANDIDATE", msgs)
        self.assertIn("2026-12-11", msgs)

    def test_candidate_verdict_has_no_candidate_blocker(self):
        self.fr.write_screen(verdict="CANDIDATE")
        d = self._overview()
        msgs = " ".join(a["message"] for a in d["alerts"])
        self.assertNotIn("нужен CANDIDATE", msgs)


class TestCoverageEndpoint(unittest.TestCase):
    """Redesign: /api/coverage считает строки только по метаданным parquet."""

    def setUp(self):
        collectors._cache.clear()
        self.tmp = Path(tempfile.mkdtemp())
        self.fr = _FakeRoot(self.tmp)
        self.fr.write_screen()
        d = self.fr.events / "l2_events"
        d.mkdir()
        pl.DataFrame({"a": list(range(3))}).write_parquet(d / "2026-11-26.parquet")
        pl.DataFrame({"a": list(range(5))}).write_parquet(d / "2026-11-27.parquet")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_rows_summed_from_metadata(self):
        with patch("src.dashboard.collectors.EVENTS_DIR", self.fr.events), \
             patch("src.dashboard.collectors.RESULTS_DIR", self.fr.results):
            d = collectors.get_coverage()
        self.assertEqual(d["dates_count"], 2)
        self.assertEqual(d["total_rows"], 8)
        self.assertEqual([x["date"] for x in d["dates"]],
                         ["2026-11-26", "2026-11-27"])
        self.assertIsInstance(d["total_size_mb"], float)
        self.assertIn("size_mb", d["dates"][0])
        self.assertEqual(d["coverage_ok_share"], 0.965)


class TestCollectorsEndpoint(unittest.TestCase):
    """Redesign: /api/collectors парсит хвост логов, а не читает их целиком."""

    def setUp(self):
        collectors._cache.clear()
        self.tmp = Path(tempfile.mkdtemp())
        self.fr = _FakeRoot(self.tmp)
        self.fr.write_log("collector/logs/ob_reconstructor.log",
                          "[stats] 100s: msgs=1000 snaps=10 active=740/744 jumps=2 "
                          "reconn=740 rows=312028\n"
                          "[stats] 200s: msgs=3000 snaps=20 active=740/744 jumps=2 "
                          "reconn=740 rows=312028\n"
                          "queue full: 1\n")
        self.fr.write_log("collector/logs/marketdata.log",
                          "[ws trades] подписан: 744 символов\n"
                          "[ws liquidation] подписан: 744 символов\n")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_parsed_from_log_tail(self):
        with patch("src.dashboard.collectors.ROOT", self.fr.root), \
             patch("src.dashboard.collectors.MARKET_DATA_DIR", self.tmp / "md"), \
             patch("src.dashboard.collectors.RAW_KLINES_DIR", self.tmp / "klines"):
            d = collectors.get_collectors()
        self.assertEqual(d["ob"]["active_symbols"], 740)
        self.assertEqual(d["ob"]["symbols_total"], 744)
        self.assertEqual(d["ob"]["rows"], 312028)
        self.assertEqual(d["ob"]["reconnects"], 740)
        self.assertEqual(d["ob"]["queue_full"], 1)
        self.assertEqual(d["ob"]["msgs_per_s"], 20.0)
        self.assertEqual(d["marketdata"]["trades_symbols"], 744)
        self.assertEqual(d["marketdata"]["liquidation_symbols"], 744)


class TestEquityEndpoint(unittest.TestCase):
    """Redesign: /api/equity = точки капитала, последние сделки, причина пустоты."""

    def setUp(self):
        collectors._cache.clear()
        self.tmp = Path(tempfile.mkdtemp())
        self.fr = _FakeRoot(self.tmp)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _equity(self):
        with patch("src.dashboard.collectors.PAPER_PORTFOLIO", self.fr.paper), \
             patch("src.dashboard.collectors.PAPER_SHADOW", self.fr.shadow), \
             patch("src.dashboard.collectors.PAPER_TRADES", self.fr.trades), \
             patch("src.dashboard.collectors.ROOT", self.fr.root):
            return collectors.get_equity()

    def test_empty_paper_explains_why(self):
        self.fr.paper.joinpath("state.json").write_text(json.dumps(
            {"balance": 1000.0, "equity_start": 1000.0, "started_at": "2026-09-01T00:00:00"}))
        d = self._equity()
        self.assertEqual(d["trades_count"], 0)
        self.assertEqual(len(d["points"]), 1)         # только старт капитала
        self.assertEqual(d["points"][0]["equity"], 1000.0)
        self.assertIn("CANDIDATE", d["empty_reason"])
        self.assertIn("2026-12-11", d["empty_reason"])

    def test_points_and_trades(self):
        self.fr.paper.joinpath("state.json").write_text(json.dumps(
            {"balance": 1010.0, "equity_start": 1000.0, "started_at": "2026-09-01T00:00:00"}))
        pl.DataFrame({
            "trade_id": ["t1"],
            "symbol": ["BTCUSDT"],
            "side": ["LONG"],
            "net_pnl": [10.0],
            "balance_after": [1010.0],
            "entry_timestamp": ["2026-09-02T00:00:00"],
            "exit_timestamp": ["2026-09-02T01:00:00"],
            "entry_fee": [0.5],
            "exit_fee": [0.5],
            "slippage_cost": [0.1],
            "reason": ["TP"],
        }).write_parquet(self.fr.trades / "t1.parquet")
        d = self._equity()
        self.assertEqual(d["trades_count"], 1)
        self.assertEqual(len(d["points"]), 2)
        self.assertEqual(d["points"][0]["equity"], 1000.0)
        self.assertEqual(d["points"][1]["equity"], 1010.0)
        self.assertEqual(d["trades"][0]["symbol"], "BTCUSDT")
        self.assertIsNone(d["empty_reason"])


class TestMilestonesEndpoint(unittest.TestCase):
    """Redesign: /api/milestones = воронка окон + дата приёмки + блокеры."""

    def setUp(self):
        collectors._cache.clear()
        self.tmp = Path(tempfile.mkdtemp())
        self.fr = _FakeRoot(self.tmp)
        self.fr.write_screen()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _milestones(self):
        with patch("src.dashboard.collectors.ROOT", self.fr.root), \
             patch("src.dashboard.collectors.RESULTS_DIR", self.fr.results):
            return collectors.get_milestones()

    def test_stages_and_acceptance(self):
        d = self._milestones()
        self.assertEqual([s["key"] for s in d["stages"]],
                         ["discovery", "validation", "oos"])
        self.assertEqual(d["stages"][0]["start"], "2026-09-06")
        self.assertEqual(d["acceptance_date"], "2026-12-11")
        self.assertTrue(d["blockers"])

    def test_blockers_mention_candidate_and_empty_windows(self):
        d = self._milestones()
        joined = " ".join(d["blockers"])
        self.assertIn("CANDIDATE", joined)
        self.assertIn("validation", joined)
        self.assertIn("OOS", joined)


class TestNextTimerRun(unittest.TestCase):
    """Таймер читается из systemd и кешируется на 60с (часто — дёшево)."""

    def setUp(self):
        collectors._cache.clear()

    def test_parses_next_time(self):
        out = ("NEXT LEFT LAST PASSED UNIT ACTIVATES\n"
               "Wed 2026-10-21 18:36:00 UTC 3min Mon 2026-10-21 18:30:00 UTC 5min "
               "l2_search.timer l2_search.service\n")
        with patch("src.dashboard.collectors.subprocess.run",
                   return_value=_FakeProc(out)) as run:
            self.assertEqual(collectors._next_timer_run(), "18:36")
            self.assertEqual(collectors._next_timer_run(), "18:36")
        self.assertEqual(run.call_count, 1)   # кеш 60с

    def test_inactive_timer_returns_none(self):
        out = ("-         Mon 2026-01-01 00:00:00 UTC  -  Wed 2026-01-01 00:00:00 UTC  -  "
               "l2_search.timer  l2_search.service\n")
        with patch("src.dashboard.collectors.subprocess.run",
                   return_value=_FakeProc(out)):
            self.assertIsNone(collectors._next_timer_run())


class TestRedesignHtml(unittest.TestCase):
    """Redesign: обзорный блок, воронка и сводка-справочник в index.html."""

    def setUp(self):
        self.html = INDEX_HTML.read_text(encoding="utf-8")

    def test_overview_bar_present(self):
        self.assertIn('id="row-overview"', self.html)
        for hid in ("ov-verdict", "ov-days", "ov-coverage", "ov-collectors", "ov-paper"):
            self.assertIn(f'id="{hid}"', self.html)

    def test_funnel_coverage_collectors_present(self):
        for hid in ("funnel", "funnel-blocker", "coverage-table", "collectors-row"):
            self.assertIn(f'id="{hid}"', self.html)

    def test_reference_blocks_moved_into_details(self):
        self.assertIn('id="ref-details"', self.html)
        for hid in ("t-risk", "t-sizing", "t-costs", "t-stakes"):
            self.assertIn(f'id="{hid}"', self.html)

    def test_new_endpoints_called(self):
        for ep in ("/overview", "/coverage", "/collectors", "/equity", "/milestones"):
            self.assertIn(f"fetchJSON('{ep}')", self.html)


if __name__ == "__main__":
    unittest.main()