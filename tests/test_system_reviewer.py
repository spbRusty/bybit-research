"""Детерминированные тесты system_reviewer (без сети, без реального opencode).

Проверяют: интервал, lock от двойного запуска, таймаут/killpg, crash opencode,
формирование отчёта (структура), ntfy failure handling, изоляцию от pipeline,
повторный запуск после ошибки.
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Без реального запуска pipeline: system_reviewer не импортирует pipeline-модули.
import src.system_reviewer as sr


class TestLock(unittest.TestCase):
    def test_lock_excludes_second(self):
        with tempfile.TemporaryDirectory() as d:
            lock = sr.Lock(Path(d) / "x.lock")
            lock2 = sr.Lock(Path(d) / "x.lock")
            self.assertTrue(lock.acquire())
            self.assertFalse(lock2.acquire())   # второй не может взять
            lock.release()
            self.assertTrue(lock2.acquire())    # после release — можно
            lock2.release()

    def test_lock_autorelease_on_release(self):
        with tempfile.TemporaryDirectory() as d:
            lock = sr.Lock(Path(d) / "x.lock")
            self.assertTrue(lock.acquire())
            lock.release()
            self.assertTrue(sr.Lock(Path(d) / "x.lock").acquire())


class TestInterval(unittest.TestCase):
    def test_default(self):
        with patch.dict(os.environ, {}, clear=False):
            self.assertEqual(sr.getenv_int("REVIEW_INTERVAL", 21600), 21600)

    def test_override(self):
        with patch.dict(os.environ, {"REVIEW_INTERVAL": "3600"}):
            self.assertEqual(sr.getenv_int("REVIEW_INTERVAL", 21600), 3600)

    def test_bad_value_falls_back(self):
        with patch.dict(os.environ, {"REVIEW_INTERVAL": "abc"}):
            self.assertEqual(sr.getenv_int("REVIEW_INTERVAL", 21600), 21600)


class TestTimeout(unittest.TestCase):
    def test_nonzero_exit_is_crash(self):
        # Подмена bin: "true" завершается 0, "false" — 1. Это НЕ реальный opencode.
        ok, out = sr.run_opencode({"x": 1}, "false", "m", timeout=5)
        self.assertFalse(ok)
        self.assertIn("EXIT", out)

    def test_bin_not_found(self):
        ok, out = sr.run_opencode({"x": 1}, "/nonexistent/bin/opencode", "m", 5)
        self.assertFalse(ok)
        self.assertEqual(out, "OPENCODE_BIN_NOT_FOUND")


class TestExtractVerdict(unittest.TestCase):
    def test_extracts_json_from_noisy_output(self):
        text = ("some tool logs\nANSI \x1b[31mred\x1b[0m\n"
                'not json\n\n{"overall_status": "PASS", '
                '"summary": "ok", "findings": []}')
        v = sr.extract_verdict(text)
        self.assertIsNotNone(v)
        self.assertEqual(v["overall_status"], "PASS")

    def test_no_verdict(self):
        self.assertIsNone(sr.extract_verdict("no json here"))
        self.assertIsNone(sr.extract_verdict(""))

    def test_verdict_with_ansi_codes(self):
        """ANSI-коды не мешают парсингу JSON-вердикта."""
        text = ("\x1b[93m\x1b[1m! \x1b[0m some log\n\x1b[0m"
                '{"overall_status": "PASS", "summary": "ok", "findings": []}')
        v = sr.extract_verdict(text)
        self.assertIsNotNone(v)
        self.assertEqual(v["overall_status"], "PASS")


class TestAgentGuard(unittest.TestCase):
    def test_fallback_to_default_is_rejected(self):
        """Если агент не primary — opencode фолбэчит на default (без AUDIT
        ограничений). Такой вывод обязан давать FAIL, а не тихий аудит."""
        with tempfile.TemporaryDirectory() as d:
            fake = Path(d) / "fake-bin"
            fake.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' 'agent \"system-reviewer\" is a subagent, "
                "not a primary agent. Falling back to default agent'\n"
                "exit 0\n")
            fake.chmod(0o755)
            ok, out = sr.run_opencode({"x": 1}, str(fake), "m", timeout=5)
            self.assertFalse(ok)
            self.assertEqual(out, "AGENT_NOT_PRIMARY_OR_MISSING")


class TestReport(unittest.TestCase):
    def _ctx(self):
        return {
            "generated_at": "2026-09-09T00:00:00+00:00",
            "git_head": "abc1234",
            "host": "test",
            "services": [
                {"unit": "u1", "scope": "--user", "active": True},
                {"unit": "u2", "scope": "--system", "active": False},
            ],
            "data_growth": {"ob_reconstructed_parquet": 112,
                            "ob_reconstructed_newest": "2026-09-09.parquet",
                            "trades_parquet": 744,
                            "trades_newest": "X.parquet",
                            "klines_parquet": 1392, "klines_newest": "K.parquet",
                            "symbols_cnt": 30},
            "ob_quality": {"symbols_reported": 10, "total_rows_5h": 100,
                           "reconnects": 5, "update_id_jumps": 0,
                           "invalid_states": 0, "errors": 0,
                           "symbols_with_zero_updates": 0},
            "recent_research": [{"file": "a.json", "verdict": "REJECT",
                                 "n_hypotheses": 37, "n_events_total": 123,
                                 "candidates": []}],
            "registry": [{"id": "H001", "status": "CANDIDATE",
                          "updated_at": "t"}],
            "paper": {"balance": 1000.0, "realized_pnl": 0.0,
                      "trade_count": 0, "updated_at": "t"},
            "disk": {"fs": "/dev/x", "size": "100G", "used": "10G",
                     "avail": "90G", "use_pct": "10%"},
            "mem_mb": {"MemTotal": 65536},
            "swap": "0 total, 0 used",
            "procs": [],
            "dashboard_state": "200",
        }

    def test_report_structure(self):
        ctx = self._ctx()
        r = sr.render_report(ctx, {"overall_status": "FAIL",
                                   "findings": [{"level": "WARNING",
                                                 "category": "data",
                                                 "finding": "x",
                                                 "evidence": "y"}],
                                   "anomalies": ["a1"],
                                   "severity": "critical",
                                   "recommended_actions": ["r1"],
                                   "tests_executed": ["t1"]},
                             "run_1", "FAIL")
        for section in ("## 1. Collector", "## 2. Data quality",
                        "## 3. Research pipeline", "## 4. Hypotheses",
                        "## 5. Orderbook", "## 6. Shadow/Paper",
                        "## 7. Infrastructure", "## 8. Statistical",
                        "## 9. Detected anomalies", "## 10. Severity",
                        "## 11. Recommended actions", "## 12. Tests executed",
                        "## 13. Findings"):
            self.assertIn(section, r)
        self.assertIn("**FAIL**", r)
        self.assertIn("STOPPED", r)   # битый сервис виден
        self.assertIn("critical", r)

    def test_should_notify(self):
        self.assertTrue(sr.should_notify("FAIL", "critical", False))
        self.assertTrue(sr.should_notify("PASS_WITH_WARNINGS", "warning", False)
                        is False)
        self.assertTrue(sr.should_notify("PASS", "info", True))
        self.assertFalse(sr.should_notify("PASS", "info", False))


class TestIsolation(unittest.TestCase):
    def test_no_pipeline_imports(self):
        """system_reviewer не импортирует pipeline-модули (изоляция)."""
        src = Path(sr.__file__).read_text()
        imports = [ln for ln in src.splitlines()
                   if ln.startswith(("import ", "from "))]
        joined = "\n".join(imports)
        for blocked in ("src.orchestrator", "src.paper", "src.pipeline",
                        "import collector", "src.dashboard"):
            self.assertNotIn(blocked, joined)


class TestNotifyFailure(unittest.TestCase):
    def test_notify_off_does_not_crash(self):
        with patch.dict(os.environ, {"NTFY_TOPIC": ""}):
            with patch("src.system_reviewer.notify", return_value=False) as m:
                r = sr.notify_review("FAIL", {"git_head": "h"}, "s", "critical")
                self.assertFalse(r)
                m.assert_called_once()


class TestRunReviewCrashHandling(unittest.TestCase):
    def test_run_review_with_failing_bin(self):
        """Падение opencode → FAIL-отчёт создаётся, ntfy вызывается."""
        with tempfile.TemporaryDirectory() as d:
            with patch.object(sr, "REPORTS_DIR", Path(d)), \
                 patch.object(sr, "RUNS_DIR", Path(d)), \
                 patch.object(sr, "gather_context") as gc, \
                 patch.object(sr, "notify_review") as nr:
                gc.return_value = {
                    "generated_at": "x", "git_head": "h", "host": "t",
                    "services": [], "data_growth": {}, "ob_quality": None,
                    "disk": {}, "mem_mb": {}, "swap": "", "procs": [],
                    "recent_research": [], "registry": [], "paper": None,
                    "dashboard_state": "000",
                }
                res = sr.run_review("/bin/false", "m", 5, False)
                self.assertEqual(res["status"], "FAIL")
                reports = list(Path(d).glob("system_review_*.md"))
                self.assertEqual(len(reports), 1)
                self.assertIn("**FAIL**", reports[0].read_text())
                nr.assert_called_once()

    def test_retry_after_failure(self):
        """lock освобождается после упавшего цикла — повторный запуск проходит."""
        with tempfile.TemporaryDirectory() as d:
            lock = sr.Lock(Path(d) / "l.lock")
            self.assertTrue(lock.acquire())
            lock.release()
            self.assertTrue(sr.Lock(Path(d) / "l.lock").acquire())


if __name__ == "__main__":
    unittest.main()