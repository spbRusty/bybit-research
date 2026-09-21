"""Data-ready gate tests: Lock, check_ready, last_run.json, idempotency."""
from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import data_ready
from src.data_ready import Lock, check_ready, config_hash, load_last_run, save_last_run


def _state(finished_iso: str, klines_max: str, cfg_hash: str | None = None):
    return {
        "finished_at_utc": finished_iso,
        "klines_max": klines_max,
        "config_hash": cfg_hash if cfg_hash is not None else config_hash(),
        "git_head": "test",
    }


class TestLock(unittest.TestCase):
    def test_second_acquire_fails(self):
        p = Path(tempfile.mkdtemp()) / "t.lock"
        l1, l2 = Lock(p), Lock(p)
        self.assertTrue(l1.acquire())
        self.assertFalse(l2.acquire())
        l1.release()
        self.assertTrue(l2.acquire())
        l2.release()

    def test_lock_auto_releases_on_process_exit(self):
        # flock освобождается ядром при СМЕРТИ ПРОЦЕССА, не при GC объекта
        import subprocess
        import sys as _sys
        p = Path(tempfile.mkdtemp()) / "t.lock"
        code = (f"import sys; sys.path.insert(0, {str(Path(__file__).resolve().parent.parent)!r}); "
                f"from pathlib import Path; from src.data_ready import Lock; "
                f"assert Lock(Path({str(p)!r})).acquire(); print('acquired')")
        r = subprocess.run([_sys.executable, "-c", code],
                           capture_output=True, text=True, timeout=30)
        self.assertEqual(r.returncode, 0, r.stderr)
        # после смерти дочернего процесса лок свободен
        l2 = Lock(p)
        self.assertTrue(l2.acquire())
        l2.release()


class TestLastRunState(unittest.TestCase):
    def test_save_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            old = data_ready.STATE_PATH
            data_ready.STATE_PATH = Path(td) / "last_run.json"
            try:
                save_last_run({"run_id": "r1", "klines_max": "2026-09-10 00:00:00"})
                self.assertEqual(load_last_run()["run_id"], "r1")
                self.assertFalse((Path(td) / "last_run.json.tmp").exists(),
                                 "tmp-файл не должен оставаться после атомарного replace")
            finally:
                data_ready.STATE_PATH = old

    def test_no_state_is_ready(self):
        # Первый запуск: нет last_run.json -> ready (не блокируем инициализацию)
        with tempfile.TemporaryDirectory() as td:
            old = data_ready.STATE_PATH
            data_ready.STATE_PATH = Path(td) / "last_run.json"
            try:
                ok, reasons, _ = check_ready()
                self.assertTrue(ok)
                self.assertEqual(reasons, [])
            finally:
                data_ready.STATE_PATH = old


class TestCooldown(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old = data_ready.STATE_PATH
        data_ready.STATE_PATH = Path(self._tmp.name) / "last_run.json"
        data_ready.STATE_PATH.write_text(
            __import__("json").dumps(_state(
                datetime.now(timezone.utc).isoformat(),
                "2026-09-10 00:00:00",
            )))

    def tearDown(self):
        data_ready.STATE_PATH = self._old
        self._tmp.cleanup()

    def test_fresh_run_blocked_by_cooldown(self):
        ok, reasons, _ = check_ready()
        self.assertFalse(ok)
        self.assertTrue(any("cooldown" in r for r in reasons))

    def test_config_change_triggers_rerun(self):
        # config_hash отличается -> ран легитимен даже в cooldown
        data_ready.STATE_PATH.write_text(
            __import__("json").dumps(_state(
                datetime.now(timezone.utc).isoformat(),
                "2026-09-10 00:00:00",
                cfg_hash="stale-hash",
            )))
        ok, _, _ = check_ready()
        self.assertTrue(ok)


class TestFrozenBoundary(unittest.TestCase):
    """Frozen boundary: snapshot действует только внутри research cycle."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._td = Path(self._tmp.name)
        self._old_dir = data_ready.RESEARCH_DIR
        self._old_state = data_ready.STATE_PATH
        data_ready.RESEARCH_DIR = self._td
        data_ready.STATE_PATH = self._td / "last_run.json"

    def tearDown(self):
        data_ready.RESEARCH_DIR = self._old_dir
        data_ready.STATE_PATH = self._old_state
        self._tmp.cleanup()

    def _snapshot(self, klines_max: str = "2026-09-20T19:55:00",
                  frozen_at: str | None = None):
        return {
            "klines_max": klines_max,
            "config_hash": "9be56a1fe207",
            "git_head": "test",
            "data_version": "1.0",
            "n_files": 10,
            "fresh_symbols": 93,
            "ob_valid_symbols": 744,
            "ob_unique_symbols": 100,
            "ob_rows": 5000,
            "frozen_at_utc": frozen_at or datetime.now(timezone.utc).isoformat(),
        }

    def test_frozen_makes_ready_with_stable_cycle_id(self):
        data_ready.save_frozen_boundary(self._snapshot())
        ok, reasons, metrics = check_ready()
        self.assertTrue(ok)
        self.assertEqual(reasons, [])
        self.assertEqual(metrics["klines"]["klines_max"].isoformat(),
                         "2026-09-20T19:55:00")
        self.assertEqual(metrics["ob"]["ob_valid_symbols"], 744)
        # klines_max приходит из снапшота, не из живого скана
        self.assertEqual(metrics["config_hash"], "9be56a1fe207")

    def test_clear_frozen_restores_full_gate(self):
        data_ready.save_frozen_boundary(self._snapshot())
        self.assertTrue(data_ready.frozen_boundary_path().exists())
        data_ready.clear_frozen_boundary()
        self.assertFalse(data_ready.frozen_boundary_path().exists())
        # после очистки — полный gate: cooldown снова блокирует
        data_ready.STATE_PATH.write_text(__import__("json").dumps(_state(
            datetime.now(timezone.utc).isoformat(),
            "2026-09-10 00:00:00")))
        with patch("src.data_ready.scan_klines",
                   return_value={"klines_max": None, "n_files": 0}), \
             patch("src.data_ready.scan_ob",
                   return_value={"ob_valid_symbols": 0}):
            ok, reasons, _ = check_ready()
        self.assertFalse(ok)
        self.assertTrue(any("cooldown" in r for r in reasons))

    def test_stale_frozen_ignored(self):
        stale = (datetime.now(timezone.utc) - data_ready.FROZEN_TTL
                 - timedelta(hours=1)).isoformat()
        snap = self._snapshot(frozen_at=stale)
        # save_frozen_boundary перезаписывает frozen_at_utc; stale пишем напрямую
        data_ready.frozen_boundary_path().write_text(
            __import__("json").dumps(snap))
        self.assertIsNone(data_ready.load_frozen_boundary())
        # gate снова обычный (не frozen) — cooldown блокирует, а не снапшот
        data_ready.STATE_PATH.write_text(__import__("json").dumps(_state(
            datetime.now(timezone.utc).isoformat(),
            "2026-09-10 00:00:00")))
        with patch("src.data_ready.scan_klines",
                   return_value={"klines_max": None, "n_files": 0}), \
             patch("src.data_ready.scan_ob",
                   return_value={"ob_valid_symbols": 0}):
            ok, reasons, _ = check_ready()
        self.assertFalse(ok)
        self.assertTrue(any("cooldown" in r for r in reasons))

    def test_no_frozen_unchanged(self):
        self.assertIsNone(data_ready.load_frozen_boundary())
        self.assertFalse(data_ready.frozen_boundary_path().exists())


if __name__ == "__main__":
    unittest.main()