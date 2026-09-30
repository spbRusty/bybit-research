"""Отчёт по итогам цикла: дедупликация RUNNING/DONE, перевод, пустой цикл."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import notify

CYCLE_ID = "9be56a1fe207:2026-09-30T03:16:00"


def _exp(exp_id: str, status: str, **over) -> dict:
    row = {"cycle_id": CYCLE_ID, "experiment_id": exp_id, "status": status,
           "mode": "REGIME_SWEEP", "final_verdict": None, "diagnosis": None}
    row.update(over)
    return row


class CycleReport(unittest.TestCase):
    def _run(self, experiments, state=None):
        with patch.object(notify, "notify") as mock:
            notify.notify_cycle_report(experiments, state or {})
        args, kwargs = mock.call_args
        return args[0], args[1], kwargs.get("tags", "")

    def test_dedups_running_and_done_pairs(self):
        raw = [_exp("E1", "RUNNING"), _exp("E1", "DONE", final_verdict="REJECT",
                                          diagnosis="no_signal")]
        title, message, tags = self._run(raw)
        self.assertEqual(title, "ИТОГ ЦИКЛА: ЭКСПЕРИМЕНТЫ")
        self.assertIn("Экспериментов завершено: 1", message)
        self.assertIn("ОТКЛОНЕНА 1", message)
        self.assertIn("нет сигнала 1", message)
        self.assertIn("Итог: новых кандидатов нет", message)
        self.assertEqual(tags, "bar_chart")

    def test_counts_each_experiment_once_regardless_of_order(self):
        raw = [_exp("E1", "DONE", final_verdict="REJECT", diagnosis="no_signal"),
               _exp("E1", "RUNNING"), _exp("E2", "RUNNING"),
               _exp("E2", "DONE", final_verdict="ACCEPT", diagnosis="no_edge")]
        _, message, tags = self._run(raw)
        self.assertIn("Экспериментов завершено: 2", message)
        self.assertIn("ПРИНЯТА 1", message)
        self.assertIn("ОТКЛОНЕНА 1", message)
        self.assertIn("Итог: есть принятые кандидаты", message)
        self.assertEqual(tags, "tada")

    def test_empty_cycle_reports_no_experiments(self):
        title, message, _ = self._run([_exp("E1", "RUNNING")])
        self.assertEqual(message, "Завершённых экспериментов нет")

    def test_state_budget_and_tests_included(self):
        raw = [_exp("E1", "DONE", final_verdict="REJECT", diagnosis="cost_sensitive")]
        _, message, _ = self._run(raw, {"cycle_id": CYCLE_ID, "budget_used": 12,
                                        "n_tests_cumulative": 480})
        self.assertIn(f"Цикл: {CYCLE_ID}", message)
        self.assertIn("Экспериментов завершено: 1 (бюджет 12)", message)
        self.assertIn("нарастающим итогом: 480", message)
        self.assertIn("чувствительно к комиссиям 1", message)


if __name__ == "__main__":
    unittest.main()
