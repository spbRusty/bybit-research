"""Tests for OOS gate fix: EV>0 + t>=2.0 required."""
import sys
sys.path.insert(0, "/home/vlad/Документы/построение")
import unittest
from unittest.mock import patch
import numpy as np
import polars as pl

from src.critic import review, CriticVerdict
from config.settings import load_toml

_R = load_toml("research.toml")


def make_result(oos_mean_net, oos_t_stat, oos_n=1000, oos_n_symbols=10):
    return {
        "created_at": "2026-01-01T00:00:00",
        "q_bh": 0.05,
        "cost_survival": 0.002,
        "n_hypotheses": 1,
        "n_events_total": 100000,
        "n_events": {"discovery": 50000, "validation": 30000, "oos": 20000},
        "discovery_results": [{
            "hypothesis_id": "TEST_HYP",
            "n": 5000,
            "n_symbols": oos_n_symbols,
            "n_months": 6,
            "t_stat": 5.0,
            "mean_net": 0.005,
            "p_value": 0.0001,
        }],
        "candidates": ["TEST_HYP"],
        "validation": {
            "TEST_HYP": {"n": 3000, "mean_net": 0.003, "t_stat": 3.0, "winrate": 0.55}
        },
        "oos": {
            "TEST_HYP": {
                "n": oos_n,
                "mean_net": oos_mean_net,
                "t_stat": oos_t_stat,
                "winrate": 0.52,
            }
        },
        "finalist": {
            "hypothesis_id": "TEST_HYP",
            "horizon_min": 30,
            "entry_side": "long",
            "condition": "pl.col('x') > 0",
            "description": "test",
        },
    }


class TestOOSGate(unittest.TestCase):

    def test_oos_ev_positive_t_low_reject(self):
        """EV>0 but t=0.06 → OOS FAIL (noise, not signal)."""
        result = make_result(oos_mean_net=0.00003, oos_t_stat=0.06)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertFalse(oos_check[1])
        self.assertIn("НЕ ПРОШЁЛ", oos_check[2])

    def test_oos_ev_positive_t_zero_reject(self):
        """EV>0 but t=0.0 → OOS FAIL."""
        result = make_result(oos_mean_net=0.001, oos_t_stat=0.0)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertFalse(oos_check[1])

    def test_oos_ev_positive_t_negative_reject(self):
        """EV>0 but t=-1.0 → OOS FAIL."""
        result = make_result(oos_mean_net=0.0001, oos_t_stat=-1.0)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertFalse(oos_check[1])

    def test_oos_ev_positive_t_pass(self):
        """EV>0 AND t>=2.0 → OOS PASS."""
        result = make_result(oos_mean_net=0.005, oos_t_stat=2.5)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertTrue(oos_check[1])
        self.assertIn("прошёл", oos_check[2])

    def test_oos_ev_positive_t_exactly_2_pass(self):
        """EV>0 AND t=2.0 exactly → OOS PASS."""
        result = make_result(oos_mean_net=0.003, oos_t_stat=2.0)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertTrue(oos_check[1])

    def test_oos_ev_negative_t_high_reject(self):
        """EV<0 even with high t → OOS FAIL."""
        result = make_result(oos_mean_net=-0.001, oos_t_stat=5.0)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertFalse(oos_check[1])

    def test_oos_ev_zero_reject(self):
        """EV=0 exactly → OOS FAIL."""
        result = make_result(oos_mean_net=0.0, oos_t_stat=3.0)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertFalse(oos_check[1])

    def test_oos_low_n_reject(self):
        """EV>0, t>=2.0, but n < min_events → OOS FAIL."""
        result = make_result(oos_mean_net=0.01, oos_t_stat=3.0, oos_n=50)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertFalse(oos_check[1])

    def test_mr_scenario_reject(self):
        """MR pipeline scenario: EV=+0.00003, t=0.06, n=10685 → OOS FAIL."""
        result = make_result(oos_mean_net=0.00003, oos_t_stat=0.06, oos_n=10685)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertFalse(oos_check[1])
        self.assertIn("НЕ ПРОШЁЛ", oos_check[2])
        overall_pass = all(ok is True for _, ok, _ in v.results)
        self.assertFalse(overall_pass)

    def test_no_candidate_oos_skip(self):
        """No candidate → OOS check is PASS (not required)."""
        result = make_result(oos_mean_net=0.001, oos_t_stat=3.0)
        result["candidates"] = []
        result.pop("finalist", None)
        v = review(result)
        oos_check = next(r for r in v.results if r[0] == "oos")
        self.assertTrue(oos_check[1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
