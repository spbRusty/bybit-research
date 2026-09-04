"""Comprehensive critic gate tests — audit of all 9 checks."""
import sys
sys.path.insert(0, "/home/vlad/Документы/построение")
import unittest
import numpy as np
import polars as pl

from src.critic import review, CriticVerdict, CHECKS
from config.settings import load_toml

_R = load_toml("research.toml")


def make_result(
    n_events_total=100000,
    n_hyp=1,
    disc_n_symbols=10,
    disc_t_stat=5.0,
    disc_mean_net=0.005,
    has_finalist=True,
    val_mean_net=0.003,
    val_t_stat=3.0,
    val_n=3000,
    oos_mean_net=0.002,
    oos_t_stat=2.5,
    oos_n=2000,
):
    """Build a research result dict for testing.

    When has_finalist=True, temporal_stability/concentration need real events.
    Without events they return UNKNOWN (not True), so v.passed=False.
    Gate-level tests use events=None and check individual gates, NOT v.passed.
    When has_finalist=False, all conditional gates return True.
    """
    r = {
        "created_at": "2026-01-01T00:00:00",
        "q_bh": 0.05,
        "cost_survival": 0.002,
        "n_hypotheses": n_hyp,
        "n_events_total": n_events_total,
        "n_events": {"discovery": 50000, "validation": 30000, "oos": 20000},
        "discovery_results": [{
            "hypothesis_id": "H1",
            "n": 5000,
            "n_symbols": disc_n_symbols,
            "n_months": 6,
            "t_stat": disc_t_stat,
            "mean_net": disc_mean_net,
            "p_value": 0.001,
        }],
        "candidates": ["H1"] if has_finalist else [],
        "validation": {
            "H1": {"n": val_n, "mean_net": val_mean_net, "t_stat": val_t_stat, "winrate": 0.55}
        },
        "oos": {
            "H1": {"n": oos_n, "mean_net": oos_mean_net, "t_stat": oos_t_stat, "winrate": 0.52}
        },
    }
    if has_finalist:
        r["finalist"] = {
            "hypothesis_id": "H1",
            "horizon_min": 30,
            "entry_side": "long",
            "condition": "pl.col('x') > 0",
            "description": "test",
        }
    return r


def get_check(v, name):
    """Get (name, ok, detail) for a check by name."""
    return next(r for r in v.results if r[0] == name)


# ---------------------------------------------------------------------------
# Determinism & verdict
# ---------------------------------------------------------------------------
class TestCriticDeterminism(unittest.TestCase):

    def test_all_checks_listed(self):
        v = review(make_result())
        check_names = [n for n, _, _ in v.results]
        for c in CHECKS:
            self.assertIn(c, check_names)

    def test_pass_no_finalist(self):
        """No finalist -> all conditional gates True, v.passed=True."""
        r = make_result(has_finalist=False)
        v = review(r)
        self.assertTrue(v.passed)

    def test_fail_on_any_false(self):
        """With finalist but events=None, temporal_stability=UNKNOWN -> not passed."""
        r = make_result(oos_t_stat=0.06, oos_mean_net=0.00003)
        v = review(r, events=None)
        self.assertFalse(v.passed)

    def test_unknown_is_not_pass(self):
        v = CriticVerdict()
        v.add("test", None, "unknown")
        self.assertFalse(v.passed)

    def test_empty_is_not_pass(self):
        v = CriticVerdict()
        self.assertFalse(v.passed)

    def test_verdict_dict_pass(self):
        r = make_result(has_finalist=False)
        v = review(r)
        d = v.to_dict()
        self.assertEqual(d["verdict"], "PASS")

    def test_verdict_dict_reject(self):
        """Finalist + events=None → temporal_stability=UNKNOWN, oos also fails."""
        r = make_result(oos_t_stat=0.06, oos_mean_net=0.00003)
        v = review(r, events=None)
        d = v.to_dict()
        self.assertEqual(d["verdict"], "REJECT")
        # fail_reason is temporal_stability (first UNKNOWN), but oos also fails
        self.assertFalse(get_check(v, "oos")[1])


# ---------------------------------------------------------------------------
# OOS gate: EV > 0 AND n >= 100 AND t >= 2.0
# ---------------------------------------------------------------------------
class TestOOSGate(unittest.TestCase):

    def test_ev_pos_t_low_reject(self):
        r = make_result(oos_mean_net=0.00003, oos_t_stat=0.06)
        ok, detail = get_check(review(r, events=None), "oos")[1:]
        self.assertFalse(ok)
        self.assertIn("НЕ ПРОШЁЛ", detail)

    def test_ev_pos_t_zero_reject(self):
        r = make_result(oos_mean_net=0.001, oos_t_stat=0.0)
        self.assertFalse(get_check(review(r, events=None), "oos")[1])

    def test_ev_pos_t_negative_reject(self):
        r = make_result(oos_mean_net=0.0001, oos_t_stat=-1.0)
        self.assertFalse(get_check(review(r, events=None), "oos")[1])

    def test_ev_pos_t_pass(self):
        r = make_result(oos_mean_net=0.005, oos_t_stat=2.5)
        self.assertTrue(get_check(review(r, events=None), "oos")[1])

    def test_ev_pos_t_exactly_2_pass(self):
        r = make_result(oos_mean_net=0.003, oos_t_stat=2.0)
        self.assertTrue(get_check(review(r, events=None), "oos")[1])

    def test_ev_pos_n_exactly_100_pass(self):
        r = make_result(oos_mean_net=0.003, oos_t_stat=2.5, oos_n=100)
        self.assertTrue(get_check(review(r, events=None), "oos")[1])

    def test_ev_pos_n_99_reject(self):
        r = make_result(oos_mean_net=0.003, oos_t_stat=2.5, oos_n=99)
        self.assertFalse(get_check(review(r, events=None), "oos")[1])

    def test_ev_negative_t_high_reject(self):
        r = make_result(oos_mean_net=-0.001, oos_t_stat=5.0)
        self.assertFalse(get_check(review(r, events=None), "oos")[1])

    def test_ev_zero_reject(self):
        r = make_result(oos_mean_net=0.0, oos_t_stat=3.0)
        self.assertFalse(get_check(review(r, events=None), "oos")[1])

    def test_low_n_reject(self):
        r = make_result(oos_mean_net=0.01, oos_t_stat=3.0, oos_n=50)
        self.assertFalse(get_check(review(r, events=None), "oos")[1])

    def test_mr_scenario_reject(self):
        """MR: EV=+0.00003, t=0.06 -> REJECT."""
        r = make_result(oos_mean_net=0.00003, oos_t_stat=0.06, oos_n=10685)
        self.assertFalse(get_check(review(r, events=None), "oos")[1])

    def test_no_candidate_skip(self):
        """No finalist -> OOS=True (nothing to check)."""
        r = make_result(has_finalist=False)
        ok, detail = get_check(review(r), "oos")[1:]
        self.assertTrue(ok)
        self.assertIn("нет кандидата", detail)


# ---------------------------------------------------------------------------
# Validation gate: EV > 0 AND n >= 100 AND t >= 2.0
# ---------------------------------------------------------------------------
class TestValidationGate(unittest.TestCase):

    def test_val_ev_pos_t_low_reject(self):
        r = make_result(val_mean_net=0.0008, val_t_stat=1.09)
        self.assertFalse(get_check(review(r, events=None), "validation")[1])

    def test_val_ev_pos_t_pass(self):
        r = make_result(val_mean_net=0.003, val_t_stat=3.0)
        self.assertTrue(get_check(review(r, events=None), "validation")[1])

    def test_val_ev_pos_t_exactly_2_pass(self):
        r = make_result(val_mean_net=0.003, val_t_stat=2.0)
        self.assertTrue(get_check(review(r, events=None), "validation")[1])

    def test_val_ev_pos_n_exactly_100_pass(self):
        r = make_result(val_mean_net=0.003, val_t_stat=2.5, val_n=100)
        self.assertTrue(get_check(review(r, events=None), "validation")[1])

    def test_val_ev_pos_n_99_reject(self):
        r = make_result(val_mean_net=0.003, val_t_stat=2.5, val_n=99)
        self.assertFalse(get_check(review(r, events=None), "validation")[1])

    def test_val_ev_negative_reject(self):
        r = make_result(val_mean_net=-0.001, val_t_stat=3.0)
        self.assertFalse(get_check(review(r, events=None), "validation")[1])

    def test_val_ev_zero_reject(self):
        r = make_result(val_mean_net=0.0, val_t_stat=3.0)
        self.assertFalse(get_check(review(r, events=None), "validation")[1])

    def test_val_low_n_reject(self):
        r = make_result(val_mean_net=0.01, val_t_stat=3.0, val_n=50)
        self.assertFalse(get_check(review(r, events=None), "validation")[1])

    def test_val_no_candidate_skip(self):
        r = make_result(has_finalist=False)
        ok, detail = get_check(review(r), "validation")[1:]
        self.assertTrue(ok)
        self.assertIn("нет кандидата", detail)


# ---------------------------------------------------------------------------
# Temporal stability
# ---------------------------------------------------------------------------
class TestTemporalStability(unittest.TestCase):

    def test_no_candidate_pass(self):
        r = make_result(has_finalist=False)
        self.assertTrue(get_check(review(r), "temporal_stability")[1])

    def test_no_events_unknown(self):
        r = make_result()
        self.assertIsNone(get_check(review(r, events=None), "temporal_stability")[1])


# ---------------------------------------------------------------------------
# Concentration
# ---------------------------------------------------------------------------
class TestConcentration(unittest.TestCase):

    def test_no_candidate_pass(self):
        r = make_result(has_finalist=False)
        self.assertTrue(get_check(review(r), "concentration")[1])

    def test_no_events_unknown(self):
        r = make_result()
        self.assertIsNone(get_check(review(r, events=None), "concentration")[1])


# ---------------------------------------------------------------------------
# Costs gate: best_t > min_t_stat (strict >)
# ---------------------------------------------------------------------------
class TestCostsGate(unittest.TestCase):

    def test_best_t_high_pass(self):
        self.assertTrue(get_check(review(make_result(disc_t_stat=5.0, has_finalist=False)), "costs")[1])

    def test_best_t_low_reject(self):
        self.assertFalse(get_check(review(make_result(disc_t_stat=1.0, has_finalist=False)), "costs")[1])

    def test_best_t_exactly_2_reject(self):
        """Strict >: t=2.0 does NOT pass > 2.0."""
        self.assertFalse(get_check(review(make_result(disc_t_stat=2.0, has_finalist=False)), "costs")[1])

    def test_negative_t_reject(self):
        self.assertFalse(get_check(review(make_result(disc_t_stat=-3.55, has_finalist=False)), "costs")[1])

    def test_negative_t_minus_2_reject(self):
        self.assertFalse(get_check(review(make_result(disc_t_stat=-2.0, has_finalist=False)), "costs")[1])

    def test_negative_t_minus_199_reject(self):
        self.assertFalse(get_check(review(make_result(disc_t_stat=-1.99, has_finalist=False)), "costs")[1])

    def test_t_zero_reject(self):
        self.assertFalse(get_check(review(make_result(disc_t_stat=0.0, has_finalist=False)), "costs")[1])

    def test_t_plus_199_reject(self):
        self.assertFalse(get_check(review(make_result(disc_t_stat=1.99, has_finalist=False)), "costs")[1])

    def test_t_plus_201_pass(self):
        self.assertTrue(get_check(review(make_result(disc_t_stat=2.01, has_finalist=False)), "costs")[1])

    def test_t_plus_355_pass(self):
        self.assertTrue(get_check(review(make_result(disc_t_stat=3.55, has_finalist=False)), "costs")[1])

    def test_message_shows_sign_for_negative(self):
        v = review(make_result(disc_t_stat=-3.55, has_finalist=False))
        _, _, detail = get_check(v, "costs")
        self.assertIn("t=-3.55", detail)
        self.assertIn("<=", detail)

    def test_message_shows_sign_for_positive(self):
        v = review(make_result(disc_t_stat=5.0, has_finalist=False))
        _, _, detail = get_check(v, "costs")
        self.assertIn("t=5.00", detail)
        self.assertIn(">", detail)


# ---------------------------------------------------------------------------
# Sample size gate
# ---------------------------------------------------------------------------
class TestSampleSize(unittest.TestCase):

    def test_large_n_pass(self):
        self.assertTrue(get_check(review(make_result(n_events_total=100000, has_finalist=False)), "sample_size")[1])

    def test_small_n_reject(self):
        self.assertFalse(get_check(review(make_result(n_events_total=50, has_finalist=False)), "sample_size")[1])


# ---------------------------------------------------------------------------
# Dependency gate
# ---------------------------------------------------------------------------
class TestDependency(unittest.TestCase):

    def test_many_symbols_pass(self):
        self.assertTrue(get_check(review(make_result(disc_n_symbols=10, has_finalist=False)), "dependency")[1])

    def test_few_symbols_reject(self):
        self.assertFalse(get_check(review(make_result(disc_n_symbols=3, has_finalist=False)), "dependency")[1])


# ---------------------------------------------------------------------------
# Combined scenarios
# ---------------------------------------------------------------------------
class TestCombinedScenarios(unittest.TestCase):

    def test_no_finalist_all_pass(self):
        self.assertTrue(review(make_result(has_finalist=False)).passed)

    def test_val_fail_oos_pass_rejects(self):
        r = make_result(val_t_stat=1.0, oos_t_stat=3.0, oos_mean_net=0.005)
        v = review(r, events=None)
        self.assertFalse(v.passed)
        self.assertFalse(get_check(v, "validation")[1])
        self.assertTrue(get_check(v, "oos")[1])

    def test_val_pass_oos_fail_rejects(self):
        r = make_result(val_t_stat=3.0, oos_t_stat=0.06, oos_mean_net=0.00003)
        v = review(r, events=None)
        self.assertFalse(v.passed)
        self.assertTrue(get_check(v, "validation")[1])
        self.assertFalse(get_check(v, "oos")[1])

    def test_both_fail_rejects(self):
        r = make_result(val_t_stat=1.0, oos_t_stat=0.06)
        v = review(r, events=None)
        self.assertFalse(v.passed)

    def test_cost_fail_rejects(self):
        self.assertFalse(review(make_result(disc_t_stat=1.0, has_finalist=False)).passed)

    def test_mr_scenario_rejects(self):
        """Full MR scenario: val t=1.09, oos t=0.06 -> REJECT."""
        r = make_result(
            val_mean_net=0.0008, val_t_stat=1.09,
            oos_mean_net=0.00003, oos_t_stat=0.06, oos_n=10685,
        )
        v = review(r, events=None)
        self.assertFalse(v.passed)
        self.assertFalse(get_check(v, "oos")[1])
        self.assertFalse(get_check(v, "validation")[1])

    def test_strong_candidate_passes(self):
        """All strong + no finalist -> all True."""
        r = make_result(disc_t_stat=5.0, val_t_stat=3.0, oos_t_stat=2.5,
                        oos_mean_net=0.005, has_finalist=False)
        self.assertTrue(review(r).passed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
