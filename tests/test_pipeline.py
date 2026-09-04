"""Tests for src/pipeline.py — StageResult, stages, freeze, acceptance report."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import numpy as np
import polars as pl

from src.pipeline import (
    StageResult,
    StageStatus,
    build_acceptance_report,
    compute_config_hash,
    freeze_finalist,
    stage_critic,
    stage_data_validation,
    stage_feature_validation,
    stage_oos_gate,
    stage_parameter_freeze,
    stage_validation_gate,
)


class TestStageResult(unittest.TestCase):
    def test_to_dict_roundtrip(self):
        r = StageResult(stage="test", status=StageStatus.PASS, run_id="r1",
                        metrics={"a": 1}, errors=["e1"], warnings=["w1"])
        d = r.to_dict()
        self.assertEqual(d["stage"], "test")
        self.assertEqual(d["status"], "PASS")
        self.assertEqual(d["metrics"]["a"], 1)
        self.assertEqual(d["errors"], ["e1"])
        self.assertEqual(d["warnings"], ["w1"])

    def test_passed_true(self):
        r = StageResult(stage="x", status=StageStatus.PASS, run_id="r")
        self.assertTrue(r.passed)

    def test_passed_false_reject(self):
        r = StageResult(stage="x", status=StageStatus.REJECT, run_id="r")
        self.assertFalse(r.passed)

    def test_passed_false_stop(self):
        r = StageResult(stage="x", status=StageStatus.STOP, run_id="r")
        self.assertFalse(r.passed)

    def test_passed_false_error(self):
        r = StageResult(stage="x", status=StageStatus.ERROR, run_id="r")
        self.assertFalse(r.passed)


class TestConfigHash(unittest.TestCase):
    def test_hash_is_12_chars(self):
        h = compute_config_hash()
        self.assertEqual(len(h), 12)
        self.assertTrue(all(c in "0123456789abcdef" for c in h))

    def test_hash_deterministic(self):
        h1 = compute_config_hash()
        h2 = compute_config_hash()
        self.assertEqual(h1, h2)


class TestDataValidation(unittest.TestCase):
    def _events(self, n=200, n_sym=10):
        syms = [f"SYM{i}" for i in range(n_sym)]
        return pl.DataFrame({
            "open_time": [1700000000000 + i * 60000 for i in range(n)],
            "symbol": [syms[i % n_sym] for i in range(n)],
            "entry_price": [100.0] * n,
            "return_5m": [0.001] * n,
        })

    def _universe(self, n=10):
        return pl.DataFrame({
            "symbol": [f"SYM{i}" for i in range(n)],
            "category": ["linear"] * n,
            "turnover_30d": [1e9] * n,
        })

    def test_pass_normal(self):
        r = stage_data_validation(self._events(), self._universe())
        self.assertTrue(r.passed)
        self.assertEqual(r.status, StageStatus.PASS)
        self.assertEqual(r.metrics["n_events"], 200)
        self.assertEqual(r.metrics["n_symbols"], 10)

    def test_stop_empty_events(self):
        r = stage_data_validation(
            pl.DataFrame({"open_time": [], "symbol": [], "entry_price": []}).cast(
                {"open_time": pl.Int64, "symbol": pl.Utf8, "entry_price": pl.Float64}),
            self._universe(),
        )
        self.assertFalse(r.passed)
        self.assertEqual(r.status, StageStatus.STOP)

    def test_stop_too_few_events(self):
        r = stage_data_validation(self._events(n=50), self._universe())
        self.assertFalse(r.passed)
        self.assertEqual(r.status, StageStatus.STOP)

    def test_stop_too_few_symbols(self):
        r = stage_data_validation(self._events(n_sym=2), self._universe(n=2))
        self.assertFalse(r.passed)
        self.assertEqual(r.status, StageStatus.STOP)

    def test_error_missing_column(self):
        ev = self._events().drop("entry_price")
        r = stage_data_validation(ev, self._universe())
        self.assertEqual(r.status, StageStatus.ERROR)

    def test_has_config_hash(self):
        r = stage_data_validation(self._events(), self._universe())
        self.assertEqual(len(r.config_hash), 12)


class TestFeatureValidation(unittest.TestCase):
    def test_pass_normal(self):
        ev = pl.DataFrame({
            "feat_a": [1.0, 2.0, 3.0],
            "feat_b": [4.0, 5.0, 6.0],
        })
        r = stage_feature_validation(ev, ["feat_a", "feat_b"])
        self.assertTrue(r.passed)

    def test_error_missing_feature(self):
        ev = pl.DataFrame({"feat_a": [1.0]})
        r = stage_feature_validation(ev, ["feat_a", "feat_b"])
        self.assertEqual(r.status, StageStatus.ERROR)

    def test_pass_high_null(self):
        ev = pl.DataFrame({
            "feat_a": [None, None, None, None, None, 1.0],
        })
        r = stage_feature_validation(ev, ["feat_a"])
        self.assertEqual(r.status, StageStatus.PASS)
        self.assertTrue(r.passed)
        self.assertTrue(any("null rate" in w for w in r.warnings))

    def test_pass_constant(self):
        ev = pl.DataFrame({
            "feat_a": [5.0, 5.0, 5.0],
        })
        r = stage_feature_validation(ev, ["feat_a"])
        self.assertEqual(r.status, StageStatus.PASS)
        self.assertTrue(r.passed)
        self.assertTrue(any("constant" in w for w in r.warnings))

    def test_metrics_count(self):
        ev = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        r = stage_feature_validation(ev, ["a", "b", "c"])
        self.assertEqual(r.metrics["n_features_checked"], 3)

    def test_warnings_dont_halt(self):
        ev = pl.DataFrame({
            "good": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "bad": [None, None, None, None, None, 1.0],
        })
        r = stage_feature_validation(ev, ["good", "bad"])
        self.assertTrue(r.passed)
        self.assertEqual(r.status, StageStatus.PASS)
        self.assertTrue(len(r.warnings) > 0)

    def test_stop_only_on_missing(self):
        ev = pl.DataFrame({"feat_a": [1.0]})
        r = stage_feature_validation(ev, ["feat_a", "feat_b"])
        self.assertEqual(r.status, StageStatus.ERROR)


class TestValidationGate(unittest.TestCase):
    def test_pass(self):
        r = stage_validation_gate({"n": 200, "mean_net": 0.001, "t_stat": 2.5})
        self.assertTrue(r.passed)

    def test_reject_low_n(self):
        r = stage_validation_gate({"n": 50, "mean_net": 0.001, "t_stat": 2.5})
        self.assertEqual(r.status, StageStatus.REJECT)

    def test_reject_negative_ev(self):
        r = stage_validation_gate({"n": 200, "mean_net": -0.001, "t_stat": 2.5})
        self.assertEqual(r.status, StageStatus.REJECT)

    def test_reject_zero_ev(self):
        r = stage_validation_gate({"n": 200, "mean_net": 0.0, "t_stat": 2.5})
        self.assertEqual(r.status, StageStatus.REJECT)

    def test_reject_low_t(self):
        r = stage_validation_gate({"n": 200, "mean_net": 0.001, "t_stat": 1.5})
        self.assertEqual(r.status, StageStatus.REJECT)

    def test_reject_nan_t(self):
        r = stage_validation_gate({"n": 200, "mean_net": 0.001, "t_stat": float("nan")})
        self.assertEqual(r.status, StageStatus.REJECT)

    def test_exact_t_pass(self):
        r = stage_validation_gate({"n": 200, "mean_net": 0.001, "t_stat": 2.0})
        self.assertTrue(r.passed)

    def test_custom_config(self):
        cfg = {"min_events": 50, "min_t_stat": 1.0}
        r = stage_validation_gate({"n": 60, "mean_net": 0.001, "t_stat": 1.5}, cfg)
        self.assertTrue(r.passed)


class TestOOSGate(unittest.TestCase):
    def test_pass(self):
        r = stage_oos_gate({"n": 200, "mean_net": 0.001, "t_stat": 2.5})
        self.assertTrue(r.passed)

    def test_reject_low_n(self):
        r = stage_oos_gate({"n": 50, "mean_net": 0.001, "t_stat": 2.5})
        self.assertEqual(r.status, StageStatus.REJECT)

    def test_reject_negative_ev(self):
        r = stage_oos_gate({"n": 200, "mean_net": -0.001, "t_stat": 2.5})
        self.assertEqual(r.status, StageStatus.REJECT)

    def test_reject_low_t(self):
        r = stage_oos_gate({"n": 200, "mean_net": 0.001, "t_stat": 1.0})
        self.assertEqual(r.status, StageStatus.REJECT)

    def test_mr_scenario_reject(self):
        r = stage_oos_gate({"n": 10685, "mean_net": 0.00003, "t_stat": 0.06})
        self.assertEqual(r.status, StageStatus.REJECT)


class TestParameterFreeze(unittest.TestCase):
    def test_no_finalist_skips(self):
        r = stage_parameter_freeze({"candidates": []}, {})
        self.assertEqual(r.status, StageStatus.SKIPPED)
        self.assertFalse(r.passed)
        self.assertTrue(r.metrics.get("skipped"))

    def test_freeze_roundtrip(self):
        result = {
            "finalist": {
                "hypothesis_id": "H001",
                "condition": "pl.col('x') > 1",
                "entry_side": "long",
                "horizon_min": 5,
                "description": "test",
            }
        }
        frozen = freeze_finalist(result)
        self.assertIn("frozen_hash", frozen)
        self.assertEqual(frozen["hypothesis_id"], "H001")

    def test_freeze_match(self):
        result = {
            "finalist": {
                "hypothesis_id": "H001",
                "condition": "pl.col('x') > 1",
                "entry_side": "long",
                "horizon_min": 5,
            }
        }
        frozen = freeze_finalist(result)
        r = stage_parameter_freeze(result, frozen)
        self.assertTrue(r.passed)
        self.assertTrue(r.metrics["match"])

    def test_freeze_drift_detected(self):
        result1 = {
            "finalist": {
                "hypothesis_id": "H001",
                "condition": "pl.col('x') > 1",
                "entry_side": "long",
                "horizon_min": 5,
            }
        }
        frozen = freeze_finalist(result1)
        result2 = {
            "finalist": {
                "hypothesis_id": "H001",
                "condition": "pl.col('x') > 2",
                "entry_side": "long",
                "horizon_min": 5,
            }
        }
        r = stage_parameter_freeze(result2, frozen)
        self.assertEqual(r.status, StageStatus.REJECT)
        self.assertFalse(r.metrics["match"])


class TestAcceptanceReport(unittest.TestCase):
    def test_all_pass(self):
        stages = [
            StageResult(stage="data_validation", status=StageStatus.PASS, run_id="r1"),
            StageResult(stage="critic", status=StageStatus.PASS, run_id="r1"),
        ]
        report = build_acceptance_report(stages, {"candidates": ["H001"]}, "r1")
        self.assertEqual(report["verdict"], "PASS")
        self.assertEqual(report["run_id"], "r1")

    def test_one_reject(self):
        stages = [
            StageResult(stage="data_validation", status=StageStatus.PASS, run_id="r1"),
            StageResult(stage="oos_gate", status=StageStatus.REJECT, run_id="r1",
                        errors=["t_stat too low"]),
        ]
        report = build_acceptance_report(stages, {}, "r1")
        self.assertEqual(report["verdict"], "REJECT")
        self.assertIn("t_stat too low", report["reject_reasons"])

    def test_has_provenance(self):
        stages = [StageResult(stage="x", status=StageStatus.PASS, run_id="r1")]
        report = build_acceptance_report(stages, {"provenance": {"a": 1}}, "r1")
        self.assertEqual(report["provenance"]["a"], 1)

    def test_skipped_does_not_affect_verdict(self):
        stages = [
            StageResult(stage="data_validation", status=StageStatus.PASS, run_id="r1"),
            StageResult(stage="parameter_freeze", status=StageStatus.SKIPPED, run_id="r1",
                        metrics={"skipped": True}),
            StageResult(stage="critic", status=StageStatus.REJECT, run_id="r1",
                        errors=["costs fail"]),
        ]
        report = build_acceptance_report(stages, {}, "r1")
        self.assertEqual(report["verdict"], "REJECT")
        self.assertNotIn("skipped", [s["stage"] for s in report["stages"]
                                     if s["status"] == "SKIPPED"
                                     and "skipped" in str(report["reject_reasons"])])

    def test_skipped_only_pass(self):
        stages = [
            StageResult(stage="data_validation", status=StageStatus.PASS, run_id="r1"),
            StageResult(stage="parameter_freeze", status=StageStatus.SKIPPED, run_id="r1"),
        ]
        report = build_acceptance_report(stages, {}, "r1")
        self.assertEqual(report["verdict"], "PASS")

    def test_no_finalist_reject_from_critic(self):
        stages = [
            StageResult(stage="data_validation", status=StageStatus.PASS, run_id="r1"),
            StageResult(stage="feature_validation", status=StageStatus.PASS, run_id="r1"),
            StageResult(stage="parameter_freeze", status=StageStatus.SKIPPED, run_id="r1"),
            StageResult(stage="critic", status=StageStatus.REJECT, run_id="r1",
                        errors=["costs: t=-3.55 <= 2.0"]),
        ]
        report = build_acceptance_report(stages, {"candidates": [], "finalist": None}, "r1")
        self.assertEqual(report["verdict"], "REJECT")
        self.assertEqual(report["finalist"], None)
        self.assertEqual(report["candidates"], [])


def _critic_events(n=200, relative_volume=4.0, is_green=True):
    from datetime import datetime, timezone, timedelta
    base = datetime(2025, 12, 25, tzinfo=timezone.utc)
    return pl.DataFrame({
        "open_time": [base + timedelta(minutes=i * 5) for i in range(n)],
        "symbol": [f"SYM{i % 5}" for i in range(n)],
        "entry_price": [100.0] * n,
        "relative_volume": [relative_volume] * n,
        "is_green": [is_green] * n,
        "return_5m": [0.005] * n,
        "mae_5m": [-0.002] * n,
        "mfe_5m": [0.008] * n,
    })


class TestCriticStage(unittest.TestCase):
    def test_critic_pass_no_candidate(self):
        events = _critic_events()
        result = {
            "candidates": [],
            "validation": {},
            "oos": {},
            "verdict": "NO_CANDIDATE",
            "n_events": {"discovery": 100, "validation": 50, "oos": 50},
            "n_events_total": 200,
            "n_hypotheses": 1,
            "discovery_results": [{"n_symbols": 5, "t_stat": 3.0}],
            "q_bh": 0.05,
        }
        r = stage_critic(result, events)
        self.assertTrue(r.passed)

    def test_critic_pass_candidate(self):
        events = _critic_events()
        result = {
            "candidates": ["H001"],
            "validation": {"H001": {"n": 150, "n_symbols": 5, "n_months": 4,
                          "t_stat": 2.5, "mean_net": 0.003, "winrate": 0.55,
                          "p_value": 0.01}},
            "oos": {"H001": {"n": 120, "n_symbols": 5, "n_months": 3,
                    "t_stat": 2.2, "mean_net": 0.002, "winrate": 0.53,
                    "p_value": 0.02}},
            "verdict": "CANDIDATE",
            "finalist": {
                "hypothesis_id": "H001",
                "condition": "(pl.col('relative_volume') > 3.0) & pl.col('is_green')",
                "entry_side": "long",
                "horizon_min": 5,
                "description": "test",
            },
            "n_events": {"discovery": 100, "validation": 50, "oos": 50},
            "n_events_total": 200,
            "n_hypotheses": 8,
            "discovery_results": [{"n_symbols": 5, "t_stat": 2.5}],
            "q_bh": 0.05,
            "cost_survival": 0.002,
        }
        r = stage_critic(result, events)
        self.assertTrue(r.passed)


# ---------------------------------------------------------------------------
# Finalist formation: gates must run BEFORE finalist is set
# ---------------------------------------------------------------------------
class TestFinalistFormation(unittest.TestCase):

    def test_val_low_t_no_finalist(self):
        val = {"n": 200, "mean_net": 0.003, "t_stat": 1.5}
        s = stage_validation_gate(val)
        self.assertEqual(s.status, StageStatus.REJECT)

    def test_oos_low_t_no_finalist(self):
        oos = {"n": 200, "mean_net": 0.003, "t_stat": 1.5}
        s = stage_oos_gate(oos)
        self.assertEqual(s.status, StageStatus.REJECT)

    def test_val_pass_oos_reject_no_finalist(self):
        val = {"n": 200, "mean_net": 0.003, "t_stat": 2.5}
        oos = {"n": 200, "mean_net": 0.003, "t_stat": 1.5}
        sv = stage_validation_gate(val)
        so = stage_oos_gate(oos)
        self.assertTrue(sv.passed)
        self.assertFalse(so.passed)

    def test_val_pass_oos_pass_finalist_eligible(self):
        val = {"n": 200, "mean_net": 0.003, "t_stat": 2.5}
        oos = {"n": 200, "mean_net": 0.003, "t_stat": 2.5}
        sv = stage_validation_gate(val)
        so = stage_oos_gate(oos)
        self.assertTrue(sv.passed)
        self.assertTrue(so.passed)

    def test_negative_t_reject(self):
        val = {"n": 200, "mean_net": 0.003, "t_stat": -3.55}
        s = stage_validation_gate(val)
        self.assertEqual(s.status, StageStatus.REJECT)

    def test_t_exactly_2_pass(self):
        val = {"n": 200, "mean_net": 0.003, "t_stat": 2.0}
        s = stage_validation_gate(val)
        self.assertTrue(s.passed)

    def test_no_finalist_skipped_freeze(self):
        result = {"candidates": [], "finalist": None}
        frozen = freeze_finalist(result)
        s = stage_parameter_freeze(result, frozen)
        self.assertEqual(s.status, StageStatus.SKIPPED)

    def test_no_candidate_not_false_pass(self):
        stages = [
            StageResult(stage="data_validation", status=StageStatus.PASS, run_id="r1"),
            StageResult(stage="parameter_freeze", status=StageStatus.SKIPPED, run_id="r1"),
            StageResult(stage="critic", status=StageStatus.REJECT, run_id="r1",
                        errors=["costs: no positive edge"]),
        ]
        report = build_acceptance_report(stages, {"candidates": [], "finalist": None}, "r1")
        self.assertEqual(report["verdict"], "REJECT")
        self.assertIsNone(report["finalist"])

    def test_verdict_deterministic(self):
        stages_a = [
            StageResult(stage="data_validation", status=StageStatus.PASS, run_id="r1"),
            StageResult(stage="parameter_freeze", status=StageStatus.SKIPPED, run_id="r1"),
            StageResult(stage="critic", status=StageStatus.REJECT, run_id="r1",
                        errors=["costs fail"]),
        ]
        stages_b = list(stages_a)
        r1 = build_acceptance_report(stages_a, {"candidates": []}, "r1")
        r2 = build_acceptance_report(stages_b, {"candidates": []}, "r2")
        self.assertEqual(r1["verdict"], r2["verdict"])


if __name__ == "__main__":
    unittest.main()
