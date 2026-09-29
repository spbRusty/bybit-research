"""Research Controller accumulative selection tests (C1-C4).

Сценарии:
1. C1: история tested/used конфигураций переносится между циклами из журнала;
   бюджеты (12/cycle, 4/family/cycle) остаются локальными текущему циклу.
2. C2: cached next_selection, чей config_hash уже использован, сбрасывается
   и выполняется новый _select_next().
3. C3: REGIME_SWEEP и CONDITIONAL с одинаковым условием -> одна сигнатура,
   дедупликация исследовательского пространства без потери identity-хеша.
4. C4: ротация области после 3 последовательных экспериментов одной области.
5. C1: исчерпанный бюджет старого цикла не блокирует новый цикл.
"""
from __future__ import annotations

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.experiment import compute_experiment_hash
from src.orchestrator import _condition_cols, _required_columns
from src.research_controller import (
    AREA_ROTATION_LIMIT,
    ResearchController,
    default_state,
    load_toml,
)

KM_A = "2026-09-20T19:55:00"
KM_B = "2026-09-22T14:50:00"
CYCLE_A = f"abc123:{KM_A}"
CYCLE_B = f"abc123:{KM_B}"

TEST_CONFIG = {
    "budget": {
        "max_experiments_per_boundary": 12,
        "max_experiments_per_family": 4,
        "max_tests_cumulative": 600,
    },
    "families": {"order": ["baseline", "generator", "mr", "ob"]},
    "horizons": {"default": [5, 10, 30]},
    "diagnosis": {"cost_sensitive": ["CONDITIONAL", "REGIME_SWEEP"]},
    "features": {
        "rv": {"column": "relative_volume", "operator": "gt",
               "entry_side": "long", "thresholds": [1.5]},
        "mom": {"column": "momentum", "operator": "gt",
                "entry_side": "long", "thresholds": [0.01]},
    },
    "regimes": {"regime_a": "pl.col('trend') == 1"},
    "combinations": {"comb_a": "pl.col('trend') == 1"},  # идентичная строка -> C3
}

RV = {"feature": "rv", "column": "relative_volume", "operator": "gt",
      "entry_side": "long", "threshold": 1.5, "horizons": [5, 10, 30]}


def _gate(km: str):
    def gate():
        return (True, [], {
            "klines": {"klines_max": km},
            "config_hash": "abc123",
            "git_head": "test",
            "data_version": "1.0",
        })
    return gate


def _reject_runner(**kw):
    return {
        "verdict": "REJECT",
        "stages": [{"stage": "critic", "status": "REJECT",
                    "errors": ["costs: high per-trade costs"], "metrics": {}}],
        "discovery_summary": {"n_evaluated": 3, "n_positive_t": 0},
        "reject_reasons": ["high costs"],
    }


class ControllerBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.td = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def make_ctl(self, km: str = KM_A):
        return ResearchController(base_dir=self.td, config=TEST_CONFIG,
                                  gate=_gate(km), runner=_reject_runner)

    def load_state(self) -> dict:
        return json.loads((self.td / "controller_state.json").read_text())

    def journal(self) -> list[dict]:
        if not (self.td / "experiments.jsonl").exists():
            return []
        return [json.loads(l) for l in
                (self.td / "experiments.jsonl").read_text().splitlines() if l.strip()]

    def seed_experiment(self, ctl, eid, mode, params, cycle_id=CYCLE_A):
        spec_records = ctl._spec_records_for(mode, params)
        chash = compute_experiment_hash(mode, params, spec_records)
        ctl._journal_append({"experiment_id": eid, "cycle_id": cycle_id,
                             "config_hash": chash, "mode": mode,
                             "parameters": params, "status": "RUNNING",
                             "created_at_utc": "2026-09-20T10:00:00"})
        ctl._journal_append({"experiment_id": eid, "cycle_id": cycle_id,
                             "config_hash": chash, "mode": mode,
                             "status": "DONE", "final_verdict": "REJECT",
                             "diagnosis": "cost_sensitive",
                             "finished_at_utc": "2026-09-20T10:05:00"})
        return chash


class TestCrossCycleHistory(ControllerBase):
    def test_cycle_reset_restores_history_keeps_budget_cycle_local(self):
        ctl = self.make_ctl()
        res_a = ctl.run_once()
        self.assertEqual(res_a["mode"], "BASELINE")  # первый эксперимент цикла
        st = self.load_state()
        self.assertEqual(st["budget_used"], 1)
        baseline_hash = st["used_config_hashes"][0]

        # новый boundary -> _reset_cycle: история из журнала, бюджет сброшен
        ctl._gate = _gate(KM_B)
        res_b = ctl.run_once()
        self.assertNotEqual(res_b["mode"], "BASELINE")  # старый baseline исключён
        self.assertNotEqual(res_b["experiment_id"], res_a["experiment_id"])
        st = self.load_state()
        self.assertEqual(st["cycle_id"], CYCLE_B)
        self.assertEqual(st["budget_used"], 1)           # свежий бюджет нового цикла
        self.assertIn(baseline_hash, st["used_config_hashes"])  # история сохранена
        self.assertEqual(len(st["used_config_hashes"]), 2)

    def test_reset_cycle_restores_hashes_and_signatures(self):
        ctl = self.make_ctl()
        chash = self.seed_experiment(ctl, "EXP_OLD", "THRESHOLD_SWEEP", RV)
        state = default_state()
        state["budget_used"] = 12
        state["family_usage"] = {"baseline": 4}
        new_state = ctl._reset_cycle(state, CYCLE_B, {"x": 1}, {})
        self.assertEqual(new_state["budget_used"], 0)
        self.assertEqual(new_state["n_tests_cumulative"], 0)
        self.assertEqual(new_state["family_usage"], {})
        self.assertIn(chash, new_state["used_config_hashes"])
        self.assertEqual(len(new_state["tested_signatures"]), 1)


class TestStaleCachedSelection(ControllerBase):
    def test_cached_selection_with_used_hash_is_reselected(self):
        ctl = self.make_ctl()
        chash = self.seed_experiment(ctl, "EXP_SEED", "THRESHOLD_SWEEP", RV)
        spec_records = ctl._spec_records_for("THRESHOLD_SWEEP", RV)
        state = {
            "state": "SELECT_NEXT",
            "cycle_id": CYCLE_A,
            "budget_used": 1,
            "used_config_hashes": [chash],
            "tested_signatures": [],
            "next_selection": {"mode": "THRESHOLD_SWEEP", "parameters": RV,
                               "config_hash": chash,
                               "hypothesis_specs": spec_records},
        }
        (self.td / "controller_state.json").write_text(json.dumps(state))

        res = ctl.run_once()
        self.assertEqual(res["state"], "SELECT_NEXT")
        lines = self.journal()
        self.assertNotEqual(lines[-1]["config_hash"], chash)
        self.assertNotEqual(lines[-1]["experiment_id"], "EXP_SEED")
        st = self.load_state()
        # перевыбранная конфигурация не повторяет использованную
        self.assertEqual(len(st["used_config_hashes"]), 2)


class TestSignatureDedup(ControllerBase):
    def test_regime_and_conditional_signature_identical(self):
        ctl = self.make_ctl()
        p_r = dict(RV)
        p_r.update(regime_key="regime_a", regime_condition="pl.col('trend') == 1")
        p_c = dict(RV)
        p_c.update(second_key="comb_a", second_condition="pl.col('trend') == 1")

        sig_r = ctl._signature_for("REGIME_SWEEP", p_r,
                                   ctl._spec_records_for("REGIME_SWEEP", p_r))
        sig_c = ctl._signature_for("CONDITIONAL", p_c,
                                   ctl._spec_records_for("CONDITIONAL", p_c))
        self.assertEqual(sig_r, sig_c)
        # identity-хеш остаётся разным (mode входит в hash)
        hash_r = compute_experiment_hash("REGIME_SWEEP", p_r,
                                         ctl._spec_records_for("REGIME_SWEEP", p_r))
        hash_c = compute_experiment_hash("CONDITIONAL", p_c,
                                         ctl._spec_records_for("CONDITIONAL", p_c))
        self.assertNotEqual(hash_r, hash_c)

    def test_select_next_skips_tested_signature_across_modes(self):
        ctl = self.make_ctl()
        p_r = dict(RV)
        p_r.update(regime_key="regime_a", regime_condition="pl.col('trend') == 1")
        sig_r = ctl._signature_for("REGIME_SWEEP", p_r,
                                   ctl._spec_records_for("REGIME_SWEEP", p_r))
        state = default_state()
        state["budget_used"] = 1
        state["used_config_hashes"] = []
        state["tested_signatures"] = [sig_r]

        sel = ctl._select_next(state, None, "cost_sensitive", allow_baseline=False)
        # CONDITIONAL rv с тем же условием пропущен -> следующий признак/rv mom
        self.assertEqual(sel["mode"], "CONDITIONAL")
        self.assertNotEqual(sel["parameters"]["column"], "relative_volume")
        # вариант rv того же смысла не выбран
        self.assertNotEqual(ctl._signature_for(
            sel["mode"], sel["parameters"], sel["hypothesis_specs"]), sig_r)

    def test_execute_tracks_signature(self):
        ctl = self.make_ctl()
        ctl.run_once()  # BASELINE
        ctl.run_once()  # следующий
        st = self.load_state()
        self.assertGreaterEqual(len(st["tested_signatures"]), 2)
        # сигнатуры в журнале восстанавливаются: reset не теряет историю
        ctl._gate = _gate(KM_B)
        ctl.run_once()
        st = self.load_state()
        self.assertGreaterEqual(len(st["tested_signatures"]), 3)


class TestAreaRotation(ControllerBase):
    def test_rotation_after_limit_prefers_other_area(self):
        ctl = self.make_ctl()
        state = default_state()
        state["budget_used"] = 1
        state["last_area"] = "relative_volume"
        state["consecutive_same_area"] = AREA_ROTATION_LIMIT

        sel = ctl._select_next(state, None, None, allow_baseline=False)
        self.assertEqual(sel["mode"], "FAMILY_SWITCH")  # ротационный режим впереди
        self.assertNotEqual(ctl._area_of(sel["parameters"]), "relative_volume")

    def test_below_limit_same_area_allowed(self):
        ctl = self.make_ctl()
        state = default_state()
        state["budget_used"] = 1
        state["last_area"] = "relative_volume"
        state["consecutive_same_area"] = AREA_ROTATION_LIMIT - 1

        sel = ctl._select_next(state, None, None, allow_baseline=False)
        self.assertEqual(ctl._area_of(sel["parameters"]), "relative_volume")

    def test_execute_updates_area_counters(self):
        ctl = self.make_ctl()
        ctl.run_once()  # BASELINE (family standard)
        ctl.run_once()  # THRESHOLD_SWEEP rv
        st = self.load_state()
        self.assertEqual(st["last_area"], "relative_volume")
        self.assertEqual(st["consecutive_same_area"], 1)


class TestCycleLocalBudget(ControllerBase):
    def test_exhausted_old_cycle_does_not_block_new_cycle(self):
        ctl = self.make_ctl()
        ctl.run_once()  # цикл A: 1/12
        st = self.load_state()
        st["budget_used"] = 12  # старый цикл исчерпан
        (self.td / "controller_state.json").write_text(json.dumps(st))

        ctl._gate = _gate(KM_B)
        res = ctl.run_once()
        self.assertEqual(res["state"], "SELECT_NEXT")
        self.assertEqual(res["budget_used"], 1)  # свежий бюджет нового цикла
        st = self.load_state()
        self.assertEqual(st["cycle_id"], CYCLE_B)
        self.assertEqual(st["n_tests_cumulative"], 3)


class TestSearchSpaceSchema(unittest.TestCase):
    """P1-регрессия: колонки гипотез Controller (hypothesis_specs) обязаны
    попадать в required_cols через extra_conditions — slim не должен вырезать
    колонки, на которые ссылается experiment condition (bug: unable to find column)."""

    @classmethod
    def setUpClass(cls):
        cls.req_base = set(_required_columns())
        cls.proj = Path(__file__).resolve().parent.parent
        cls.cfg = load_toml(str(cls.proj / "config" / "research_controller.toml"))

    def _search_space_conditions(self) -> list[str]:
        """Все condition-строки реального search-space (как их строит experiment)."""
        conds = []
        for meta in self.cfg.get("features", {}).values():
            base = f"pl.col('{meta['column']}') {meta['operator']}"
            for sec in ("regimes", "combinations"):
                for extra in self.cfg.get(sec, {}).values():
                    conds.append(f"({base} 1.0) & ({extra})")
        return conds

    def test_extra_condition_column_added_to_required(self):
        # колонка, отсутствующая в базовом наборе, добавляется через extra_conditions
        non_base = "breakout_failure"
        self.assertNotIn(non_base, self.req_base)
        req = set(_required_columns((f"pl.col('{non_base}') > 0.5",)))
        self.assertIn(non_base, req)

    def test_multiple_extra_conditions_all_added(self):
        conds = ("pl.col('breakout_failure') > 0.5",
                 "pl.col('corr_eth_60') > 0.6",
                 "pl.col('fng_value') < 25")
        req = set(_required_columns(conds))
        for c in ("breakout_failure", "corr_eth_60", "fng_value"):
            self.assertIn(c, req)

    def test_any_search_space_column_survives_slim(self):
        # весь search-space: каждая колонка condition обязана попасть в required
        for cond in self._search_space_conditions():
            cols = set(_condition_cols(cond))
            self.assertTrue(cols)
            req = set(_required_columns((cond,)))
            missing = cols - req
            self.assertEqual(missing, set(), f"cond: {cond}")

    def test_full_search_space_conditions_collectively(self):
        # множественные extra_conditions: одна гипотеза со всеми regime/combination
        conds = [f"pl.col('{meta['column']}') {meta['operator']} 1.0"
                 for meta in self.cfg.get("features", {}).values()]
        req = set(_required_columns(tuple(conds)))
        for meta in self.cfg.get("features", {}).values():
            self.assertIn(meta["column"], req)

    def test_search_space_is_nonempty(self):
        self.assertGreaterEqual(len(self.cfg.get("features", {})), 1)


class TestDiagnosis(ControllerBase):
    """Диагноз REJECT следует за сигналом (t), а не за текстом critic.

    Регрессия: отрицательный discovery t (реальный кейс t=-77.63)
    ошибочно классифицировался как cost_sensitive — проверка critic
    "costs" это порог t (best_t > min_t_stat), а не стресс издержек.
    """

    def _cost_report(self, errors=("costs: high per-trade costs",),
                     ds_t: float | None = None):
        ds = {"n_evaluated": 3, "n_positive_t": 1}
        if ds_t is not None:
            ds["max_t_stat"] = ds_t
        return {
            "verdict": "REJECT",
            "stages": [{"stage": "critic", "status": "REJECT",
                        "errors": list(errors), "metrics": {}}],
            "discovery_summary": ds,
            "reject_reasons": list(errors),
        }

    def test_negative_t_is_no_signal_not_cost_sensitive(self):
        ctl = self.make_ctl()
        r = self._cost_report(ds_t=-77.63)
        self.assertEqual(ctl._diagnose(r), "no_signal")

    def test_zero_t_is_no_signal(self):
        ctl = self.make_ctl()
        r = self._cost_report(ds_t=0.0)
        self.assertEqual(ctl._diagnose(r), "no_signal")

    def test_weak_positive_t_is_weak_signal(self):
        ctl = self.make_ctl()
        r = self._cost_report(ds_t=1.5)
        self.assertEqual(ctl._diagnose(r), "weak_signal")

    def test_positive_t_at_threshold_is_weak_signal(self):
        ctl = self.make_ctl()
        r = self._cost_report(ds_t=2.0)
        self.assertEqual(ctl._diagnose(r), "weak_signal")

    def test_strong_t_with_cost_reject_is_cost_sensitive(self):
        ctl = self.make_ctl()
        r = self._cost_report(ds_t=2.5)
        self.assertEqual(ctl._diagnose(r), "cost_sensitive")

    def test_real_report_without_discovery_summary_parses_text(self):
        r = self._cost_report(errors=(
            "costs: сильнейший discovery t=-77.63 <= 2.0 (порог: > 2.0, "
            "нужен положительный t), издержки 0.20% кругового оборота",))
        ctl = self.make_ctl()
        self.assertEqual(ctl._diagnose(r), "no_signal")

    def test_temporal_unstable_rule_kept(self):
        ctl = self.make_ctl()
        r = self._cost_report(errors=(
            "temporal_stability: доля=40% НЕСТАБИЛЬНО",))
        self.assertEqual(ctl._diagnose(r), "unstable")

    def test_concentration_dependency_rules_kept(self):
        ctl = self.make_ctl()
        r = self._cost_report(errors=("concentration: top1=95%",))
        self.assertEqual(ctl._diagnose(r), "concentrated")
        r2 = self._cost_report(errors=("dependency: 2 символа",))
        self.assertEqual(ctl._diagnose(r2), "concentrated")

    def test_validation_and_oos_fail_kept(self):
        ctl = self.make_ctl()
        r = {"verdict": "REJECT", "stages": [
            {"stage": "validation_gate", "status": "REJECT", "errors": ["x"]}],
            "discovery_summary": {"n_evaluated": 3, "n_positive_t": 1}}
        self.assertEqual(ctl._diagnose(r), "validation_fail")
        r2 = {"verdict": "REJECT", "stages": [
            {"stage": "oos_gate", "status": "REJECT", "errors": ["x"]}],
            "discovery_summary": {"n_evaluated": 3, "n_positive_t": 1}}
        self.assertEqual(ctl._diagnose(r2), "oos_fail")

    def test_no_candidate_verdict_kept(self):
        ctl = self.make_ctl()
        r = {"verdict": "NO_CANDIDATE", "stages": [],
             "discovery_summary": {"n_evaluated": 3, "n_positive_t": 0}}
        self.assertEqual(ctl._diagnose(r), "no_candidates")


if __name__ == "__main__":
    unittest.main()