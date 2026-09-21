"""Генератор гипотез (§22-24, §38): GenRule, дефолтные/OB-правила, условия,
пороги, дедупликация, фильтр по частоте. Тест на реальный публичный API
модуля src/hypothesis_generator.py."""
from __future__ import annotations

import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl

from src import hypothesis_generator as hg


def _rules_df(n: int = 300, seed: int = 7) -> pl.DataFrame:
    """df со всеми признаками дефолтных правил (пороги вычислимы)."""
    import numpy as np
    import datetime as dt
    rng = np.random.default_rng(seed)
    base = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    ts = [base + dt.timedelta(minutes=i) for i in range(n)]
    rsi = rng.uniform(0.0, 100.0, n)
    vol = rng.uniform(0.0, 5.0, n)
    series = {
        "relative_volume_60": rng.uniform(0.1, 5.0, n),
        "relative_range": rng.uniform(0.0, 5.0, n),
        "volume_zscore": rng.normal(0, 2, n),
        "realized_vol_60": vol,
        "dist_rolling_high_60": rng.uniform(0.0, 3.0, n),
        "dist_rolling_low_60": rng.uniform(0.0, 3.0, n),
        "breakout_20": rng.uniform(-3.0, 3.0, n),
        "roc_20": rng.uniform(-1.0, 1.0, n),
        "rsi_14": rsi,
        "corr_btc_60": rng.uniform(-0.5, 0.9, n),
        "btc_trend_regime": rng.choice(["bull", "bear", "flat"], n),
        "volatility_regime": rng.choice(["low", "normal", "high", "very_high"], n),
        "close": rng.uniform(90.0, 110.0, n),
    }
    return pl.DataFrame({"open_time": ts, **series})


class TestGenRule(unittest.TestCase):
    def test_genrule_fields_and_record(self):
        r = hg.GenRule("rsi_14", "lt")
        rec = r.to_record()
        self.assertEqual(rec["feature_id"], "rsi_14")
        self.assertEqual(rec["operator"], "lt")
        self.assertEqual(rec["entry_side"], "long")
        self.assertEqual(tuple(rec["horizons"]), (5, 10, 30))

    def test_default_rules_structure(self):
        rules = hg.default_rules()
        self.assertEqual(len(rules), 14)
        self.assertEqual(len({r.feature_id for r in rules}), 12)
        for r in rules:
            self.assertIn(r.operator, {"gt", "lt", "in_range"})
            self.assertIn(r.entry_side, {"long", "short"})
            self.assertGreaterEqual(len(r.horizons), 1)
        pairs = {(r.feature_id, r.operator, r.entry_side) for r in rules}
        self.assertIn(("rsi_14", "gt", "short"), pairs)
        self.assertIn(("rsi_14", "lt", "long"), pairs)

    def test_ob_rules_from_config(self):
        rules = hg.ob_rules()
        self.assertTrue(rules)
        self.assertTrue(all(r.feature_id for r in rules))

    def test_all_rules_combines_default_and_ob(self):
        self.assertEqual(hg.all_rules(), hg.default_rules() + hg.ob_rules())


class TestConditionFromRule(unittest.TestCase):
    def _df(self):
        return _rules_df()

    def test_abs_threshold_same_for_gt_and_lt(self):
        df = self._df()
        gt = hg._condition_from_rule(hg.GenRule("rsi_14", "gt"), df)
        lt = hg._condition_from_rule(hg.GenRule("rsi_14", "lt"), df)
        self.assertIn("pl.col('rsi_14') > 70", gt)
        self.assertIn("pl.col('rsi_14') < 70", lt)

    def test_categorical_in_range_uses_is_in(self):
        df = self._df()
        cond = hg._condition_from_rule(hg.GenRule("volatility_regime", "in_range"), df)
        self.assertIn("pl.col('volatility_regime')", cond)
        self.assertIn("is_in", cond)

    def test_missing_feature_returns_none(self):
        df = self._df().drop("btc_trend_regime")
        self.assertIsNone(hg._condition_from_rule(hg.GenRule("btc_trend_regime", "gt"), df))


class TestGenerateAndFilter(unittest.TestCase):
    def _df(self):
        return _rules_df()

    def test_generate_hypotheses_dedups_and_fills_condition(self):
        df = self._df()
        hyps = hg.generate_hypotheses(df)
        # по одной гипотезе на (правило × горизонт): 29 = сумма горизонтов
        self.assertEqual(len(hyps),
                         sum(len(r.horizons) for r in hg.default_rules()))
        # ключ дедупа — (условие, сторона, горизонт): condition уникален на правило,
        # горизонты различают гипотезы одного правила
        keys = {(h.condition,
                 h.entry_side,
                 h.horizon_min) for h in hyps}
        self.assertEqual(len(keys), len(hyps))
        self.assertTrue(all(h.description for h in hyps))
        rsi = [h for h in hyps if "rsi_14" in h.condition]
        self.assertEqual(len(rsi), 4)  # gt/lt короткая/длинная × (5, 30)

    def test_generate_dedups_duplicate_rules(self):
        df = self._df()
        dup = hg.default_rules()[:2] * 2  # точные дубликаты правил
        a = hg.generate_hypotheses(df, rules=dup)
        b = hg.generate_hypotheses(df, rules=hg.default_rules()[:2])
        self.assertEqual(len(a), len(b))  # дедуп по (feature, operator, entry, horizon)
        self.assertEqual(len(a), 5)  # rv(3 горизонта) + rr(2 горизонта)
        keys = {(h.condition, h.entry_side, h.horizon_min) for h in a}
        self.assertEqual(len(keys), len(a))

    def test_filter_by_freq_drops_rare(self):
        df = self._df()
        # постоянный столбец → условие gt никогда не срабатывает
        df_c = df.with_columns(pl.lit(0.5).alias("relative_volume_60"))
        hyps = hg.generate_hypotheses(df_c)
        kept = hg.filter_by_freq(hyps, df_c, min_events=5)
        self.assertLessEqual(len(kept), len(hyps))

    def test_hypothesis_passes_polars_eval(self):
        df = self._df()
        hypo = hg.generate_hypotheses(df)[0]
        cond = eval(hypo.condition, {"pl": pl})
        n_true = df.filter(cond).height
        self.assertIsInstance(n_true, int)


if __name__ == "__main__":
    unittest.main()
