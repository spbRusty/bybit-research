"""Расширенный генератор гипотез (§22-24, §38): категории, комбо ≤2 условий,
квантильные/направленно-зависимые пороги, is_spike → context-признаки."""
from __future__ import annotations

import re

import numpy as np
import polars as pl

from src import hypothesis_generator as hg


def _spiky_df(n=900, seed=3) -> pl.DataFrame:
    """Синтетика с редкими объёмными выбросами: prefilter относительного объёма > 3."""
    import datetime as dt
    base = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    ts = [base + dt.timedelta(minutes=i) for i in range(n)]
    rng = np.random.default_rng(seed)
    c = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
    v = rng.integers(100, 200, n).astype(float)
    v[::37] = 5000.0
    return pl.DataFrame({
        "open_time": ts,
        "open": c * 0.999, "high": c * 1.002, "low": c * 0.998,
        "close": c, "volume": v, "turnover": v * c, "is_green": True,
    })


def test_extended_and_combo_rules_exist_without_mr_sma120():
    feats = {r.feature_id for r in hg.extended_rules()}
    assert "mr_sma120" not in feats
    assert {"roc_5", "mean_reversion_score", "vol_expansion",
            "volume_anomaly_60", "mk_funding_rate"} <= feats
    combos = hg.combo_rules()
    assert combos and all(c.aux_feature_id for c in combos)
    assert all(c.max_conditions == 2 for c in combos)
    assert hg.all_rules() == (hg.default_rules() + hg.extended_rules()
                              + hg.combo_rules() + hg.mr_conditional_rules()
                              + hg.ob_rules())


def test_mr_conditional_rules_use_absolute_threshold_and_vol_regime():
    rules = hg.mr_conditional_rules()
    assert len(rules) == 6
    assert all(r.feature_id == "mr_sma120" and r.operator == "lt" for r in rules)
    assert all(r.aux_feature_id == "volatility_regime" for r in rules)
    assert {r.abs_threshold for r in rules} == {-0.05, -0.03}
    df = hg_all_cols()
    conds = [hg._condition_from_rule(r, df) for r in rules]
    assert all(c and " & " in c for c in conds)
    assert any("< -0.05" in c for c in conds) and any("< -0.03" in c for c in conds)
    both = [c for r, c in zip(rules, conds) if r.aux_values == ("high", "very_high")]
    vh = [c for r, c in zip(rules, conds) if r.aux_values == ("very_high",)]
    assert both and all("'high'" in c and "'very_high'" in c for c in both)
    assert vh and all("'very_high'" in c and "'high'" not in c for c in vh)
    hyps = hg.generate_hypotheses(df, rules=rules)
    assert len(hyps) == 6


def test_combo_condition_has_exactly_two_features():
    df = hg_all_cols()
    for rule in hg.combo_rules():
        cond = hg._condition_from_rule(rule, df)
        assert cond and " & " in cond, rule.feature_id
        assert len(re.findall(r"pl\.col\(", cond)) == 2, cond
        assert rule.feature_id in cond and rule.aux_feature_id in cond
    single = hg.extended_rules()[0]
    assert " & " not in hg._condition_from_rule(single, df)


def test_rsi_direction_aware_and_quantile_thresholds():
    df = pl.DataFrame({"rsi_14": np.linspace(0, 100, 500)})
    gt = hg._condition_from_rule(hg.GenRule("rsi_14", "gt"), df)
    lt = hg._condition_from_rule(hg.GenRule("rsi_14", "lt"), df)
    assert float(re.search(r"> ([\d.]+)", gt).group(1)) == 70.0
    assert float(re.search(r"< ([\d.]+)", lt).group(1)) == 30.0
    # признак снят с абсолютного порога → считается по процентилю (0.9 конфига)
    df2 = df.with_columns(relative_volume_60=np.linspace(0, 10, 500))
    q = hg._condition_from_rule(hg.GenRule("relative_volume_60", "gt"), df2)
    assert abs(float(re.search(r"> ([\d.]+)", q).group(1)) - 9.0) < 0.2


def test_is_spike_derived_makes_context_features_available():
    import src.features as F
    out = F.add_features(_spiky_df())
    assert "is_spike" in out.columns and out["is_spike"].sum() > 0
    for c in ("event_intensity_60", "event_intensity_240",
              "event_clustering", "same_event_count", "event_sequence"):
        assert c in out.columns
    cond = hg._condition_from_rule(
        hg.GenRule("event_intensity_60", "gt"), out)
    assert cond and "event_intensity_60" in cond


def hg_all_cols() -> pl.DataFrame:
    """Feature-frame со всеми колонками, нужными комбо-правилам."""
    df = _spiky_df()
    n = df.height
    return df.with_columns([
        pl.lit(0.0).alias("roc_5"),
        pl.lit(1.0).alias("ma_slope_20"),
        pl.lit(0.5).alias("mean_reversion_score"),
        pl.lit(0.0).alias("dist_rolling_low_120"),
        pl.lit(0.0).alias("volume_anomaly_60"),
        pl.lit(0.0).alias("breakout_magnitude"),
        pl.lit(0.0).alias("event_intensity_60"),
        pl.lit(0.0).alias("mk_funding_rate"),
        pl.lit(-0.10).alias("mr_sma120"),
        pl.Series("trend_regime", ["trend"] * n),
        pl.Series("volatility_regime", ["high"] * n),
        pl.Series("volume_regime", ["high"] * n),
        pl.Series("session_overlap", ["asia_europe"] * n),
    ])
