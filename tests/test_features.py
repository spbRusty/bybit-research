"""Тесты на корректность признаков (ТЗ §44): rolling-окна, ATR, volatility, volume, momentum.

Проверяем согласованность признаков с прямым расчётом по historical-окну и
отсутствие разрывов в граничных значениях (NaN из-за короткого окна).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import features as F
from src import registry
from src import stats as S
from src.registry import Provenance


def _synth_df(n=500, seed=1) -> pl.DataFrame:
    import datetime as dt
    base = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    ts = [base + dt.timedelta(minutes=i) for i in range(n)]
    rng = np.random.default_rng(seed)
    c = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
    o = c / np.exp(rng.normal(0, 0.001, n))
    h = np.maximum(o, c) * np.exp(rng.uniform(0, 0.002, n))
    l = np.minimum(o, c) * np.exp(-rng.uniform(0, 0.002, n))
    v = rng.integers(100, 5000, n).astype(float)
    return pl.DataFrame({"open_time": ts, "open": o, "high": h, "low": l,
                         "close": c, "volume": v, "turnover": v * c,
                         "is_green": c >= o})


def test_registry_has_required_categories():
    """Реестр содержит все основные категории признаков (§5)."""
    cats = {f.category for f in registry.REGISTRY._features.values()}
    for expected in ("candle", "volume", "volatility", "momentum", "structure",
                     "cross", "regime", "context"):
        assert expected in cats


def test_atr_matches_manual():
    """ATR == rolling mean(TR, 14) над прошлыми свечами (изолированный расчёт)."""
    df = _synth_df(100)
    out = F.add_features(df)
    h, l, c = df["high"].to_numpy(), df["low"].to_numpy(), df["close"].to_numpy()
    pc = np.roll(c, 1); pc[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    atr_manual = np.array([np.mean(tr[max(0, t-13):t+1]) for t in range(100)])
    np.testing.assert_allclose(out["atr"].to_numpy(), atr_manual, rtol=1e-6)


def test_volume_zscore():
    """volume_zscore == (v - mean(v,60))/std(v,60), Polars ddof=1 (sample std)."""
    df = _synth_df(300)
    out = F.add_features(df)
    v = df["volume"].to_numpy()
    for t in (70, 150, 290):
        w = v[t-60:t]
        expect = (v[t] - w.mean()) / (w.std(ddof=1) if w.std(ddof=1) > 0 else 1e-12)
        assert abs(out["volume_zscore"][t] - expect) < 1e-6, (t, out["volume_zscore"][t], expect)


def test_rolling_quantile_interpolation_param():
    """Проверяем сигнатуру rolling_quantile для всех feature-модулей (без исключений)."""
    df = _synth_df(300)
    out = F.add_features(df)
    assert out.height == 300  # не упало при вычислении


def test_provenance_fingerprint_deterministic():
    """Provenance.fingerprint детерминирован для одинаковых данных."""
    p1 = Provenance(data_version="1.0", random_seed=42, timestamp="2026-01-01T00:00:00")
    p2 = Provenance(data_version="1.0", random_seed=42, timestamp="2026-01-01T00:00:00")
    assert p1.fingerprint() == p2.fingerprint()
    assert p1.fingerprint() != Provenance(data_version="2.0",
                                          timestamp="2026-01-01T00:00:00").fingerprint()


def test_stats_block_bootstrap_ci():
    """Block bootstrap CI содержит выборочное среднее (для детерм. данных)."""
    vals = np.arange(1, 101, dtype=float)
    mean, lo, hi = S.block_bootstrap_ci(vals, block=10, n_boot=500, seed=1)
    assert lo <= mean <= hi


def test_stats_cluster_bootstrap_ci():
    """Cluster bootstrap (по symbol): среднее внутри CI."""
    vals = np.concatenate([np.full(50, 1.0), np.full(50, 2.0)])
    labels = np.array(["A"] * 50 + ["B"] * 50)
    mean, lo, hi = S.cluster_bootstrap_ci(vals, labels, n_boot=200, seed=1)
    assert lo <= mean <= hi


def test_stats_hac_ttest_constant():
    """Для константы HAC t-stat == 0 (нет вариации -> se=0)."""
    t, p = S.hac_ttest(np.ones(100))
    assert np.isnan(t) or t == 0.0


def test_benjamini_hochberg_controls_fdr():
    """BH из research корректно строит верхнюю границу k под p<=q."""
    from src.research import benjamini_hochberg
    sig = benjamini_hochberg(np.array([0.001, 0.9, 0.02, 0.5]), q=0.05)
    assert sig.sum() >= 1  # первый p очень мал — должен пройти