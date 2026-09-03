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


def test_market_bucket_no_lookahead():
    """bucket_ms = floor(ts/60s) — тики попадают в свою минуту, без заглядывания вперёд."""
    from src.features_market import _bucket_ms
    df = pl.DataFrame({"ts": [0, 59_999, 60_000, 119_999, 120_000]})
    got = _bucket_ms(df)["bucket_ms"].to_list()
    assert got == [0, 0, 60_000, 60_000, 120_000]


def test_market_trades_features():
    """mk_buy_vol/sell_vol/flow_imb считаются по направлению сделок в бакете."""
    from src.features_market import _trades_features
    df = pl.DataFrame({
        "ts": [0, 1000, 2000, 61_000],
        "is_buy": [True, False, True, True],
        "price": [100.0, 101.0, 102.0, 103.0],
        "size": [1.0, 2.0, 3.0, 4.0],
    })
    g = _trades_features(df).sort("bucket_ms")
    r0 = g.filter(pl.col("bucket_ms") == 0)
    assert r0["mk_buy_vol"][0] == 1.0 + 3.0      # покупки в 1-й минуте
    assert r0["mk_sell_vol"][0] == 2.0           # продажи в 1-й минуте
    assert abs(r0["mk_flow_imb"][0] - (4.0 - 2.0) / 6.0) < 1e-9
    assert r0["mk_trade_count"][0] == 3
    assert abs(r0["mk_notional"][0]
               - (100 + 101 * 2 + 102 * 3)) < 1e-6
    # 2-я минута (ts=61s) — отдельный бакет
    r1 = g.filter(pl.col("bucket_ms") == 60_000)
    assert r1["mk_trade_count"][0] == 1


def test_market_orderbook_sparse_fill():
    """Sparse-дельта стакана: bid/ask last non-NaN по уровню в бакете."""
    from src.features_market import _orderbook_features
    df = pl.DataFrame({
        "ts": [0, 1, 2],
        "seq": [0, 1, 2],
        "level": [1, 1, 2],
        "bid_px": [100.0, float("nan"), 99.0],
        "bid_sz": [10.0, float("nan"), 5.0],
        "ask_px": [float("nan"), 101.0, float("nan")],
        "ask_sz": [float("nan"), 50.0, float("nan")],
    })
    r = _orderbook_features(df)
    assert r["mk_best_bid"][0] == 100.0
    assert r["mk_best_ask"][0] == 101.0
    assert r["bid1_px"][0] == 100.0   # level 1, а не 99.0 с level 2
    assert r["mk_depth_bid5"][0] == 15.0   # 10 + 5


def test_market_futures_and_ratio_last():
    """funding/OI/ratio берут последний non-NaN в минуте."""
    from src.features_market import _futures_features, _ratio_features
    fut = pl.DataFrame({"ts": [0, 1000], "funding_rate": [0.001, 0.002],
                        "oi": [100.0, 101.0], "mark_px": [100.0, 100.5]})
    g = _futures_features(fut)
    assert g["mk_funding_rate"][0] == 0.002
    assert g["mk_oi"][0] == 101.0
    ratio = pl.DataFrame({"ts": [3000], "buy_ratio": [0.55]})
    rr = _ratio_features(ratio)
    assert rr["mk_buy_ratio"][0] == 0.55


def test_benjamini_hochberg_controls_fdr():
    """BH из research корректно строит верхнюю границу k под p<=q."""
    from src.research import benjamini_hochberg
    sig = benjamini_hochberg(np.array([0.001, 0.9, 0.02, 0.5]), q=0.05)
    assert sig.sum() >= 1  # первый p очень мал — должен пройти


def test_time_features_no_overflow():
    """§18 min_from_day/min_from_funding без int8-overflow (255 > 127)."""
    import datetime as dt
    from src.features import time_features
    base = dt.datetime(2026, 9, 3, tzinfo=dt.timezone.utc)
    df = pl.DataFrame({"open_time": [
        base.replace(hour=4, minute=15),
        base.replace(hour=10, minute=0),
        base.replace(hour=21, minute=30),
    ]})
    out = time_features(df)
    assert out["min_from_day"].to_list() == [255, 600, 1290]
    # funding на 00/08/16 UTC: час 10 -> (600-2*60)%480=0; час 21 -> (1290-5*60)%480=30
    assert out["min_from_funding"].to_list() == [15, 0, 30]


def test_regime_derived_features():
    """§20: range_regime=trend_regime, btc_trend=up/down/flat, correlation порог."""
    from src.features_regime import add_regime_features
    n = 30
    df = pl.DataFrame({
        "trend_strength": [10.0] * n,
        "volume": [100.0] * n,
        "volatility_regime": ["high"] * n,
        "btc_trend_regime": [1] * 10 + [-1] * 10 + [0] * 10,
        "corr_btc_60": [0.8] * n,
    })
    out = add_regime_features(df)
    assert (out["range_regime"] == out["trend_regime"]).all()
    assert out["btc_trend"].head(10).to_list() == ["up"] * 10
    assert out["btc_trend"].slice(10, 10).to_list() == ["down"] * 10
    assert out["btc_trend"].tail(10).to_list() == ["flat"] * 10
    assert out["btc_volatility"].to_list() == ["high"] * n
    assert out["correlation_regime"].to_list() == ["high"] * n
    assert out["low_volatility_regime"].to_list() == [0] * n


def test_context_run_length_and_sequence():
    """§19: same_event_count = длина серии, event_sequence = позиция в серии."""
    import datetime as dt
    from src.features_context import add_context_features
    base = dt.datetime(2026, 9, 3, tzinfo=dt.timezone.utc)
    spikes = [0, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1]
    df = pl.DataFrame({
        "open_time": [base.replace(minute=i) for i in range(len(spikes))],
        "is_spike": [bool(s) for s in spikes],
    })
    out = add_context_features(df)
    assert out["same_event_count"].to_list() == \
        [None, None, None, 2, 2, None, 1, None, None, 3, 3, 3]
    assert out["event_sequence"].to_list() == \
        [None, None, None, 1, 2, None, 1, None, None, 1, 2, 3]


def test_external_features_registered_and_join():
    """§21: fng_value/btc_dominance/total_market_cap зарегистрированы; join не падает при пустом."""
    from src import registry
    ids = {f.id for f in registry.REGISTRY._features.values() if f.category == "external"}
    assert {"fng_value", "btc_dominance", "total_market_cap"} <= ids
    from src import features_external as fext
    import datetime as dt
    base = dt.datetime(2026, 9, 3, tzinfo=dt.timezone.utc)
    df = pl.DataFrame({"open_time": [base.replace(minute=i) for i in range(3)]})
    out = fext.add_external_features(df)
    for c in ("fng_value", "btc_dominance", "total_market_cap"):
        assert c in out.columns


def test_market_breadth_regime():
    """§17: breadth = (adv-dec)/total; market_breadth_regime = 1 при breadth>0."""
    import datetime as dt
    from src.features import add_features
    from src.features_breadth import compute_breadth
    base = dt.datetime(2026, 9, 3, tzinfo=dt.timezone.utc)
    t = [base.replace(minute=m) for m in range(6)]
    symA = pl.DataFrame({"open_time": t, "is_green": [True] * 6})
    symB = pl.DataFrame({"open_time": t, "is_green": [False] * 6})
    bf = compute_breadth({("A", "linear"): symA, ("B", "linear"): symB})
    assert bf.sort("open_time")["breadth"].to_list() == [0.0] * 6
    # при breadth == 0 -> market_breadth_regime == 0
    df = pl.DataFrame({"open_time": t, "open": [1.0] * 6, "high": [2.0] * 6,
                       "low": [0.5] * 6, "close": [1.0] * 6,
                       "volume": [1000.0] * 6, "turnover": [1e6] * 6})
    out = add_features(df, breadth=bf)
    assert "market_breadth_regime" in out.columns
    assert out["market_breadth_regime"].to_list() == [0] * 6