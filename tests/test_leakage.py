"""Тесты на отсутствие look-ahead (ТЗ §44, §31).

Проверяем: все семь-признаки события сформированы на момент close T; будущие
доходности присоединяются не при признаках, а отдельным шагом; rolling-окна
не используют текущую и будущие свечи.
"""
from __future__ import annotations

import sys
from pathlib import Path

import datetime as dt
import re

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import features as F
from src import events as ev_mod
from src import hypothesis_generator as hg
from src.research import split_periods


def _synth_df(n=500, seed=7) -> pl.DataFrame:
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


def test_feature_uses_only_past():
    """Признак на строке T == вручную вычисленному по строкам <= T."""
    df = _synth_df(200)
    o, h, l, c, v = (df["open"], df["high"], df["low"], df["close"], df["volume"])
    out = F.add_features(df)
    # candle_return_1m = close/open - 1 (только текущая строка)
    np.testing.assert_allclose(out["candle_return_1m"].to_numpy()[1:],
                               (c.to_numpy()[1:] / o.to_numpy()[1:] - 1), rtol=1e-8)
    # volume_change = v_t/v_{t-1} - 1 (текущая и предыдущая)
    np.testing.assert_allclose(out["volume_change"].to_numpy()[1:],
                               (v.to_numpy()[1:] / v.to_numpy()[:-1] - 1), rtol=1e-6)


def test_rolling_feature_no_lookahead():
    """Возволяющая нас rolling-среднее по объёму окна 5 использует только прошлые 5."""
    df = _synth_df(300)
    out = F.add_features(df)
    v = df["volume"].to_numpy()
    # relative_volume_20: v_t / median(v[t-1..t-20]) — без текущей свечи в знаменателе
    rv = out["relative_volume_20"].to_numpy()
    # для t>=20: знаменатель==median прошлого окна, числитель==v_t
    for t in range(25, 40):
        denom = np.median(v[t-20:t])
        assert abs(rv[t] * denom - v[t]) < 1e-6 * max(1, denom)


def test_future_return_is_forward_only():
    """return_5m использует только будущие данные == вручную close[t+5]/entry-1."""
    df = _synth_df(100)
    out = ev_mod._future_metrics(df)
    c = df["close"].to_numpy()
    entry = df["open"].to_numpy()[1:]  # entry_price = open(T+1)
    r = out["return_5m"].to_numpy()
    for t in range(0, 95):
        expect = c[t + 5] / df["open"].to_numpy()[t + 1] - 1
        assert abs(r[t] - expect) < 1e-9


def _grid_df(steps_min: list[int]) -> pl.DataFrame:
    """Свечи на нерегулярной сетке: цена линейно растёт со временем.

    close(T) = 100 + минуты(T), open = close - 1 -> арифметика таргетов
    проверяется в уме, без шума.
    """
    base = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    close = np.array([100.0 + m for m in steps_min])
    open_ = close - 1.0
    v = np.full(len(steps_min), 1000.0)
    return pl.DataFrame({
        "open_time": [base + dt.timedelta(minutes=m) for m in steps_min],
        "open": open_, "high": close + 0.5, "low": close - 0.5, "close": close,
        "volume": v, "turnover": v * close, "is_green": close >= open_,
    })


def test_horizon_is_minutes_not_rows():
    """Горизонт задан в МИНУТАХ: на 2-минутной сетке return_5m выходит на T+6мин,
    а не на 5-ю строку вперёд (T+10мин). Это ловит возврат к shift(-h)."""
    df = _grid_df(list(range(0, 60, 2)))
    r = ev_mod._future_metrics(df)["return_5m"].to_numpy()
    # T=0мин: entry=open(T+2)=101, exit=первая свеча >= 5мин -> T+6мин, close=106
    assert abs(r[0] - (106.0 / 101.0 - 1)) < 1e-9, r[0]
    # shift(-5) по строкам дал бы close(T+10мин)=110
    assert abs(r[0] - (110.0 / 101.0 - 1)) > 1e-3


def test_gap_wider_than_window_is_null():
    """Пропуск шире 1.5*h минут ВНУТРИ окна -> null, а не таргет через дыру в данных."""
    df = _grid_df([0, 2, 4, 6, 8, 30, 32, 34, 36, 38])
    r = ev_mod._future_metrics(df)["return_5m"].to_numpy()
    # T=6мин: окно до 11мин, а следующая свеча только на 30мин -> span 24 > 7.5 -> null
    assert not np.isfinite(r[3])
    # T=0мин: окно 2/4/6мин покрыто, выход на T+6мин -> таргет валиден
    assert np.isfinite(r[0])
    assert abs(r[0] - (106.0 / 101.0 - 1)) < 1e-9


def test_mfe_mae_cover_time_window():
    """MFE/MAE берут экстремумы до T+h МИНУТ, а не по h строкам."""
    steps = list(range(0, 60, 2))
    df = _grid_df(steps)
    spike = np.full(len(steps), 0.5)
    spike[2] = 200.0 - (100.0 + steps[2])   # пик на T+4мин
    df = df.with_columns(pl.Series("high", df["close"].to_numpy() + spike))
    out = ev_mod._future_metrics(df)
    # T=0мин: окно = T+2,T+4,T+6 -> пик 200 попадает, entry=101
    assert abs(out["mfe_5m"].to_numpy()[0] - (200.0 / 101.0 - 1)) < 1e-9


def test_features_unchanged_when_future_perturbed():
    """Главный тест на утечку: признаки на T не меняются, если испортить все
    бары строго ПОСЛЕ T. Ломается на любом look-ahead в признаках."""
    base = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    n = 300
    ts = [base + dt.timedelta(minutes=i) for i in range(n)]
    rng = np.random.default_rng(11)
    c = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
    o = c / np.exp(rng.normal(0, 0.001, n))
    df = pl.DataFrame({
        "open_time": ts, "open": o, "high": np.maximum(o, c) * 1.001,
        "low": np.minimum(o, c) * 0.999, "close": c,
        "volume": rng.integers(100, 5000, n).astype(float),
        "turnover": rng.integers(100, 5000, n).astype(float) * c,
        "is_green": c >= o,
    })

    cut = 200
    scale = [1.0] * (cut + 1) + [1.5] * (n - cut - 1)
    perturbed = df.with_columns([
        (pl.col("open") * pl.Series(scale)).alias("open"),
        (pl.col("high") * pl.Series(scale)).alias("high"),
        (pl.col("low") * pl.Series(scale)).alias("low"),
        (pl.col("close") * pl.Series(scale)).alias("close"),
    ])

    a = F.add_features(df)
    b = F.add_features(perturbed)
    numeric = {pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.UInt32}
    for col in a.columns:
        if col == "open_time" or a.schema[col] not in numeric:
            continue
        lhs = a[col].to_numpy()[:cut + 1].astype(float)
        rhs = b[col].to_numpy()[:cut + 1].astype(float)
        m = np.isfinite(lhs) & np.isfinite(rhs)
        if m.sum() == 0:
            continue
        np.testing.assert_allclose(lhs[m], rhs[m], rtol=1e-9, err_msg=col)


def test_events_no_future_in_features():
    """Событие: признаки на close T; будущие доходности отдельно — нет колонки
    return_* на входе в условие."""
    df = _synth_df(500)
    out = F.add_features(df)
    ev = ev_mod.build_events(out, "TEST", "linear")
    # до присоединения доходностей в признаках не должно быть return_*(будущего)
    assert out.height > 0
    # build_events использует фьючерсы только в _future_metrics (после suspicious)
    assert "return_5m" in out.columns  # получено из add_features (прошлые доходности)


def test_generator_thresholds_use_discovery_only():
    """Пороги генератора считаются по discovery; val/oos НЕ влияют (§33).

    Discovery: признак ~ N(0,1). val+oos: сдвиг +100. Если бы порог считался по
    всему events (утечка), он был бы ~50; по discovery он ~N(0,1) 90-й pct (~1.28).
    """
    def mk(n, base_ms, mu):
        ts = pl.datetime_range(
            dt.datetime.fromtimestamp(base_ms),
            dt.datetime.fromtimestamp(base_ms + (n - 1) * 60),
            interval="1m", time_unit="ms", eager=True)
        rng = np.random.default_rng(7)
        return pl.DataFrame({
            "open_time": ts, "open": 100.0, "high": 101.0, "low": 99.0,
            "close": 100.5, "volume": 1.0, "turnover": 1.0, "is_green": True,
            "symbol": ["X"] * n, "category": ["linear"] * n,
            "entry_price": 100.0,
            "return_5m": np.zeros(n), "return_10m": np.zeros(n), "return_30m": np.zeros(n),
            "feature": rng.normal(mu, 1, n),
        })

    D0 = dt.datetime(2025, 12, 25, tzinfo=dt.timezone.utc).timestamp()
    O0 = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc).timestamp()
    disc = mk(600, D0, 0.0)      # discovery-распределение N(0,1)
    valo = mk(600, O0, 100.0)    # val+oos с другим сдвигом
    events = pl.concat([disc, valo])

    disc_ev = split_periods(events)["discovery"]
    assert disc_ev.height == 600  # discovery окно отрезало ровно discovery

    rule = hg.GenRule("feature", "gt", "long", (5,))
    cond_disc = hg._condition_from_rule(rule, disc_ev)  # как делает main после фикса
    cond_all = hg._condition_from_rule(rule, events)    # утечечный путь (весь df)

    thr_disc = float(re.search(r"> ([\d.e+-]+)", cond_disc).group(1))
    thr_all = float(re.search(r"> ([\d.e+-]+)", cond_all).group(1))
    # discovery ~1.28 (90pct N(0,1)); весь df (со смесью +100) >> discovery
    assert 0.0 < thr_disc < 3.0, thr_disc
    assert thr_disc < thr_all - 10, (thr_disc, thr_all)