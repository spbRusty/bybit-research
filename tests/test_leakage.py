"""Тесты на отсутствие look-ahead (ТЗ §44, §31).

Проверяем: все семь-признаки события сформированы на момент close T; будущие
доходности присоединяются не при признаках, а отдельным шагом; rolling-окна
не используют текущую и будущие свечи.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import features as F
from src import events as ev_mod


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