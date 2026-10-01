"""Проверка экрана: быстрый sweep через cumsum == наивный пересчёт масок."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.screen import MIN_N, QUANTILES, sweep


def _brute(feat, x, y, cost):
    out = []
    for thr in np.unique(np.quantile(x[np.isfinite(x) & np.isfinite(y)], QUANTILES)):
        for side in ("long", "short"):
            mask = np.isfinite(x) & np.isfinite(y)
            mask &= x > thr if side == "long" else x < thr
            r = y[mask]
            n = r.size
            if n < MIN_N:
                continue
            sign = 1.0 if side == "long" else -1.0
            mean = float(r.mean())
            sd = float(r.std(ddof=1))
            out.append((thr, side, n, sign * mean,
                        sign * mean / (sd / np.sqrt(n)), sign * mean - cost))
    return out


def test_sweep_matches_brute_force_with_nans_and_duplicates():
    rng = np.random.default_rng(7)
    x = rng.normal(size=4000)
    y = 0.0008 * x + rng.normal(scale=0.01, size=4000)
    x[rng.random(4000) < 0.05] = np.nan          # пропуски в признаке
    y[rng.random(4000) < 0.05] = np.nan          # пропуски в таргете
    x[::7] = 0.42                                 # много равных значений -> tie на порогах
    cost = 0.002

    got = sweep("f", x, {5: y}, cost)
    want = _brute("f", x, y, cost)

    assert len(got) == len(want) > 0
    for g, (thr, side, n, mean, t, mnet) in zip(got, want):
        assert (g["threshold"], g["side"]) == (thr, side)
        assert g["n"] == n
        assert np.isclose(g["mean_net"], mnet, atol=1e-12)
        assert np.isclose(g["t_stat"], t, rtol=1e-9, atol=1e-12)


def test_sweep_skips_horizons_with_too_few_rows():
    rng = np.random.default_rng(1)
    x = rng.normal(size=200)
    y = rng.normal(size=200)
    y[:150] = np.nan                                # остаётся 50 < MIN_N
    assert sweep("f", x, {5: y}, 0.002) == []
