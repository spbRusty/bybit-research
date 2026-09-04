"""Статистический контур (ТЗ §26): зависимость наблюдений, bootstrap, Newey-West.

Простого ttest_1samp (scipy) недостаточно — события одной монеты и близкие
события зависимы. Реализуем: block bootstrap (temporal), cluster bootstrap
(по symbol), Newey-West / HAC. Стандартный t-test остаётся вспомогательной
метрикой, но не единственным доказательством.
"""
from __future__ import annotations

import numpy as np
from scipy import stats

from config.settings import load_toml

_R = load_toml("research.toml")


# --------------------------------------------------------------------------
# Block bootstrap (temporal) — зависимость близких по времени наблюдений (§26)
# --------------------------------------------------------------------------

def block_bootstrap_ci(values: np.ndarray, block: int = 25,
                       n_boot: int = 2000, alpha: float = 0.05,
                       seed: int = 42) -> tuple[float, float, float]:
    """Доверительный интервал среднего через block bootstrap."""
    vals = np.asarray(values, dtype=float)
    vals = vals[~np.isnan(vals)]
    n = vals.size
    if n == 0:
        return (np.nan, np.nan, np.nan)
    if n <= block:
        block = max(1, n // 2)
    rng = np.random.default_rng(seed)
    n_blocks = int(np.ceil(n / block))
    all_starts = rng.integers(0, n, size=(n_boot, n_blocks))
    offsets = np.arange(block)
    CHUNK = 100
    block_means = np.empty((n_boot, n_blocks), dtype=np.float64)
    for i in range(0, n_boot, CHUNK):
        chunk = min(CHUNK, n_boot - i)
        idx = ((all_starts[i:i+chunk, :, None] + offsets[None, None, :]) % n)
        block_means[i:i+chunk] = vals[idx].mean(axis=2)
    means = block_means.mean(axis=1)
    lo, hi = np.percentile(means, (100 * alpha / 2, 100 * (1 - alpha / 2)))
    return (vals.mean(), float(lo), float(hi))


# --------------------------------------------------------------------------
# Cluster bootstrap (по symbol) — зависимость внутри монеты (§26)
# --------------------------------------------------------------------------

def cluster_bootstrap_ci(values: np.ndarray, labels: np.ndarray,
                         n_boot: int = 2000, alpha: float = 0.05,
                         seed: int = 42) -> tuple[float, float, float]:
    """Доверительный интервал через ресемплирование целых кластеров (symbol).

    Каждая итерация выбирает с повторением кластеры (монеты) и усредняет
    их наблюдения — корректно при внутрикластерной зависимости.
    """
    vals = np.asarray(values, dtype=float)
    labels = np.asarray(labels)
    mask = ~np.isnan(vals)
    vals, labels = vals[mask], labels[mask]
    n = vals.size
    if n == 0:
        return (np.nan, np.nan, np.nan)
    clusters, inv = np.unique(labels, return_inverse=True)
    n_clusters = clusters.size
    rng = np.random.default_rng(seed)
    order = np.argsort(inv)
    sorted_inv = inv[order]
    sorted_vals = vals[order]
    cluster_starts = np.searchsorted(sorted_inv, np.arange(n_clusters))
    cluster_counts = np.diff(np.append(cluster_starts, n))
    cluster_sums = np.add.reduceat(sorted_vals, cluster_starts)
    cluster_means = cluster_sums / np.maximum(cluster_counts, 1)
    chosen = rng.choice(n_clusters, size=(n_boot, n_clusters), replace=True)
    sel_counts = cluster_counts[chosen]
    sel_sums = cluster_means[chosen] * sel_counts
    means = sel_sums.sum(axis=1) / sel_counts.sum(axis=1)
    lo, hi = np.percentile(means, (100 * alpha / 2, 100 * (1 - alpha / 2)))
    return (vals.mean(), float(lo), float(hi))


# --------------------------------------------------------------------------
# Newey-West / HAC t-statistic — автокорреляция (§26)
# --------------------------------------------------------------------------

def _nw_variance(x: np.ndarray, max_lag: int | None = None) -> float:
    """Оценка долгосрочной дисперсии Ньюи-Веста (HAC) для среднего."""
    x = np.asarray(x, dtype=float)
    x = x[~np.isnan(x)]
    n = x.size
    if n == 0:
        return np.nan
    xc = x - x.mean()
    if max_lag is None:
        max_lag = int(np.floor(4 * (n / 100) ** (2 / 9)))  # правило Ньюи-Веста
    max_lag = max(1, min(max_lag, n - 1))
    var0 = (xc @ xc) / n
    cov_sum = 0.0
    for l in range(1, max_lag + 1):
        gamma = np.dot(xc[:-l], xc[l:]) / n
        w = 1.0 - l / (max_lag + 1)  # ядро Бартлетта
        cov_sum += 2 * w * gamma
    return var0 + cov_sum


def hac_ttest(values: np.ndarray, mu: float = 0.0) -> tuple[float, float]:
    """t-статистика с HAC-поправкой на автокорреляцию. Возвращает (t, p)."""
    x = np.asarray(values, dtype=float)
    x = x[~np.isnan(x)]
    n = x.size
    if n == 0:
        return (np.nan, np.nan)
    se = np.sqrt(_nw_variance(x) / n)
    if se == 0 or np.isnan(se):
        return (np.nan, np.nan)
    t = (x.mean() - mu) / se
    p = 2 * (1 - stats.t.cdf(abs(t), df=max(1, n - 1)))
    return (float(t), float(p))


# --------------------------------------------------------------------------
# Полная зависимостная проверка для подвыборки событий
# --------------------------------------------------------------------------

def dependency_stats(events_returns: np.ndarray, symbols: list[str],
                     block: int = 25, seed: int = 42) -> dict:
    """Полный набор зависимостных метрик для одной гипотезы."""
    ret = np.asarray(events_returns, dtype=float)
    sym_arr = np.asarray(symbols, dtype=str)
    # 1. OLS t-test (вспомогательная метрика)
    t_ols, p_ols = stats.ttest_1samp(ret, 0.0)
    # 2. Block bootstrap (temporal)
    m_bb, lo_bb, hi_bb = block_bootstrap_ci(ret, block=block, seed=seed)
    # 3. Cluster bootstrap (symbol)
    m_cb, lo_cb, hi_cb = cluster_bootstrap_ci(ret, sym_arr, seed=seed)
    # 4. Newey-West HAC
    t_hac, p_hac = hac_ttest(ret)
    return {
        "t_ols": float(t_ols), "p_ols": float(p_ols),
        "block_bootstrap_mean": m_bb, "block_bootstrap_ci": [lo_bb, hi_bb],
        "cluster_bootstrap_mean": m_cb, "cluster_bootstrap_ci": [lo_cb, hi_cb],
        "t_hac": t_hac, "p_hac": p_hac,
        "n": int(ret.size), "n_symbols": int(np.unique(sym_arr).size),
    }
