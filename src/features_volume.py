"""Расширенные признаки объёма (ТЗ §7, §11). Без look-ahead: окна по прошлым свечам.

Окна: 5, 10, 20, 30, 60, 120, 240 (параметризованы в конфиге при необходимости).
Не создаём комбинаторный взрыв — типовые окна для ключевых признаков.
"""
from __future__ import annotations

import polars as pl

from src.registry import Feature, register


_WINDOWS = (5, 10, 20, 30, 60, 120, 240)


def _mk(fid, name, desc, formula, lookback=1, cost="low", realtime=True):
    return Feature(fid, name, "volume", desc, formula, "features_volume",
                   "1m", lookback, ("ohlcv",), realtime, cost)


# Базовые объёмные признаки
register(_mk("volume", "Объём", "объём свечи", "volume"))
register(_mk("quote_volume", "Объём в USD", "turnover", "turnover"))
register(_mk("volume_change", "Изменение объёма", "v_t / v_{t-1} - 1", "v/v_prev-1", 2))
register(_mk("volume_accel", "Ускорение объёма", "diff(volume_change)", "dv/dt", 3))
register(_mk("volume_percentile", "Процентиль объёма (60)", "rank(v,60)", "pct_rank(v,60)", 60))
register(_mk("volume_zscore", "z-score объёма (60)", "(v-mean(v,60))/std(v,60)",
             "z(v,60)", 60))
register(_mk("volume_persistence", "Устойчивость объёма",
             "corr(v_t, v_{t-1}, 60)", "corr(v,v_prev,60)", 60))
register(_mk("volume_range_ratio", "Объём/диапазон", "v/(h-l)", "v/(h-l)"))
register(_mk("volume_abs_ret_ratio", "Объём/абс. доходность", "v/|ret|", "v/|ret|"))

# rolling-объёмы и тренды
for w in _WINDOWS:
    register(_mk(f"volume_ma_{w}", f"Скользящее среднее объёма ({w})",
                 f"mean(v,{w})", f"mean(v,{w})", w))
    register(_mk(f"volume_std_{w}", f"Ст.откл. объёма ({w})",
                 f"std(v,{w})", f"std(v,{w})", w))
    register(_mk(f"volume_trend_{w}", f"Тренд объёма ({w})",
                 f"slope(v,{w})", f"slope(v,{w})", w))

# относительный объём и аномалии на разных окнах
for w in (20, 60):
    register(_mk(f"relative_volume_{w}", f"Относительный объём ({w})",
                 f"v / median(v,{w})", f"v/median(v,{w})", w))
    register(_mk(f"volume_anomaly_{w}", f"Аномалия объёма ({w})",
                 f"(v - mean(v,{w}))/std(v,{w})", f"z(v,{w})", w))

# Ликвидность (§11)
register(_mk("liquidity_stability", "Устойчивость ликвидности",
             "доля свечей с объёмом >= медианы за период (240)",
             "stable(v,240)", 240, cost="medium"))
register(_mk("liquidity_percentile_60", "Процентиль ликвидности (60)",
             "pct_rank(v,60)", "pct_rank(v,60)", 60))
register(_mk("liquidity_zscore_60", "z-score ликвидности (60)",
             "z(v,60)", "z(v,60)", 60))
register(_mk("liquidity_change_60", "Изменение ликвидности",
             "liquidity_t/liquidity_{t-1} - 1", "d(liq)", 61))


def add_volume_features(df: pl.DataFrame, windows: tuple[int, ...] = _WINDOWS) -> pl.DataFrame:
    """Объёмные признаки. Требуется volume/turnover, candle_range, candle_return_1m.

    Без look-ahead: rolling-окна по предыдущим свечам (shift 1 внутри окна).
    """
    v = pl.col("volume")
    rng = pl.col("candle_range").clip(1e-12)
    ret = pl.col("candle_return_1m").abs().clip(1e-12)
    out = df.with_columns([
        v.shift(1).rolling_mean(60, min_samples=1).alias("_v_ma60"),
        v.shift(1).rolling_std(60, min_samples=1).clip(1e-12).alias("_v_std60"),
        v.shift(1).rolling_median(60, min_samples=1).alias("_v_med60"),
    ])
    out = out.with_columns([
        v.alias("volume"),
        pl.col("turnover").alias("quote_volume"),
        (v / v.shift(1).clip(1e-12) - 1).alias("volume_change"),
        (v - v.shift(1)).alias("volume_accel"),
        (v / pl.col("_v_med60").clip(1e-12)).alias("volume_percentile"),
        ((v - pl.col("_v_ma60")) / pl.col("_v_std60")).alias("volume_zscore"),
        (v / v.shift(1).rolling_mean(60, min_samples=1).clip(1e-12)).alias("volume_persistence"),
        (v / rng).alias("volume_range_ratio"),
        (v / ret).alias("volume_abs_ret_ratio"),
        (v / pl.col("_v_med60").clip(1e-12)).alias("relative_volume_60"),
        # relative_volume (bare) — contract для prefilter events.py (§16)
        (v / pl.col("_v_med60").clip(1e-12)).alias("relative_volume"),
        # ликвидность (§11)
        (v >= pl.col("_v_med60")).rolling_mean(240, min_samples=1)
        .alias("liquidity_stability"),
    ])
    # rolling-признаки на разных окнах
    for w in windows:
        out = out.with_columns([
            v.shift(1).rolling_mean(w, min_samples=1).alias(f"volume_ma_{w}"),
            v.shift(1).rolling_std(w, min_samples=1).alias(f"volume_std_{w}"),
            (v.shift(1).rolling_mean(w, min_samples=1) -
             v.shift(1).rolling_mean(w, min_samples=1).shift(w))
            .alias(f"volume_trend_{w}"),
            (v / v.shift(1).rolling_median(w, min_samples=1).clip(1e-12))
            .alias(f"relative_volume_{w}"),
            ((v - v.shift(1).rolling_mean(w, min_samples=1)) /
             v.shift(1).rolling_std(w, min_samples=1).clip(1e-12))
            .alias(f"volume_anomaly_{w}"),
        ])
    return out.drop(["_v_ma60", "_v_std60", "_v_med60"])
