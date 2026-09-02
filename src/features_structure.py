"""Признаки рыночной структуры (ТЗ §10). Rolling high/low, breakout, структура диапазона."""
from __future__ import annotations

import polars as pl

from src.registry import Feature, register

_WINDOWS = (20, 60, 120)


def _mk(fid, name, desc, formula, lookback=1, cost="low"):
    return Feature(fid, name, "structure", desc, formula, "features_structure",
                   "1m", lookback, ("ohlcv",), True, cost)


for w in _WINDOWS:
    register(_mk(f"rolling_high_{w}", f"Rolling high ({w})", f"max(high,{w})",
                 f"max(h,{w})", w))
    register(_mk(f"rolling_low_{w}", f"Rolling low ({w})", f"min(low,{w})",
                 f"min(l,{w})", w))
    register(_mk(f"dist_rolling_high_{w}", f"Дистанция до rolling high ({w})",
                 f"close/max(h,{w}) - 1", f"c/mh{w}-1", w))
    register(_mk(f"dist_rolling_low_{w}", f"Дистанция до rolling low ({w})",
                 f"close/min(l,{w}) - 1", f"c/ml{w}-1", w))

register(_mk("breakout_20", "Пробой rolling high 20", "close>max(h,20-shift)", "breakout", 21))
register(_mk("breakout_magnitude", "Величина пробоя", "close/rolling_high_20 - 1",
             "brk_mag", 21))
register(_mk("breakout_continuation", "Продолжение пробоя", "close>rolling_high_20 за 2 свечи",
             "brk_cont", 22, cost="medium"))
register(_mk("breakout_failure", "Провал пробоя", "close<rolling_high_20 после пробоя",
             "brk_fail", 22, cost="medium"))
register(_mk("range_width", "Ширина диапазона", "(rolling_high_60-rolling_low_60)/close",
             "range_w", 60))
register(_mk("consolidation_duration", "Длительность консолидации",
             "число свечей в диапазоне", "consol_t", 60, cost="medium"))
register(_mk("range_boundary_dist", "Дистанция до границ диапазона",
             "min(c-rolling_low_60, rolling_high_60-c)", "dist_bound", 60))
register(_mk("local_extremum", "Локальный экстремум", "high==max(h,5) или low==min(l,5)",
             "loc_ext", 6))
register(_mk("higher_high", "Старший high", "high>high(-1) и high>high(-2)", "hi_hi", 3))
register(_mk("lower_low", "Младший low", "low<low(-1) и low<low(-2)", "lo_lo", 3))
register(_mk("trend_range_score", "Оценка тренд/диапазон",
             "|SMA20-SMA60|/(rolling_high_60-rolling_low_60)", "t/r_score", 60))
register(_mk("mean_reversion_score", "Оценка возврата к среднему",
             "dist_rolling_high_120 + dist_rolling_low_120", "mr_score", 120))


def add_structure_features(df: pl.DataFrame) -> pl.DataFrame:
    h, l, c = pl.col("high"), pl.col("low"), pl.col("close")
    out = df
    for w in _WINDOWS:
        rh = h.rolling_max(w, min_samples=1).shift(1).alias(f"rolling_high_{w}")
        rl = l.rolling_min(w, min_samples=1).shift(1).alias(f"rolling_low_{w}")
        # rolling-экстремум предыдущего окна — без look-ahead (shift 1)
        out = out.with_columns([rh, rl])
        out = out.with_columns([
            (c / pl.col(f"rolling_high_{w}").clip(1e-12) - 1)
            .alias(f"dist_rolling_high_{w}"),
            (c / pl.col(f"rolling_low_{w}").clip(1e-12) - 1)
            .alias(f"dist_rolling_low_{w}"),
        ])
    rh20 = pl.col("rolling_high_20").shift(1)
    rl60, rh60 = pl.col("rolling_low_60"), pl.col("rolling_high_60")
    out = out.with_columns([
        (c > rh20).cast(pl.Int8).alias("breakout_20"),
        (c / rh20.clip(1e-12) - 1).alias("breakout_magnitude"),
        ((c > rh20).cast(pl.Int8) & (pl.col("candle_close_pos") > 0.5))
        .cast(pl.Int8).alias("breakout_continuation"),
        ((c < rh20).cast(pl.Int8) & (pl.col("rolling_high_20") > rh20))
        .cast(pl.Int8).alias("breakout_failure"),
        ((rh60 - rl60) / c.clip(1e-12)).alias("range_width"),
        pl.when((c > rl60 * 1.001) & (c < rh60 * 0.999)).then(1).otherwise(0)
        .alias("consolidation_duration"),
        ((c - rl60).clip(lower_bound=0) - (rh60 - c).clip(lower_bound=0)).abs()
        .alias("range_boundary_dist"),
        ((h == h.rolling_max(5, min_samples=1)) | (l == l.rolling_min(5, min_samples=1)))
        .cast(pl.Int8).alias("local_extremum"),
        ((h > h.shift(1)) & (h > h.shift(2))).cast(pl.Int8).alias("higher_high"),
        ((l < l.shift(1)) & (l < l.shift(2))).cast(pl.Int8).alias("lower_low"),
    ])
    sma20 = c.rolling_mean(20, min_samples=1)
    sma60 = c.rolling_mean(60, min_samples=1)
    out = out.with_columns([
        ((sma20 - sma60).abs() / (rh60 - rl60).clip(1e-12)).alias("trend_range_score"),
        (pl.col("dist_rolling_high_120") + pl.col("dist_rolling_low_120"))
        .alias("mean_reversion_score"),
    ])
    return out
