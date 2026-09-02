"""Расширенные признаки волатильности (ТЗ §8). Без look-ahead: окна по прошлым свечам."""
from __future__ import annotations

import polars as pl

from src.registry import Feature, register

_WINDOWS = (10, 20, 60)


def _mk(fid, name, desc, formula, lookback=1, cost="low"):
    return Feature(fid, name, "volatility", desc, formula, "features_volatility",
                   "1m", lookback, ("ohlcv",), True, cost)


# --- Базовые меры ---
register(_mk("atr", "ATR", "средний true range (14)", "mean(TR,14)", 15))
register(_mk("atr_norm", "Нормализованный ATR", "ATR/close", "ATR/close", 15))
register(_mk("true_range", "True range", "max(h-l,|h-pc|,|l-pc|)", "TR", 2))
register(_mk("normalized_range", "Нормализованный диапазон", "(h-l)/close", "(h-l)/close"))

# --- Rolling волатильность ---
for w in _WINDOWS:
    register(_mk(f"realized_vol_{w}", f"Реализованная волатильность ({w})",
                 f"std(return,{w})", f"std(ret,{w})", w))
    register(_mk(f"vol_percentile_{w}", f"Процентиль волатильности ({w})",
                 f"pct_rank(realized_vol,{w})", f"pct_rank(vol,{w})", w * 2))
    register(_mk(f"vol_zscore_{w}", f"z-score волатильности ({w})",
                 f"z(realized_vol,{w})", f"z(vol,{w})", w * 2))

# Parkinson (по high/low) и Garman-Klass варианты
register(_mk("parkinson_vol_20", "Волатильность Паркинсона (20)",
             "0.5*mean((ln(h/l))^2)", "Parkinson(20)", 20))
register(_mk("vol_change_60", "Изменение волатильности", "vol_t/vol_{t-60} - 1",
             "d(vol)", 61))
register(_mk("vol_accel", "Ускорение волатильности", "diff(vol_change)", "d2(vol)", 62))
register(_mk("vol_compression", "Сжатие волатильности", "realized_vol_60/realized_vol_240 - 1",
             "vol60/vol240-1", 241, cost="medium"))
register(_mk("vol_expansion", "Расширение волатильности", "realized_vol_60/realized_vol_10 - 1",
             "vol60/vol10-1", 61))

# Volatility regime
register(_mk("volatility_regime", "Режим волатильности",
             "very_low|low|normal|high|very_high (по процентилю 240)",
             "regime(vol)", 241, cost="medium"))


def add_volatility_features(df: pl.DataFrame) -> pl.DataFrame:
    """Волатильность на минутных свечах. Требует candle_true_range, candle_return_1m."""
    tr = pl.col("candle_true_range")
    c = pl.col("close").clip(1e-12)
    ret = pl.col("candle_return_1m")
    h, l = pl.col("high"), pl.col("low")
    out = df.with_columns([
        tr.rolling_mean(14, min_samples=1).alias("atr"),
        (tr.rolling_mean(14, min_samples=1) / c).alias("atr_norm"),
        tr.alias("true_range"),
        ((h - l) / c).alias("normalized_range"),
        ret.rolling_std(60, min_samples=1).alias("realized_vol_60"),
        (0.5 * ((h / l.clip(1e-12)).log().pow(2))
         .rolling_mean(20, min_samples=1)).sqrt().alias("parkinson_vol_20"),
    ])
    # производные (нужен второй проход для rolling-pct-rank)
    for w in _WINDOWS:
        out = out.with_columns(ret.rolling_std(w, min_samples=1).alias(f"realized_vol_{w}"))
    rv60 = pl.col("realized_vol_60")
    rv240 = ret.rolling_std(240, min_samples=1).alias("realized_vol_240")
    out = out.with_columns([rv240, (rv60 / rv240 - 1).alias("vol_compression"),
                            (rv60 / ret.rolling_std(10, min_samples=1).clip(1e-12) - 1)
                            .alias("vol_expansion"),
                            (rv60 - rv60.shift(60)).alias("vol_change_60")])
    # percentile/zscore волатильности + regime
    out = out.with_columns([
        rv60.rolling_quantile(0.9, interpolation="nearest", window_size=120,
                              min_samples=1).alias("vol_p90_120"),
        ((rv60 - rv60.rolling_mean(120, min_samples=1)) /
         rv60.rolling_std(120, min_samples=1).clip(1e-12)).alias("vol_zscore_60"),
    ])
    p_vl = rv240.rolling_quantile(0.1, interpolation="nearest", window_size=240, min_samples=1)
    p_l = rv240.rolling_quantile(0.3, interpolation="nearest", window_size=240, min_samples=1)
    p_h = rv240.rolling_quantile(0.7, interpolation="nearest", window_size=240, min_samples=1)
    p_vh = rv240.rolling_quantile(0.9, interpolation="nearest", window_size=240, min_samples=1)
    out = out.with_columns(
        pl.when(rv60 <= p_vl).then(pl.lit("very_low"))
         .when(rv60 <= p_l).then(pl.lit("low"))
         .when(rv60 <= p_h).then(pl.lit("normal"))
         .when(rv60 <= p_vh).then(pl.lit("high"))
         .otherwise(pl.lit("very_high")).alias("volatility_regime"))
    return out
