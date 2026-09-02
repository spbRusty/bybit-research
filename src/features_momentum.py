"""Расширенные признаки momentum (ТЗ §9). Без look-ahead: подтверждённые значения индикаторов."""
from __future__ import annotations

import polars as pl

from src.registry import Feature, register

_WINDOWS = (5, 20, 60)


def _mk(fid, name, desc, formula, lookback=1, cost="low"):
    return Feature(fid, name, "momentum", desc, formula, "features_momentum",
                   "1m", lookback, ("ohlcv",), True, cost)


register(_mk("roc_5", "ROC 5", "close/close(-5)-1", "ROC(5)", 6))
register(_mk("roc_20", "ROC 20", "close/close(-20)-1", "ROC(20)", 21))
register(_mk("momentum_10", "Momentum 10", "close-close(-10)", "MOM(10)", 11))
register(_mk("rsi_14", "RSI 14", "RSI(14)", "RSI", 15))
register(_mk("ema_20", "EMA 20", "EMA(close,20)", "EMA(20)", 20))
register(_mk("sma_20", "SMA 20", "SMA(close,20)", "SMA(20)", 20))
register(_mk("ema_dist_20", "Дистанция от EMA 20", "close/EMA20 - 1", "c/ema20-1", 20))
register(_mk("sma_dist_20", "Дистанция от SMA 20", "close/SMA20 - 1", "c/sma20-1", 20))
register(_mk("ma_slope_20", "Наклон MA 20", "SMA20_t/SMA20_{t-5} - 1", "slope(MA20)", 25))
register(_mk("momentum_accel", "Ускорение momentum", "diff(roc_20)", "d2(close)", 22))
register(_mk("short_long_mom_ratio", "Соотношение краткосрочного моментума к долгосрочному",
             "roc_5/roc_60", "roc5/roc60", 61))
register(_mk("trend_strength", "Сила тренда", "|SMA20-SMA60|/ATR",
             "|ma20-ma60|/atr", 61))


def _ema(s: pl.Expr, span: int) -> pl.Expr:
    # ewm_mean (Polars) — экспоненциальное сглаживание с alpha=2/(span+1)
    return s.ewm_mean(alpha=2.0 / (span + 1), min_samples=1)


def add_momentum_features(df: pl.DataFrame) -> pl.DataFrame:
    c = pl.col("close")
    out = df.with_columns([
        (c / c.shift(5) - 1).alias("roc_5"),
        (c / c.shift(20) - 1).alias("roc_20"),
        (c - c.shift(10)).alias("momentum_10"),
        _rsi(c, 14).alias("rsi_14"),
        _ema(c, 20).alias("ema_20"),
        c.rolling_mean(20, min_samples=1).alias("sma_20"),
    ])
    ema20 = pl.col("ema_20")
    sma20 = pl.col("sma_20")
    out = out.with_columns([
        (c / ema20 - 1).alias("ema_dist_20"),
        (c / sma20 - 1).alias("sma_dist_20"),
        (sma20 / sma20.shift(5) - 1).alias("ma_slope_20"),
        (pl.col("roc_20") - pl.col("roc_20").shift(1)).alias("momentum_accel"),
    ])
    # SMA60 для trend_strength и short/long ratio
    sma60 = c.rolling_mean(60, min_samples=1).alias("sma_60")
    out = out.with_columns([
        sma60,
        (pl.col("roc_5").clip(-0.2, 0.2) / pl.col("roc_20").clip(-0.2, 0.2))
        .alias("short_long_mom_ratio"),
        ((sma20 - sma60).abs() / pl.col("atr").clip(1e-12)).alias("trend_strength"),
    ])
    return out


def _rsi(c: pl.Expr, period: int) -> pl.Expr:
    """RSI по Уайлдеру: EWM коэффициент 1/period."""
    delta = c.diff()
    up = delta.clip(lower_bound=0).ewm_mean(alpha=1.0 / period, min_samples=1)
    down = (-delta).clip(lower_bound=0).ewm_mean(alpha=1.0 / period, min_samples=1)
    rs = up / down.clip(1e-12)
    return (100 - 100 / (1 + rs)).alias(f"rsi_{period}")
