"""Признаки свечей (ТЗ §15, §24-26). Все признаки в момент T используют данные <= T (ТЗ §27).

Временные признаки — по UTC. BTC — фактор рынка, не фильтр входа (§25).
"""
from __future__ import annotations

import polars as pl
from config.settings import load_toml

_FEAT = load_toml("features.toml")
_MKT = load_toml("market.toml")


def time_features(df: pl.DataFrame) -> pl.DataFrame:
    """§15: hour_utc, minute_utc, day_of_week, day_of_month, week_of_year, session."""
    df = df.with_columns([
        pl.col("open_time").dt.hour().alias("hour_utc"),
        pl.col("open_time").dt.minute().alias("minute_utc"),
        pl.col("open_time").dt.weekday().alias("day_of_week"),
        pl.col("open_time").dt.day().alias("day_of_month"),
        pl.col("open_time").dt.week().alias("week_of_year"),
    ])
    return df.with_columns(
        pl.when(pl.col("hour_utc").is_between(0, 6)).then(pl.lit("asia"))
         .when(pl.col("hour_utc").is_between(7, 12)).then(pl.lit("europe"))
         .otherwise(pl.lit("america")).alias("session"))


def candle_features(df: pl.DataFrame) -> pl.DataFrame:
    """§24: доходности, range/body/фитили, объём, относительный объём, волатильность.

    rolling-окна считаются по прошедшим свечам (shift на 1) — без lookahead.
    """
    vol_med = _FEAT["vol_window_min"]
    out = df.with_columns([
        pl.col("close").pct_change().alias("return_1m"),
        (pl.col("high") - pl.col("low")).alias("range"),
        ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("range_pct"),
        (pl.col("close") - pl.col("open")).abs().alias("body"),
        ((pl.col("close") - pl.col("open")).abs() / pl.col("close")).alias("body_pct"),
        ((pl.col("high") - pl.max_horizontal("open", "close")) / pl.col("close"))
        .alias("upper_wick"),
        ((pl.min_horizontal("open", "close") - pl.col("low")) / pl.col("close"))
        .alias("lower_wick"),
    ])
    # производные над базовыми полями (нужно два прохода: ссылки в том же with_columns нельзя)
    out = out.with_columns([
        # относительный объём: текущий / медиана окна ПРЕДЫДУЩИХ свечей
        (pl.col("volume") / pl.col("volume").shift(1)
         .rolling_median(vol_med, min_samples=1)).alias("relative_volume"),
        # z-score объёма по окну предыдущих свечей
        ((pl.col("volume") - pl.col("volume").shift(1)
          .rolling_mean(vol_med, min_samples=1)) /
         pl.col("volume").shift(1).rolling_std(vol_med, min_samples=1).clip(1e-12))
        .alias("volume_zscore"),
        # относительный диапазон: (high-low)/close / медиану окна предыдущих свечей
        (pl.col("range_pct") / pl.col("range_pct").shift(1)
         .rolling_median(vol_med, min_samples=1)).alias("relative_range"),
        # относительная волатильность: текущий диапазон / среднеисторический (день)
        (pl.col("range_pct") / pl.col("range_pct").shift(1)
         .rolling_mean(1440, min_samples=1)).alias("relative_volatility"),
    ])
    # многоминутные доходности (3/5/10/15/30) — закрытие T к закрытию T-n
    for n in (3, 5, 10, 15, 30):
        out = out.with_columns(
            (pl.col("close") / pl.col("close").shift(n) - 1).alias(f"return_{n}m"))
    return out


def btc_features(df: pl.DataFrame, btc: pl.DataFrame) -> pl.DataFrame:
    """§25: возврат/объём/волатильность BTC, выровненные к свече символа без lookahead."""
    w = _FEAT["btc_vol_window_min"]
    b = btc.sort("open_time").with_columns([
        pl.col("close").pct_change().alias("btc_return"),
        (pl.col("volume") / pl.col("volume").shift(1)
         .rolling_median(w, min_samples=1)).alias("btc_relative_volume"),
        ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("btc_range"),
    ])
    b = b.select([
        "open_time", "btc_return", "btc_relative_volume", "btc_range",
        pl.col("btc_range").rolling_mean(w, min_samples=1).alias("btc_volatility"),
    ])
    # asof: для каждой свечи символа берём последнее значение BTC <= T
    return df.join_asof(b, on="open_time", strategy="backward")


def add_features(df: pl.DataFrame, btc: pl.DataFrame | None = None) -> pl.DataFrame:
    df = time_features(df)
    df = candle_features(df)
    if btc is not None:
        df = btc_features(df, btc)
    return df