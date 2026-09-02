"""Оркестратор признаков (ТЗ §5-21). Надстройка над модулями feature-категорий.

add_features(df, btc, eth=None, is_spike=None) собирает полный feature-space из
зарегистрированных модулей. Каждый генератор вызывается только при наличии
необходимых входных колонок (устойчиво к отсутствию данных рынка).
"""
from __future__ import annotations

import polars as pl

from src import features_candle as fc
from src import features_volume as fv
from src import features_volatility as fvol
from src import features_momentum as fm
from src import features_structure as fs
from src import features_cross as fx
from src import features_regime as fr
from src import features_context as fctx
from src.registry import REGISTRY


def _has(df: pl.DataFrame, col: str) -> bool:
    return col in df.columns


def time_features(df: pl.DataFrame) -> pl.DataFrame:
    """Временные признаки (§15/§18): hour_utc, minute_utc, day_of_week, session."""
    return df.with_columns([
        pl.col("open_time").dt.hour().alias("hour_utc"),
        pl.col("open_time").dt.minute().alias("minute_utc"),
        pl.col("open_time").dt.weekday().alias("day_of_week"),
        pl.col("open_time").dt.day().alias("day_of_month"),
        pl.col("open_time").dt.week().alias("week_of_year"),
    ]).with_columns(
        pl.when(pl.col("hour_utc").is_between(0, 6)).then(pl.lit("asia"))
         .when(pl.col("hour_utc").is_between(7, 12)).then(pl.lit("europe"))
         .otherwise(pl.lit("america")).alias("session"))


def add_features(df: pl.DataFrame, btc: pl.DataFrame | None = None,
                 eth: pl.DataFrame | None = None,
                 is_spike: pl.Series | None = None) -> pl.DataFrame:
    """Полный feature-space. Возвращает df с зарегистрированными признаками.

    Порядок важен: базовые (candle) -> производные (volume/vol/momentum/...) ->
    структура/режим -> cross (требует market) -> context (требует is_spike).
    """
    # 0. Время (контракт H001-H008: hour_utc для сессий)
    out = time_features(df)
    # 1. Базовые свечи (всегда доступны)
    out = fc.add_candle_features(out)
    # backward-compat алиасы для H001-H008 (старые имена wick/body)
    out = out.with_columns([
        pl.col("candle_upper_wick").alias("upper_wick"),
        pl.col("candle_lower_wick").alias("lower_wick"),
        pl.col("candle_body").alias("body"),
        pl.col("candle_range").alias("range"),
    ])
    out = fc.add_multi_timeframe_returns(out)
    out = fc.add_return_derivatives(out)
    out = fv.add_volume_features(out)
    out = fvol.add_volatility_features(out)   # требует candle_true_range, candle_return_1m
    out = fm.add_momentum_features(out)       # требует atr, roc_20 (из candles/vol)

    # 2. Структура (требует candle-колонки)
    out = fs.add_structure_features(out)

    # 3. Cross-признаки (требуют roc_20 из momentum + candle_return_1m)
    if btc is not None:
        out = fx.add_cross_features(out, btc, "btc")
    if eth is not None:
        out = fx.add_cross_features(out, eth, "eth")

    # 4. Режим (требует trend_strength, volume; btc_trend_regime при наличии btc)
    out = fr.add_regime_features(out)

    # 5. Контекст событий (требует is_spike)
    if is_spike is not None and len(is_spike) == out.height:
        out = out.with_columns(pl.Series("is_spike", is_spike))
        out = fctx.add_context_features(out)

    return out
