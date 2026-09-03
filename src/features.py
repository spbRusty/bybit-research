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
from src import features_market as fmarket
from src import features_external as fext
from src.registry import REGISTRY


def _has(df: pl.DataFrame, col: str) -> bool:
    return col in df.columns


def time_features(df: pl.DataFrame) -> pl.DataFrame:
    """Временные признаки (§15/§18): час, минута, день недели, сессия и смещения."""
    out = df.with_columns([
        pl.col("open_time").dt.hour().alias("hour_utc"),
        pl.col("open_time").dt.minute().alias("minute_utc"),
        pl.col("open_time").dt.weekday().alias("day_of_week"),
        pl.col("open_time").dt.day().alias("day_of_month"),
        pl.col("open_time").dt.week().alias("week_of_year"),
    ]).with_columns(
        pl.when(pl.col("hour_utc").is_between(0, 6)).then(pl.lit("asia"))
         .when(pl.col("hour_utc").is_between(19, 23)).then(pl.lit("asia"))
         .when(pl.col("hour_utc").is_between(7, 12)).then(pl.lit("europe"))
         .otherwise(pl.lit("america")).alias("session"),
        (pl.col("hour_utc").cast(pl.Int32) * 60
         + pl.col("minute_utc").cast(pl.Int32)).alias("min_from_day"),
        pl.col("minute_utc").alias("min_from_hour"),
    )
    # funding на 00/08/16 UTC — минуты с последнего funding (480 = 8ч)
    out = out.with_columns(
        ((pl.col("min_from_day")
          - (pl.col("hour_utc").cast(pl.Int32) % 8) * 60) % 480)
        .alias("min_from_funding"))
    # session overlap: перекрытие asia/europe и europe/america
    out = out.with_columns(
        pl.when(pl.col("hour_utc").is_between(7, 12)).then(pl.lit("asia_europe"))
         .when(pl.col("hour_utc").is_between(19, 23)).then(pl.lit("asia_america"))
         .otherwise(pl.lit("single")).alias("session_overlap"))
    return out


def add_features(df: pl.DataFrame, btc: pl.DataFrame | None = None,
                 eth: pl.DataFrame | None = None,
                 is_spike: pl.Series | None = None,
                 market_symbol: str | None = None,
                 breadth: pl.DataFrame | None = None) -> pl.DataFrame:
    """Полный feature-space. Возвращает df с зарегистрированными признаками.

    Порядок важен: базовые (candle) -> производные (volume/vol/momentum/...) ->
    структура/режим -> cross (требует market) -> context (требует is_spike).
    При market_symbol не None добавляются рыночные тиковые признаки (§12-15)
    по открытому времени свечи (без look-ahead: бакет T = тики [T, T+60s)).
    breadth (§17) — cross-sectional DataFrame [open_time, breadth], join по
    открытому времени свечи (forward-only, ничего "из будущего").
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

    # 3b. Рыночная широта (§17) — join перед режимами, чтобы market_breadth_regime
    #     считался как производный. join как есть; реестр/производные отсутствие
    #     колонки переживают (функции проверяют наличие колонок).
    if breadth is not None and breadth.height:
        out = out.join(breadth, on="open_time", how="left")

    # 4. Режим (требует trend_strength, volume; btc_trend_regime при наличии btc)
    out = fr.add_regime_features(out)

    # 5. Контекст событий (требует is_spike)
    if is_spike is not None and len(is_spike) == out.height:
        out = out.with_columns(pl.Series("is_spike", is_spike))
        out = fctx.add_context_features(out)

    # 6. Рыночные тиковые признаки (§12-15) — join по open_time, если есть данные
    if market_symbol is not None:
        try:
            mk = fmarket.build_market_features(market_symbol)
            if mk.height:
                out = out.join(mk, on="open_time", how="left")
        except Exception:
            pass  # рыночных данных нет — признаки пропущены, пайплайн не ломаем

    # 7. Внешние источники (§21): Fear&Greed + CoinGecko (без ключа)
    out = fext.add_external_features(out)

    return out
