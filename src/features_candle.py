"""Расширенные признаки цены и свечи (ТЗ §5A, §6).

Все признаки используют данные <= T (нет look-ahead). Гистерезис — закрытие
свечи; rolling-окна считаются по предыдущим свечам (shift на 1).
"""
from __future__ import annotations

import polars as pl

from src.registry import Feature, register


def _mk(fid: str, name: str, desc: str, formula: str, lookback: int = 1,
        timeframe: str = "1m", data: tuple[str, ...] = ("ohlcv",),
        realtime: bool = True, cost: str = "low") -> Feature:
    return Feature(fid, name, "candle", desc, formula, "features_candle",
                   timeframe, lookback, data, realtime, cost)


# Регистрация признаков (описание в Registry)
_CANDLE_FEATURES = [
    _mk("candle_return_1m", "Доходность свечи", "close/open - 1", "close/open-1"),
    _mk("candle_log_return", "Лог-доходность", "ln(close/open)", "ln(close/open)"),
    _mk("candle_body", "Тело свечи", "|close-open|", "|close-open|"),
    _mk("candle_body_abs", "Абсолютное тело", "|close-open|/close", "|close-open|/close"),
    _mk("candle_range", "Диапазон", "high-low", "high-low"),
    _mk("candle_upper_wick", "Верхняя тень", "(high-max(o,c))/close", "(high-max(o,c))/close"),
    _mk("candle_lower_wick", "Нижняя тень", "(min(o,c)-low)/close", "(min(o,c)-low)/close"),
    _mk("candle_body_range", "Тело/диапазон", "|c-o|/(h-l)", "|c-o|/(h-l)"),
    _mk("candle_upper_wick_range", "Верхняя тень/диапазон", "(h-max(o,c))/(h-l)", "(h-max(o,c))/(h-l)"),
    _mk("candle_lower_wick_range", "Нижняя тень/диапазон", "(min(o,c)-l)/(h-l)", "(min(o,c)-l)/(h-l)"),
    _mk("candle_close_pos", "Позиция close в диапазоне", "(c-l)/(h-l)", "(c-l)/(h-l)", 1),
    _mk("candle_open_pos", "Позиция open в диапазоне", "(o-l)/(h-l)", "(o-l)/(h-l)", 1),
    _mk("candle_direction", "Направление свечи", "sign(c-o)", "sign(c-o)", 1),
    _mk("candle_gap", "Гэп относительно prev close", "o/close_prev - 1", "o/close_prev-1", 2),
    _mk("candle_true_range", "True range", "max(h-l,|h-pc|,|l-pc|)", "max(h-l,|h-pc|,|l-pc|)", 2),
]
for f in _CANDLE_FEATURES:
    register(f)


def add_candle_features(df: pl.DataFrame) -> pl.DataFrame:
    """Свечные признаки (полиморфные, без look-ahead)."""
    o, h, l, c = (pl.col("open"), pl.col("high"), pl.col("low"), pl.col("close"))
    pc = c.shift(1)  # предыдущий close
    rng = (h - l).clip(lower_bound=1e-12)
    out = df.with_columns([
        (c / o - 1).alias("candle_return_1m"),
        (c / o).log().alias("candle_log_return"),
        (c - o).abs().alias("candle_body"),
        ((c - o).abs() / c.clip(1e-12)).alias("candle_body_abs"),
        rng.alias("candle_range"),
        ((h - pl.max_horizontal(o, c)) / c.clip(1e-12)).alias("candle_upper_wick"),
        ((pl.min_horizontal(o, c) - l) / c.clip(1e-12)).alias("candle_lower_wick"),
        ((c - o).abs() / rng).alias("candle_body_range"),
        ((h - pl.max_horizontal(o, c)) / rng).alias("candle_upper_wick_range"),
        ((pl.min_horizontal(o, c) - l) / rng).alias("candle_lower_wick_range"),
        ((c - l) / rng).alias("candle_close_pos"),
        ((o - l) / rng).alias("candle_open_pos"),
        (c - o).sign().alias("candle_direction"),
        (o / pc - 1).alias("candle_gap"),
        (pl.max_horizontal(rng, (h - pc).abs(), (l - pc).abs())).alias("candle_true_range"),
    ])
    # high/low относительно предыдущей свечи + дистанции
    out = out.with_columns([
        (h - h.shift(1)).alias("dist_prev_high"),
        (l - l.shift(1)).alias("dist_prev_low"),
        ((h - h.shift(1)) / c.clip(1e-12)).alias("dist_prev_high_pct"),
        ((l - l.shift(1)) / c.clip(1e-12)).alias("dist_prev_low_pct"),
    ])
    # relative_range: (h-l)/close / медиану окна ПРЕДЫДУЩИХ свечей — contract для prefilter
    out = out.with_columns([
        (rng / c.clip(1e-12)).alias("range_pct"),
        (rng / c.clip(1e-12))
        .shift(1).rolling_median(60, min_samples=1).clip(1e-12).alias("_range_med60"),
    ])
    out = out.with_columns(
        (pl.col("range_pct") / pl.col("_range_med60")).alias("relative_range"))
    return out.drop("_range_med60")


# --- Многоминутные доходности (§6) ---
_HORIZONS = (3, 5, 10, 15, 30, 60, 240)  # 1h=60, 4h=240 (минуты)
for _n in _HORIZONS:
    register(_mk(f"return_{_n}m", f"Доходность {_n} мин", f"close/close(-{_n}) - 1",
                 f"close/close(-{_n})-1", lookback=_n + 1))


def add_multi_timeframe_returns(df: pl.DataFrame) -> pl.DataFrame:
    """Доходности на горизонтах 3m..4h (закрытие T к закрытию T-n, без look-ahead)."""
    out = df
    for n in _HORIZONS:
        out = out.with_columns(
            (pl.col("close") / pl.col("close").shift(n) - 1).alias(f"return_{n}m"))
    return out


# --- Ускорение доходности / momentum ratio (§6) ---
register(_mk("return_accel", "Ускорение доходности", "(r_t - r_{t-1})/r_{t-1}",
             "diff(return)/return", lookback=3))
register(_mk("short_long_ratio", "Отношение краткосрочной к долгосрочной",
             "return_5m / return_60m", "return_5m/return_60m", lookback=61))
register(_mk("return_momentum_change", "Изменение momentum",
             "return_10m - return_30m", "r10-r30", lookback=31))


def add_return_derivatives(df: pl.DataFrame) -> pl.DataFrame:
    r = pl.col("return_5m")
    return df.with_columns([
        (r - r.shift(1)).alias("return_accel"),
        (pl.col("return_5m").clip(-0.2, 0.2) / pl.col("return_60m").clip(-0.2, 0.2))
        .alias("short_long_ratio"),
        (pl.col("return_10m") - pl.col("return_30m")).alias("return_momentum_change"),
    ])
