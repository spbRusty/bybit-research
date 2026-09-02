"""Волатильность (ТЗ §9, §37): оценка Гармана–Класса по дневным свечам + волатильность признаков.

Гарман–Класс (annualized):
  sigma^2 = 0.5 * ln(H/L)^2 - (2*ln2 - 1) * ln(C/O)^2
  sigma_ann = sqrt(252 * mean(sigma^2))
"""
from __future__ import annotations

import polars as pl


def garman_klass_daily(df: pl.DataFrame) -> pl.DataFrame:
    """Минутные свечи -> дневные метрики Гармана–Класса.

    Возвращает: date, gk_ann (годовая волатильность за день).
    """
    day = (df.with_columns(pl.col("open_time").dt.date().alias("date"))
             .group_by("date")
             .agg(pl.col("open").first().alias("o"),
                  pl.col("high").max().alias("h"),
                  pl.col("low").min().alias("l"),
                  pl.col("close").last().alias("c")))
    day = day.with_columns(
        (0.5 * (pl.col("h") / pl.col("l")).log().pow(2)
         - (2 * pl.lit(2).log() - 1) * (pl.col("c") / pl.col("o")).log().pow(2))
        .clip(lower_bound=0).alias("gk_var"))
    return day.with_columns(
        (pl.col("gk_var").sqrt() * pl.lit(252.0).sqrt()).alias("gk_ann"))


def rolling_gk(df: pl.DataFrame, window_days: int) -> pl.DataFrame:
    """Скользящая волатильность Гармана–Класса (окно в днях) -> vols/sqrt(252/y)."""
    day = garman_klass_daily(df)
    # средняя дневная дисперсия за окно -> годовая
    return day.with_columns(
        pl.col("gk_var").rolling_mean(window_size=window_days, min_samples=1)
        .mul(252).sqrt().alias(f"vol_gk_{window_days}d"))


def vol_features(df: pl.DataFrame, window_min: int = 60) -> pl.DataFrame:
    """Признаки волатильности на минутных свечах (окно в минутах, без lookahead)."""
    return df.with_columns(
        ((pl.col("high") - pl.col("low")) / pl.col("close"))
        .rolling_mean(window_size=window_min, min_samples=1)
        .alias(f"range_mean_{window_min}m"),
        ((pl.col("high") - pl.col("low")) / pl.col("close"))
        .rolling_std(window_size=window_min, min_samples=1)
        .alias(f"range_std_{window_min}m"),
    )


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from pathlib import Path
    from src.data import read_raw
    df = read_raw(Path.home() / "Документы/bybit_rs/data/klines/linear/BTCUSDT_linear_1m.parquet")
    print(garman_klass_daily(df).tail(3))
    print(rolling_gk(df, 30).tail(3))