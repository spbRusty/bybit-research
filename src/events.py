"""События (ТЗ §16, §28, §27): предварительный фильтр свечей -> события + будущие доходности.

Свеча проходит предфильтр (relative_volume > X и/или relative_range > Y) ->
становится потенциальным событием. К событию присоединяются признаки на момент
T (close свечи). Будущие доходности считаются от ENTRY = open(T+1) до close(T+1+h)
либо до стопа/тейка (MFE/MAE). Никаких данных после T в признаках нет.
"""
from __future__ import annotations

import polars as pl
from config.settings import EVENTS_DIR
from config.settings import load_toml

_FEAT = load_toml("features.toml")
_RISK = load_toml("risk.toml")


def suspicious_candles(df: pl.DataFrame,
                       min_rvol: float | None = None,
                       min_rrange: float | None = None) -> pl.DataFrame:
    """Свечи, прошедшие предварительный фильтр (пороги из конфига по умолчанию)."""
    rvol = _FEAT["prefilter_rel_volume"] if min_rvol is None else min_rvol
    rrange = _FEAT["prefilter_rel_range"] if min_rrange is None else min_rrange
    return df.filter((pl.col("relative_volume") > rvol) |
                     (pl.col("relative_range") > rrange))


def _future_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Будущие доходности и MFE/MAE для каждой свечи (строго вперёд от open(T+1)).

    return_{h}m = close(T+h) / open(T+1) - 1
    mfe_{h}m = max(high[T+1..T+h]) / open(T+1) - 1
    mae_{h}m = min(low[T+1..T+h]) / open(T+1) - 1
    Перекрытие окна с пропуском данных -> null (оценивается len окна).
    """
    out = df.with_columns(pl.col("open").shift(-1).alias("entry_price"))
    for h in _FEAT["future_horizons_min"]:
        # будущее окно [T+1..T+h]: reverse -> rolling(назад) -> reverse (forward rolling)
        mfe = (pl.col("high").shift(-1).reverse()
               .rolling_max(h, min_samples=h).reverse())
        mae = (pl.col("low").shift(-1).reverse()
               .rolling_min(h, min_samples=h).reverse())
        out = out.with_columns([
            (pl.col("close").shift(-h) / pl.col("entry_price") - 1)
            .alias(f"return_{h}m"),
            (mfe / pl.col("entry_price") - 1).alias(f"mfe_{h}m"),
            (mae / pl.col("entry_price") - 1).alias(f"mae_{h}m"),
        ])
    return out


def build_events(df: pl.DataFrame, symbol: str, category: str) -> pl.DataFrame:
    """Полный контур: признаки -> предфильтр -> события + будущие доходности."""
    cand = suspicious_candles(df)
    if cand.height == 0:
        return cand
    ev = _future_metrics(cand)
    ev = ev.with_columns([
        pl.lit(symbol).alias("symbol"),
        pl.lit(category).alias("category"),
        pl.col("open_time").dt.strftime("%Y%m%dT%H%M%SZ").alias("event_id"),
    ])
    # события в конце файла без полного будущего окна исключаются (null)
    ev = ev.drop_nulls(subset=["entry_price"])
    return ev


def save_events(events: pl.DataFrame, name: str) -> None:
    path = EVENTS_DIR / f"{name}_events.parquet"
    events.write_parquet(path)


def load_events(name: str) -> pl.DataFrame:
    path = EVENTS_DIR / f"{name}_events.parquet"
    return pl.read_parquet(path) if path.exists() else None