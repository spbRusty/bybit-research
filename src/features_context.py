"""Признаки контекста предыдущих событий (ТЗ §19). Требуют наличие событий на df."""
from __future__ import annotations

import polars as pl

from src.registry import Feature, register


def _mk(fid, name, desc, formula, lookback=1, cost="low", data=("ohlcv",)):
    return Feature(fid, name, "context", desc, formula, "features_context",
                   "1m", lookback, data, True, cost)


register(_mk("time_since_last_spike", "Время после последнего всплеска",
             "open_time - open_time(последний spike)", "t_since_spike", 1440))
register(_mk("event_intensity_60", "Интенсивность событий (60)",
             "число событий за 60 мин", "n_events_60", 60, cost="medium"))
register(_mk("event_intensity_240", "Интенсивность событий (240)",
             "число событий за 240 мин", "n_events_240", 240, cost="medium"))
register(_mk("event_clustering", "Кластерность событий",
             "события подряд (2+ за 5 мин)", "cluster", 6))
register(_mk("cooldown", "Cooldown после события", "0 если время после события",
             "cooldown", 6))
register(_mk("dist_from_prev_event", "Расстояние от предыдущего события",
             "open_time - prev_event_time", "dist_prev", 1440))


def add_context_features(df: pl.DataFrame) -> pl.DataFrame:
    """Контекст событий. df должен содержать колонку-флаг события (is_spike).

    Если колонки is_spike нет — добавляется пустой контекст (признаки null).
    """
    if "is_spike" not in df.columns:
        return df
    spike = pl.col("is_spike")
    t = pl.col("open_time")
    return df.with_columns([
        # время после последнего spike (в минутах), кат-вперёд
        pl.when(spike).then(t).otherwise(None)
        .forward_fill().alias("_last_spike_t"),
    ]).with_columns([
        ((t - pl.col("_last_spike_t")).dt.total_minutes()).alias("time_since_last_spike"),
        spike.rolling_sum(60, min_samples=1).alias("event_intensity_60"),
        spike.rolling_sum(240, min_samples=1).alias("event_intensity_240"),
        spike.rolling_sum(6, min_samples=1).alias("event_clustering"),
        (spike.rolling_sum(6, min_samples=1) > 0).cast(pl.Int8).alias("cooldown"),
    ]).drop("_last_spike_t")
