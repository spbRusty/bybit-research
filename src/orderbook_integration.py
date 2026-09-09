"""Orderbook integration — joins OB features to candle events with temporal correctness.

Batching: parquet reads grouped by (symbol, date) for performance.
Temporal invariant: predictor timestamp <= event T (never after T).
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from config.settings import MARKET_DATA_DIR, load_toml
from src.orderbook_features import (
    DEPTH_LEVELS,
    FEATURE_VERSION,
    RECONSTRUCTION_VERSION,
    compute_all_features,
)

logger = logging.getLogger(__name__)

_CFG = load_toml("orderbook_research.toml")
_OB_RULES_CFG = load_toml("ob_hypothesis_rules.toml")
_RECON_DIR = MARKET_DATA_DIR / "orderbook" / "reconstructed"

STALE_THRESHOLD_SEC = 120
MISSING_THRESHOLD_SEC = 600
INTEGRATION_VERSION = "2.0"


def _integration_config_hash() -> str:
    parts = sorted(f"{k}={v}" for k, v in _CFG.items())
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:12]


def _ob_rules_hash() -> str:
    parts = sorted(f"{k}={v}" for k, v in _OB_RULES_CFG.items())
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:12]


def _date_range(start_ms: int, end_ms: int) -> list[str]:
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    dates = []
    d = start_dt.date()
    while d <= end_dt.date():
        dates.append(d.isoformat())
        d += timedelta(days=1)
    return dates


def join_ob_features(events: pl.DataFrame) -> pl.DataFrame:
    """Join OB predictor features to events via temporal lookup.

    Returns events DataFrame with OB columns appended (LEFT JOIN).
    """
    if events.height == 0:
        return events

    ob_columns = _get_ob_columns()

    results = []
    for symbol in events["symbol"].unique().sort().to_list():
        sym_events = events.filter(pl.col("symbol") == symbol)
        sym_result = _process_symbol(symbol, sym_events, ob_columns)
        results.append(sym_result)

    return pl.concat(results)


def _process_symbol(
    symbol: str,
    events: pl.DataFrame,
    ob_columns: list[str],
) -> pl.DataFrame:
    """Process all events for one symbol. Batch parquet reads by date."""
    if events.height == 0:
        return events

    ob_cols_with_quality = ob_columns + ["ob_data_quality"]

    event_times_ms = events["open_time"].cast(pl.Datetime("ms")).cast(pl.Int64).to_list()

    min_ms = min(event_times_ms)
    max_ms = max(event_times_ms)
    # Extend backward 5 min to catch snapshots on the previous calendar day
    # (e.g. event at midnight 09-06, snapshot at 23:59 on 09-05)
    date_strs = _date_range(min_ms - 300_000, max_ms + 60_000)

    frames = []
    for ds in date_strs:
        path = _RECON_DIR / symbol / f"{ds}.parquet"
        if path.exists():
            df = pl.read_parquet(path)
            if df.height > 0:
                frames.append(df)

    if not frames:
        for c in ob_columns:
            if c not in events.columns:
                events = events.with_columns(pl.lit(None, dtype=pl.Float64).alias(c))
        events = events.with_columns(pl.lit("missing").alias("ob_data_quality"))
        return events

    ob_data = pl.concat(frames).sort("timestamp_ms")
    features = compute_all_features(ob_data)

    feat_cols = [c for c in features.columns if c not in ("timestamp_ms", "update_id", "symbol")]

    events = events.with_row_index("_idx")
    events_keyed = events.with_columns(
        pl.col("open_time").cast(pl.Datetime("ms")).cast(pl.Int64).alias("_event_ts_ms")
    )

    feat_select = ["timestamp_ms", "gap_detected"] + [c for c in ob_columns if c in feat_cols]
    feat_for_join = features.select(feat_select).sort("timestamp_ms")

    events_keyed = events_keyed.sort("_event_ts_ms")
    joined = events_keyed.join_asof(
        feat_for_join,
        left_on="_event_ts_ms",
        right_on="timestamp_ms",
        strategy="backward",
    )

    joined = joined.with_columns(
        ((pl.col("_event_ts_ms") - pl.col("timestamp_ms")) / 1000).alias("_lag_sec")
    )

    # Quality: gap_detected → "gap"; lag ≤ 120s → "ok"; ≤ 600s → "stale"; else → "missing"
    joined = joined.with_columns(
        pl.when(pl.col("gap_detected").fill_null(False))
        .then(pl.lit("gap"))
        .when(pl.col("_lag_sec").fill_null(999999) <= STALE_THRESHOLD_SEC)
        .then(pl.lit("ok"))
        .when(pl.col("_lag_sec").fill_null(999999) <= MISSING_THRESHOLD_SEC)
        .then(pl.lit("stale"))
        .otherwise(pl.lit("missing"))
        .alias("ob_data_quality")
    )

    for c in ob_columns:
        if c in feat_cols:
            joined = joined.with_columns(
                pl.when(pl.col("ob_data_quality").is_in(["ok", "stale"]))
                .then(pl.col(c))
                .otherwise(None)
                .alias(c)
            )
        else:
            joined = joined.with_columns(pl.lit(None).alias(c))

    result = joined.drop("_idx", "_event_ts_ms", "timestamp_ms", "gap_detected", "_lag_sec")

    return result


def _get_ob_columns() -> list[str]:
    """Return list of OB feature columns (excluding metadata)."""
    cols = []
    cols.extend(["ob_best_bid", "ob_best_ask", "ob_mid", "ob_spread_bps", "ob_spread_abs"])
    for n in DEPTH_LEVELS:
        cols.append(f"ob_depth_bid_{n}")
        cols.append(f"ob_depth_ask_{n}")
        cols.append(f"ob_depth_total_{n}")
        cols.append(f"ob_imbalance_{n}")
        cols.append(f"ob_vwap_bid_{n}")
        cols.append(f"ob_vwap_ask_{n}")
    cols.append("ob_top1_share")
    cols.append("ob_top5_share")
    for n in DEPTH_LEVELS:
        for iv in _CFG.get("change_intervals", [1, 6, 60]):
            cols.append(f"ob_depth_change_{n}_{iv}")
    for iv in _CFG.get("change_intervals", [1, 6, 60]):
        cols.append(f"ob_spread_change_{iv}")
    for n in DEPTH_LEVELS:
        for iv in _CFG.get("change_intervals", [1, 6, 60]):
            cols.append(f"ob_imbalance_change_{n}_{iv}")
    for n in DEPTH_LEVELS:
        for lb in _CFG.get("lookback_snapshots", [5, 12, 60]):
            cols.append(f"ob_depth_volatility_{n}_{lb}")
    for lb in _CFG.get("lookback_snapshots", [5, 12, 60]):
        cols.append(f"ob_spread_volatility_{lb}")
    for n in DEPTH_LEVELS:
        for lb in _CFG.get("lookback_snapshots", [5, 12, 60]):
            cols.append(f"ob_imbalance_volatility_{n}_{lb}")
    return cols


def get_ob_provenance() -> dict:
    return {
        "ob_integration_version": INTEGRATION_VERSION,
        "ob_integration_config_hash": _integration_config_hash(),
        "ob_feature_version": FEATURE_VERSION,
        "ob_reconstruction_version": RECONSTRUCTION_VERSION,
        "ob_hypothesis_rules_hash": _ob_rules_hash(),
    }
