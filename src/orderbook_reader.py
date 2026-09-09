"""Orderbook reader — reads reconstructed snapshots from Phase 0 storage.

Provides get_window() for extracting [T-N, T+M] from continuous dataset.
This module is read-only: never modifies market data.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from config.settings import MARKET_DATA_DIR, load_toml

logger = logging.getLogger(__name__)

_CFG = load_toml("orderbook_research.toml")
_RECON_DIR = MARKET_DATA_DIR / "orderbook" / "reconstructed"


def _date_range(start_ms: int, end_ms: int) -> list[str]:
    """Return list of date strings (YYYY-MM-DD) covering [start_ms, end_ms]."""
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    dates = []
    d = start_dt.date()
    while d <= end_dt.date():
        dates.append(d.isoformat())
        d += timedelta(days=1)
    return dates


def read_reconstructed(symbol: str, date_str: str) -> pl.DataFrame | None:
    """Read reconstructed snapshots for a symbol on a given date.

    Returns DataFrame with columns:
        timestamp_ms, update_id, symbol, best_bid, best_ask,
        spread_bps, mid_price, levels (nested)

    Returns None if file doesn't exist.
    """
    path = _RECON_DIR / symbol / f"{date_str}.parquet"
    if not path.exists():
        return None
    return pl.read_parquet(path)


def get_window(
    symbol: str,
    event_ts_ms: int,
    pre_minutes: int | None = None,
    post_minutes: int | None = None,
) -> pl.DataFrame:
    """Extract orderbook window [T-N, T+M] from continuous dataset.

    Args:
        symbol: Trading symbol (e.g. "BTCUSDT")
        event_ts_ms: Event timestamp T in milliseconds
        pre_minutes: How many minutes before T to include (default from config)
        post_minutes: How many minutes after T to include (default from config)

    Returns:
        DataFrame with reconstructed snapshots in [T-N, T+M].
        Empty DataFrame if no data available.
    """
    pre_min = pre_minutes or _CFG.get("pre_event_minutes", 10)
    post_min = post_minutes or _CFG.get("post_event_minutes", 20)

    start_ms = event_ts_ms - pre_min * 60_000
    end_ms = event_ts_ms + post_min * 60_000

    dates = _date_range(start_ms, end_ms)
    frames = []
    for d in dates:
        df = read_reconstructed(symbol, d)
        if df is not None and df.height > 0:
            frames.append(df)

    if not frames:
        return pl.DataFrame()

    combined = pl.concat(frames)
    # Filter to exact window
    return combined.filter(
        (pl.col("timestamp_ms") >= start_ms) &
        (pl.col("timestamp_ms") <= end_ms)
    ).sort("timestamp_ms")


def get_pre_event(
    symbol: str,
    event_ts_ms: int,
    pre_minutes: int | None = None,
) -> pl.DataFrame:
    """Extract PRE-event orderbook data [T-N, T].

    Only data with timestamp ≤ T is returned.
    Safe for predictor features — no future leakage.
    """
    pre_min = pre_minutes or _CFG.get("pre_event_minutes", 10)
    start_ms = event_ts_ms - pre_min * 60_000

    dates = _date_range(start_ms, event_ts_ms)
    frames = []
    for d in dates:
        df = read_reconstructed(symbol, d)
        if df is not None and df.height > 0:
            frames.append(df)

    if not frames:
        return pl.DataFrame()

    combined = pl.concat(frames)
    return combined.filter(
        (pl.col("timestamp_ms") >= start_ms) &
        (pl.col("timestamp_ms") <= event_ts_ms)
    ).sort("timestamp_ms")


def get_post_event(
    symbol: str,
    event_ts_ms: int,
    post_minutes: int | None = None,
) -> pl.DataFrame:
    """Extract POST-event orderbook data [T, T+M].

    Only data with timestamp > T is returned.
    For response/dynamics analysis — NOT for predictor features.
    """
    post_min = post_minutes or _CFG.get("post_event_minutes", 20)
    end_ms = event_ts_ms + post_min * 60_000

    dates = _date_range(event_ts_ms, end_ms)
    frames = []
    for d in dates:
        df = read_reconstructed(symbol, d)
        if df is not None and df.height > 0:
            frames.append(df)

    if not frames:
        return pl.DataFrame()

    combined = pl.concat(frames)
    return combined.filter(
        (pl.col("timestamp_ms") > event_ts_ms) &
        (pl.col("timestamp_ms") <= end_ms)
    ).sort("timestamp_ms")


def list_symbols() -> list[str]:
    """List all symbols with reconstructed data."""
    if not _RECON_DIR.exists():
        return []
    return sorted([
        p.name for p in _RECON_DIR.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    ])


def list_dates(symbol: str) -> list[str]:
    """List all dates with reconstructed data for a symbol."""
    sym_dir = _RECON_DIR / symbol
    if not sym_dir.exists():
        return []
    return sorted([
        p.stem for p in sym_dir.glob("*.parquet")
    ])


def storage_stats() -> dict:
    """Return storage statistics for reconstructed orderbook data."""
    if not _RECON_DIR.exists():
        return {"total_files": 0, "total_bytes": 0, "symbols": 0}

    total_bytes = 0
    total_files = 0
    symbols = 0

    for sym_dir in _RECON_DIR.iterdir():
        if sym_dir.is_dir() and not sym_dir.name.startswith("."):
            symbols += 1
            for f in sym_dir.glob("*.parquet"):
                total_bytes += f.stat().st_size
                total_files += 1

    return {
        "total_files": total_files,
        "total_bytes": total_bytes,
        "total_gb": round(total_bytes / (1024**3), 3),
        "symbols": symbols,
    }
