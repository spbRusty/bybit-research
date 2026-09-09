"""Orderbook feature extraction — converts reconstructed snapshots to versioned features.

Architecture: reconstructed orderbook → feature extraction → versioned dataset.
This module is read-only on source data. Never modifies reconstructed snapshots.

Temporal rule: predictor features use ONLY snapshots with timestamp <= T.
The caller (get_pre_event) enforces this. This module computes features from
whatever DataFrame it receives — it trusts the caller on temporal boundaries.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from config.settings import MARKET_DATA_DIR, ROOT, load_toml

logger = logging.getLogger(__name__)

_CFG = load_toml("orderbook_research.toml")
_RECON_DIR = MARKET_DATA_DIR / "orderbook" / "reconstructed"

DEPTH_LEVELS: list[int] = _CFG.get("depth_levels", [1, 3, 5, 10, 20])
LOOKBACKS: list[int] = _CFG.get("lookback_snapshots", [5, 12, 60])
CHANGE_INTERVALS: list[int] = _CFG.get("change_intervals", [1, 6, 60])
SNAPSHOT_FREQ_SEC: int = _CFG.get("snapshot_frequency_sec", 5)
RECONSTRUCTION_VERSION: str = _CFG.get("reconstruction_version", "1.0")
FEATURE_VERSION = "1.0"


def _config_hash() -> str:
    parts = sorted(f"{k}={v}" for k, v in _CFG.items())
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:12]


def _bid_col(n: int) -> str:
    return f"bid_px_{n}"


def _bid_sz_col(n: int) -> str:
    return f"bid_sz_{n}"


def _ask_col(n: int) -> str:
    return f"ask_px_{n}"


def _ask_sz_col(n: int) -> str:
    return f"ask_sz_{n}"


def _depth_bid_expr(n: int) -> pl.Expr:
    cols = [_bid_sz_col(i) for i in range(1, n + 1)]
    return pl.sum_horizontal(cols).alias(f"ob_depth_bid_{n}")


def _depth_ask_expr(n: int) -> pl.Expr:
    cols = [_ask_sz_col(i) for i in range(1, n + 1)]
    return pl.sum_horizontal(cols).alias(f"ob_depth_ask_{n}")


def _vwap_bid_expr(n: int) -> pl.Expr:
    num = pl.sum_horizontal([pl.col(_bid_col(i)) * pl.col(_bid_sz_col(i)) for i in range(1, n + 1)])
    den = pl.sum_horizontal([pl.col(_bid_sz_col(i)) for i in range(1, n + 1)])
    return pl.when(den > 0).then(num / den).otherwise(None).alias(f"ob_vwap_bid_{n}")


def _vwap_ask_expr(n: int) -> pl.Expr:
    num = pl.sum_horizontal([pl.col(_ask_col(i)) * pl.col(_ask_sz_col(i)) for i in range(1, n + 1)])
    den = pl.sum_horizontal([pl.col(_ask_sz_col(i)) for i in range(1, n + 1)])
    return pl.when(den > 0).then(num / den).otherwise(None).alias(f"ob_vwap_ask_{n}")


def _top1_share_expr() -> pl.Expr:
    bid1 = pl.col("bid_sz_1")
    ask1 = pl.col("ask_sz_1")
    total = bid1 + ask1
    return pl.when(total > 0).then(bid1 / total).otherwise(None).alias("ob_top1_share")


def _top5_share_expr() -> pl.Expr:
    bid5 = pl.sum_horizontal([pl.col(_bid_sz_col(i)) for i in range(1, 6)])
    ask5 = pl.sum_horizontal([pl.col(_ask_sz_col(i)) for i in range(1, 6)])
    total = bid5 + ask5
    return pl.when(total > 0).then(bid5 / total).otherwise(None).alias("ob_top5_share")


def compute_snapshot_features(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return df

    # Pass 1: aliases + depth totals (no cross-references)
    pass1: list[pl.Expr] = [
        pl.col("best_bid").alias("ob_best_bid"),
        pl.col("best_ask").alias("ob_best_ask"),
        pl.col("mid_price").alias("ob_mid"),
        pl.col("spread_bps").alias("ob_spread_bps"),
        (pl.col("best_ask") - pl.col("best_bid")).alias("ob_spread_abs"),
    ]
    for n in DEPTH_LEVELS:
        pass1.append(_depth_bid_expr(n))
        pass1.append(_depth_ask_expr(n))
    df = df.with_columns(pass1)

    # Pass 2: depth totals (referenced by pass3)
    pass2: list[pl.Expr] = []
    for n in DEPTH_LEVELS:
        pass2.append(
            (pl.col(f"ob_depth_bid_{n}") + pl.col(f"ob_depth_ask_{n}")).alias(f"ob_depth_total_{n}")
        )
    df = df.with_columns(pass2)

    # Pass 3: imbalance, vwap, shares (reference depth_total columns)
    pass3: list[pl.Expr] = []
    for n in DEPTH_LEVELS:
        pass3.append(
            pl.when(pl.col(f"ob_depth_total_{n}") > 0)
            .then((pl.col(f"ob_depth_bid_{n}") - pl.col(f"ob_depth_ask_{n}")) / pl.col(f"ob_depth_total_{n}"))
            .otherwise(None)
            .alias(f"ob_imbalance_{n}")
        )
        pass3.append(_vwap_bid_expr(n))
        pass3.append(_vwap_ask_expr(n))
    pass3.append(_top1_share_expr())
    pass3.append(_top5_share_expr())

    return df.with_columns(pass3)


def compute_change_features(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return df

    exprs: list[pl.Expr] = []
    for n in DEPTH_LEVELS:
        for iv in CHANGE_INTERVALS:
            exprs.append(
                (pl.col(f"ob_depth_bid_{n}").diff(iv) + pl.col(f"ob_depth_ask_{n}").diff(iv))
                .alias(f"ob_depth_change_{n}_{iv}")
            )
    for iv in CHANGE_INTERVALS:
        exprs.append(
            pl.col("ob_spread_bps").diff(iv).alias(f"ob_spread_change_{iv}")
        )
    for n in DEPTH_LEVELS:
        for iv in CHANGE_INTERVALS:
            exprs.append(
                pl.col(f"ob_imbalance_{n}").diff(iv).alias(f"ob_imbalance_change_{n}_{iv}")
            )

    return df.with_columns(exprs)


def compute_volatility_features(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return df

    exprs: list[pl.Expr] = []
    for n in DEPTH_LEVELS:
        for lb in LOOKBACKS:
            exprs.append(
                pl.col(f"ob_depth_total_{n}").rolling_std(lb).alias(f"ob_depth_volatility_{n}_{lb}")
            )
    for lb in LOOKBACKS:
        exprs.append(
            pl.col("ob_spread_bps").rolling_std(lb).alias(f"ob_spread_volatility_{lb}")
        )
    for n in DEPTH_LEVELS:
        for lb in LOOKBACKS:
            exprs.append(
                pl.col(f"ob_imbalance_{n}").rolling_std(lb).alias(f"ob_imbalance_volatility_{n}_{lb}")
            )

    return df.with_columns(exprs)


def compute_all_features(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return df

    result = compute_snapshot_features(df)
    result = compute_change_features(result)
    result = compute_volatility_features(result)

    ch = _config_hash()
    result = result.with_columns([
        pl.lit(FEATURE_VERSION).alias("feature_version"),
        pl.lit(RECONSTRUCTION_VERSION).alias("reconstruction_version"),
        pl.lit(SNAPSHOT_FREQ_SEC).alias("snapshot_freq_sec"),
        pl.lit(ch).alias("config_hash"),
    ])

    return result


def get_features(
    symbol: str,
    event_ts_ms: int,
    pre_minutes: int | None = None,
    post_minutes: int | None = None,
) -> pl.DataFrame:
    from src.orderbook_reader import get_window

    df = get_window(symbol, event_ts_ms, pre_minutes, post_minutes)
    if df.height == 0:
        return pl.DataFrame()

    return compute_all_features(df)


def get_predictor_features(
    symbol: str,
    event_ts_ms: int,
    pre_minutes: int | None = None,
) -> pl.DataFrame:
    from src.orderbook_reader import get_pre_event

    df = get_pre_event(symbol, event_ts_ms, pre_minutes)
    if df.height == 0:
        return pl.DataFrame()

    return compute_all_features(df)


def get_response_features(
    symbol: str,
    event_ts_ms: int,
    post_minutes: int | None = None,
) -> pl.DataFrame:
    from src.orderbook_reader import get_post_event

    df = get_post_event(symbol, event_ts_ms, post_minutes)
    if df.height == 0:
        return pl.DataFrame()

    return compute_all_features(df)
