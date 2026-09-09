"""Leak test: run the same 8-symbol feature loop twice in one process.
If RSS after run2 ≈ RSS after run1 → allocator reuses freed buffers (no leak,
just peak accumulation). If RSS grows ~same amount again → real leak.
"""
from __future__ import annotations

import gc
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl

from config.settings import load_toml
from src import data as data_mod
from src import events as events_mod
from src import features as features_mod
from src import features_breadth as breadth_mod
from src import volatility as vol_mod


def rss_mb() -> int:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * 4 // 1024


def run_once(universe, btc, eth, label: str) -> int:
    univ_dfs = data_mod.load_universe_data(universe)
    breadth = breadth_mod.compute_breadth(univ_dfs)
    all_events = []
    for row in universe.iter_rows(named=True):
        df = univ_dfs.get((row["symbol"], row["category"]))
        if df is None:
            continue
        df = features_mod.add_features(df, btc, eth,
                                       market_symbol=row["symbol"], breadth=breadth)
        gk = vol_mod.rolling_gk(df, 30).select(["date", "vol_gk_30d"])
        df = df.join(gk, left_on=pl.col("open_time").dt.date(),
                     right_on="date", how="left")
        ev = events_mod.build_events(df, row["symbol"], row["category"])
        if ev.height:
            all_events.append(ev)
    all_cols = sorted(set(c for ev in all_events for c in ev.columns))
    aligned = [ev.select(all_cols) for ev in all_events]
    events = pl.concat(aligned)
    n = events.height
    rss = rss_mb()
    del univ_dfs, breadth, all_events, aligned, events
    gc.collect()
    print(f"  {label}: n_events={n} rss={rss_mb()} MB", flush=True)
    return n


def main() -> None:
    _FEAT = load_toml("features.toml")
    universe = data_mod.liquidity_universe(verbose=False).head(8)
    btc = data_mod.load_validated(_FEAT["btc_symbol"], "linear")
    eth = data_mod.load_validated(_FEAT["eth_symbol"], "linear")

    run_once(universe, btc, eth, "run1")
    gc.collect()
    print(f"  after gc: rss={rss_mb()} MB", flush=True)
    run_once(universe, btc, eth, "run2")
    gc.collect()
    print(f"  after gc: rss={rss_mb()} MB", flush=True)


if __name__ == "__main__":
    main()