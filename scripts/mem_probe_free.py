"""Decisive probe: does freeing each symbol's candles + events (streaming)
bend the RSS curve vs accumulating everything?

Compares two strategies on same symbols (first 40):
  A) accumulate all_events (current) — baseline
  B) per-symbol: build events, free the candle df, keep only event df
Measures RSS after each symbol in both modes.
"""
from __future__ import annotations

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


def main() -> None:
    _FEAT = load_toml("features.toml")
    universe = data_mod.liquidity_universe(verbose=False).head(40)
    btc = data_mod.load_validated(_FEAT["btc_symbol"], "linear")
    eth = data_mod.load_validated(_FEAT["eth_symbol"], "linear")

    # Mode A: current orchestrator flow
    print("=== MODE A: accumulate all (current) ===", flush=True)
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
        n = sum(e.height for e in all_events)
        print(f"  A sym={row['symbol']:14s} acc_events={n:7d} rss={rss_mb():5d}", flush=True)
    del univ_dfs, btc, eth, breadth, all_events

    # Mode B: per-symbol processing, free candles immediately
    print("=== MODE B: free per-symbol candles ===", flush=True)
    btc = data_mod.load_validated(_FEAT["btc_symbol"], "linear")
    eth = data_mod.load_validated(_FEAT["eth_symbol"], "linear")
    univ_dfs = data_mod.load_universe_data(universe)
    breadth = breadth_mod.compute_breadth(univ_dfs)
    all_events_b = []
    for row in universe.iter_rows(named=True):
        df = univ_dfs.pop((row["symbol"], row["category"]))  # FREE the candle after use
        if df is None:
            continue
        df = features_mod.add_features(df, btc, eth,
                                       market_symbol=row["symbol"], breadth=breadth)
        gk = vol_mod.rolling_gk(df, 30).select(["date", "vol_gk_30d"])
        df = df.join(gk, left_on=pl.col("open_time").dt.date(),
                     right_on="date", how="left")
        ev = events_mod.build_events(df, row["symbol"], row["category"])
        if ev.height:
            all_events_b.append(ev)
        n = sum(e.height for e in all_events_b)
        print(f"  B sym={row['symbol']:14s} acc_events={n:7d} rss={rss_mb():5d}", flush=True)


if __name__ == "__main__":
    main()