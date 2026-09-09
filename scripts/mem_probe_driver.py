"""Probe: what drives RSS growth — accumulated events, held candles (univ_dfs),
or per-symbol feature transient? Measures RSS at 3 points per symbol:
  a) after load (before feature) — held candles so far
  b) during add_features (peak) — transient wide df
  c) after build_events — accumulated events so far
Shows which region grows with symbol count.
"""
from __future__ import annotations

import sys
import time
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
        return int(f.read().split()[1]) * 4 // 1024  # pages*4KB / 1024 = MB


def main() -> None:
    _FEAT = load_toml("features.toml")
    universe = data_mod.liquidity_universe(verbose=False)
    universe = universe.head(50)  # probe up to 50, stop before OOM risk ~ each sym ~+80-160MB
    btc = data_mod.load_validated(_FEAT["btc_symbol"], "linear")
    eth = data_mod.load_validated(_FEAT["eth_symbol"], "linear")
    univ_dfs = data_mod.load_universe_data(universe)
    breadth = breadth_mod.compute_breadth(univ_dfs)
    print(f"baseline_after_load rss={rss_mb()} MB univ={len(univ_dfs)}", flush=True)

    all_events = []
    for i, row in enumerate(universe.iter_rows(named=True)):
        df = univ_dfs.get((row["symbol"], row["category"]))
        if df is None:
            continue
        r_before = rss_mb()
        df = features_mod.add_features(df, btc, eth,
                                       market_symbol=row["symbol"], breadth=breadth)
        r_after_feat = rss_mb()
        gk = vol_mod.rolling_gk(df, 30).select(["date", "vol_gk_30d"])
        df = df.join(gk, left_on=pl.col("open_time").dt.date(),
                     right_on="date", how="left")
        ev = events_mod.build_events(df, row["symbol"], row["category"])
        r_after_events = rss_mb()
        if ev.height:
            all_events.append(ev)
        n_ev = sum(e.height for e in all_events)
        print(f"sym={row['symbol']:16s} pre={r_before:5d} "
              f"post_features={r_after_feat:5d} post_events={r_after_events:5d} "
              f"acc_events={n_ev:7d}", flush=True)
        if n_ev > 2_600_000:  # safety: ~5GB expected at ~2.6M, stop
            print("SAFETY STOP: accumulated events > 2.6M", flush=True)
            break
        del df, ev
        # NOTE: univ_dfs entry for this symbol is intentionally KEPT (held candles)


if __name__ == "__main__":
    main()