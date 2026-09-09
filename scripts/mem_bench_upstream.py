"""Phase 2 memory benchmark — RSS at each pipeline stage vs symbol count.

Usage: .venv/bin/python scripts/mem_bench_upstream.py --limit N
Tracks: baseline | candles | breadth | features | events | aligned | events_concat.
Prints a JSON row per stage. Safe: max 20 symbols.
"""
from __future__ import annotations

import argparse
import gc
import json
import resource
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


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def rss_now_mb() -> float:
    # Current RSS from /proc/self/statm (not max)
    with open("/proc/self/statm") as f:
        pages = int(f.read().split()[1])
    page_kb = 4  # standard
    return pages * page_kb / 1024.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, required=True)
    args = ap.parse_args()

    _FEAT = load_toml("features.toml")
    universe = data_mod.liquidity_universe(verbose=False)
    if args.limit:
        universe = universe.head(args.limit)

    rows = []
    t_start = time.time()

    # Stage: baseline
    gc.collect()
    rows.append({"stage": "baseline", "rss": round(rss_now_mb()), "t": round(time.time() - t_start, 1)})

    # Stage: candles (btc + eth + univ_dfs)
    t = time.time()
    btc = data_mod.load_validated(_FEAT["btc_symbol"], "linear")
    eth = data_mod.load_validated(_FEAT["eth_symbol"], "linear")
    univ_dfs = data_mod.load_universe_data(universe)
    rows.append({"stage": "candles_loaded", "n_sym": len(univ_dfs),
                 "rss": round(rss_now_mb()), "t": round(time.time() - t, 1)})

    # Stage: breadth
    t = time.time()
    breadth = breadth_mod.compute_breadth(univ_dfs)
    gc.collect()
    rows.append({"stage": "breadth", "rss": round(rss_now_mb()), "t": round(time.time() - t, 1)})

    # Stage: features loop (each symbol)
    t = time.time()
    all_events = []
    n_loaded = 0
    for row in universe.iter_rows(named=True):
        df = univ_dfs.get((row["symbol"], row["category"]))
        if df is None:
            continue
        df = features_mod.add_features(df, btc, eth,
                                       market_symbol=row["symbol"],
                                       breadth=breadth)
        gk = vol_mod.rolling_gk(df, 30).select(["date", "vol_gk_30d"])
        df = df.join(gk, left_on=pl.col("open_time").dt.date(),
                     right_on="date", how="left")
        ev = events_mod.build_events(df, row["symbol"], row["category"])
        if ev.height:
            all_events.append(ev)
        n_loaded += 1
        rows.append({"stage": f"feature_{row['symbol']}", "sym": row["symbol"],
                     "n_events": sum(e.height for e in all_events),
                     "rss": round(rss_now_mb())})
    gc.collect()
    total_ev = sum(e.height for e in all_events)
    rows.append({"stage": "features_done", "n_sym": n_loaded,
                 "n_events_total": total_ev, "events_list": len(all_events),
                 "rss": round(rss_now_mb()), "t": round(time.time() - t, 1)})

    # Stage: aligned (build all_cols + aligned)
    t = time.time()
    all_cols = sorted(set(c for ev in all_events for c in ev.columns))
    aligned = []
    for ev in all_events:
        missing = [c for c in all_cols if c not in ev.columns]
        for c in missing:
            ev = ev.with_columns(pl.lit(None).alias(c))
        aligned.append(ev.select(all_cols))
    gc.collect()
    rows.append({"stage": "aligned", "n_cols": len(all_cols),
                 "rss": round(rss_now_mb()), "t": round(time.time() - t, 1)})

    # Stage: concat
    t = time.time()
    events = pl.concat(aligned)
    gc.collect()
    rows.append({"stage": "events_concat", "n_rows": events.height,
                 "n_cols": len(events.columns),
                 "rss": round(rss_now_mb()), "t": round(time.time() - t, 1)})

    # Stage: after freeing upstream (simulate del of univ_dfs/btc/eth/breadth/all_events/aligned)
    t = time.time()
    del univ_dfs, btc, eth, breadth, all_events, aligned
    gc.collect()
    rows.append({"stage": "after_free_upstream", "rss": round(rss_now_mb()),
                 "t": round(time.time() - t, 1)})

    # Print summary
    print(json.dumps({"limit": args.limit, "stages": rows}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()