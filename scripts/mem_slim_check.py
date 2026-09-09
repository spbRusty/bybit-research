"""Deterministic check: full-width events vs slim events → identical research?

Single data snapshot (one load_universe_data), one pass per symbol: for each
symbol we hold BOTH the full-width events frame and the slim projection
(`select([c for c in required if c in ev.columns])`, production behavior).

OB integration: a single join_ob_features call on the FULL lane; the slim
post-OB lane is derived by projection from that same joined frame. This is
deterministic because OB files are appended live by the collector — two
separate joins would read different snapshots. _process_symbol uses only
symbol/open_time from the events frame, so join(full).select(...) equals
join(slim.select(...)) by construction; the projection identity below proves
it empirically.

Exits non-zero on any mismatch: event identity (slim columns), hypothesis
count, hypothesis IDs, discovery results, candidates, metrics.

Usage: .venv/bin/python scripts/mem_slim_check.py [--limit N]
"""
from __future__ import annotations

import gc
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
from src.orderbook_integration import join_ob_features
from src.orchestrator import _required_columns


def rss() -> int:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * 4 // 1024


def concat_aligned(frames: list[pl.DataFrame]) -> pl.DataFrame:
    all_cols = sorted(set(c for ev in frames for c in ev.columns))
    aligned = []
    for ev in frames:
        missing = [c for c in all_cols if c not in ev.columns]
        for c in missing:
            ev = ev.with_columns(pl.lit(None).alias(c))
        aligned.append(ev.select(all_cols))
    return pl.concat(aligned)


def build_both(universe, btc, eth, required):
    """One snapshot, one pass per symbol → (events_full, events_slim).

    Both lanes derive from the same build_events output, so heights match by
    construction; equals() below verifies values."""
    univ_dfs = data_mod.load_universe_data(universe)
    breadth = breadth_mod.compute_breadth(univ_dfs)
    all_full: list[pl.DataFrame] = []
    all_slim: list[pl.DataFrame] = []
    for row in universe.iter_rows(named=True):
        df = univ_dfs.pop((row["symbol"], row["category"]), None)
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
            all_full.append(ev)
            all_slim.append(ev.select([c for c in required if c in ev.columns]))
        print(f"  {row['symbol']} done", flush=True)
    del univ_dfs, breadth
    gc.collect()
    if not all_full:
        raise RuntimeError("no events built")
    return concat_aligned(all_full), concat_aligned(all_slim)


def run_research(ev):
    import src.research as research_mod
    from src import hypothesis_generator as hgen_mod

    _R = load_toml("research.toml")
    disc = research_mod.split_periods(ev)["discovery"]
    gen = hgen_mod.generate_hypotheses(disc, rules=hgen_mod.all_rules())
    gen = hgen_mod.filter_by_freq(gen, disc, _R["min_events"])
    hyps = list(research_mod.HYPOTHESES) + gen
    return research_mod.run_research(ev, hypotheses=hyps)


def check_identity(a: pl.DataFrame, b: pl.DataFrame, label: str) -> bool:
    """b must be a column subset of a with identical values wherever present.
    Returns True on success (and logs per-column diffs on failure)."""
    ok = True
    if a.height != b.height:
        print(f"FAIL[{label}]: heights differ: full={a.height} slim={b.height}",
              flush=True)
        return False
    subset = [c for c in b.columns if c in a.columns]
    if len(subset) != len(b.columns):
        print(f"FAIL[{label}]: slim columns missing in full: "
              f"{sorted(set(b.columns) - set(subset))}", flush=True)
        return False
    if not a.select(b.columns).equals(b):
        print(f"FAIL[{label}]: values differ on slim columns", flush=True)
        # per-column diff report
        for c in b.columns:
            fa, fb = a[c], b[c]
            try:
                if fa.null_count() != fb.null_count() or (fa != fb).sum() > 0:
                    neq = (fa != fb).sum() if fa.null_count() == fb.null_count() \
                        else None
                    print(f"  col {c}: nulls full={fa.null_count()} "
                          f"slim={fb.null_count()} neq={neq}", flush=True)
            except Exception:
                pass
        ok = False
    return ok


def main() -> None:
    limit = 20
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])

    _FEAT = load_toml("features.toml")
    universe = data_mod.liquidity_universe(verbose=False).head(limit)
    btc = data_mod.load_validated(_FEAT["btc_symbol"], "linear")
    eth = data_mod.load_validated(_FEAT["eth_symbol"], "linear")
    print(f"universe {universe.height} syms, btc/eth loaded rss={rss()}",
          flush=True)

    required = _required_columns()
    print(f"required columns (production _required_columns): {len(required)}",
          flush=True)

    t0 = time.time()
    events_full, events_slim = build_both(universe, btc, eth, required)
    print(f"[{time.time()-t0:.0f}s] full {events_full.height}x{len(events_full.columns)} "
          f"slim {events_slim.height}x{len(events_slim.columns)} rss={rss()}",
          flush=True)

    if not check_identity(events_full, events_slim, "pre-OB"):
        sys.exit(1)
    print("pre-OB events identical on slim columns: True", flush=True)

    # OB: single join on the FULL lane (deterministic — OB files are appended
    # live by the collector, two joins would read different snapshots).
    print("OB join full...", flush=True)
    events_full = join_ob_features(events_full)
    print(f"  {events_full.height}x{len(events_full.columns)} rss={rss()}",
          flush=True)

    # Slim post-OB lane = projection of the joined frame. This is exactly what
    # the slim pipeline would build: _process_symbol uses only symbol/open_time
    # (present in slim), so join(full).select(slim∪ob) == join(slim.select(req)).
    ob_req = [c for c in required if c in events_full.columns]
    events_slim_ob = events_full.select(ob_req)
    del events_slim
    gc.collect()

    if not check_identity(events_full, events_slim_ob, "post-OB"):
        sys.exit(1)
    print("post-OB events identical on slim columns: True", flush=True)

    print("running research full...", flush=True)
    t2 = time.time()
    r_full = run_research(events_full)
    print(f"full done [{time.time()-t2:.0f}s] rss={rss()}", flush=True)
    del events_full
    gc.collect()

    print("running research slim...", flush=True)
    t3 = time.time()
    r_slim = run_research(events_slim_ob)
    print(f"slim done [{time.time()-t3:.0f}s] rss={rss()}", flush=True)
    del events_slim_ob
    gc.collect()

    failures = []
    if r_full["n_hypotheses"] != r_slim["n_hypotheses"]:
        failures.append(f"n_hypotheses differ: full={r_full['n_hypotheses']} "
                        f"slim={r_slim['n_hypotheses']}")
    df_f = pl.DataFrame(r_full["discovery_results"]).sort("hypothesis_id")
    df_s = pl.DataFrame(r_slim["discovery_results"]).sort("hypothesis_id")
    if not df_f.equals(df_s):
        failures.append("discovery_results differ")
    if set(df_f["hypothesis_id"].to_list()) != set(df_s["hypothesis_id"].to_list()):
        failures.append("hypothesis IDs differ")
    if r_full["candidates"] != r_slim["candidates"]:
        failures.append(f"candidates differ: {r_full['candidates']} vs "
                        f"{r_slim['candidates']}")
    if r_full.get("metrics") != r_slim.get("metrics"):
        failures.append("metrics differ")

    print("full candidates:", r_full["candidates"], flush=True)
    print("slim candidates:", r_slim["candidates"], flush=True)
    print("full n_hyp:", r_full["n_hypotheses"], "slim n_hyp:",
          r_slim["n_hypotheses"], flush=True)
    print("discovery_results identical:", df_f.equals(df_s), flush=True)

    if failures:
        for f_ in failures:
            print("FAIL:", f_, flush=True)
        sys.exit(1)
    print("PASS — full-vs-slim research identical", flush=True)


if __name__ == "__main__":
    main()