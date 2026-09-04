"""MR Hypothesis Research Pipeline — standalone, no architecture changes.

FEATURE: close / SMA_120 - 1
EVENT:   feature < threshold AND candle_direction = 1
THRESHOLDS: -0.01, -0.02, -0.03, -0.05
ENTRY: open(T+1), LONG
HORIZONS: 10m, 30m
COST: 0.20% round-trip (fixed)
"""
from __future__ import annotations
import sys, json, warnings, logging
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.WARNING)

sys.path.insert(0, "/home/vlad/Документы/построение")

import numpy as np
import polars as pl
from datetime import datetime
from scipy import stats as sp_stats
from pathlib import Path

from src import data as data_mod
from src.research import test_hypothesis, benjamini_hochberg, split_periods, Hypothesis
from src.critic import review, _temporal_stability, _concentration
from config.settings import load_toml

_R = load_toml("research.toml")
W = 120

# ═══════════════════════════════════════════════════════════════
# 1. LOAD DATA + COMPUTE FEATURE
# ═══════════════════════════════════════════════════════════════

print("=" * 70)
print("MR RESEARCH PIPELINE")
print(f"Feature: close / SMA_120 - 1")
print(f"Timestamp: {datetime.utcnow().isoformat()}")
print("=" * 70)

# Load universe
universe = data_mod.liquidity_universe()
print(f"Universe: {universe.height} symbols")

# Load + compute per symbol
all_chunks = []
n_loaded = 0
for row in universe.iter_rows(named=True):
    df = data_mod.load_validated(row["symbol"], row["category"])
    if df is None or df.height < W + 60:
        continue

    c = df["close"]
    # SMA_120 with shift(1) — no lookahead
    sma120 = c.rolling_mean(W, min_samples=1).shift(1)
    mr_score = (c / sma120.clip(1e-12) - 1).alias("mr_sma120")

    # Direction
    is_green = df["is_green"]

    # Future returns (already computed by events pipeline, but we need them here)
    # We compute fresh to ensure correctness
    r10 = c.shift(-10) / c - 1
    r30 = c.shift(-30) / c - 1

    # entry_price = open(T+1)
    entry = df["open"].shift(-1)

    chunk = df.select(["open_time"]).with_columns([
        pl.lit(row["symbol"]).alias("symbol"),
    ]).with_columns([
        mr_score,
        is_green,
        r10.alias("return_10m"),
        r30.alias("return_30m"),
        entry.alias("entry_price"),
    ])

    # Add hourly features for critic
    chunk = chunk.with_columns([
        pl.col("open_time").dt.hour().alias("hour_utc"),
    ])

    all_chunks.append(chunk)
    n_loaded += 1
    if n_loaded % 25 == 0:
        print(f"  loaded {n_loaded} symbols...")

print(f"Loaded {n_loaded} symbols")

events = pl.concat(all_chunks)
print(f"Total events: {events.height}")

# ═══════════════════════════════════════════════════════════════
# 2. DEFINE HYPOTHESES (4 thresholds × 2 horizons = 8)
# ═══════════════════════════════════════════════════════════════

thresholds = [-0.01, -0.02, -0.03, -0.05]
horizons = [10, 30]

hypotheses = []
for thr in thresholds:
    for h in horizons:
        hid = f"MR_SMA120_t{abs(thr)*100:.0f}p_h{h}"
        cond = f"(pl.col('mr_sma120') < {thr}) & pl.col('is_green')"
        hyp = Hypothesis(
            hypothesis_id=hid,
            description=f"MR: close/SMA120-1 < {thr} + green → long {h}m",
            condition=cond,
            entry_side="long",
            horizon_min=h,
        )
        hypotheses.append(hyp)

print(f"\nHypotheses: {len(hypotheses)}")
for h in hypotheses:
    print(f"  {h.hypothesis_id}: {h.description}")

# ═══════════════════════════════════════════════════════════════
# 3. SPLIT PERIODS
# ═══════════════════════════════════════════════════════════════

periods = split_periods(events)
disc, val, oos = periods["discovery"], periods["validation"], periods["oos"]
print(f"\nDiscovery: {disc.height} events")
print(f"Validation: {val.height} events")
print(f"OOS: {oos.height} events")

# ═══════════════════════════════════════════════════════════════
# 4. DISCOVERY: test all hypotheses
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("DISCOVERY PHASE")
print("=" * 70)

cost = _R["survival_cost"]  # 0.002 = 0.20%
q = _R["bh_q"]  # 0.05

disc_results = []
for i, hyp in enumerate(hypotheses):
    m = test_hypothesis(disc, hyp, cost)
    m.update({
        "hypothesis_id": hyp.hypothesis_id,
        "description": hyp.description,
        "entry_side": hyp.entry_side,
        "horizon_min": hyp.horizon_min,
        "threshold": float(hyp.condition.split("< ")[1].split(")")[0]),
    })
    disc_results.append(m)
    print(f"  {hyp.hypothesis_id}: n={m['n']:>6}  mean_net={m['mean_net']:+.6f}  "
          f"t={m['t_stat']:>6.2f}  wr={m['winrate']:.1%}")

df_disc = pl.DataFrame(disc_results).sort("t_stat", descending=True)

# BH correction
p_arr = np.nan_to_num(df_disc["p_value"].to_numpy(), nan=1.0)
sig_bh = benjamini_hochberg(p_arr, q)
print(f"\nBH significant (q={q}): {sig_bh.sum()}/{len(sig_bh)}")

# Apply gates
mask = (
    sig_bh &
    (df_disc["n"].to_numpy() >= _R["min_events"]) &
    (df_disc["n_symbols"].to_numpy() >= _R["min_unique_symbols"]) &
    (df_disc["n_months"].to_numpy() >= _R["min_months"]) &
    (df_disc["t_stat"].to_numpy() >= _R["min_t_stat"]) &
    (df_disc["mean_net"].to_numpy() > 0)
)
candidates = df_disc.filter(pl.Series(mask))
print(f"Discovery candidates: {candidates.height}")

if candidates.height == 0:
    print("\n*** NO CANDIDATE FROM DISCOVERY ***")
    print("Checking which gate failed...")
for r in df_disc.iter_rows(named=True):
        fails = []
        if not sig_bh[df_disc["hypothesis_id"].to_list().index(r["hypothesis_id"])]:
            fails.append("BH")
        if r["n"] < _R["min_events"]:
            fails.append(f"n={r['n']}<{_R['min_events']}")
        if r["n_symbols"] < _R["min_unique_symbols"]:
            fails.append(f"symbols={r['n_symbols']}<{_R['min_unique_symbols']}")
        if r["n_months"] < _R["min_months"]:
            fails.append(f"months={r['n_months']}<{_R['min_months']}")
        if r["t_stat"] < _R["min_t_stat"]:
            fails.append(f"t={r['t_stat']:.2f}<{_R['min_t_stat']}")
        if r["mean_net"] <= 0:
            fails.append(f"net={r['mean_net']:.6f}<=0")
        print(f"  {r['hypothesis_id']}: {', '.join(fails) if fails else 'PASS'}")
else:
    # Select best candidate
    best = candidates.row(0, named=True)
    print(f"\nBest candidate: {best['hypothesis_id']}")
    print(f"  threshold: {best['threshold']}")
    print(f"  horizon: {best['horizon_min']}m")
    print(f"  n: {best['n']}")
    print(f"  mean_net: {best['mean_net']:+.6f}")
    print(f"  t_stat: {best['t_stat']:.2f}")
    print(f"  winrate: {best['winrate']:.1%}")

# ═══════════════════════════════════════════════════════════════
# 5. VALIDATION (with fixed threshold from Discovery)
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("VALIDATION PHASE")
print("=" * 70)

validation_results = {}
if candidates.height > 0:
    best_hyp = next(h for h in hypotheses if h.hypothesis_id == best["hypothesis_id"])
    mv = test_hypothesis(val, best_hyp, cost)
    validation_results[best["hypothesis_id"]] = mv
    print(f"  {best['hypothesis_id']}:")
    print(f"    n={mv['n']}, mean_net={mv['mean_net']:+.6f}, t={mv['t_stat']:.2f}")
    print(f"    winrate={mv['winrate']:.1%}")
    val_pass = mv.get("mean_net", -1) > 0 and mv.get("n", 0) >= _R["min_events"]
    print(f"    PASS: {val_pass}")
else:
    print("  No candidate to validate.")

# ═══════════════════════════════════════════════════════════════
# 6. OOS (with fixed threshold from Discovery)
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("OOS PHASE")
print("=" * 70)

oos_results = {}
if candidates.height > 0:
    mo = test_hypothesis(oos, best_hyp, cost)
    oos_results[best["hypothesis_id"]] = mo
    print(f"  {best['hypothesis_id']}:")
    print(f"    n={mo['n']}, mean_net={mo['mean_net']:+.6f}, t={mo['t_stat']:.2f}")
    print(f"    winrate={mo['winrate']:.1%}")
    oos_pass = mo.get("mean_net", -1) > 0 and mo.get("n", 0) >= _R["min_events"]
    print(f"    PASS: {oos_pass}")
else:
    print("  No candidate for OOS.")

# ═══════════════════════════════════════════════════════════════
# 7. COST STRESS
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("COST STRESS TEST")
print("=" * 70)

if candidates.height > 0:
    print(f"\n  {best['hypothesis_id']} (threshold={best['threshold']}, h={best['horizon_min']}m)")
    print(f"  {'Cost':>8} {'mean_net':>12} {'t_stat':>8} {'winrate':>8}")
    print(f"  {'-'*40}")
    for cost_stress in _R["cost_grid_round_trip"]:
        ms = test_hypothesis(disc, best_hyp, cost_stress)
        ok = ms["mean_net"] > 0
        print(f"  {cost_stress:>8.4f} {ms['mean_net']:+12.6f} {ms['t_stat']:>8.2f} "
              f"{ms['winrate']:>8.1%} {'✓' if ok else '✗'}")

# ═══════════════════════════════════════════════════════════════
# 8. OVERLAP-ADJUSTED (block by symbol, 30-min blocks)
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("OVERLAP-ADJUSTED (30-min blocks per symbol)")
print("=" * 70)

if candidates.height > 0:
    cond = eval(best_hyp.condition, {"pl": pl})
    sub = disc.filter(cond).filter(
        pl.col(f"return_{best['horizon_min']}m").is_not_null() &
        pl.col("entry_price").is_not_null())

    # Block: 30-minute windows per symbol
    sub = sub.with_columns([
        (pl.col("open_time").dt.epoch("ms") // (30 * 60_000)).alias("block"),
    ])
    # Keep first event per block
    overlap_free = sub.sort(["symbol", "open_time"]).unique(
        ["symbol", "block"], keep="first")

    side = 1.0
    ret_of = overlap_free[f"return_{best['horizon_min']}m"].to_numpy() * side - cost
    t_of, p_of = sp_stats.ttest_1samp(ret_of, 0.0)
    print(f"  Before dedup: n={sub.height}")
    print(f"  After dedup:  n={len(overlap_free)}")
    print(f"  mean_net={ret_of.mean():+.6f}, t={t_of:.2f}, wr={( ret_of > 0).mean():.1%}")
else:
    print("  No candidate.")

# ═══════════════════════════════════════════════════════════════
# 9. BOOTSTRAP CI (block bootstrap, n_boot=2000)
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("BOOTSTRAP CI (block bootstrap, n_boot=2000)")
print("=" * 70)

if candidates.height > 0:
    from src.stats import dependency_stats
    ret_arr = sub[f"return_{best['horizon_min']}m"].to_numpy() * side - cost
    dep = dependency_stats(ret_arr, sub["symbol"].to_list())
    ci_block = dep["block_bootstrap_ci"]
    ci_cluster = dep["cluster_bootstrap_ci"]
    print(f"  Block bootstrap 95% CI: [{ci_block[0]:+.6f}, {ci_block[1]:+.6f}]")
    print(f"  Cluster bootstrap 95% CI: [{ci_cluster[0]:+.6f}, {ci_cluster[1]:+.6f}]")
    print(f"  t_hac={dep['t_hac']:.2f}, p_hac={dep['p_hac']:.4f}")
    ci_contains_zero = (ci_block[0] <= 0 <= ci_block[1])
    print(f"  CI contains 0: {ci_contains_zero}")

# ═══════════════════════════════════════════════════════════════
# 10. TEMPORAL STABILITY
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("TEMPORAL STABILITY")
print("=" * 70)

if candidates.height > 0:
    finalist = {
        "hypothesis_id": best["hypothesis_id"],
        "horizon_min": best["horizon_min"],
        "entry_side": "long",
        "condition": best_hyp.condition,
        "description": best["description"],
        "cost": cost,
    }
    stab = _temporal_stability(events, finalist, cost)
    print(f"  n_months={stab['n']}, n_pos={stab['n_pos']}, "
          f"pos_share={stab['pos_share']:.0%}")
    print(f"  worst_month={stab['worst']:+.6f}, best_month={stab['best']:+.6f}")
    ok_stab = stab["pos_share"] >= _R["stability_pos_share"]
    print(f"  PASS (>= {_R['stability_pos_share']:.0%}): {ok_stab}")

# ═══════════════════════════════════════════════════════════════
# 11. CONCENTRATION
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("CONCENTRATION")
print("=" * 70)

if candidates.height > 0:
    conc = _concentration(events, finalist, cost)
    print(f"  n_symbols={conc['n_symbols']}")
    print(f"  top1_share={conc['top1_share']:.0%}, top5_share={conc['top5_share']:.0%}")
    ok_conc = conc["top1_share"] <= _R["max_symbol_concentration"]
    print(f"  PASS (<= {_R['max_symbol_concentration']:.0%}): {ok_conc}")

# ═══════════════════════════════════════════════════════════════
# 12. FULL CRITIC
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("FULL CRITIC REVIEW")
print("=" * 70)

result = {
    "created_at": datetime.utcnow().isoformat(),
    "q_bh": q,
    "cost_survival": cost,
    "n_hypotheses": len(hypotheses),
    "n_events_total": events.height,
    "n_events": {k: v.height for k, v in periods.items()},
    "discovery_results": disc_results,
    "candidates": candidates.select("hypothesis_id").to_series().to_list() if candidates.height else [],
    "validation": validation_results,
    "oos": oos_results,
    "verdict": "NO_CANDIDATE",
}

if candidates.height > 0:
    result["finalist"] = {
        "hypothesis_id": best["hypothesis_id"],
        "horizon_min": best["horizon_min"],
        "entry_side": "long",
        "condition": best_hyp.condition,
        "description": best["description"],
    }
    if val_pass and oos_pass:
        result["verdict"] = "CANDIDATE"

verdict = review(result, events)

for n, ok, d in verdict.results:
    status = "PASS" if ok is True else "FAIL" if ok is False else "UNKNOWN"
    print(f"  [{status}] {n}: {d}")

print(f"\n  VERDICT: {'PASS' if verdict.passed else 'REJECT'}")
if not verdict.passed:
    print(f"  REASON: {verdict.fail_reason}")

# ═══════════════════════════════════════════════════════════════
# 13. SAVE RESULTS
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("SAVING RESULTS")
print("=" * 70)

from src.research import save_result
from src.critic import save_report

rid = save_result(result)
print(f"  Research result: {rid}")

rid2 = save_report(result, verdict)
print(f"  Critic report: {rid2}")

# Also save detailed MR report
report_lines = [
    "# MR Hypothesis Research Report",
    f"Date: {datetime.utcnow().isoformat()}",
    f"Feature: close / SMA_120 - 1",
    f"Thresholds tested: {thresholds}",
    f"Horizons: {horizons}",
    f"Cost: {cost:.2%} round-trip",
    "",
    "## Discovery Results",
    f"Hypotheses tested: {len(hypotheses)}",
    f"Total events: {events.height}",
    f"Discovery events: {disc.height}",
    "",
    "### All hypotheses (sorted by t-stat):",
]
for r in df_disc.iter_rows(named=True):
    report_lines.append(
        f"  {r['hypothesis_id']}: n={r['n']}, net={r['mean_net']:+.6f}, "
        f"t={r['t_stat']:.2f}, wr={r['winrate']:.1%}, thr={r['threshold']}")

if candidates.height > 0:
    report_lines += [
        "",
        f"### Selected: {best['hypothesis_id']}",
        f"  Threshold: {best['threshold']}",
        f"  Horizon: {best['horizon_min']}m",
        f"  Discovery: n={best['n']}, net={best['mean_net']:+.6f}, t={best['t_stat']:.2f}",
        "",
        "### Validation",
        f"  n={mv['n']}, net={mv['mean_net']:+.6f}, t={mv['t_stat']:.2f}",
        f"  PASS: {val_pass}",
        "",
        "### OOS",
        f"  n={mo['n']}, net={mo['mean_net']:+.6f}, t={mo['t_stat']:.2f}",
        f"  PASS: {oos_pass}",
    ]

report_lines += [
    "",
    "## Critic Verdict",
    f"  {verdict.passed}",
    f"  {verdict.fail_reason or 'No failures'}",
    "",
    "## All checks:",
]
for n, ok, d in verdict.results:
    status = "PASS" if ok is True else "FAIL" if ok is False else "UNKNOWN"
    report_lines.append(f"  [{status}] {n}: {d}")

report_path = Path("/home/vlad/Документы/построение/docs/mr_research_report_20260904.txt")
report_path.write_text("\n".join(report_lines))
print(f"  Full report: {report_path}")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
