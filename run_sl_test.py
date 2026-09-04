"""SL hypothesis test — lightweight discovery, full gates on finalists."""
from __future__ import annotations
import sys, json, logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np
import polars as pl
from scipy import stats

from src.research import (
    Hypothesis, split_periods, benjamini_hochberg,
    _R, HYPOTHESES,
)
from src.stats import dependency_stats

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SL_LEVELS = [0.0003, 0.0005, 0.001, 0.002, 0.005]
COST_GRID = [0.0010, 0.0015, 0.0020, 0.0030]


def make_sl_hyps(sl: float) -> list[Hypothesis]:
    hyps = []
    for h in HYPOTHESES:
        hyps.append(Hypothesis(
            hypothesis_id=f"{h.hypothesis_id}_SL{int(sl*10000)}bp",
            description=f"{h.description} [SL={sl*100:.2f}%]",
            condition=h.condition, entry_side=h.entry_side,
            horizon_min=h.horizon_min, stop_loss=sl, version="1.0",
        ))
    return hyps


def quick_test(events: pl.DataFrame, hyp: Hypothesis, cost: float) -> dict:
    base = {"n": 0, "n_symbols": 0, "n_months": 0, "t_stat": np.nan,
            "p_value": np.nan, "mean_net": np.nan, "median_net": np.nan,
            "winrate": np.nan, "error": ""}
    try:
        cond = eval(hyp.condition, {"pl": pl})
    except Exception as e:
        base["error"] = f"condition: {e}"
        return base
    sub = events.filter(cond).filter(
        pl.col(hyp.target_column).is_not_null() &
        pl.col("entry_price").is_not_null())
    n = sub.height
    if n == 0:
        return base
    side = 1.0 if hyp.entry_side == "long" else -1.0
    if hyp.stop_loss is not None and hyp.mae_column in sub.columns:
        raw_ret = sub[hyp.target_column].to_numpy() * side
        mae = sub[hyp.mae_column].to_numpy() * side
        ret = np.where(mae < -hyp.stop_loss, -hyp.stop_loss, raw_ret) - cost
    else:
        ret = sub[hyp.target_column].to_numpy() * side - cost
    t, p = stats.ttest_1samp(ret, 0.0)
    return {**base, "n": n, "n_symbols": int(sub["symbol"].n_unique()),
            "n_months": int(sub["open_time"].dt.strftime("%Y-%m").n_unique()),
            "t_stat": float(t), "p_value": float(p),
            "mean_net": float(ret.mean()), "median_net": float(np.median(ret)),
            "winrate": float((ret > 0).mean())}


def full_test(events: pl.DataFrame, hyp: Hypothesis, cost: float) -> dict:
    base = quick_test(events, hyp, cost)
    if base["n"] == 0:
        return base
    side = 1.0 if hyp.entry_side == "long" else -1.0
    sub = events.filter(eval(hyp.condition, {"pl": pl})).filter(
        pl.col(hyp.target_column).is_not_null() &
        pl.col("entry_price").is_not_null())
    if hyp.stop_loss is not None and hyp.mae_column in sub.columns:
        raw_ret = sub[hyp.target_column].to_numpy() * side
        mae = sub[hyp.mae_column].to_numpy() * side
        ret = np.where(mae < -hyp.stop_loss, -hyp.stop_loss, raw_ret) - cost
    else:
        ret = sub[hyp.target_column].to_numpy() * side - cost
    dep = dependency_stats(ret, sub["symbol"].to_list())
    base.update({"bootstrap_ci": dep["block_bootstrap_ci"],
                 "cluster_ci": dep["cluster_bootstrap_ci"],
                 "t_hac": dep["t_hac"], "p_hac": dep["p_hac"],
                 "ev_annualized": float(ret.mean()) * 1440 * 365 / hyp.horizon_min})
    return base


def run_pipeline(events: pl.DataFrame) -> dict:
    periods = split_periods(events)
    disc, val, oos = periods["discovery"], periods["validation"], periods["oos"]

    logger.info("=== PHASE 1: DISCOVERY (quick, no bootstrap) ===")
    all_hyps = []
    for sl in SL_LEVELS:
        all_hyps.extend(make_sl_hyps(sl))
    logger.info(f"Total hypotheses: {len(all_hyps)}")

    rows = []
    for i, hyp in enumerate(all_hyps):
        m = quick_test(disc, hyp, _R["survival_cost"])
        m.update({"hypothesis_id": hyp.hypothesis_id, "stop_loss": hyp.stop_loss,
                  "horizon_min": hyp.horizon_min, "entry_side": hyp.entry_side})
        rows.append(m)
    df = pl.DataFrame(rows).sort("t_stat", descending=True)

    p_arr = np.nan_to_num(df["p_value"].to_numpy(), nan=1.0)
    sig_bh = benjamini_hochberg(p_arr, _R["bh_q"])
    mask = (sig_bh & (df["n"].to_numpy() >= _R["min_events"]) &
            (df["n_symbols"].to_numpy() >= _R["min_unique_symbols"]) &
            (df["n_months"].to_numpy() >= _R["min_months"]) &
            (df["t_stat"].to_numpy() >= _R["min_t_stat"]) &
            (df["mean_net"].to_numpy() > 0))
    candidates = df.filter(pl.Series(mask))
    logger.info(f"Candidates after gates: {candidates.height}")
    if candidates.height == 0:
        return {"verdict": "NO_CANDIDATE", "n_hyps": len(all_hyps),
                "discovery_top20": df.head(20).to_dicts()}

    logger.info("=== PHASE 2: COST STRESS ===")
    cost_results = {}
    for cost in COST_GRID:
        cr = []
        for cid in candidates["hypothesis_id"].to_list():
            hyp = next(h for h in all_hyps if h.hypothesis_id == cid)
            m = quick_test(disc, hyp, cost)
            m["hypothesis_id"] = cid
            cr.append(m)
        cost_results[f"cost_{cost*100:.2f}%"] = cr
        logger.info(f"  {cost*100:.2f}%: {sum(1 for r in cr if r['mean_net'] > 0)}/{len(cr)} survive")

    logger.info("=== PHASE 3: VALIDATION + OOS ===")
    finalists = []
    for cid in candidates["hypothesis_id"].to_list():
        hyp = next(h for h in all_hyps if h.hypothesis_id == cid)
        mv = quick_test(val, hyp, _R["survival_cost"])
        mo = quick_test(oos, hyp, _R["survival_cost"])
        logger.info(f"  {cid}: val={mv['mean_net']:+.6f} t={mv['t_stat']:.3f} | "
                    f"oos={mo['mean_net']:+.6f} t={mo['t_stat']:.3f}")
        if mv.get("mean_net", 0) > 0 and mo.get("mean_net", 0) > 0 \
                and mo.get("n", 0) >= _R["min_events"]:
            finalists.append(cid)
    if not finalists:
        return {"verdict": "NO_CANDIDATE", "overall_verdict": "REJECT",
                "n_hyps": len(all_hyps), "candidates": candidates["hypothesis_id"].to_list(),
                "cost_stress": cost_results, "discovery_top20": df.head(20).to_dicts()}

    logger.info(f"=== PHASE 4: FULL TEST ({len(finalists)} finalists) ===")
    full_results = {}
    for cid in finalists:
        hyp = next(h for h in all_hyps if h.hypothesis_id == cid)
        logger.info(f"  Testing {cid} with bootstrap...")
        full_results[cid] = {
            "discovery": full_test(disc, hyp, _R["survival_cost"]),
            "validation": full_test(val, hyp, _R["survival_cost"]),
            "oos": full_test(oos, hyp, _R["survival_cost"]),
        }

    logger.info("=== PHASE 5: TEMPORAL STABILITY ===")
    stability = {}
    for cid in finalists:
        hyp = next(h for h in all_hyps if h.hypothesis_id == cid)
        side = 1.0 if hyp.entry_side == "long" else -1.0
        oos_sub = oos.filter(
            eval(hyp.condition, {"pl": pl}) &
            pl.col(hyp.target_column).is_not_null() &
            pl.col("entry_price").is_not_null())
        if oos_sub.height == 0:
            stability[cid] = {"pos_month_share": 0, "n_months": 0}
            continue
        if hyp.stop_loss is not None and hyp.mae_column in oos_sub.columns:
            raw_ret = oos_sub[hyp.target_column].to_numpy() * side
            mae_arr = oos_sub[hyp.mae_column].to_numpy() * side
            oos_ret = np.where(mae_arr < -hyp.stop_loss, -hyp.stop_loss, raw_ret) - _R["survival_cost"]
        else:
            oos_ret = oos_sub[hyp.target_column].to_numpy() * side - _R["survival_cost"]
        months = oos_sub["open_time"].dt.strftime("%Y-%m").to_list()
        month_rets = {}
        for m, r in zip(months, oos_ret):
            month_rets.setdefault(m, []).append(r)
        month_means = {m: np.mean(rs) for m, rs in month_rets.items()}
        pos_share = sum(1 for v in month_means.values() if v > 0) / max(len(month_means), 1)
        stability[cid] = {"pos_month_share": float(pos_share), "n_months": len(month_means)}
        logger.info(f"  {cid}: pos_months={pos_share:.1%} n_months={len(month_means)}")

    logger.info("=== PHASE 6: CRITIC ===")
    critic = []
    for cid in finalists:
        fr = full_results[cid]
        st = stability.get(cid, {})
        issues = []
        if fr["validation"].get("mean_net", 0) <= 0:
            issues.append(f"val mean_net={fr['validation']['mean_net']:+.6f} <= 0")
        if fr["validation"].get("t_stat", 0) < _R["min_t_stat"]:
            issues.append(f"val t={fr['validation']['t_stat']:.3f} < {_R['min_t_stat']}")
        if fr["oos"].get("mean_net", 0) <= 0:
            issues.append(f"oos mean_net={fr['oos']['mean_net']:+.6f} <= 0")
        if fr["oos"].get("n", 0) < _R["min_events"]:
            issues.append(f"oos n={fr['oos']['n']} < {_R['min_events']}")
        if st.get("pos_month_share", 0) < _R["stability_pos_share"]:
            issues.append(f"pos_months={st.get('pos_month_share',0):.1%} < {_R['stability_pos_share']:.0%}")
        for cost_key, cost_rows in cost_results.items():
            cr = next((r for r in cost_rows if r["hypothesis_id"] == cid), None)
            if cr and cr.get("mean_net", 0) <= 0:
                issues.append(f"fails at {cost_key}")
        v = "PASS" if not issues else f"REJECT: {'; '.join(issues)}"
        critic.append({"hypothesis_id": cid, "verdict": v})
        logger.info(f"  {cid}: {v}")

    overall = "CANDIDATE" if all(c["verdict"] == "PASS" for c in critic) else "REJECT"
    return {"verdict": "CANDIDATE", "overall_verdict": overall, "n_hyps": len(all_hyps),
            "n_candidates": candidates.height, "finalists": finalists,
            "cost_stress": cost_results, "full_results": full_results,
            "temporal_stability": stability, "critic": critic,
            "discovery_top20": df.head(20).to_dicts()}


if __name__ == "__main__":
    events = pl.read_parquet("data/events/signal_events/all_events.parquet")
    logger.info(f"Loaded {events.height:,} events")
    result = run_pipeline(events)
    out_path = Path("data/research/results")
    out_path.mkdir(parents=True, exist_ok=True)
    rid = f"sl_test_{__import__('datetime').datetime.utcnow():%Y%m%dT%H%M%SZ}"
    (out_path / f"{rid}.json").write_text(json.dumps(result, indent=2, default=str))
    print("\n" + "=" * 60)
    print(f"VERDICT: {result.get('verdict')}")
    print(f"OVERALL: {result.get('overall_verdict')}")
    print(f"Finalists: {result.get('finalists', [])}")
    print("=" * 60)
