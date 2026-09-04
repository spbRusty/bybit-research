"""Unified orchestrator — single command runs full pipeline with structured stages.

Runs the complete research pipeline with StageResult at every step, producing
machine-readable acceptance reports.

Usage:
  .venv/bin/python -m src.orchestrator [--limit N] [--category linear|spot] [--mr-control]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import LOGS_DIR, RESULTS_DIR, REPORTS_DIR, load_toml
from src import data as data_mod
from src import events as events_mod
from src import features as features_mod
from src import features_breadth as breadth_mod
from src import hypothesis_generator as hgen_mod
from src import research as research_mod
from src import volatility as vol_mod
from src.pipeline import (
    StageResult,
    StageStatus,
    build_acceptance_report,
    compute_config_hash,
    freeze_finalist,
    stage_critic,
    stage_data_validation,
    stage_feature_validation,
    stage_oos_gate,
    stage_parameter_freeze,
    stage_validation_gate,
)

logger = logging.getLogger("orchestrator")

_FEAT = load_toml("features.toml")
_R = load_toml("research.toml")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(module)s %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(LOGS_DIR / "orchestrator.log", encoding="utf-8")])


def _log_stage(s: StageResult) -> None:
    logger.info("STAGE %s: %s (%s)", s.stage, s.status.value,
                "; ".join(s.errors) if s.errors else "OK")


def run_pipeline(limit: int | None = None, category: str | None = None,
                 mr_control: bool = False) -> dict:
    """Full pipeline with structured stage results. Returns acceptance report."""
    t0 = datetime.now(tz=timezone.utc)
    run_id = t0.strftime("%Y%m%dT%H%M%SZ")
    stages: list[StageResult] = []

    logger.info("=== ORCHESTRATOR: %s (run_id=%s) ===", t0.isoformat(), run_id)

    # --- 1. UNIVERSE ---
    universe = data_mod.liquidity_universe()
    if category:
        universe = universe.filter(pl.col("category") == category)
    if limit:
        universe = universe.head(limit)
    logger.info("Universe: %d symbols", universe.height)

    # --- 2. DATA + FEATURES + EVENTS ---
    btc = _load_symbol(_FEAT["btc_symbol"], "linear")
    eth = _load_symbol(_FEAT["eth_symbol"], "linear")
    univ_dfs = data_mod.load_universe_data(universe)
    breadth = breadth_mod.compute_breadth(univ_dfs)
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

    if not all_events:
        s = StageResult(stage="data_loading", status=StageStatus.ERROR,
                        run_id=run_id, config_hash=compute_config_hash(),
                        errors=["No events produced"])
        stages.append(s)
        _log_stage(s)
        return build_acceptance_report(stages, {}, run_id)

    all_cols = sorted(set(c for ev in all_events for c in ev.columns))
    aligned = []
    for ev in all_events:
        missing = [c for c in all_cols if c not in ev.columns]
        for c in missing:
            ev = ev.with_columns(pl.lit(None).alias(c))
        aligned.append(ev.select(all_cols))
    events = pl.concat(aligned)
    logger.info("Events: %d", events.height)

    # --- 3. DATA VALIDATION GATE ---
    s = stage_data_validation(events, universe)
    stages.append(s)
    _log_stage(s)
    if s.status == StageStatus.STOP:
        return build_acceptance_report(stages, {}, run_id)

    # --- 4. FEATURE VALIDATION ---
    s = stage_feature_validation(events)
    stages.append(s)
    _log_stage(s)

    # --- 5. HYPOTHESIS GENERATION ---
    if mr_control:
        hypotheses = _mr_hypotheses()
    else:
        baseline = research_mod.HYPOTHESES
        disc_events = research_mod.split_periods(events)["discovery"]
        generated = hgen_mod.generate_hypotheses(disc_events)
        generated = hgen_mod.filter_by_freq(generated, disc_events,
                                            _R["min_events"])
        hypotheses = list(baseline) + generated
    logger.info("Hypotheses: %d", len(hypotheses))

    # --- 6. RESEARCH (discovery + validation + OOS metrics) ---
    result = research_mod.run_research(events, hypotheses=hypotheses)
    rid = research_mod.save_result(result)
    logger.info("Research %s: candidates=%s", rid, result["candidates"])

    # --- 7. VALIDATION GATE (per candidate) ---
    passed_val: list[str] = []
    for cid in result.get("candidates", []):
        if cid in result.get("validation", {}):
            s = stage_validation_gate(result["validation"][cid])
            stages.append(s)
            _log_stage(s)
            if s.passed:
                passed_val.append(cid)

    # --- 8. OOS GATE (per validation-passed candidate) ---
    passed_oos: list[str] = []
    for cid in passed_val:
        if cid in result.get("oos", {}):
            s = stage_oos_gate(result["oos"][cid])
            stages.append(s)
            _log_stage(s)
            if s.passed:
                passed_oos.append(cid)

    # --- 9. FINALIST (only after both gates pass) ---
    if passed_oos:
        cid = passed_oos[0]
        hyp = next(h for h in hypotheses if h.hypothesis_id == cid)
        result["finalist"] = {
            "hypothesis_id": cid, "horizon_min": hyp.horizon_min,
            "entry_side": hyp.entry_side, "condition": hyp.condition,
            "description": hyp.description,
        }
        result["verdict"] = "CANDIDATE"
        logger.info("Finalist: %s", cid)

    # --- 10. PARAMETER FREEZE ---
    frozen = freeze_finalist(result)
    s = stage_parameter_freeze(result, frozen)
    stages.append(s)
    _log_stage(s)

    # --- 11. CRITIC ---
    s = stage_critic(result, events)
    stages.append(s)
    _log_stage(s)

    # --- 12. ACCEPTANCE REPORT ---
    report = build_acceptance_report(stages, result, run_id)
    report_path = RESULTS_DIR / f"acceptance_{run_id}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False,
                                      default=str))
    logger.info("Acceptance report: %s (verdict=%s)", report_path,
                report["verdict"])

    dt = (datetime.now(tz=timezone.utc) - t0).total_seconds()
    logger.info("=== DONE in %.1fs: verdict=%s ===", dt, report["verdict"])
    return report


def _load_symbol(symbol: str, category: str) -> pl.DataFrame | None:
    try:
        return data_mod.load_validated(symbol, category)
    except Exception as e:
        logger.warning("Failed to load %s/%s: %s", symbol, category, e)
        return None


def _mr_hypotheses() -> list[research_mod.Hypothesis]:
    """MR control hypotheses: close/SMA_120-1 < threshold + green → long."""
    W = 120
    thresholds = [-0.01, -0.02, -0.03, -0.05]
    horizons = [10, 30]
    hyps = []
    for thr in thresholds:
        for h in horizons:
            hid = f"MR_SMA120_t{abs(thr)*100:.0f}p_h{h}"
            cond = (f"(pl.col('mr_sma120') < {thr}) & pl.col('is_green')")
            hyps.append(research_mod.Hypothesis(
                hypothesis_id=hid,
                description=f"MR: close/SMA120-1 < {thr} + green → long {h}m",
                condition=cond, entry_side="long", horizon_min=h))
    return hyps


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Unified orchestrator: full pipeline with structured stages")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--category", type=str, default=None,
                    choices=["linear", "spot"])
    ap.add_argument("--mr-control", action="store_true",
                    help="Run MR hypothesis control test only")
    args = ap.parse_args()
    setup_logging()
    report = run_pipeline(limit=args.limit, category=args.category,
                          mr_control=args.mr_control)
    sys.exit(0 if report["verdict"] in ("PASS", "REJECT") else 1)
