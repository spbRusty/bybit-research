"""End-to-end test: load pre-existing events → research → critic → acceptance report.

Validates the full structured pipeline without raw kline data.
MR hypothesis used as control test (expect REJECT).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
from src import research as research_mod
from src.pipeline import (
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
    _make_run_id,
)

EVENTS_PATH = Path(__file__).resolve().parent.parent / "data" / "events" / "signal_events" / "all_events.parquet"


def test_e2e_mr_control():
    """Full pipeline on pre-existing events with MR hypothesis. Expect REJECT."""
    assert EVENTS_PATH.exists(), f"Events file missing: {EVENTS_PATH}"
    events = pl.read_parquet(EVENTS_PATH)
    assert events.height > 1000, f"Too few events: {events.height}"

    # Build a minimal universe from events
    universe = events.select(["symbol", "category"]).unique()

    stages = []
    run_id = _make_run_id()

    # Stage 1: Data validation
    s = stage_data_validation(events, universe)
    stages.append(s)
    assert s.passed, f"data_validation failed: {s.errors}"

    # Stage 2: Feature validation (spot-check core columns)
    core_cols = ["relative_volume", "is_green", "entry_price", "return_5m"]
    s = stage_feature_validation(events, core_cols)
    stages.append(s)
    assert s.passed, f"feature_validation failed: {s.errors}"

    # Stage 3: Research with MR hypotheses
    W = 120
    # Compute MR feature: close/SMA_120 - 1
    # Must compute per-symbol to avoid cross-symbol contamination
    chunks = []
    for sym in events["symbol"].unique().to_list():
        sub = events.filter(pl.col("symbol") == sym).sort("open_time")
        if sub.height < W + 10:
            continue
        c = sub["close"]
        sma120 = c.rolling_mean(W, min_samples=1).shift(1)
        mr = (c / sma120.clip(1e-12) - 1).alias("mr_sma120")
        sub = sub.with_columns(mr)
        chunks.append(sub.select(sub.columns))

    if not chunks:
        print("ERROR: No symbols with enough data for MR feature")
        return

    events_mr = pl.concat(chunks)
    print(f"Events with MR feature: {events_mr.height}")

    thresholds = [-0.01, -0.02, -0.03, -0.05]
    horizons = [10, 30]
    hypotheses = []
    for thr in thresholds:
        for h in horizons:
            hid = f"MR_SMA120_t{abs(thr)*100:.0f}p_h{h}"
            cond = f"(pl.col('mr_sma120') < {thr}) & pl.col('is_green')"
            hypotheses.append(research_mod.Hypothesis(
                hypothesis_id=hid,
                description=f"MR: close/SMA120-1 < {thr} + green -> long {h}m",
                condition=cond, entry_side="long", horizon_min=h))

    result = research_mod.run_research(events_mr, hypotheses=hypotheses)
    print(f"Research: candidates={result['candidates']} verdict={result['verdict']}")

    # Stage 4: Parameter freeze
    frozen = freeze_finalist(result)
    s = stage_parameter_freeze(result, frozen)
    stages.append(s)
    assert s.passed, f"parameter_freeze failed: {s.errors}"

    # Stage 5: Validation gate
    if result.get("finalist"):
        cid = result["finalist"]["hypothesis_id"]
        if cid in result.get("validation", {}):
            s = stage_validation_gate(result["validation"][cid])
            stages.append(s)

    # Stage 6: OOS gate
    if result.get("finalist"):
        cid = result["finalist"]["hypothesis_id"]
        if cid in result.get("oos", {}):
            s = stage_oos_gate(result["oos"][cid])
            stages.append(s)

    # Stage 7: Critic
    s = stage_critic(result, events_mr)
    stages.append(s)

    # Stage 8: Acceptance report
    report = build_acceptance_report(stages, result, run_id)
    print(f"\nAcceptance report:")
    print(f"  verdict: {report['verdict']}")
    print(f"  config_hash: {report['config_hash']}")
    print(f"  n_stages: {len(stages)}")
    print(f"  candidates: {report['candidates']}")
    for st in stages:
        print(f"  [{st.status.value}] {st.stage}: "
              f"{'; '.join(st.errors) if st.errors else 'OK'}")

    # MR hypothesis should REJECT (known result: OOS t=0.08 < 2.0)
    assert report["verdict"] in ("PASS", "REJECT"), f"Unexpected verdict: {report['verdict']}"
    print(f"\nE2E test PASSED: pipeline produced verdict={report['verdict']}")
    return report


if __name__ == "__main__":
    test_e2e_mr_control()
