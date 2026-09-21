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
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import LOGS_DIR, RESULTS_DIR, REPORTS_DIR, load_toml
from src import candle_trigger as ct_mod
from src import data as data_mod
from src import events as events_mod
from src import features as features_mod
from src import features_breadth as breadth_mod
from src import hypothesis_generator as hgen_mod
from src import research as research_mod
from src import volatility as vol_mod
from src.orderbook_integration import (
    join_ob_features,
    get_ob_provenance,
    _get_ob_columns,
    assert_no_post_signal_features,
    build_post_signal_dataset,
)
from src.pipeline import (
    StageResult,
    StageStatus,
    build_acceptance_report,
    compute_config_hash,
    freeze_finalist,
    stage_critic,
    stage_data_validation,
    stage_feature_validation,
    stage_ob_feature_validation,
    stage_oos_gate,
    stage_parameter_freeze,
    stage_validation_gate,
)
from src.registry import (
    HypothesisLifecycle,
    HypothesisStatus,
    update_from_research,
    get_eligible_for_paper,
    get_shadow_eligible,
)

from src.notify import (
    notify_hypothesis_candidate,
    notify_hypothesis_validated,
    notify_paper_started,
    notify_research,
    notify_shadow_summary,
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


_COND_COL_RE = re.compile(r"pl\.col\('([^']+)'\)")


def _condition_cols(condition: str) -> set[str]:
    """Колонки, упомянутые в polars-условии гипотезы (pl.col('X'))."""
    return set(_COND_COL_RE.findall(condition))


def _required_columns(extra_conditions: tuple[str, ...] = ()) -> list[str]:
    """Колонки, реально потребляемые downstream (research, critic, shadow, triggers).

    Выводятся из фактических потребителей, НЕ хардкодятся:
      - baseline H001-H008 (research.HYPOTHESES) condition columns;
      - mr-control условия;
      - feature_ids правил генерации (hgen.all_rules: default + OB-правила);
      - фичи candle trigger (orderbook_capture.toml RULES);
      - таргеты return_/mfe_/mae_{h}m из features.toml future_horizons_min;
      - мета: open_time, symbol, category, event_id, entry_price;
      - vol_gk_30d (consumer: paper.shadow_run / paper_run_backtest — риск-стоп);
      - OB feature columns (_get_ob_columns) + ob_data_quality.
    """
    cols: set[str] = set()
    # 1. Условия гипотез (baseline + mr-control)
    for hyp in list(research_mod.HYPOTHESES) + _mr_hypotheses():
        cols |= _condition_cols(hyp.condition)
    # 2. Правила генерации гипотез (candle + OB)
    for rule in hgen_mod.all_rules():
        cols.add(rule.feature_id)
    # 3. Candle trigger правила (фичи, читаемые по последней свече)
    for rule in ct_mod.RULES:
        cols.add(rule.feature)
    # 4. Таргеты
    for h in _FEAT["future_horizons_min"]:
        cols |= {f"return_{h}m", f"mfe_{h}m", f"mae_{h}m"}
    # 5. Мета + OB-качество
    cols |= {"open_time", "symbol", "category", "event_id", "entry_price"}
    # 6. Риск-колонка для paper (shadow_run / paper_run_backtest)
    cols |= {"vol_gk_30d"}
    cols |= set(_get_ob_columns()) | {"ob_data_quality"}
    return sorted(cols)


def _tag_report(report: dict, experiment_id: str | None,
                cycle_id: str | None) -> dict:
    """Проставляет experiment_id/cycle_id Research Controller в acceptance report."""
    if experiment_id is not None:
        report["experiment_id"] = experiment_id
    if cycle_id is not None:
        report["cycle_id"] = cycle_id
    return report


def run_pipeline(limit: int | None = None, category: str | None = None,
                 mr_control: bool = False,
                 hypothesis_specs: list[research_mod.Hypothesis] | None = None,
                 auto_paper: bool = False,
                 experiment_id: str | None = None,
                 cycle_id: str | None = None) -> dict:
    """Full pipeline with structured stage results. Returns acceptance report.

    Research Controller hooks:
      - hypothesis_specs: при задании заменяет штатную генерацию гипотез (§6);
      - auto_paper=False: гарантированно запрещает auto trigger MODE A (§14);
      - experiment_id/cycle_id: попадают в acceptance report.
    """
    t0 = datetime.now(tz=timezone.utc)
    run_id = t0.strftime("%Y%m%dT%H%M%SZ")
    stages: list[StageResult] = []

    logger.info("=== ORCHESTRATOR: %s (run_id=%s%s) ===", t0.isoformat(), run_id,
                f", experiment={experiment_id}" if experiment_id else "")

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
    klines_max = None
    for df in univ_dfs.values():
        mx = df["open_time"].max()
        klines_max = mx if klines_max is None or mx > klines_max else klines_max
    oos_end = klines_max.isoformat() if klines_max is not None else None
    breadth = breadth_mod.compute_breadth(univ_dfs)
    extra_conds = tuple(h.condition for h in hypothesis_specs) if hypothesis_specs else ()
    required_cols = _required_columns(extra_conds)
    all_events = []
    n_loaded = 0
    full_cols: set[str] = set()
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
            full_cols |= set(ev.columns)
            ev = ev.select([c for c in required_cols if c in ev.columns])
            all_events.append(ev)
        del df, ev
        n_loaded += 1
    del univ_dfs, breadth, btc, eth

    if not all_events:
        s = StageResult(stage="data_loading", status=StageStatus.ERROR,
                        run_id=run_id, config_hash=compute_config_hash(),
                        errors=["No events produced"])
        stages.append(s)
        _log_stage(s)
        return _tag_report(build_acceptance_report(stages, {}, run_id),
                           experiment_id, cycle_id)

    all_cols = sorted(set(c for ev in all_events for c in ev.columns))
    aligned = []
    for ev in all_events:
        missing = [c for c in all_cols if c not in ev.columns]
        for c in missing:
            ev = ev.with_columns(pl.lit(None).alias(c))
        aligned.append(ev.select(all_cols))
    events = pl.concat(aligned)
    del all_events, aligned
    logger.info("Events: %d (candle features only)", events.height)

    # --- 2b. ORDERBOOK FEATURE INTEGRATION ---
    events = join_ob_features(events)
    ob_cols = [c for c in events.columns if c.startswith("ob_")]
    logger.info("OB integration: %d OB columns added", len(ob_cols))

    # Assert: каждая колонка, реально требуемая downstream и существовавшая в
    # полном (не-slim) событии хотя бы для одного символа, присутствует в slim.
    missing_req = sorted(set(required_cols) & full_cols - set(events.columns))
    if missing_req:
        raise RuntimeError(f"Slimming dropped required columns: {missing_req}")
    missing_ob = sorted((set(_get_ob_columns()) | {"ob_data_quality"})
                        - set(events.columns))
    if missing_ob:
        raise RuntimeError(f"OB columns missing after join: {missing_ob}")

    # --- 2c. POST-SIGNAL OB DATASET (отдельный артефакт, вне same-T) ---
    # Пост-сигнальные фичи (reconstructed_captures) НЕ входят в same-T
    # predictive набор: guard не допускает ob_post_*/post_signal в events
    # (look-ahead bias). Они сохраняются отдельным research-артефактом.
    assert_no_post_signal_features(events)
    try:
        post_ds = build_post_signal_dataset(events)
        if post_ds.height:
            logger.info("Post-signal OB dataset: %d events (%s)",
                        post_ds.height,
                        "data/research/post_signal/post_signal_events.parquet")
    except Exception as e:
        logger.warning("Post-signal OB build failed (research continues): %s", e)

    # --- 3. DATA VALIDATION GATE ---
    s = stage_data_validation(events, universe)
    stages.append(s)
    _log_stage(s)
    if s.status == StageStatus.STOP:
        return _tag_report(build_acceptance_report(stages, {}, run_id),
                           experiment_id, cycle_id)

    # --- 4. FEATURE VALIDATION ---
    s = stage_feature_validation(events)
    stages.append(s)
    _log_stage(s)

    # --- 4b. ORDERBOOK FEATURE VALIDATION (per-feature coverage gate) ---
    s = stage_ob_feature_validation(events)
    stages.append(s)
    _log_stage(s)
    ob_ineligible = set()
    if s.status != StageStatus.SKIPPED and s.metrics:
        ob_ineligible = set(s.metrics.get("ineligible_features", []))

    # --- 5. CANDLE TRIGGER → ORDERBOOK CAPTURE (post-validation gate) ---
    # Обязательно после data/feature validation: триггерный файл в _TRIGGERS_DIR
    # наблюдается Rust-collector'ом → подписка на реальный OB-захват не должна
    # стартовать ранее, чем прошли все валидационные гейты.
    triggers = ct_mod.evaluate_all_triggers(events)
    if triggers:
        logger.info("Candle triggers fired: %d (orderbook captures queued)",
                     len(triggers))
    else:
        logger.info("Candle triggers: none fired this run")

    # --- 6. HYPOTHESIS GENERATION ---
    if hypothesis_specs is not None:
        # Research Controller инжектирует готовый набор гипотез вместо штатной
        # генерации (baseline + generator + mr). Приёмочные пороги не меняются.
        hypotheses = list(hypothesis_specs)
        logger.info("Hypotheses: injected %d spec(s) by Research Controller",
                    len(hypotheses))
    elif mr_control:
        hypotheses = _mr_hypotheses()
    else:
        baseline = research_mod.HYPOTHESES
        disc_events = research_mod.split_periods(events)["discovery"]
        generated = hgen_mod.generate_hypotheses(disc_events, rules=hgen_mod.all_rules())
        generated = hgen_mod.filter_by_freq(generated, disc_events,
                                            _R["min_events"])
        hypotheses = list(baseline) + generated
    if ob_ineligible:
        hypotheses = [h for h in hypotheses
                      if not (_condition_cols(h.condition) & ob_ineligible)]
    logger.info("Hypotheses: %d", len(hypotheses))

    # --- 6. RESEARCH (discovery + validation + OOS metrics) ---
    result = research_mod.run_research(events, hypotheses=hypotheses,
                                       oos_end=oos_end)
    rid = research_mod.save_result(result)
    logger.info("Research %s: candidates=%s", rid, result["candidates"])

    for cid in result.get("candidates", []):
        hyp = next((h for h in hypotheses if h.hypothesis_id == cid), None)
        if hyp:
            try:
                notify_hypothesis_candidate(cid, hyp.description)
            except Exception as e:
                logger.warning("notify_candidate failed: %s", e)

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

    # --- 11b. REGISTRY UPDATE (AFTER Critic) ---
    critic_passed = (s.status == StageStatus.PASS)
    registry = HypothesisLifecycle()
    if critic_passed:
        transitions = update_from_research(registry, result)
        if transitions:
            for t in transitions:
                logger.info("Lifecycle: %s %s -> %s", t["hypothesis_id"], t["from"], t["to"])
                if t["to"] == "VALIDATED":
                    hyp = next((h for h in hypotheses if h.hypothesis_id == t["hypothesis_id"]), None)
                    if hyp:
                        try:
                            notify_hypothesis_validated(t["hypothesis_id"], hyp.description)
                        except Exception as e:
                            logger.warning("notify_validated failed: %s", e)
    else:
        logger.warning("Critic REJECT — skipping registry update (no VALIDATED transitions)")
        transitions = []
    eligible_a = get_eligible_for_paper(registry)
    eligible_b = get_shadow_eligible(registry)
    logger.info("Registry: %d validated (MODE A), %d candidates (MODE B)",
                len(eligible_a), len(eligible_b))

    # --- 12. ACCEPTANCE REPORT ---
    report = _tag_report(build_acceptance_report(stages, result, run_id),
                         experiment_id, cycle_id)
    report_path = RESULTS_DIR / f"acceptance_{run_id}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False,
                                      default=str))
    logger.info("Acceptance report: %s (verdict=%s)", report_path,
                report["verdict"])

    # --- 13. SHADOW PAPER (MODE B) ---
    # auto_paper=False (Research Controller) гарантирует, что paper.py не
    # запускается вообще: ни MODE B, ни MODE A. PAPER — только вручную.
    if not auto_paper:
        logger.info("auto_paper=False: shadow/MODE A paper skipped "
                    "(paper is manual under Research Controller)")
    elif eligible_b:
        try:
            from src.paper import shadow_run
            shadow_hyps = [h for h in hypotheses if h.hypothesis_id in eligible_b]
            if shadow_hyps:
                shadow_result = shadow_run(events, shadow_hyps)
                logger.info("Shadow paper: %d trades, PnL=%.4f",
                            shadow_result["trades_executed"],
                            shadow_result["realized_pnl"])
                report["shadow_paper"] = shadow_result
                try:
                    notify_shadow_summary(
                        shadow_result["trades_executed"],
                        shadow_result["realized_pnl"],
                        shadow_result["balance"],
                    )
                except Exception as e:
                    logger.warning("notify_shadow_summary failed: %s", e)
        except Exception as e:
            logger.warning("Shadow paper failed: %s", e)

    # --- 14. AUTO TRIGGER MODE A ---
    if auto_paper and eligible_a:
        try:
            triggered = auto_paper_trigger(registry)
            if triggered:
                logger.info("AUTO TRIGGER: Started MODE A for %s", triggered)
                report["auto_triggered"] = triggered
                try:
                    notify_paper_started("VALIDATED", triggered[0])
                except Exception as e:
                    logger.warning("notify_paper_started failed: %s", e)
        except Exception as e:
            logger.warning("Auto paper trigger failed: %s", e)

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


def auto_research_loop(interval_sec: int = 3600, limit: int | None = None) -> None:
    """Run pipeline on schedule. Blocks until interrupted.

    Args:
        interval_sec: Seconds between pipeline runs (default: 3600 = 1 hour)
        limit: Max symbols to process (None = all)
    """
    import time
    from src.paper import shadow_run
    from src.registry import get_shadow_eligible

    logger.info("=== AUTO RESEARCH LOOP: interval=%ds ===", interval_sec)
    run_count = 0

    while True:
        try:
            run_count += 1
            logger.info("--- Run %d starting ---", run_count)

            report = run_pipeline(limit=limit)
            logger.info("Run %d: verdict=%s", run_count, report["verdict"])

            # Shadow paper: execute trades for CANDIDATE hypotheses
            if report.get("candidates"):
                try:
                    events = _load_latest_events()
                    if events is not None and events.height > 0:
                        registry = HypothesisLifecycle()
                        shadow_ids = get_shadow_eligible(registry)
                        if shadow_ids:
                            from src.research import HYPOTHESES
                            shadow_hyps = [h for h in HYPOTHESES
                                          if h.hypothesis_id in shadow_ids]
                            if shadow_hyps:
                                shadow_result = shadow_run(events, shadow_hyps)
                                logger.info("Shadow paper: %d trades, PnL=%.4f",
                                            shadow_result["trades_executed"],
                                            shadow_result["realized_pnl"])
                except Exception as e:
                    logger.warning("Shadow paper failed: %s", e)

            logger.info("--- Run %d done ---", run_count)

        except Exception as e:
            logger.error("Pipeline error: %s", e)

        logger.info("Sleeping %ds until next run...", interval_sec)
        time.sleep(interval_sec)


def _load_latest_events():
    from config.settings import EVENTS_DIR
    files = sorted(EVENTS_DIR.glob("*_events.parquet"))
    if not files:
        return None
    return pl.read_parquet(files[-1])


def gated_research_once(limit: int | None = None,
                        category: str | None = None,
                        mr_control: bool = False) -> dict:
    """One gated research run: data-ready gate -> pipeline -> reviewer hook.

    Идемпотентен: flock-лок research.lock, состояние last_run.json.
    Недостаточно данных -> вердикт SKIP (exit 0 для таймера).
    """
    from src.data_ready import (Lock, check_ready, git_head, load_last_run,
                                save_last_run)
    from src.pipeline import _make_run_id

    lock = Lock()
    if not lock.acquire():
        logger.info("Gated run SKIP: another research run is in progress")
        return {"verdict": "SKIP", "reason": "lock"}

    try:
        ready, reasons, metrics = check_ready()
        if not ready:
            logger.info("Gated run SKIP (%s)", "; ".join(reasons))
            return {"verdict": "SKIP", "reasons": reasons,
                    "metrics": {"klines_max": metrics["klines"]["klines_max"]}}

        logger.info("Gated run: data-ready OK, launching pipeline")
        report = run_pipeline(limit=limit, category=category, mr_control=mr_control)

        try:
            notify_research(report, report["verdict"] in ("PASS", "CANDIDATE"),
                            paper=None)
        except Exception as e:
            logger.warning("notify_research failed: %s", e)

        # Post-research state для идемпотентности gate
        klines_max = metrics["klines"]["klines_max"]
        if klines_max is not None:
            save_last_run({
                "run_id": report.get("run_id") or _make_run_id(),
                "finished_at_utc": datetime.now(tz=timezone.utc).isoformat(),
                "verdict": report["verdict"],
                "config_hash": metrics["config_hash"],
                "git_head": git_head(),
                "klines_max": str(klines_max),
                "ob_valid_symbols": metrics["ob"]["ob_valid_symbols"],
                "n_events": report.get("n_events", {}),
            })
            logger.info("Saved last_run.json: klines_max=%s verdict=%s",
                        klines_max, report["verdict"])

        try:
            _post_research_reviewer(run_id=report.get("run_id"))
        except Exception as e:
            logger.warning("Reviewer hook failed: %s", e)
        return report
    finally:
        lock.release()


def _post_research_reviewer(run_id: str | None = None) -> None:
    """Post-research reviewer hook: detached system_reviewer --once.

    Reviewer не влияет на research (try/except + detached процесс):
    его FAIL не должен ронять gate/ран.
    """
    try:
        import os
        import subprocess
        env = dict(os.environ)
        env.setdefault("REVIEW_MODEL", "opencode/big-pickle")
        env["REVIEW_NOTIFY_ON_PASS"] = "0"
        log = open(LOGS_DIR / "reviewer_hook.log", "a")
        subprocess.Popen(
            [sys.executable, "-m", "src.system_reviewer", "--once"],
            env=env, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        logger.info("Reviewer hook spawned (run=%s)", run_id or "-")
    except Exception as e:
        logger.warning("Reviewer hook failed: %s", e)


def auto_paper_trigger(registry: HypothesisLifecycle | None = None) -> list[str]:
    """Start MODE A paper for newly VALIDATED hypotheses.

    Returns list of hypothesis IDs that were triggered.
    """
    from src.paper import load_state, save_state
    from src.registry import get_eligible_for_paper

    if registry is None:
        registry = HypothesisLifecycle()

    eligible = get_eligible_for_paper(registry)
    if not eligible:
        return []

    state = load_state()
    already_active = state.get("hypothesis_id")
    triggered = []

    for hyp_id in eligible:
        if already_active == hyp_id:
            continue

        logger.info("AUTO TRIGGER: Starting MODE A paper for %s", hyp_id)
        state["mode"] = "VALIDATED"
        state["hypothesis_id"] = hyp_id
        state["started_at"] = datetime.now(tz=timezone.utc).isoformat()
        save_state(state)
        triggered.append(hyp_id)
        break

    return triggered


def premature_paper_guard(report: dict) -> tuple[bool, str]:
    """Block paper trading unless full pipeline completed.

    Returns (allowed, reason).
    """
    verdict = report.get("verdict")
    has_finalist = report.get("finalist") is not None
    has_candidates = bool(report.get("candidates"))

    if verdict == "STOP":
        return False, "Pipeline stopped (data validation failed)"
    if verdict == "ERROR":
        return False, "Pipeline error"
    if verdict == "NO_CANDIDATE":
        return False, "No candidates passed discovery"
    if has_candidates and not has_finalist:
        return False, "Candidates exist but none passed all gates"
    if not has_finalist:
        return False, "No finalist"
    if verdict not in ("PASS", "REJECT"):
        return False, f"Unexpected verdict: {verdict}"

    stages = report.get("stages", [])
    required = {"data_validation", "feature_validation", "critic", "parameter_freeze"}
    present = {s.get("stage") for s in stages}
    missing = required - present
    if missing:
        return False, f"Missing required stages: {missing}"

    return True, "All gates passed"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Unified orchestrator: full pipeline with structured stages")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--category", type=str, default=None,
                    choices=["linear", "spot"])
    ap.add_argument("--mr-control", action="store_true",
                    help="Run MR hypothesis control test only")
    ap.add_argument("--auto", action="store_true",
                    help="Run in auto-loop mode (continuous pipeline execution)")
    ap.add_argument("--interval", type=int, default=3600,
                    help="Interval in seconds for auto-loop (default: 3600)")
    ap.add_argument("--gated", action="store_true",
                    help="Single gated run: data-ready gate + pipeline + reviewer hook")
    args = ap.parse_args()
    setup_logging()

    if args.auto:
        auto_research_loop(interval_sec=args.interval, limit=args.limit)
    elif args.gated:
        report = gated_research_once(limit=args.limit, category=args.category,
                                     mr_control=args.mr_control)
        # SKIP — нормальный исход для таймера при нехватке данных (exit 0)
        ok = report["verdict"] in ("PASS", "REJECT", "CANDIDATE", "SKIP")
        sys.exit(0 if ok else 1)
    else:
        report = run_pipeline(limit=args.limit, category=args.category,
                              mr_control=args.mr_control)
        sys.exit(0 if report["verdict"] in ("PASS", "REJECT") else 1)
