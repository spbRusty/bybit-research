"""L2 (orderbook-feature) hypothesis screening driver.

Standalone driver for the new orderbook-feature event class. It reuses the
candle screen machinery in ``src/screen.py`` (quantile sweep, cheap stats,
acceptance gates) and the BH correction in ``src/research.py`` — no statistics
are reimplemented here.

Run: ``python -m src.l2_screen [--events PATH] [--out PATH] [--top N] [--full-eval K]``
Events path override: ``--events`` or the ``L2_EVENTS`` env var (default
``data/events/signal_events/l2_events.parquet``).

Hypothesis generation choice
----------------------------
Generic quantile sweep restricted to ``ob_*`` columns (the whitelist prefix in
``config/l2_research.toml``), NOT ``hypothesis_generator.ob_rules()``. Rationale:
the sweep path reuses ``screen.sweep`` / ``screen.cheap_stats`` /
``screen.cond_of`` / ``screen.meets`` / ``research.benjamini_hochberg`` /
``research.test_hypothesis`` verbatim, and produces one p-value per
(feature, threshold, side, horizon) so BH can be applied over the FULL L2
hypothesis set of the run. ``ob_rules()`` would only test one hardcoded
threshold per feature and would still need its own scoring loop.

Entry side / return sign
------------------------
``return_{h}m`` in the events file is the raw long-side forward return
(``close(T+h)/open(T+1)-1``, see ``src/events.py``); it is NOT signed by the
signal side. ``screen.sweep`` pairs ``gt`` with ``long`` and ``lt`` with
``short`` by convention. For L2 the prescribed side per (feature, operator)
lives in ``config/ob_hypothesis_rules.toml`` (``entry_side``). This driver
therefore remaps each swept row to the whitelist's ``entry_side`` where a rule
exists, flipping the sign of ``gross_mean``/``t_stat`` and recomputing
``mean_net`` accordingly. The two-sided p-value is invariant under a side flip,
so BH is unaffected. (feature, operator) pairs absent from the whitelist keep
screen's default gt->long / lt->short convention.

Verdicts
--------
* NOT_READY    — events file missing, or present but no usable ob_* features /
                  horizons / hypotheses. Notification sent (explanatory), exit 0.
* PENDING_DATA — events exist and were scored, but validation/oos windows are
                 not yet populated (before 2027-01-04). Explicitly inconclusive,
                 NOT a candidate. Notification sent, exit 0.
* CANDIDATE / NO_CANDIDATE — full acceptance on/after 2027-01-04, mirroring
                 ``screen.py`` semantics. Notification sent, exit 0.

Every run dumps all scored rows (pre-finalist) to
``data/research/results/l2_screen_allrows_{UTCts}.parquet`` so a future combined
BH across candle+L2 families can be recomputed from raw p-values
(``combined_bh_pending: true`` in the JSON output).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import EVENTS_DIR, RESULTS_DIR, load_toml
from src import research as R
from src import screen as S

logger = logging.getLogger("l2_screen")

_L2 = load_toml("l2_research.toml")
PERIODS: dict[str, tuple[str, str]] = {
    name: (start, end) for name, (start, end) in _L2["periods"].items()}
GATES: dict = _L2["gates"]
FEATURE_PREFIX: str = _L2["features"]["whitelist_prefix"]
ACCEPTANCE_DATE = datetime(2027, 1, 4, tzinfo=timezone.utc)
NTFY_URL = "https://ntfy.sh/openagent_trade"

_OB_RULES = load_toml("ob_hypothesis_rules.toml")
OB_SIDE: dict[tuple[str, str], str] = {
    (r["feature_id"], r["operator"]): r.get("entry_side", "long")
    for r in _OB_RULES.get("ob_features", [])}


def _check_gates() -> None:
    """Pre-registration integrity: L2 gates must not be weaker than research.toml."""
    r = load_toml("research.toml")
    weaker = [k for k in ("min_events", "min_unique_symbols", "min_months", "min_t_stat")
              if GATES[k] < r[k]]
    if GATES["bh_q"] > r["bh_q"]:            # smaller q = stricter
        weaker.append("bh_q")
    if GATES["survival_cost"] < r["survival_cost"]:
        weaker.append("survival_cost")
    if weaker:
        raise ValueError(f"l2_research.toml gates weaker than research.toml: {weaker}")


def _notify(verdict: str, body: str) -> None:
    try:
        r = subprocess.run(
            ["curl", "-sS", "-H", f"Title: L2-скрин: {verdict}", "-d", body, NTFY_URL],
            capture_output=True, text=True, timeout=20)
        if r.returncode != 0:
            logger.warning("ntfy curl rc=%s: %s", r.returncode, r.stderr.strip())
    except Exception as e:  # noqa: BLE001 - notification must never crash the driver
        logger.warning("ntfy error: %s", e)


def _save(out: dict, ts: str) -> Path:
    path = RESULTS_DIR / f"l2_screen_{ts}.json"
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    return path


def _emit(out: dict, ts: str) -> int:
    path = _save(out, ts)
    print(json.dumps(out, ensure_ascii=False, default=str))
    logger.info("saved %s", path)
    return 0


def _remap_sides(rows: list[dict], cost: float) -> None:
    """Apply ob_hypothesis_rules.toml entry_side; flip gross/t and recompute net."""
    for r in rows:
        want = OB_SIDE.get((r["feature"], r["op"]))
        if want and want != r["side"]:
            r["side"] = want
            r["t_stat"] = -r["t_stat"]
            r["gross_mean"] = -r["gross_mean"]
            r["mean_net"] = r["gross_mean"] - cost


def _top_by_gross(rows: list[dict], n: int) -> list[dict]:
    keys = ("feature", "op", "threshold", "side", "horizon_min",
            "n", "t_stat", "gross_mean", "mean_net", "bh")
    return [{k: r[k] for k in keys}
            for r in sorted(rows, key=lambda r: -r["gross_mean"])[:n]]


def _bp(x: float) -> str:
    """Доля → б.п. со знаком, напр. ``+12.3``."""
    return f"{x * 10000:+.1f}"


def _explain(verdict: str) -> str:
    """Пояснение вердикта простым языком (даты берутся из конфига, не хардкод)."""
    disc, val = PERIODS["discovery"][1], PERIODS["validation"][1]
    final = ACCEPTANCE_DATE.strftime("%Y-%m-%d")
    return {
        "NOT_READY": ("События ещё не собраны или в них нет признаков ob_* / горизонтов — "
                      "скрининг не запускался. Это не сигнал торговать."),
        "PENDING_DATA": (f"Вывод пока ранний: discovery-период идёт (закроется {disc}), "
                         f"validation/oos окна ещё не наступили (validation — {val}); "
                         f"честный вердикт не раньше {final}. Это не сигнал торговать."),
        "CANDIDATE": ("Найдена гипотеза, прошедшая все ворота (BH-значимость, validation и "
                      "oos-окно) — кандидат на дальнейшую проверку. Совмещённая BH-коррекция "
                      "candle+L2 ещё не выполнена, поэтому это не сигнал торговать."),
        "NO_CANDIDATE": ("Значимых гипотез, покрывающих издержки на выживание, нет — "
                         "окончательного кандидата на текущих данных нет. "
                         "Это не сигнал торговать."),
    }[verdict]


def _top_lines(rows: list[dict], n: int = 3) -> list[str]:
    return [f"{r['feature']} {r['op']} {r['threshold']:.6g} | {r['side']} | "
            f"{r['horizon_min']}m | gross {_bp(r['gross_mean'])} bp | "
            f"нетто {_bp(r['mean_net'])} bp"
            for r in _top_by_gross(rows, n)]


def _notify_report(verdict: str, *, coverage: str = "", screening: str = "",
                   top: list[str] | None = None, extra: str = "",
                   next_step: str = "") -> None:
    """Собрать русскоязычный многострочный отчёт и отправить его в ntfy."""
    sections = [f"Вердикт: {verdict}\n{_explain(verdict)}"]
    if coverage:
        sections.append(f"Данные:\n{coverage}")
    if screening:
        sections.append(f"Скрининг:\n{screening}")
    if extra:
        sections.append(extra)
    if top:
        sections.append("Топ-3 по gross:\n" + "\n".join(top))
    if next_step:
        sections.append(f"Что дальше: {next_step}")
    sections.append("L2-поиск идёт ежедневно автоматически; "
                     "дашборд: http://127.0.0.1:8420")
    body = "\n\n".join(sections)
    if len(body) > 3500:                      # ntfy limit guard
        body = body[:3497] + "..."
    _notify(verdict, body)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="L2 orderbook-feature hypothesis screen")
    ap.add_argument("--events",
                    default=os.environ.get("L2_EVENTS") or str(EVENTS_DIR / "l2_events.parquet"))
    ap.add_argument("--out", default=None)
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--full-eval", type=int, default=120,
                    help="максимум кандидатов на полный test_hypothesis")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    t0 = time.time()
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    src = Path(args.events)

    try:
        _check_gates()
    except ValueError as e:
        logger.error("%s", e)
        return 1

    if not src.exists():
        out = {"created_at": datetime.now(tz=timezone.utc).isoformat(),
               "verdict": "NOT_READY",
               "reason": f"events file not found: {src}",
               "events_path": str(src)}
        _notify_report("NOT_READY", coverage=f"файл событий не найден: {src}")
        return _emit(out, ts)

    cost = float(GATES["survival_cost"])
    q_bh = float(GATES["bh_q"])
    schema = dict(pl.scan_parquet(src).collect_schema())
    feats = [n for n in S.numeric_features(schema) if n.startswith(FEATURE_PREFIX)]
    horizons = [h for h in S.HORIZONS if f"return_{h}m" in schema]
    targets = [f"return_{h}m" for h in horizons]
    logger.info("features=%d horizons=%s cost=%.4f", len(feats), horizons, cost)

    if not feats or not horizons:
        out = {"created_at": datetime.now(tz=timezone.utc).isoformat(),
               "verdict": "NOT_READY",
               "reason": f"no {FEATURE_PREFIX}* numeric features or return horizons in {src}",
               "events_path": str(src), "n_features": len(feats), "horizons": horizons}
        _notify_report("NOT_READY",
                       coverage=f"файл: {src}\nпризнаков ob_*: {len(feats)}, "
                                f"горизонтов: {len(horizons)}")
        return _emit(out, ts)

    rows: list[dict] = []
    for i in range(0, len(feats), S.CHUNK):
        chunk = feats[i:i + S.CHUNK]
        part = S.read_period(src, ["open_time", *chunk, *targets], "discovery",
                             periods=PERIODS)
        ys = {h: part[f"return_{h}m"].to_numpy().astype(np.float64) for h in horizons}
        for f in chunk:
            x = part[f].to_numpy().astype(np.float64)
            if np.isfinite(x).any():
                rows.extend(S.sweep(f, x, ys, cost))
        logger.info("swept %d/%d features -> %d candidates (%.0fs)",
                    min(i + S.CHUNK, len(feats)), len(feats), len(rows), time.time() - t0)

    if not rows:
        out = {"created_at": datetime.now(tz=timezone.utc).isoformat(),
               "verdict": "NOT_READY",
               "reason": f"no hypotheses reached MIN_N={S.MIN_N} in discovery period",
               "events_path": str(src), "n_features": len(feats), "horizons": horizons}
        _notify_report("NOT_READY",
                       coverage=f"файл: {src}\nпризнаков ob_*: {len(feats)}, "
                                f"горизонтов: {len(horizons)}\n"
                                f"гипотез с MIN_N={S.MIN_N} в discovery: 0")
        return _emit(out, ts)

    _remap_sides(rows, cost)

    sig = R.benjamini_hochberg(np.nan_to_num([r["p_value"] for r in rows], nan=1.0), q_bh)
    for r, s in zip(rows, sig):
        r["bh"] = bool(s)
    logger.info("candidates=%d, BH-significant=%d (%.0fs)",
                len(rows), int(sig.sum()), time.time() - t0)

    allrows_path = RESULTS_DIR / f"l2_screen_allrows_{ts}.parquet"
    pl.DataFrame(rows).write_parquet(allrows_path)

    ot = pl.read_parquet(src, columns=["open_time", "symbol"])
    dates_covered = [str(ot["open_time"].min())[:10], str(ot["open_time"].max())[:10]]
    counts = {name: ot.filter((pl.col("open_time") >= S._dt(start))
                              & (pl.col("open_time") < S._dt(end))).height
              for name, (start, end) in PERIODS.items()}
    now = datetime.now(tz=timezone.utc)
    periods_complete = now >= ACCEPTANCE_DATE and all(counts.values())
    logger.info("periods=%s complete=%s dates=%s", counts, periods_complete, dates_covered)

    if not periods_complete:
        out = {
            "created_at": now.isoformat(),
            "verdict": "PENDING_DATA",
            "reason": "validation/oos windows not yet populated; acceptance cannot be evaluated",
            "q_bh": q_bh, "cost_survival": cost,
            "n_hypotheses": len(rows), "rows_covered": len(rows),
            "n_features_swept": len(feats),
            "dates_covered": dates_covered,
            "n_events": counts,
            "periods_complete": False,
            "n_bh_significant": int(sig.sum()),
            "bh_significant_count": int(sig.sum()),
            "top_10_by_gross": _top_by_gross(rows, 10),
            "combined_bh_pending": True,
            "allrows_path": str(allrows_path),
            "events_path": str(src),
            "note": "INCONCLUSIVE: not a candidate; re-run on/after 2027-01-04",
        }
        best = max(r["gross_mean"] for r in rows)
        covers = "покрывает" if best >= cost else "НЕ покрывает"
        _notify_report(
            "PENDING_DATA",
            coverage=f"{dates_covered[0]}..{dates_covered[1]}, "
                     f"дат: {ot['open_time'].dt.date().n_unique()}, "
                     f"строк: {ot.height}, "
                     f"символов: {ot['symbol'].n_unique()}, "
                     f"признаков ob_*: {len(feats)}",
            screening=f"гипотез прогнано: {len(rows)}, BH-значимых (q={q_bh}): {int(sig.sum())}\n"
                      f"лучший gross: {_bp(best)} bp из {cost * 10000:.1f} bp издержек "
                      f"— издержки {covers}",
            top=_top_lines(rows),
            next_step=f"окно validation откроется {PERIODS['validation'][0]}, "
                      f"oos — {PERIODS['oos'][0]}; честный вердикт возможен "
                      f"не раньше {ACCEPTANCE_DATE:%Y-%m-%d}")
        return _emit(out, ts)

    # --- Full acceptance (all periods populated, on/after 2027-01-04) ---
    surv = sorted((r for r in rows if r["bh"] and r["mean_net"] > 0
                   and r["t_stat"] >= GATES["min_t_stat"]),
                  key=lambda r: -r["t_stat"])
    if len(surv) > args.full_eval:
        logger.warning("выживших %d > --full-eval %d: беру топ по discovery t",
                       len(surv), args.full_eval)
        surv = surv[:args.full_eval]

    hyps = [R.Hypothesis(f"L2{i:05d}",
                         f"{r['feature']} {r['op']} {r['threshold']:.6g} "
                         f"({r['side']}, {r['horizon_min']}m)",
                         S.cond_of(r["feature"], r["op"], r["threshold"]),
                         r["side"], r["horizon_min"])
            for i, r in enumerate(surv)]
    need = sorted({h.condition.split("'")[1] for h in hyps})
    ev = {p: S.read_period(src, ["open_time", "symbol", "entry_price", *targets, *need],
                           p, periods=PERIODS)
          for p in ("discovery", "validation", "oos")}
    logger.info("события: " + ", ".join(f"{k}={v.height}" for k, v in ev.items()))
    by_id = {h.hypothesis_id: h for h in hyps}

    cheap_d = {h.hypothesis_id: S.cheap_stats(ev["discovery"], h, cost) for h in hyps}
    ok = [hid for hid, m in cheap_d.items() if S.meets(m, gates=GATES)]
    ok.sort(key=lambda hid: -(cheap_d[hid]["t_stat"] or -9))
    if len(ok) > args.full_eval:
        ok = ok[:args.full_eval]
    cheap_v = {hid: S.cheap_stats(ev["validation"], by_id[hid], cost) for hid in ok}
    cheap_o = {hid: S.cheap_stats(ev["oos"], by_id[hid], cost) for hid in ok}
    finalists = [hid for hid in ok if S.meets(cheap_v[hid], gates=GATES)]
    logger.info("acceptance=%d validation=%d", len(ok), len(finalists))

    stress: dict[str, dict] = {
        f"{float(cg):.4f}": {hid: S.cheap_stats(ev["oos"], by_id[hid], float(cg)).get("mean_net")
                             for hid in finalists}
        for cg in GATES["cost_grid_round_trip"]}

    full: dict[str, dict] = {}
    validation: dict[str, dict] = {}
    oos: dict[str, dict] = {}
    for hid in finalists:
        h = by_id[hid]
        for period, sink in (("discovery", full), ("validation", validation), ("oos", oos)):
            m = R.test_hypothesis(ev[period], h, cost)
            m.update({"hypothesis_id": hid, "condition": h.condition,
                      "entry_side": h.entry_side, "horizon_min": h.horizon_min,
                      "description": h.description})
            sink[hid] = m

    surv_key = f"{cost:.4f}"
    winners = [hid for hid in finalists
               if (stress[surv_key].get(hid) or -9) > 0
               and (oos[hid].get("t_stat") or -9) > 0]
    winners.sort(key=lambda hid: -(oos[hid].get("t_stat") or -9))

    out = {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "q_bh": q_bh, "cost_survival": cost,
        "n_hypotheses": len(rows), "n_features_swept": len(feats),
        "n_events_total": int(ev["discovery"].height),
        "n_events": {k: int(v.height) for k, v in ev.items()},
        "n_bh_significant": int(sig.sum()), "n_full_eval": len(hyps),
        "n_acceptance": len(ok), "n_validation": len(finalists),
        "discovery_results": sorted(full.values(), key=lambda m: -(m.get("t_stat") or -9)),
        "candidates": winners, "validation": validation, "oos": oos,
        "cost_stress_oos_mean_net": stress,
        "best_gross_mean_discovery": float(max(r["gross_mean"] for r in rows)),
        "sweep_top_by_gross": _top_by_gross(rows, 50),
        "finalist": None, "verdict": "NO_CANDIDATE",
        "combined_bh_pending": True,
        "allrows_path": str(allrows_path),
        "events_path": str(src),
        "periods_complete": True,
    }
    if winners:
        hid = winners[0]
        fin = {k: v for k, v in full[hid].items() if k != "p_value"}
        fin.update({"hypothesis_id": hid, "condition": by_id[hid].condition,
                    "entry_side": by_id[hid].entry_side,
                    "horizon_min": by_id[hid].horizon_min,
                    "description": by_id[hid].description,
                    "target_column": by_id[hid].target_column})
        out["finalist"] = fin
        out["verdict"] = "CANDIDATE"

    best = max(r["gross_mean"] for r in rows)
    covers = "покрывает" if best >= cost else "НЕ покрывает"
    coverage = (f"{dates_covered[0]}..{dates_covered[1]}, "
                f"дат: {ot['open_time'].dt.date().n_unique()}, строк: {ot.height}, "
                f"символов: {ot['symbol'].n_unique()}, признаков ob_*: {len(feats)}\n"
                + ", ".join(f"{k}: {v.height}" for k, v in ev.items()))
    screening = (f"гипотез прогнано: {len(rows)}, BH-значимых (q={q_bh}): {int(sig.sum())}, "
                 f"прошли отбор: {len(ok)}, финалистов validation: {len(finalists)}\n"
                 f"лучший gross: {_bp(best)} bp из {cost * 10000:.1f} bp издержек "
                 f"— издержки {covers}")
    if out["verdict"] == "CANDIDATE":
        f = out["finalist"]
        gross = f["mean_net"] + cost    # test_hypothesis возвращает только net
        _notify_report(
            "CANDIDATE",
            coverage=coverage, screening=screening, top=_top_lines(rows),
            extra=(f"Финалист:\n{f['condition']} | {f['entry_side']} | {f['horizon_min']}m | "
                   f"n={f['n']} | t={f['t_stat']:.2f} | gross {_bp(gross)} bp | "
                   f"нетто {_bp(f['mean_net'])} bp"),
            next_step=f"окна заполнены (validation закрылось {PERIODS['validation'][1]}); "
                      f"дальше — совмещённая BH-коррекция candle+L2, "
                      f"решение о торговле только после неё")
    else:
        _notify_report(
            "NO_CANDIDATE",
            coverage=coverage, screening=screening, top=_top_lines(rows),
            next_step=f"окна заполнены (validation закрылось {PERIODS['validation'][1]}); "
                      f"следующий шанс появится только с новыми данными или новыми гипотезами")
    return _emit(out, ts)


if __name__ == "__main__":
    sys.exit(main())