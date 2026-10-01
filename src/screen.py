"""Экран поиска рабочей гипотезы.

Читает all_events.parquet, генерирует условия "<признак> >/< квантиль" по всем
числовым same-T признакам, гоняет discovery, корректирует множественное
тестирование (БХ), выживших — в validation и OOS, снимает стресс по сетке
издержек и отдаёт финалиста для critic/paper.

Пороги считаются ТОЛЬКО по discovery (test_generator_thresholds_use_
discovery_only). Признаки с будущими колонками (return_/mfe_/mae_/entry_price)
исключены: они дали бы заведомо положительный, но бессмысленный результат.

Запуск: python -m src.screen [--top N] [--full-eval K]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import EVENTS_DIR, RESULTS_DIR, load_toml
from src import research as R

logger = logging.getLogger("screen")

_FEAT = load_toml("features.toml")
_R = load_toml("research.toml")

HORIZONS: list[int] = list(_FEAT["future_horizons_min"])
QUANTILES = [0.10, 0.20, 0.80, 0.90]
MIN_N = 100
CHUNK = 32
NUMERIC = (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64,
           pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)
SKIP_EXACT = {"entry_price", "open_time", "symbol", "category", "event_id",
              "ob_data_quality"}
SKIP_PREFIX = ("return_", "mfe_", "mae_", "ob_post_", "post_signal")
PERIODS = {name: (start, end) for name, start, end in _R["sample_periods"]}


def _dt(s: str) -> pl.Expr:
    y, m, d = (int(p) for p in s.split("-"))
    return pl.datetime(y, m, d, time_unit="ms")


def read_period(src: Path, cols: list[str], period: str) -> pl.DataFrame:
    start, end = PERIODS[period]
    return (pl.scan_parquet(src)
            .select(cols)
            .filter((pl.col("open_time") >= _dt(start)) & (pl.col("open_time") < _dt(end)))
            .collect())


def numeric_features(schema: dict) -> list[str]:
    return [n for n, dtype in schema.items()
            if n not in SKIP_EXACT and not n.startswith(SKIP_PREFIX)
            and dtype in NUMERIC]


def sweep(feat: str, x: np.ndarray, ys: dict[int, np.ndarray], cost: float) -> list[dict]:
    """Все пороги/стороны/горизонты одного признака через префиксные суммы.

    Признак сортируется один раз; каждый порог — это индекс в отсортированном
    массиве, а суммы берутся разностью cumsum. O(N log N) на признак, а не
    O(N) на каждый из порогов.
    """
    out: list[dict] = []
    for h, y in ys.items():
        m = np.isfinite(x) & np.isfinite(y)
        if int(m.sum()) < MIN_N:
            continue
        xs, yv = x[m], y[m]
        order = np.argsort(xs, kind="stable")
        xs, yv = xs[order], yv[order]
        cs = np.concatenate([[0.0], np.cumsum(yv)])
        css = np.concatenate([[0.0], np.cumsum(yv * yv)])
        n = xs.shape[0]
        for thr in np.unique(np.quantile(xs, QUANTILES)):
            j_lt = int(np.searchsorted(xs, thr, side="left"))
            j_gt = int(np.searchsorted(xs, thr, side="right"))
            for side, nn, s, ss in (
                    ("long", n - j_gt, cs[-1] - cs[j_gt], css[-1] - css[j_gt]),
                    ("short", j_lt, cs[j_lt], css[j_lt])):
                if nn < MIN_N:
                    continue
                mean = s / nn
                var = (ss - nn * mean * mean) / (nn - 1)
                if not np.isfinite(var) or var <= 0:
                    continue
                sign = 1.0 if side == "long" else -1.0
                gross = sign * mean
                t = sign * mean / np.sqrt(var / nn)
                out.append({
                    "feature": feat, "op": "gt" if side == "long" else "lt",
                    "threshold": float(thr), "side": side, "horizon_min": h,
                    "n": int(nn), "t_stat": float(t),
                    "p_value": float(2 * stats.t.sf(abs(t), nn - 1)),
                    "gross_mean": float(gross),
                    "mean_net": float(gross - cost),
                })
    return out


def cond_of(feat: str, op: str, thr: float) -> str:
    return f"pl.col('{feat}') {'>' if op == 'gt' else '<'} {thr!r}"


def cheap_stats(df: pl.DataFrame, hyp: R.Hypothesis, cost: float) -> dict:
    """n / t / mean_net + диверсия символов и месяцев БЕЗ бутстрепов.

    Предфильтр перед test_hypothesis: внутри него block-bootstrap на
    ~2M строк разворачивает (n_boot, ceil(n/25)) стартов — десятки секунд
    и ~3 ГБ на кандидата, а отсеивать такие условия всё равно проще
    дешёвым t-тестом и groupby.
    """
    try:
        cond = eval(hyp.condition, {"pl": pl})
    except Exception:
        return {"n": 0, "error": "condition"}
    sub = df.filter(cond).filter(
        pl.col(hyp.target_column).is_not_null() & pl.col("entry_price").is_not_null())
    n = sub.height
    if n == 0:
        return {"n": 0}
    sign = 1.0 if hyp.entry_side == "long" else -1.0
    ret = sub[hyp.target_column].to_numpy() * sign - cost
    t, _ = stats.ttest_1samp(ret, 0.0)
    return {"n": int(n), "t_stat": float(t), "mean_net": float(ret.mean()),
            "n_symbols": int(sub["symbol"].n_unique()),
            "n_months": int(sub["open_time"].dt.strftime("%Y-%m").n_unique())}


def meets(m: dict) -> bool:
    return ((m.get("n") or 0) >= _R["min_events"]
            and (m.get("n_symbols") or 0) >= _R["min_unique_symbols"]
            and (m.get("n_months") or 0) >= _R["min_months"]
            and (m.get("t_stat") or -9) >= _R["min_t_stat"]
            and (m.get("mean_net") or -9) > 0)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Screen for a working hypothesis")
    ap.add_argument("--events", default=str(EVENTS_DIR / "all_events.parquet"))
    ap.add_argument("--out", default=None)
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--full-eval", type=int, default=120,
                    help="максимум кандидатов на полный test_hypothesis")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    t0 = time.time()
    cost = float(_R["survival_cost"])
    q_bh = float(_R["bh_q"])
    src = Path(args.events)

    feats = numeric_features(dict(pl.scan_parquet(src).collect_schema()))
    targets = [f"return_{h}m" for h in HORIZONS]
    logger.info("features=%d horizons=%s cost=%.4f", len(feats), HORIZONS, cost)

    rows: list[dict] = []
    for i in range(0, len(feats), CHUNK):
        chunk = feats[i:i + CHUNK]
        part = read_period(src, ["open_time", *chunk, *targets], "discovery")
        ys = {h: part[f"return_{h}m"].to_numpy().astype(np.float64) for h in HORIZONS}
        for f in chunk:
            x = part[f].to_numpy().astype(np.float64)
            if np.isfinite(x).any():
                rows.extend(sweep(f, x, ys, cost))
        logger.info("swept %d/%d features -> %d candidates (%.0fs)",
                    min(i + CHUNK, len(feats)), len(feats), len(rows), time.time() - t0)
    if not rows:
        logger.error("нет кандидатов")
        return 1

    sig = R.benjamini_hochberg(np.nan_to_num([r["p_value"] for r in rows], nan=1.0), q_bh)
    for r, s in zip(rows, sig):
        r["bh"] = bool(s)
    logger.info("кандидатов=%d, BH-значимых=%d (%.0fs)",
                len(rows), int(sig.sum()), time.time() - t0)

    surv = sorted((r for r in rows if r["bh"] and r["mean_net"] > 0
                   and r["t_stat"] >= _R["min_t_stat"]),
                  key=lambda r: -r["t_stat"])
    if len(surv) > args.full_eval:
        logger.warning("выживших %d > --full-eval %d: беру топ по discovery t "
                       "(БХ уже применён ко всем %d кандидатам)",
                       len(surv), args.full_eval, len(rows))
        surv = surv[:args.full_eval]
    logger.info("в полную оценку: %d", len(surv))

    hyps = [R.Hypothesis(f"S{i:05d}",
                         f"{r['feature']} {r['op']} {r['threshold']:.6g} "
                         f"({r['side']}, {r['horizon_min']}m)",
                         cond_of(r["feature"], r["op"], r["threshold"]),
                         r["side"], r["horizon_min"])
            for i, r in enumerate(surv)]

    need = sorted({h.condition.split("'")[1] for h in hyps})
    ev = {p: read_period(src, ["open_time", "symbol", "entry_price", *targets, *need], p)
          for p in ("discovery", "validation", "oos")}
    logger.info("события: " + ", ".join(f"{k}={v.height}" for k, v in ev.items()))

    by_id = {h.hypothesis_id: h for h in hyps}

    # Полная оценка (с бутстрепами) нужна только финалистам: на кандидата с
    # ~1M строк block-bootstrap разворачивает (2000, ceil(n/25)) стартов.
    # Поэтому сначала дешёвые метрики на всех трёх периодах, дорогой test_hypothesis
    # — только на прошедших.
    cheap_d = {h.hypothesis_id: cheap_stats(ev["discovery"], h, cost) for h in hyps}
    ok = [hid for hid, m in cheap_d.items() if meets(m)]
    ok.sort(key=lambda hid: -(cheap_d[hid]["t_stat"] or -9))
    if len(ok) > args.full_eval:
        logger.warning("acceptance-кандидатов %d > --full-eval %d: беру топ "
                       "по discovery t", len(ok), args.full_eval)
        ok = ok[:args.full_eval]
    logger.info("прошли acceptance на discovery: %d (из %d)", len(ok), len(hyps))

    cheap_v = {hid: cheap_stats(ev["validation"], by_id[hid], cost) for hid in ok}
    cheap_o = {hid: cheap_stats(ev["oos"], by_id[hid], cost) for hid in ok}
    finalists = [hid for hid in ok if meets(cheap_v[hid])]
    logger.info("прошли validation: %d", len(finalists))

    stress: dict[str, dict] = {
        f"{float(cg):.4f}": {hid: cheap_stats(ev["oos"], by_id[hid], float(cg)).get("mean_net")
                             for hid in finalists}
        for cg in _R["cost_grid_round_trip"]}

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
        "sweep_top_by_gross": [
            {k: r[k] for k in ("feature", "op", "threshold", "side", "horizon_min",
                               "n", "t_stat", "gross_mean", "mean_net", "bh")}
            for r in sorted(rows, key=lambda r: -r["gross_mean"])[:50]],
        "finalist": None, "verdict": "NO_CANDIDATE",
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

    path = Path(args.out) if args.out else RESULTS_DIR / (
        f"screen_{datetime.now(tz=timezone.utc):%Y%m%dT%H%M%SZ}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str))

    print("\n" + "=" * 72)
    print(f"кандидатов={len(rows)}  BH-значимых={int(sig.sum())}  acceptance={len(ok)}  "
          f"validation={len(finalists)}  выжили по OOS+издержкам={len(winners)}")
    print(f"вердикт: {out['verdict']}   ({time.time()-t0:.0f}s)")
    if out["finalist"]:
        f = out["finalist"]
        mv, mo = validation[f["hypothesis_id"]], oos[f["hypothesis_id"]]
        print(f"\nФИНАЛИСТ {f['hypothesis_id']}: {f['condition']}  "
              f"{f['entry_side']} {f['horizon_min']}m")
        print(f"  discovery : n={f['n']:<6} t={f['t_stat']:6.2f} EV={f['mean_net']:+.5f}")
        print(f"  validation: n={mv['n']:<6} t={mv['t_stat']:6.2f} EV={mv['mean_net']:+.5f}")
        print(f"  oos       : n={mo['n']:<6} t={mo['t_stat']:6.2f} EV={mo['mean_net']:+.5f}")
        print("  стресс по издержкам (oos mean_net):")
        for cg, mp in stress.items():
            v = mp.get(f["hypothesis_id"])
            print(f"    {cg}: " + (f"{v:+.5f}" if v is not None else "n/a"))
    else:
        best = max(r["gross_mean"] for r in rows)
        print("\nФиналиста нет: ни одно условие не имеет положительного матожидания "
              f"после издержек {cost:.4f}.")
        print(f"Лучшее валовое матожидание по discovery: {best:+.5f} "
              f"({best*1e4:+.2f} б.п.) -> безубыточно только при круговом обороте "
              f"< {best*1e4:.1f} б.п.")
        print("\nТоп по валовому матожиданию (gross vs net после издержек):")
        for r in sorted(rows, key=lambda r: -r["gross_mean"])[:args.top]:
            print(f"  {r['feature']:<28} {r['op']:<2} {r['threshold']:>12.6g}"
                  f"  {r['side']:<5} {r['horizon_min']:>3}m  n={r['n']:<7}"
                  f" t={r['t_stat']:7.2f} gross={r['gross_mean']:+.5f}"
                  f" net={r['mean_net']:+.5f} bh={int(r['bh'])}")
    print("=" * 72)
    print("сохранено:", path)
    return 0 if out["verdict"] == "CANDIDATE" else 2


if __name__ == "__main__":
    sys.exit(main())
