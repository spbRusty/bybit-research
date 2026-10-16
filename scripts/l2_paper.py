"""Shadow-paper (MODE B) the newest L2 screen candidates forward.

Invoked after the L2 screen on each l2_search cycle. Two properties matter and
are why this is not a one-liner: (1) events are read only after
`l2_processed_until` so a 3h re-run never re-trades an event, and (2) the trade
detail log is exported to parquet and then rotated down, because a single
candidate matches tens of thousands of events per day and would otherwise grow
data/paper/shadow/state.json by tens of MB per run. Cumulative accounting lives
in the scalar counters (balance/trade_count/win_count), which rotation leaves
untouched.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import EVENTS_DIR, RESULTS_DIR
from src import paper as P
from src import research as R
from src import screen as S

logger = logging.getLogger("l2_paper")

TRADE_LOG_KEEP = 500
MAX_CANDIDATES = 3


def _candidates() -> list[dict]:
    files = sorted(RESULTS_DIR.glob("l2_screen_allrows_*.parquet"))
    if not files:
        logger.warning("нет l2_screen allrows в %s — пейпер не запущен", RESULTS_DIR)
        return []
    rows = pl.read_parquet(files[-1])
    picked = (rows
              .filter((pl.col("mean_net") > 0) & pl.col("bh"))
              .sort("mean_net", descending=True)
              .head(MAX_CANDIDATES)
              .to_dicts())
    logger.info("allrows=%s строк=%d positive-net+bh=%d",
                files[-1].name, rows.height, len(picked))
    for r in picked:
        logger.info("  кандидат: %s %s %.6g (%s, %sm) net=%+.2fbp n=%d",
                    r["feature"], r["op"], r["threshold"], r["side"],
                    r["horizon_min"], r["mean_net"] * 1e4, r["n"])
    return picked


def _columns(cands: list[dict]) -> list[str]:
    cols = {"open_time", "symbol", "entry_price"}
    for r in cands:
        h = int(r["horizon_min"])
        cols |= {r["feature"], f"return_{h}m", f"mfe_{h}m", f"mae_{h}m"}
    return sorted(cols)


def _events(columns: list[str], cutoff: str | None) -> pl.DataFrame:
    files = sorted((EVENTS_DIR / "l2_events").glob("*.parquet"))
    if not files:
        logger.warning("нет партиций l2_events — пейпер не запущен")
        return pl.DataFrame(schema={c: pl.Float64 for c in columns})
    if cutoff is None:
        files = files[-1:]
    else:
        files = [p for p in files if p.stem >= cutoff[:10]] or files[-1:]
    ev = pl.concat([pl.read_parquet(p, columns=columns) for p in files])
    if cutoff:
        ev = ev.filter(pl.col("open_time") > datetime.fromisoformat(cutoff))
    return ev.sort("open_time")


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cands = _candidates()
    if not cands:
        return 0

    hyps = [R.Hypothesis(
                f"L2{i:05d}",
                f"{r['feature']} {r['op']} {r['threshold']:.6g} "
                f"({r['side']}, {r['horizon_min']}m)",
                S.cond_of(r["feature"], r["op"], r["threshold"]),
                r["side"], int(r["horizon_min"]))
            for i, r in enumerate(cands)]

    state0 = P.load_shadow_state()
    cutoff = state0.get("l2_processed_until")
    n_before = len(state0.get("trades", []))

    ev = _events(_columns(cands), cutoff)
    if ev.height == 0:
        logger.info("нет новых событий после %s", cutoff)
        return 0
    logger.info("события к разбору: %d (cutoff=%s)", ev.height, cutoff)

    res = P.shadow_run(ev, hyps)

    state = P.load_shadow_state()
    fresh = state.get("trades", [])[n_before:]
    if fresh:
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = P.PAPER_SHADOW / f"trades_{ts}.parquet"
        pl.DataFrame(fresh).write_parquet(out)
        logger.info("выгружено %d сделок -> %s", len(fresh), out.name)

    state["trades"] = state.get("trades", [])[-TRADE_LOG_KEEP:]
    state["l2_processed_until"] = ev["open_time"].max().isoformat()
    state["l2_hypotheses"] = [f"{r['feature']} {r['op']} {r['threshold']:.6g}"
                              for r in cands]
    P.save_shadow_state(state)

    logger.info("shadow: %s", json.dumps(res, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
