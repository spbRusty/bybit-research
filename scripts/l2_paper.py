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
import re
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

logger = logging.getLogger("l2_paper")

TRADE_LOG_KEEP = 500


def _candidate() -> dict | None:
    """Торгуем ТОЛЬКО финалиста скрина с вердиктом CANDIDATE.

    PENDING_DATA / NO_CANDIDATE / NOT_READY не дают права на paper: иначе
    discovery-подгонка превращается в ложное «почти-результат».
    """
    files = sorted(RESULTS_DIR.glob("l2_screen_*.json"))
    if not files:
        logger.warning("нет l2_screen JSON в %s — пейпер не запущен", RESULTS_DIR)
        return None
    d = json.loads(files[-1].read_text())
    verdict = d.get("verdict")
    fin = d.get("finalist")
    if verdict != "CANDIDATE" or not fin:
        logger.info("последний скрин: %s (%s) — paper не запускается",
                    verdict, files[-1].name)
        return None
    logger.info("кандидат: %s | %s | %sm | net=%+.2fbp",
                fin.get("condition"), fin.get("entry_side"),
                fin.get("horizon_min"), (fin.get("mean_net") or 0) * 1e4)
    return fin


def _columns(fin: dict) -> list[str]:
    h = int(fin["horizon_min"])
    m = re.search(r"pl\.col\('([^']+)'\)", fin["condition"])
    cols = {"open_time", "symbol", "category", "entry_price",
            f"return_{h}m", f"mfe_{h}m", f"mae_{h}m"}
    if m:
        cols.add(m.group(1))
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
    if "category" in ev.columns:
        ev = ev.filter(pl.col("category") == "linear")
    if cutoff:
        ev = ev.filter(pl.col("open_time") > datetime.fromisoformat(cutoff))
    return ev.sort("open_time")


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    fin = _candidate()
    if fin is None:
        return 0

    hyps = [R.Hypothesis(
        "L2CAND",
        f"{fin['condition']} ({fin['entry_side']}, {fin['horizon_min']}m)",
        fin["condition"], fin["entry_side"], int(fin["horizon_min"]))]

    state0 = P.load_shadow_state()
    cutoff = state0.get("l2_processed_until")
    n_before = len(state0.get("trades", []))

    ev = _events(_columns(fin), cutoff)
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
    state["l2_hypotheses"] = [fin["condition"]]
    P.save_shadow_state(state)

    logger.info("shadow: %s", json.dumps(res, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
