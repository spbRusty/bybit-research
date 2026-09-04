"""Dashboard data collectors. Read parquet/JSON from disk, compute panel metrics.

No pipeline logic changes. Pure read-only access to existing artifacts.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    RAW_KLINES_DIR, MARKET_DATA_DIR, CLEAN_CANDLES, FEATURES_DIR,
    EVENTS_DIR, HYPOTHESES_DIR, RESULTS_DIR, REPORTS_DIR,
    PAPER_DIR, PAPER_PORTFOLIO, PAPER_TRADES, LOGS_DIR, ROOT,
)


STALE_THRESHOLD_S = 300  # 5 minutes — no update = stale


def _safe(v):
    """Convert non-JSON-serializable values (datetime, Timestamp) to str."""
    if isinstance(v, (datetime,)):
        return v.isoformat()
    # Polars Timestamps, Datetimes, Dates
    if hasattr(v, 'isoformat'):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return v


def _sanitize(d: dict) -> dict:
    """Sanitize a single dict for JSON."""
    return {k: _safe(v) for k, v in d.items()}


def _mtime(p: Path) -> float:
    """mtime, 0.0 if absent."""
    try:
        return p.stat().st_mtime
    except (OSError, FileNotFoundError):
        return 0.0


def _age_s(p: Path) -> float:
    """Seconds since last modification. Inf if missing."""
    mt = _mtime(p)
    if mt == 0:
        return float("inf")
    return time.time() - mt


def _stale(p: Path, threshold: float = STALE_THRESHOLD_S) -> bool:
    return _age_s(p) > threshold


# ─── 1. System Status ───────────────────────────────────────────────

def get_system_status() -> dict:
    now = time.time()
    log_files = {
        "orchestrator": LOGS_DIR / "orchestrator.log",
        "pipeline": LOGS_DIR / "pipeline.log",
    }
    collector_log = ROOT / "collector" / "logs" / "marketdata.log"

    logs = {}
    for name, path in log_files.items():
        mt = _mtime(path)
        logs[name] = {
            "exists": path.exists(),
            "mtime": datetime.fromtimestamp(mt, tz=timezone.utc).isoformat() if mt else None,
            "age_s": round(_age_s(path), 1),
            "stale": _stale(path, 3600),  # logs stale after 1 hour
        }
    # collector
    mt = _mtime(collector_log)
    logs["collector"] = {
        "exists": collector_log.exists(),
        "mtime": datetime.fromtimestamp(mt, tz=timezone.utc).isoformat() if mt else None,
        "age_s": round(_age_s(collector_log), 1),
        "stale": _stale(collector_log, 300),
    }

    # data directories
    dirs = {
        "raw_klines": RAW_KLINES_DIR,
        "market_data": MARKET_DATA_DIR,
        "clean_candles": CLEAN_CANDLES,
        "features": FEATURES_DIR,
        "events": EVENTS_DIR,
        "hypotheses": HYPOTHESES_DIR,
        "results": RESULTS_DIR,
        "paper": PAPER_DIR,
    }
    directories = {}
    for name, d in dirs.items():
        directories[name] = {
            "exists": d.exists(),
            "path": str(d),
        }

    return {"timestamp": datetime.now(timezone.utc).isoformat(), "logs": logs, "directories": directories}


# ─── 2. Data Download Status ───────────────────────────────────────

def get_data_status() -> dict:
    stats = {}
    for cat in ("linear", "spot"):
        cat_dir = RAW_KLINES_DIR / cat
        if not cat_dir.exists():
            stats[cat] = {"files": 0, "total_size_mb": 0, "newest": None, "oldest": None}
            continue
        files = sorted(cat_dir.glob("*_1m.parquet"))
        if not files:
            stats[cat] = {"files": 0, "total_size_mb": 0, "newest": None, "oldest": None}
            continue
        sizes = sum(f.stat().st_size for f in files)
        # Read timestamps from first/last file (sample)
        oldest_ts = newest_ts = None
        try:
            first = pl.read_parquet(files[0], columns=["open_time"])
            oldest_ts = str(first["open_time"].min())
        except Exception:
            pass
        try:
            last = pl.read_parquet(files[-1], columns=["open_time"])
            newest_ts = str(last["open_time"].max())
        except Exception:
            pass
        stats[cat] = {
            "files": len(files),
            "total_size_mb": round(sizes / 1e6, 1),
            "newest": newest_ts,
            "oldest": oldest_ts,
        }

    # collector market data
    collector_stats = {}
    for stream in ("trades", "orderbook", "futures", "liquidation", "ratio"):
        stream_dir = MARKET_DATA_DIR / stream / "linear"
        if not stream_dir.exists():
            collector_stats[stream] = 0
            continue
        collector_stats[stream] = len(list(stream_dir.glob("*.parquet")))

    return {
        "klines": stats,
        "collector": collector_stats,
        "total_klines_files": sum(s["files"] for s in stats.values()),
        "total_size_mb": round(sum(s["total_size_mb"] for s in stats.values()), 1),
    }


# ─── 3. Market Metrics ─────────────────────────────────────────────

def get_market_metrics() -> dict:
    """Read latest futures data: funding, OI, bid-ask for top symbols."""
    futures_dir = MARKET_DATA_DIR / "futures" / "linear"
    if not futures_dir.exists():
        return {"symbols": [], "summary": {}}

    files = sorted(futures_dir.glob("*.parquet"))
    rows = []
    for f in files[:50]:  # limit to 50 for speed
        try:
            df = pl.read_parquet(f)
            if df.height == 0:
                continue
            last = _sanitize(df.tail(1).to_dicts()[0])
            sym = f.stem
            rows.append({
                "symbol": sym,
                "funding_rate": last.get("funding_rate"),
                "oi": last.get("oi"),
                "oi_value": last.get("oi_value"),
                "last_px": last.get("last_px"),
                "mark_px": last.get("mark_px"),
                "bid1_px": last.get("bid1_px"),
                "ask1_px": last.get("ask1_px"),
                "bid1_sz": last.get("bid1_sz"),
                "ask1_sz": last.get("ask1_sz"),
            })
        except Exception:
            continue

    # Summary: avg funding, total OI
    if rows:
        funding_vals = [r["funding_rate"] for r in rows if r["funding_rate"] is not None]
        oi_vals = [r["oi_value"] for r in rows if r["oi_value"] is not None]
        summary = {
            "symbols_count": len(rows),
            "avg_funding_rate": round(sum(funding_vals) / len(funding_vals), 6) if funding_vals else None,
            "total_oi_usd": round(sum(oi_vals), 0) if oi_vals else None,
        }
    else:
        summary = {}

    return {"symbols": rows[:20], "summary": summary}  # top 20 for display


# ─── 4. Signals ────────────────────────────────────────────────────

def get_signals() -> dict:
    """Read latest signal events parquet."""
    events_path = EVENTS_DIR / "all_events.parquet"
    if not events_path.exists():
        return {"count": 0, "symbols": [], "recent": []}

    try:
        df = pl.read_parquet(events_path)
    except Exception:
        return {"count": 0, "symbols": [], "recent": []}

    if df.height == 0:
        return {"count": 0, "symbols": [], "recent": []}

    sym_counts = [_sanitize(d) for d in (df.group_by("symbol").agg(pl.len().alias("count"))
                  .sort("count", descending=True).to_dicts())]

    recent_cols = [c for c in ["symbol", "category", "open_time", "entry_price",
                               "relative_volume", "relative_range",
                               "return_5m", "return_15m", "return_30m"]
                   if c in df.columns]
    recent = [_sanitize(d) for d in df.select(recent_cols).tail(10).to_dicts()]

    return {
        "count": df.height,
        "symbols": sym_counts[:20],
        "recent": recent,
    }


# ─── 5. Hypotheses & Research ──────────────────────────────────────

def get_hypotheses() -> dict:
    # Load hypothesis definitions
    hyp_path = HYPOTHESES_DIR / "hypotheses_v1.json"
    hypotheses = []
    if hyp_path.exists():
        try:
            hypotheses = json.loads(hyp_path.read_text())
            if isinstance(hypotheses, dict):
                hypotheses = hypotheses.get("hypotheses", [])
        except Exception:
            pass

    # Latest acceptance report
    acceptance = None
    results_files = sorted(RESULTS_DIR.glob("acceptance_*.json"), reverse=True)
    if results_files:
        try:
            acceptance = json.loads(results_files[0].read_text())
        except Exception:
            pass

    # Research results summary
    research_files = sorted(RESULTS_DIR.glob("research_*.json"), reverse=True)
    research_summary = []
    for rf in research_files[:5]:
        try:
            r = json.loads(rf.read_text())
            research_summary.append({
                "run_id": r.get("run_id"),
                "timestamp": r.get("timestamp"),
                "n_hypotheses": r.get("n_hypotheses"),
                "n_candidates": len(r.get("candidates", [])),
                "verdict": r.get("verdict"),
                "best_hypothesis": (r.get("finalist") or {}).get("hypothesis_id"),
                "best_t_stat": (r.get("finalist") or {}).get("t_stat"),
            })
        except Exception:
            continue

    return {
        "count": len(hypotheses),
        "hypotheses": hypotheses[:10],
        "acceptance": acceptance,
        "research_runs": research_summary,
    }


# ─── 6. Paper Trading ──────────────────────────────────────────────

def get_paper_trading() -> dict:
    state_path = PAPER_PORTFOLIO / "state.json"
    state = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text())
        except Exception:
            pass

    trades_path = PAPER_TRADES / "trades.parquet"
    trades = []
    if trades_path.exists():
        try:
            df = pl.read_parquet(trades_path)
            trades = [_sanitize(d) for d in df.tail(10).to_dicts()]
        except Exception:
            pass

    return {
        "state": state,
        "trades_count": len(trades),
        "trades": trades,
    }


# ─── 7. Logs ───────────────────────────────────────────────────────

def get_logs(n: int = 50) -> dict:
    """Last N lines from orchestrator.log + pipeline.log."""
    out = {}
    for name in ("orchestrator", "pipeline"):
        path = LOGS_DIR / f"{name}.log"
        if not path.exists():
            out[name] = []
            continue
        try:
            lines = path.read_text().splitlines()
            out[name] = lines[-n:]
        except Exception:
            out[name] = []
    return out
