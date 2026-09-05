"""Dashboard data collectors. Read-only access to existing pipeline artifacts.

All data comes from existing files on disk. No pipeline logic changes.
If a data source doesn't exist — returns None/empty, never fabricates values.
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

_cache: dict[str, tuple[float, object]] = {}


def _cached(key: str, ttl: int, fn):
    now = time.time()
    if key in _cache and now - _cache[key][0] < ttl:
        return _cache[key][1]
    result = fn()
    _cache[key] = (now, result)
    return result


def _mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except (OSError, FileNotFoundError):
        return 0.0


def _age_s(p: Path) -> float:
    mt = _mtime(p)
    return time.time() - mt if mt else float("inf")


def _safe(v):
    if isinstance(v, datetime):
        return v.isoformat()
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return v


def _sanitize(d: dict) -> dict:
    return {k: _safe(v) for k, v in d.items()}


# ─── Config readers (cached, no changes to logic) ──────────────────

def _load_toml(name: str) -> dict:
    from config.settings import load_toml
    return load_toml(name)


# ─── 1. Paper Trading ──────────────────────────────────────────────

def get_paper() -> dict:
    from config.settings import load_toml
    risk = load_toml("risk.toml")

    STATE_FILE = PAPER_PORTFOLIO / "state.json"
    state = None
    state_exists = STATE_FILE.exists()

    if state_exists:
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception:
            state = None

    trades_files = sorted(PAPER_TRADES.glob("paper_*.parquet"))
    all_trades = []
    for tf in trades_files:
        try:
            df = pl.read_parquet(tf)
            all_trades.extend(df.to_dicts())
        except Exception:
            continue

    has_state = state is not None and state.get("trade_count", 0) > 0
    has_trades = len(all_trades) > 0

    if not has_state and not has_trades:
        status = "NOT READY"
        status_reason = "No trades executed"
    else:
        status = "STOPPED"
        status_reason = "Not running (manual mode)"

    pnl_pct = None
    profit_factor = None
    if state and state.get("trade_count", 0) > 0 and state.get("balance"):
        pnl_pct = (state["balance"] - risk["equity_start"]) / risk["equity_start"] * 100
        if state.get("loss_count", 0) > 0 and state.get("win_count", 0) > 0:
            avg_win = state.get("realized_pnl", 0) / max(state["win_count"], 1)
            profit_factor = state.get("win_count", 0) * avg_win / max(abs(state.get("realized_pnl", 0) - state.get("win_count", 0) * avg_win), 0.01) if state.get("realized_pnl", 0) != 0 else None

    return {
        "status": status,
        "status_reason": status_reason,
        "state_exists": state_exists,
        "balance": state.get("balance") if state else None,
        "equity_start": risk["equity_start"],
        "realized_pnl": state.get("realized_pnl") if state else None,
        "pnl_pct": pnl_pct,
        "trade_count": state.get("trade_count", 0) if state else 0,
        "win_count": state.get("win_count", 0) if state else 0,
        "loss_count": state.get("loss_count", 0) if state else 0,
        "win_rate": state.get("win_rate", 0.0) if state else 0.0,
        "profit_factor": profit_factor,
        "max_drawdown": state.get("max_drawdown", 0.0) if state else 0.0,
        "fees": state.get("fees", 0.0) if state else 0.0,
        "slippage": state.get("slippage", 0.0) if state else 0.0,
        "open_positions": state.get("open_positions", []) if state else [],
        "trades_count": len(all_trades),
        "trades": all_trades[-10:] if all_trades else [],
    }


# ─── 2. Risk / Position Sizing ─────────────────────────────────────

def get_risk_params() -> dict:
    from config.settings import load_toml
    risk = load_toml("risk.toml")

    equity = 10_000.0
    state_file = PAPER_PORTFOLIO / "state.json"
    if state_file.exists():
        try:
            st = json.loads(state_file.read_text())
            if st.get("balance", 0) > 0:
                equity = st["balance"]
        except Exception:
            pass

    risk_usd = equity * risk["risk_per_trade_pct"]

    return {
        "equity_start": risk["equity_start"],
        "equity_current": equity,
        "risk_per_trade_pct": risk["risk_per_trade_pct"],
        "risk_per_trade_usd": round(risk_usd, 2),
        "stop_k_vol": risk["stop_k_vol"],
        "take_k_stop": risk["take_k_stop"],
        "max_positions": 1,
        "max_risk_pct": risk["risk_per_trade_pct"] * 100,
        "instrument_min_lot": risk["instrument_min_lot"],
        "instrument_qty_step": risk["instrument_qty_step"],
        "instrument_min_notional": risk["instrument_min_notional"],
        "instrument_min_stake_usdt": round(risk["instrument_min_notional"], 2),
    }


# ─── 3. Trading Costs ──────────────────────────────────────────────

def get_trading_costs() -> dict:
    from config.settings import load_toml
    risk = load_toml("risk.toml")
    research = load_toml("research.toml")

    return {
        "fee_round_trip": risk["fee_round_trip"],
        "fee_round_trip_pct": risk["fee_round_trip"] * 100,
        "slippage_bps": risk["slippage_bps"],
        "slippage_pct": risk["slippage_bps"] / 100,
        "backtest_cost_grid": research.get("cost_grid_round_trip", []),
        "survival_cost": research.get("survival_cost"),
    }


# ─── 4. Instrument Info ────────────────────────────────────────────

def get_instrument_info() -> dict:
    instruments_file = MARKET_DATA_DIR / "symbols" / "linear.txt"
    has_symbols = instruments_file.exists()
    n_symbols = 0
    if has_symbols:
        try:
            n_symbols = len([l for l in instruments_file.read_text().splitlines() if l.strip()])
        except Exception:
            pass

    return {
        "available": False,
        "reason": "Collector fetches instruments-info from Bybit but does NOT save per-symbol specs (minOrderQty, qtyStep, tickSize). Only symbol list is preserved.",
        "symbols_count": n_symbols,
        "fields_missing": ["minOrderQty", "qtyStep", "tickSize", "minNotional", "pricePrecision", "qtyPrecision"],
    }


# ─── 5. Stake Levels (derived from risk params) ────────────────────

def get_stake_levels() -> dict:
    from config.settings import load_toml
    risk = load_toml("risk.toml")

    equity = risk["equity_start"]
    state_file = PAPER_PORTFOLIO / "state.json"
    if state_file.exists():
        try:
            st = json.loads(state_file.read_text())
            if st.get("balance", 0) > 0:
                equity = st["balance"]
        except Exception:
            pass

    risk_pct = risk["risk_per_trade_pct"]
    min_lot = risk["instrument_min_lot"]
    min_notional = risk["instrument_min_notional"]
    qty_step = risk["instrument_qty_step"]

    risk_usd = equity * risk_pct

    levels = []
    for mult in [1.0]:
        stake = round(risk_usd * mult, 2)
        pct = round(risk_pct * mult * 100, 2)
        levels.append({
            "level": 1,
            "stake_usdt": stake,
            "pct_bankroll": pct,
        })

    return {
        "current_level": 1,
        "min_stake_usdt": round(min_notional, 2),
        "max_stake_usdt": round(equity * risk_pct, 2),
        "current_stake_usdt": round(risk_usd, 2),
        "qty_step": qty_step,
        "min_lot": min_lot,
        "levels": levels,
        "note": "Position sizing: qty = risk_usd / stop_distance, rounded down to qty_step. Single-level system (risk_per_trade_pct of current equity).",
    }


# ─── 6. Win Rate by Stake Level ────────────────────────────────────

def get_winrate_by_stake() -> list[dict]:
    trades_files = sorted(PAPER_TRADES.glob("paper_*.parquet"))
    all_trades = []
    for tf in trades_files:
        try:
            df = pl.read_parquet(tf)
            all_trades.extend(df.to_dicts())
        except Exception:
            continue

    if not all_trades:
        return [{"level": 1, "trades": 0, "win_rate": None, "pnl": None, "avg_pnl": None, "note": "No trades"}]

    by_level: dict[int, list] = {}
    for t in all_trades:
        level = 1
        by_level.setdefault(level, []).append(t)

    results = []
    for level, trades in sorted(by_level.items()):
        n = len(trades)
        wins = sum(1 for t in trades if t.get("win", False))
        pnls = [t.get("net_pnl", 0) for t in trades]
        total_pnl = sum(pnls)
        results.append({
            "level": level,
            "trades": n,
            "win_rate": round(wins / n * 100, 1) if n else None,
            "pnl": round(total_pnl, 4),
            "avg_pnl": round(total_pnl / n, 4) if n else None,
        })
    return results


# ─── 7. Pipeline Status ────────────────────────────────────────────

def get_pipeline_status() -> dict:
    results_files = sorted(RESULTS_DIR.glob("acceptance_*.json"), reverse=True)
    if not results_files:
        return {"has_report": False, "stages": [], "verdict": None}

    try:
        report = json.loads(results_files[0].read_text())
    except Exception:
        return {"has_report": False, "stages": [], "verdict": None}

    stages = report.get("stages", [])

    pipeline_order = [
        "data_validation", "feature_validation", "hypothesis_generation",
        "discovery", "validation_gate", "oos_gate", "parameter_freeze", "critic"
    ]
    stage_map = {s["stage"]: s["status"] for s in stages}

    pipeline = []
    for name in pipeline_order:
        status = stage_map.get(name, "NOT RUN")
        pipeline.append({"stage": name, "status": status})

    has_finalist = report.get("finalist") is not None
    candidates = report.get("candidates", [])
    n_hyp = report.get("n_hypotheses", 0)
    n_events = report.get("n_events_total", 0)

    return {
        "has_report": True,
        "run_id": report.get("run_id"),
        "timestamp": report.get("timestamp"),
        "verdict": report.get("verdict"),
        "n_hypotheses": n_hyp,
        "n_events_total": n_events,
        "candidates": candidates,
        "n_candidates": len(candidates),
        "finalist": report.get("finalist"),
        "reject_reasons": report.get("reject_reasons", []),
        "stages": stages,
        "pipeline": pipeline,
    }


# ─── 8. Hypotheses ─────────────────────────────────────────────────

def get_hypotheses() -> dict:
    hyp_path = HYPOTHESES_DIR / "hypotheses_v1.json"
    hypotheses = []
    if hyp_path.exists():
        try:
            data = json.loads(hyp_path.read_text())
            if isinstance(data, list):
                hypotheses = data
            elif isinstance(data, dict):
                hypotheses = data.get("hypotheses", [])
        except Exception:
            pass

    research_files = sorted(RESULTS_DIR.glob("research_*.json"), reverse=True)
    research_runs = []
    for rf in research_files[:5]:
        try:
            r = json.loads(rf.read_text())
            discovery = r.get("discovery_results", {})
            n_discovered = len(discovery) if isinstance(discovery, dict) else 0
            research_runs.append({
                "run_id": r.get("run_id"),
                "timestamp": r.get("created_at"),
                "n_hypotheses": r.get("n_hypotheses"),
                "n_discovered": n_discovered,
                "n_candidates": len(r.get("candidates", [])),
                "verdict": r.get("verdict"),
            })
        except Exception:
            continue

    return {
        "count": len(hypotheses),
        "hypotheses": hypotheses[:15],
        "research_runs": research_runs,
    }


# ─── 9. Data Download ──────────────────────────────────────────────

def get_data_status() -> dict:
    stats = {}
    for cat in ("linear", "spot"):
        cat_dir = RAW_KLINES_DIR / cat
        if not cat_dir.exists():
            stats[cat] = {"files": 0, "total_size_mb": 0, "newest": None, "oldest": None, "lag_min": None}
            continue
        files = sorted(cat_dir.glob("*_1m.parquet"))
        if not files:
            stats[cat] = {"files": 0, "total_size_mb": 0, "newest": None, "oldest": None, "lag_min": None}
            continue
        sizes = sum(f.stat().st_size for f in files)
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

        lag_min = None
        if newest_ts:
            try:
                newest_dt = datetime.fromisoformat(newest_ts.replace("Z", "+00:00"))
                lag_s = (datetime.now(timezone.utc) - newest_dt).total_seconds()
                lag_min = round(lag_s / 60, 1)
            except Exception:
                pass

        is_stale = lag_min is not None and lag_min > 60 * 24
        status = "STALE" if is_stale else "LIVE" if lag_min is not None and lag_min < 60 else "UNKNOWN"

        stats[cat] = {
            "files": len(files),
            "total_size_mb": round(sizes / 1e6, 1),
            "newest": newest_ts,
            "oldest": oldest_ts,
            "lag_min": lag_min,
            "status": status,
        }

    return {
        "klines": stats,
        "total_files": sum(s["files"] for s in stats.values()),
        "total_size_mb": round(sum(s["total_size_mb"] for s in stats.values()), 1),
    }


# ─── 10. Collector Market Data ─────────────────────────────────────

def get_market_data() -> dict:
    streams = {}
    for stream in ("trades", "orderbook", "futures", "liquidation", "ratio"):
        stream_dir = MARKET_DATA_DIR / stream / "linear"
        if not stream_dir.exists():
            streams[stream] = {"count": 0, "status": "NO DATA"}
            continue
        files = list(stream_dir.glob("*.parquet"))
        newest_mtime = 0
        total_records = 0
        for f in files:
            mt = f.stat().st_mtime
            if mt > newest_mtime:
                newest_mtime = mt
            try:
                df = pl.read_parquet(f, columns=[])
                total_records += df.height
            except Exception:
                pass

        age = time.time() - newest_mtime if newest_mtime else float("inf")
        lag_min = round(age / 60, 1) if age < float("inf") else None

        if age < 300:
            status = "LIVE"
        elif age < 3600:
            status = "STALE"
        else:
            status = "STOPPED"

        streams[stream] = {
            "count": len(files),
            "status": status,
            "lag_min": lag_min,
            "records": total_records,
        }

    return {"streams": streams}


# ─── 11. Market Metrics ────────────────────────────────────────────

def get_market_metrics() -> dict:
    def _load():
        futures_dir = MARKET_DATA_DIR / "futures" / "linear"
        if not futures_dir.exists():
            return {"symbols": [], "summary": {}}
        files = sorted(futures_dir.glob("*.parquet"))
        rows = []
        for f in files[:50]:
            try:
                df = pl.read_parquet(f)
                if df.height == 0:
                    continue
                last = _sanitize(df.tail(1).to_dicts()[0])
                rows.append({
                    "symbol": f.stem,
                    "funding_rate": last.get("funding_rate"),
                    "oi": last.get("oi"),
                    "oi_value": last.get("oi_value"),
                    "last_px": last.get("last_px"),
                    "bid1_px": last.get("bid1_px"),
                    "ask1_px": last.get("ask1_px"),
                })
            except Exception:
                continue
        if rows:
            fr = [r["funding_rate"] for r in rows if r["funding_rate"] is not None]
            oi = [r["oi_value"] for r in rows if r["oi_value"] is not None]
            summary = {
                "symbols_count": len(rows),
                "avg_funding_rate": round(sum(fr) / len(fr), 6) if fr else None,
                "total_oi_usd": round(sum(oi), 0) if oi else None,
            }
        else:
            summary = {}
        return {"symbols": rows[:20], "summary": summary}
    return _cached("market", 60, _load)


# ─── 12. Signals / Events ──────────────────────────────────────────

def get_signals() -> dict:
    def _load():
        events_path = EVENTS_DIR / "all_events.parquet"
        if not events_path.exists():
            return {"count": 0, "symbols": []}
        try:
            df = pl.read_parquet(events_path)
        except Exception:
            return {"count": 0, "symbols": []}
        if df.height == 0:
            return {"count": 0, "symbols": []}
        sym_counts = [_sanitize(d) for d in (df.group_by("symbol").agg(pl.len().alias("count"))
                      .sort("count", descending=True).to_dicts())]
        return {"count": df.height, "symbols": sym_counts[:15]}
    return _cached("signals", 30, _load)


# ─── 13. Orderbook Captures ─────────────────────────────────────────

def get_captures() -> dict:
    """Orderbook capture status: pending triggers + completed captures."""
    from config.settings import MARKET_DATA_DIR

    triggers_dir = MARKET_DATA_DIR.parent / "triggers"
    captures_dir = MARKET_DATA_DIR / "orderbook" / "captures"

    # Pending triggers
    pending = []
    if triggers_dir.exists():
        for f in sorted(triggers_dir.glob("*.json")):
            try:
                data = json.loads(f.read_text())
                pending.append({
                    "event_id": data.get("event_id", f.stem),
                    "symbol": data.get("symbol", "?"),
                    "created_at": data.get("created_at"),
                    "duration_sec": data.get("capture_duration_sec", 0),
                })
            except Exception:
                pending.append({"event_id": f.stem, "symbol": "?"})

    # Completed captures (directories with meta.json)
    captures = []
    if captures_dir.exists():
        entries = sorted(captures_dir.iterdir(), key=lambda d: d.stat().st_mtime, reverse=True)

        parquet_files = [e for e in entries if e.is_file() and e.suffix == ".parquet"]
        meta_files = {e.stem: e for e in entries if e.is_file() and e.name.endswith(".meta.json")}

        for pf in parquet_files:
            event_id = pf.stem
            meta = {}
            mf = meta_files.get(f"{event_id}.meta")
            if not mf:
                mf = captures_dir / f"{event_id}.meta.json"
            if mf.exists():
                try:
                    meta = json.loads(mf.read_text())
                except Exception:
                    pass
            size_mb = round(pf.stat().st_size / 1e6, 2)
            captures.append({
                "event_id": meta.get("event_id", event_id),
                "symbol": meta.get("symbol", event_id.split("_")[1] if "_" in event_id else "?"),
                "started_at": meta.get("started_at"),
                "duration_sec": meta.get("capture_duration_sec", 0),
                "records": meta.get("records", 0),
                "size_mb": size_mb,
                "age_min": round((time.time() - pf.stat().st_mtime) / 60, 1),
            })

    cfg = _load_toml("orderbook_capture.toml")
    return {
        "pending_count": len(pending),
        "pending": pending,
        "captures_count": len(captures),
        "captures": captures[:20],
        "max_concurrent": cfg.get("capture", {}).get("max_concurrent", 10),
        "cooldown_sec": cfg.get("cooldown_sec", 300),
        "duration_sec": cfg.get("capture", {}).get("duration_sec", 1200),
    }


# ─── 14. System Status ─────────────────────────────────────────────

def get_system_status() -> dict:
    log_files = {
        "orchestrator": LOGS_DIR / "orchestrator.log",
        "pipeline": LOGS_DIR / "pipeline.log",
    }
    collector_log = ROOT / "collector" / "logs" / "marketdata.log"

    logs = {}
    for name, path in log_files.items():
        mt = _mtime(path)
        age = _age_s(path)
        logs[name] = {
            "exists": path.exists(),
            "mtime": datetime.fromtimestamp(mt, tz=timezone.utc).isoformat() if mt else None,
            "age_s": round(age, 1),
            "stale": age > 3600,
        }
    mt = _mtime(collector_log)
    age = _age_s(collector_log)
    logs["collector"] = {
        "exists": collector_log.exists(),
        "mtime": datetime.fromtimestamp(mt, tz=timezone.utc).isoformat() if mt else None,
        "age_s": round(age, 1),
        "stale": age > 300,
    }

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "logs": logs,
        "any_stale": any(l["stale"] for l in logs.values()),
    }


# ─── 14. Logs ──────────────────────────────────────────────────────

def get_logs(n: int = 30) -> dict:
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
    collector_log = ROOT / "collector" / "logs" / "marketdata.log"
    if collector_log.exists():
        try:
            lines = collector_log.read_text().splitlines()
            out["collector"] = lines[-10:]
        except Exception:
            out["collector"] = []
    else:
        out["collector"] = []
    return out
