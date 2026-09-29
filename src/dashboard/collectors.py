"""Dashboard data collectors. Read-only access to existing pipeline artifacts.

All data comes from existing files on disk. No pipeline logic changes.
If a data source doesn't exist — returns None/empty, never fabricates values.
"""
from __future__ import annotations

import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    RAW_KLINES_DIR, MARKET_DATA_DIR, CLEAN_CANDLES, FEATURES_DIR,
    EVENTS_DIR, HYPOTHESES_DIR, RESULTS_DIR, REPORTS_DIR, RESEARCH_DIR,
    PAPER_DIR, PAPER_PORTFOLIO, PAPER_SHADOW, PAPER_TRADES, LOGS_DIR, ROOT,
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
        status_reason = "Сделок ещё не было"
    else:
        status = "STOPPED"
        status_reason = "Не запущено (ручной режим)"

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
        "mode": state.get("mode") if state else None,
        "hypothesis_id": state.get("hypothesis_id") if state else None,
        "started_at": state.get("started_at") if state else None,
        "provenance": state.get("provenance", {}) if state else {},
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
        "reason": "Коллектор получает instruments-info от Bybit, но НЕ сохраняет спецификации по символам (minOrderQty, qtyStep, tickSize). Сохраняется только список символов.",
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
        "note": "Размер позиции: qty = risk_usd / stop_distance, округление вниз до qty_step. Одноуровневая система (risk_per_trade_pct от текущего капитала).",
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

    last_acceptance = None
    acceptance_files = sorted(RESULTS_DIR.glob("acceptance_*.json"), reverse=True)
    if acceptance_files:
        try:
            last_acceptance = json.loads(acceptance_files[0].read_text())
        except Exception:
            pass

    last_verdict = last_acceptance.get("verdict") if last_acceptance else None
    last_candidates = last_acceptance.get("candidates", []) if last_acceptance else []

    accepted_ids = {c.get("hypothesis_id") for c in last_candidates}
    for h in hypotheses:
        if last_verdict in ("REJECT", "NO_CANDIDATE") and h.get("status") == "CANDIDATE":
            h["display_status"] = "REJECTED_BY_PIPELINE"
        elif h.get("hypothesis_id") in accepted_ids:
            h["display_status"] = "CANDIDATE"
        else:
            h["display_status"] = h.get("status", "UNKNOWN")

    research_files = sorted(RESULTS_DIR.glob("research_*.json"), reverse=True)
    research_runs = []
    for rf in research_files[:5]:
        try:
            r = json.loads(rf.read_text())
            discovery = r.get("discovery_results", {})
            n_discovered = len(discovery) if isinstance(discovery, dict) else 0
            research_runs.append({
                "run_id": r.get("run_id") or rf.stem.replace("research_", ""),
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
    """Heavy scan (50 parquet per category) — cache 60s like market."""
    def _load():
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
            # Sort by mtime descending, sample up to 50 files for newest
            mtime_sorted = sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)
            for f in mtime_sorted[:5]:
                try:
                    df = pl.read_parquet(f, columns=["open_time"])
                    mx = df["open_time"].max()
                    if mx is not None:
                        if newest_ts is None or str(mx) > newest_ts:
                            newest_ts = str(mx)
                except Exception:
                    continue

            lag_min = None
            if newest_ts:
                try:
                    newest_dt = datetime.fromisoformat(newest_ts)
                    if newest_dt.tzinfo is None:
                        newest_dt = newest_dt.replace(tzinfo=timezone.utc)
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
    return _cached("data_status", 60, _load)


# ─── 10. Collector Market Data ─────────────────────────────────────

def get_market_data() -> dict:
    """Records count is expensive (2800+ parquet files) — cache 60s like market."""
    def _load():
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
                    n = pl.scan_parquet(f).select(pl.len()).collect().item()
                    total_records += n
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
    return _cached("market_data", 60, _load)


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
            # Lazy scan: читается только колонка symbol, а не весь файл (~6.8 GB RAM)
            sym_counts = (pl.scan_parquet(events_path)
                          .group_by("symbol")
                          .agg(pl.len().alias("count"))
                          .sort("count", descending=True)
                          .collect())
        except Exception:
            return {"count": 0, "symbols": []}
        if sym_counts.height == 0:
            return {"count": 0, "symbols": []}
        total = int(sym_counts["count"].sum())
        symbols = [_sanitize(d) for d in sym_counts.to_dicts()]
        return {"count": total, "symbols": symbols[:15]}
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
        "research": LOGS_DIR / "research_cycle.log",
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


# ─── 15. Order Book Collector ──────────────────────────────────────

def get_ob_collector() -> dict:
    """Order Book collector metrics from _metrics.jsonl (live MARKET_DATA_DIR tree)."""
    metrics_path = MARKET_DATA_DIR / "orderbook" / "reconstructed" / "_metrics.jsonl"
    
    if not metrics_path.exists():
        return {
            "status": "NOT RUNNING",
            "reason": "No metrics file found",
            "symbols": [],
            "total_updates": 0,
            "total_rows": 0,
            "total_reconnects": 0,
            "total_errors": 0,
            "disk_free_gb": None,
        }
    
    by_symbol = {}
    try:
        size = metrics_path.stat().st_size
        with open(metrics_path) as f:
            f.seek(max(0, size - 2 * 1024 * 1024))
            f.readline()
            for line in deque(f, maxlen=20000):
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                    by_symbol[e.get("symbol", "?")] = e
                except json.JSONDecodeError:
                    continue
    except Exception:
        return {"status": "ERROR", "reason": "Cannot read metrics file"}
    
    if not by_symbol:
        return {"status": "NO DATA", "reason": "Metrics file empty"}
    
    symbols = []
    total_updates = 0
    total_rows = 0
    total_reconnects = 0
    total_errors = 0
    disk_free = None
    latest_ts = None
    
    for sym, last in by_symbol.items():
        updates = last.get("updates_received", 0)
        rows = last.get("rows_written", 0)
        reconnects = last.get("reconnects", 0)
        errors = last.get("errors", 0)
        is_valid = last.get("is_valid", False)
        jumps = last.get("update_id_jumps", 0)
        invalid_secs = last.get("invalid_state_duration_secs", 0)
        
        total_updates += updates
        total_rows += rows
        total_reconnects += reconnects
        total_errors += errors
        
        if disk_free is None and last.get("disk_free_gb") is not None:
            disk_free = last["disk_free_gb"]
        
        ts = last.get("timestamp")
        if ts and (latest_ts is None or ts > latest_ts):
            latest_ts = ts
        
        status = "OK" if is_valid else "NO DATA"
        if invalid_secs > 60:
            status = "GAP"
        
        symbols.append({
            "symbol": sym,
            "status": status,
            "updates": updates,
            "rows": rows,
            "reconnects": reconnects,
            "errors": errors,
            "update_id_jumps": jumps,
            "invalid_state_secs": invalid_secs,
            "is_valid": is_valid,
            "last_timestamp": ts,
        })
    
    symbols.sort(key=lambda x: x["updates"], reverse=True)
    
    overall_status = "RUNNING" if any(s["is_valid"] for s in symbols) else "NO DATA"
    if total_errors > 0:
        overall_status = "ERRORS"
    
    return {
        "status": overall_status,
        "symbols_count": len(symbols),
        "total_updates": total_updates,
        "total_rows": total_rows,
        "total_reconnects": total_reconnects,
        "total_errors": total_errors,
        "disk_free_gb": disk_free,
        "latest_timestamp": latest_ts,
        "symbols": symbols[:30],
    }


# ─── 16. Candle Collector ──────────────────────────────────────────

def get_candle_collector() -> dict:
    """Candle collector status from kline files. Heavy scan (50 file per cat) — cache 60s."""
    def _load():
        stats = {}
        total_files = 0
        total_size = 0
        total_records = 0
        
        for cat in ("linear", "spot"):
            cat_dir = RAW_KLINES_DIR / cat
            if not cat_dir.exists():
                stats[cat] = {"files": 0, "total_size_mb": 0, "newest": None, "oldest": None, "lag_min": None, "status": "NO DATA"}
                continue
            
            files = sorted(cat_dir.glob("*_1m.parquet"))
            if not files:
                stats[cat] = {"files": 0, "total_size_mb": 0, "newest": None, "oldest": None, "lag_min": None, "status": "NO DATA"}
                continue
            
            sizes = sum(f.stat().st_size for f in files)
            total_files += len(files)
            total_size += sizes
            
            oldest_ts = newest_ts = None
            cat_records = 0
            try:
                first = pl.read_parquet(files[0], columns=["open_time"])
                oldest_ts = str(first["open_time"].min())
            except Exception:
                pass
            # Sort by mtime descending, sample up to 50 files for newest
            mtime_sorted = sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)
            for f in mtime_sorted[:5]:
                try:
                    df = pl.read_parquet(f, columns=["open_time"])
                    mx = df["open_time"].max()
                    if mx is not None:
                        if newest_ts is None or str(mx) > newest_ts:
                            newest_ts = str(mx)
                except Exception:
                    continue
            
            lag_min = None
            status = "UNKNOWN"
            if newest_ts:
                try:
                    newest_dt = datetime.fromisoformat(newest_ts)
                    if newest_dt.tzinfo is None:
                        newest_dt = newest_dt.replace(tzinfo=timezone.utc)
                    lag_s = (datetime.now(timezone.utc) - newest_dt).total_seconds()
                    lag_min = round(lag_s / 60, 1)
                    if lag_min < 60:
                        status = "LIVE"
                    elif lag_min < 60 * 24:
                        status = "STALE"
                    else:
                        status = "OLD"
                except Exception:
                    pass
            
            stats[cat] = {
                "files": len(files),
                "total_size_mb": round(sizes / 1e6, 1),
                "newest": newest_ts,
                "oldest": oldest_ts,
                "lag_min": lag_min,
                "status": status,
            }
        
        return {
            "status": "RUNNING" if total_files > 0 else "NO DATA",
            "categories": stats,
            "total_files": total_files,
            "total_size_mb": round(total_size / 1e6, 1),
        }
    return _cached("candle_collector", 60, _load)


# ─── 17. Data Quality ──────────────────────────────────────────────

def get_data_quality() -> dict:
    """Data quality metrics (OB metrics + kline scan, sampled 50/cat, cached 60s)."""
    def _load():
        quality = {
            "orderbook": {"ok": 0, "stale": 0, "gap": 0, "missing": 0},
            "klines": {"ok": 0, "stale": 0, "missing": 0},
        }

        # Check OB quality from metrics
        metrics_path = MARKET_DATA_DIR / "orderbook" / "reconstructed" / "_metrics.jsonl"
        ob_ok = 0
        ob_gap = 0
        ob_stale = 0
        ob_missing = 0
        if metrics_path.exists():
            try:
                # Только хвост файла: последняя запись на символ (полный файл — 3.4M строк)
                by_symbol: dict[str, dict] = {}
                size = metrics_path.stat().st_size
                tail_bytes = 2 * 1024 * 1024
                with open(metrics_path) as f:
                    f.seek(max(0, size - tail_bytes))
                    f.readline()  # отбросить неполную первую строку
                    for line in deque(f, maxlen=20000):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            e = json.loads(line)
                            by_symbol[e.get("symbol", "?")] = e
                        except json.JSONDecodeError:
                            continue
            except Exception:
                pass
            for e in by_symbol.values():
                if e.get("is_valid"):
                    ob_ok += 1
                elif e.get("updates_received", 0) > 0:
                    ob_gap += 1
                else:
                    ob_missing += 1
                if e.get("invalid_state_duration_secs", 0) > 60:
                    ob_stale += 1
            quality["orderbook"] = {"ok": ob_ok, "stale": ob_stale, "gap": ob_gap, "missing": ob_missing}

        for cat in ("linear", "spot"):
            cat_dir = RAW_KLINES_DIR / cat
            if not cat_dir.exists():
                continue
            files = sorted(cat_dir.glob("*_1m.parquet"),
                           key=lambda f: f.stat().st_mtime, reverse=True)[:50]
            for f in files:
                try:
                    df = pl.scan_parquet(f).select(pl.col("open_time")).collect()
                    if df.height == 0:
                        quality["klines"]["missing"] += 1
                        continue
                    last_ts = df["open_time"].max()
                    lag = (datetime.now(timezone.utc) - last_ts.replace(tzinfo=timezone.utc)).total_seconds() / 60
                    if lag < 60:
                        quality["klines"]["ok"] += 1
                    elif lag < 60 * 24:
                        quality["klines"]["stale"] += 1
                    else:
                        quality["klines"]["missing"] += 1
                except Exception:
                    quality["klines"]["missing"] += 1

        return quality
    return _cached("data_quality", 60, _load)


# ─── 18. Research Conclusion ───────────────────────────────────────

def get_research_conclusion() -> dict:
    """Current research conclusion from pipeline results."""
    results_files = sorted(RESULTS_DIR.glob("acceptance_*.json"), reverse=True)
    
    if not results_files:
        return {
            "status": "NO DATA",
            "reason": "No acceptance reports found",
            "verdict": None,
        }
    
    try:
        report = json.loads(results_files[0].read_text())
    except Exception:
        return {"status": "ERROR", "reason": "Cannot read acceptance report"}
    
    verdict = report.get("verdict", "UNKNOWN")
    reject_reasons = report.get("reject_reasons", [])
    candidates = report.get("candidates", [])
    finalist = report.get("finalist")
    n_hyp = report.get("n_hypotheses", 0)
    n_events = report.get("n_events_total", 0)
    
    if verdict == "PASS" and finalist:
        conclusion = "VALIDATED EDGE FOUND"
        reason = f"Финалист: {finalist.get('hypothesis_id', 'Н/Д')}"
    elif verdict == "NO_CANDIDATE":
        conclusion = "NO STATISTICALLY VALIDATED EDGE"
        reason = f"Проверено гипотез: {n_hyp}, кандидатов: {len(candidates)}, не прошли все проверки: 0"
    elif verdict == "REJECT":
        conclusion = "CANDIDATES REJECTED"
        reason = reject_reasons[0] if reject_reasons else "Причина отклонения неизвестна"
    elif verdict == "STOP":
        conclusion = "PIPELINE STOPPED"
        reason = reject_reasons[0] if reject_reasons else "Провалена валидация данных"
    else:
        conclusion = "UNKNOWN"
        reason = f"Вердикт: {verdict}"
    
    return {
        "status": conclusion,
        "reason": reason,
        "verdict": verdict,
        "n_hypotheses": n_hyp,
        "n_events": n_events,
        "n_candidates": len(candidates),
        "candidates": candidates[:5],
        "finalist": finalist,
        "reject_reasons": reject_reasons[:3],
        "timestamp": report.get("timestamp"),
    }


# ─── 18b. Research Cycle / Paper handoff ──────────────────────────

def get_research_cycle() -> dict:
    """Frozen-cycle progress + Research→Paper handoff + current paper binding."""
    ctrl = {}
    p = RESEARCH_DIR / "controller_state.json"
    if p.exists():
        try:
            ctrl = json.loads(p.read_text())
        except Exception:
            ctrl = {}

    frozen = {}
    fp = RESEARCH_DIR / "frozen_boundary.json"
    if fp.exists():
        try:
            frozen = json.loads(fp.read_text())
        except Exception:
            frozen = {}

    experiment = None
    eid = ctrl.get("last_experiment_id")
    if eid:
        ap = RESEARCH_DIR / "experiments" / f"{eid}.json"
        if ap.exists():
            try:
                art = json.loads(ap.read_text())
                experiment = {
                    "experiment_id": art.get("experiment_id"),
                    "mode": art.get("mode"),
                    "config_hash": art.get("config_hash"),
                    "status": art.get("status"),
                    "final_verdict": art.get("final_verdict"),
                    "diagnosis": (art.get("reject_diagnosis") or {}).get("category"),
                    "n_hypotheses": len(art.get("hypothesis_specs") or []),
                }
            except Exception:
                experiment = None

    ctl_cfg = _load_toml("research_controller.toml")
    paper = get_paper()
    return {
        "state": ctrl.get("state"),
        "cycle_id": ctrl.get("cycle_id"),
        "frozen": {
            "config_hash": frozen.get("config_hash"),
            "klines_max": frozen.get("klines_max"),
            "frozen_at_utc": frozen.get("frozen_at_utc"),
        } if frozen else None,
        "budget_used": ctrl.get("budget_used", 0),
        "budget_max": int(ctl_cfg.get("budget", {}).get(
            "max_experiments_per_boundary", 12)),
        "last_mode": ctrl.get("last_mode"),
        "next_mode": (ctrl.get("next_selection") or {}).get("mode"),
        "pass_pending": ctrl.get("pass_pending"),
        "paper_handoff": ctrl.get("paper_handoff"),
        "experiment": experiment,
        "paper": {
            "mode": paper.get("mode"),
            "hypothesis_id": paper.get("hypothesis_id"),
            "status": paper.get("status"),
            "balance": paper.get("balance"),
            "realized_pnl": paper.get("realized_pnl"),
            "pnl_pct": paper.get("pnl_pct"),
            "trade_count": paper.get("trade_count"),
            "started_at": paper.get("started_at"),
            "provenance": paper.get("provenance"),
        },
    }


# ─── 18c. Research Controller — real state from controller files ──

def _read_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _latest_frozen_boundary() -> dict | None:
    """Latest frozen boundary the runner saved (research_cycle.log), if any."""
    log_path = LOGS_DIR / "research_cycle.log"
    if not log_path.exists():
        return None
    try:
        lines = log_path.read_text(errors="replace").splitlines()[-200:]
    except Exception:
        return None
    import re
    for line in reversed(lines):
        m = re.search(r"Frozen boundary saved: klines_max=(\S+) cycle_id=(\S+)", line)
        if m:
            return {"klines_max": m.group(1), "cycle_id": m.group(2)}
        m = re.search(r"New cycle boundary from data: klines_max=(\S+) config_hash=(\S+)", line)
        if m:
            return {"klines_max": m.group(1), "cycle_id": None}
    return None


def get_research_controller() -> dict:
    """Research Controller real state: controller_state.json + journal artifact + runner log."""
    ctrl = _read_json_file(RESEARCH_DIR / "controller_state.json")
    ctl_cfg = _load_toml("research_controller.toml")
    budget_max = int(ctl_cfg.get("budget", {}).get("max_experiments_per_boundary", 12))
    budget_used = int(ctrl.get("budget_used", 0) or 0)

    last_exp = None
    eid = ctrl.get("last_experiment_id")
    if eid:
        art = _read_json_file(RESEARCH_DIR / "experiments" / f"{eid}.json")
        diag = art.get("reject_diagnosis") or {}
        last_exp = {
            "experiment_id": eid,
            "mode": art.get("mode") or ctrl.get("last_mode"),
            "status": art.get("status"),
            "final_verdict": art.get("final_verdict"),
            "diagnosis": diag.get("category") or ctrl.get("last_diagnosis"),
            "reject_reasons": diag.get("reject_reasons", []),
            "finished_at_utc": art.get("finished_at_utc"),
        }

    ns = ctrl.get("next_selection") or {}
    params = ns.get("parameters") or {}
    next_exp = None
    if ns:
        next_exp = {
            "mode": ns.get("mode"),
            "feature": params.get("feature"),
            "operator": params.get("operator"),
            "entry_side": params.get("entry_side"),
            "threshold": params.get("threshold"),
            "regime_key": params.get("regime_key"),
            "horizons": params.get("horizons"),
            "config_hash": ns.get("config_hash"),
        }

    frozen = _latest_frozen_boundary()
    state = ctrl.get("state")
    src = "controller_state.json"

    if not ctrl:
        status, reason = "ERROR", "Н/Д (controller_state.json)"
    elif budget_used >= budget_max:
        status, reason = "BUDGET_EXHAUSTED", f"бюджет {budget_used}/{budget_max} израсходован"
    elif state == "RUNNING":
        status, reason = "RUNNING", f"идёт эксперимент: {eid}"
    elif state == "SELECT_NEXT":
        status, reason = "WAITING_FOR_NEW_DATA", f"выбор следующего, остаток бюджета {budget_max - budget_used}"
    elif state == "PASS_PENDING_PAPER":
        status, reason = "PASS_PENDING_PAPER", "ожидает валидации бумаги"
    elif state == "STOPPED":
        status, reason = "STOPPED", ctrl.get("stop_reason") or "модуль остановлен"
    else:
        status, reason = state or "UNKNOWN", f"источник: {src}"

    return {
        "status": status,
        "status_reason": reason,
        "state": state,
        "cycle_id": ctrl.get("cycle_id"),
        "budget_used": budget_used,
        "budget_max": budget_max,
        "boundary_klines_max": (ctrl.get("boundary") or {}).get("klines_max"),
        "boundary_config_hash": (ctrl.get("boundary") or {}).get("gate_config_hash"),
        "frozen_next": frozen,
        "last_experiment": last_exp,
        "last_diagnosis": ctrl.get("last_diagnosis"),
        "last_mode": ctrl.get("last_mode"),
        "next_experiment": next_exp,
        "pass_pending": ctrl.get("pass_pending"),
        "stop_reason": ctrl.get("stop_reason"),
        "updated_at_utc": ctrl.get("updated_at_utc"),
        "source": src,
    }


def get_research_progress() -> dict:
    """Progress of the current controller cycle from experiments.jsonl + artifacts."""
    ctrl = _read_json_file(RESEARCH_DIR / "controller_state.json")
    ctl_cfg = _load_toml("research_controller.toml")
    budget_max = int(ctl_cfg.get("budget", {}).get("max_experiments_per_boundary", 12))
    budget_used = int(ctrl.get("budget_used", 0) or 0)
    cycle_id = ctrl.get("cycle_id")

    journal_path = RESEARCH_DIR / "experiments.jsonl"
    exps: dict[str, dict] = {}
    journal_ok = False
    if journal_path.exists():
        try:
            for line in journal_path.read_text(errors="replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                eid = rec.get("experiment_id")
                if not eid:
                    continue
                entry = exps.setdefault(eid, {})
                entry.update(rec)  # terminal record wins (last line per experiment)
                for k in ("mode", "parameters", "created_at_utc"):
                    if k in rec and k not in entry:
                        entry[k] = rec[k]
            journal_ok = True
        except Exception:
            pass

    in_cycle = [e for e in exps.values() if e.get("cycle_id") == cycle_id]

    n_running = sum(1 for e in in_cycle if e.get("status") == "RUNNING")
    n_pass = sum(1 for e in in_cycle if e.get("final_verdict") in ("PASS", "CANDIDATE"))
    n_reject = sum(1 for e in in_cycle if e.get("final_verdict") == "REJECT")
    n_failed = sum(1 for e in in_cycle if e.get("status") == "FAILED")

    last_exp = None
    if in_cycle:
        last = max(in_cycle, key=lambda e: e.get("finished_at_utc") or e.get("created_at_utc") or "")
        last_reject_reason = None
        if last.get("final_verdict") == "REJECT":
            art = _read_json_file(RESEARCH_DIR / "experiments" / f"{last['experiment_id']}.json")
            diag = art.get("reject_diagnosis") or {}
            reasons = diag.get("reject_reasons", [])
            last_reject_reason = reasons[0] if reasons else None
        last_exp = {
            "experiment_id": last.get("experiment_id"),
            "mode": last.get("mode"),
            "status": last.get("status"),
            "final_verdict": last.get("final_verdict"),
            "finished_at_utc": last.get("finished_at_utc"),
            "reject_reason": last_reject_reason,
        }

    state = ctrl.get("state")
    if not ctrl:
        status, reason = "ERROR", "Н/Д (controller_state.json)"
    elif budget_used >= budget_max:
        status, reason = "BUDGET_EXHAUSTED", f"бюджет {budget_used}/{budget_max} израсходован"
    elif state == "RUNNING":
        status, reason = "RUNNING", f"идёт эксперимент: {(last_exp or {}).get('experiment_id')}"
    elif state == "SELECT_NEXT":
        status, reason = "WAITING_FOR_NEW_DATA", f"выбор следующего, остаток бюджета {budget_max - budget_used}"
    elif state == "PASS_PENDING_PAPER":
        status, reason = "PASS_PENDING_PAPER", "ожидает валидации бумаги"
    else:
        status, reason = state or "UNKNOWN", "источник: controller_state.json"

    return {
        "status": status,
        "status_reason": reason,
        "cycle_id": cycle_id,
        "budget_used": budget_used,
        "budget_max": budget_max,
        "journal_ok": journal_ok,
        "experiments_in_cycle": len(in_cycle),
        "counts": {"RUNNING": n_running, "PASS": n_pass, "REJECT": n_reject, "FAILED": n_failed},
        "last_experiment": last_exp,
    }


# ─── 19. Research Gate ────────────────────────────────────────────

def get_research_gate() -> dict:
    """Research gate status from last_run.json + research_timer.log."""
    import re

    last_run = None
    last_run_path = RESEARCH_DIR / "last_run.json"
    if last_run_path.exists():
        try:
            last_run = json.loads(last_run_path.read_text())
        except Exception:
            pass

    timer_log = LOGS_DIR / "research_timer.log"
    last_skip = None
    cooldown_until = None
    if timer_log.exists():
        try:
            lines = timer_log.read_text().splitlines()
            for line in reversed(lines[-100:]):
                if "SKIP" in line:
                    last_skip = line.strip()
                    m = re.search(r"cooldown until (\d{4}-\d{2}-\d{2} \d{2}:\d{2})", line)
                    if m:
                        cooldown_until = m.group(1) + " UTC"
                    break
        except Exception:
            pass

    now = datetime.now(timezone.utc)

    if last_skip and cooldown_until:
        try:
            cd_dt = datetime.strptime(cooldown_until.replace(" UTC", ""), "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            if now < cd_dt:
                status = "SKIP"
                reason = last_skip.split("SKIP")[-1].strip().strip("()")
                next_run = cooldown_until
            else:
                status = "READY"
                reason = "Пауза истекла"
                next_run = None
        except Exception:
            status = "UNKNOWN"
            reason = "Не удалось разобрать паузу"
            next_run = None
    elif last_run and last_run.get("verdict"):
        status = "READY"
        reason = f"Last run verdict: {last_run['verdict']}"
        next_run = None
    else:
        status = "READY"
        reason = "Предыдущих запусков нет"
        next_run = None

    return {
        "status": status,
        "reason": reason,
        "next_run_utc": next_run,
        "last_run_id": last_run.get("run_id") if last_run else None,
        "last_run_verdict": last_run.get("verdict") if last_run else None,
        "last_run_finished": last_run.get("finished_at_utc") if last_run else None,
    }


# ─── 20. Alerts ────────────────────────────────────────────────────

def get_alerts() -> list[dict]:
    """Active alerts based on system state."""
    alerts = []
    
    # Check OB collector
    ob = get_ob_collector()
    if ob["status"] == "NOT RUNNING":
        alerts.append({"level": "error", "source": "Коллектор стакана", "message": "Коллектор не запущен"})
    elif ob["status"] == "NO DATA":
        alerts.append({"level": "warning", "source": "Коллектор стакана", "message": "Данные не поступают"})
    elif ob["status"] == "ERRORS":
        alerts.append({"level": "error", "source": "OB Collector", "message": f"{ob['total_errors']} write errors"})
    
    if ob.get("disk_free_gb") is not None and ob["disk_free_gb"] < 5:
        alerts.append({"level": "critical", "source": "Disk", "message": f"Low disk space: {ob['disk_free_gb']:.1f} GB"})
    
    # Check candle collector
    cc = get_candle_collector()
    if cc["status"] == "NO DATA":
        alerts.append({"level": "warning", "source": "Коллектор свечей", "message": "Нет данных свечей"})
    
    # Check data quality
    dq = get_data_quality()
    if dq["orderbook"]["gap"] > 0:
        alerts.append({"level": "warning", "source": "Data Quality", "message": f"{dq['orderbook']['gap']} OB gaps detected"})
    
    # Check pipeline
    pipeline = get_pipeline_status()
    if pipeline.get("verdict") == "STOP":
        alerts.append({"level": "error", "source": "Пайплайн", "message": "Пайплайн остановлен"})
    elif pipeline.get("verdict") == "ERROR":
        alerts.append({"level": "error", "source": "Пайплайн", "message": "Ошибка пайплайна"})
    
    # Check system status
    status = get_system_status()
    if status.get("any_stale"):
        alerts.append({"level": "warning", "source": "Система", "message": "Некоторые лог-файлы устарели"})
    
    return alerts


# ─── 20. Shadow Paper Status ──────────────────────────────────────

def get_shadow_paper() -> dict:
    """Shadow paper (MODE B) status and summary."""
    shadow_file = PAPER_SHADOW / "state.json"
    portfolio_file = PAPER_PORTFOLIO / "state.json"

    shadow_state = None
    if shadow_file.exists():
        try:
            shadow_state = json.loads(shadow_file.read_text())
        except Exception:
            shadow_state = None

    portfolio_state = None
    if portfolio_file.exists():
        try:
            portfolio_state = json.loads(portfolio_file.read_text())
        except Exception:
            portfolio_state = None

    if shadow_state is None and portfolio_state is None:
        return {"status": "NO DATA", "shadow_trades": 0, "shadow_pnl": 0.0}

    shadow_trades = (shadow_state or {}).get("trades", [])
    validated_trades = [t for t in (portfolio_state or {}).get("trades", [])
                        if t.get("mode") == "VALIDATED"]

    shadow_pnl = sum(t.get("net_pnl", 0) for t in shadow_trades)
    shadow_wins = sum(1 for t in shadow_trades if t.get("net_pnl", 0) > 0)

    by_hyp = {}
    for t in shadow_trades:
        hid = t.get("hypothesis_id", "?")
        if hid not in by_hyp:
            by_hyp[hid] = {"trades": 0, "pnl": 0.0, "wins": 0}
        by_hyp[hid]["trades"] += 1
        by_hyp[hid]["pnl"] += t.get("net_pnl", 0)
        if t.get("net_pnl", 0) > 0:
            by_hyp[hid]["wins"] += 1

    return {
        "status": (shadow_state or {}).get("mode", "UNKNOWN"),
        "hypothesis_id": (shadow_state or {}).get("hypothesis_id"),
        "balance": (shadow_state or {}).get("balance"),
        "shadow_trades": len(shadow_trades),
        "shadow_pnl": round(shadow_pnl, 4),
        "shadow_win_rate": round(shadow_wins / len(shadow_trades), 4) if shadow_trades else 0.0,
        "validated_trades": len(validated_trades),
        "by_hypothesis": by_hyp,
        "total_trades": len(shadow_trades) + len(validated_trades),
        "realized_pnl": (shadow_state or {}).get("realized_pnl", 0.0),
    }


def get_logs(n: int = 30) -> dict:
    """Последние N строк логов: research-цикл, orchestrator, collector."""
    def _tail(path: Path) -> list[str]:
        if not path.exists():
            return []
        try:
            return path.read_text(errors="replace").splitlines()[-n:]
        except Exception:
            return []

    return {
        "research": _tail(LOGS_DIR / "research_cycle.log"),
        "collector": _tail(ROOT / "collector" / "logs" / "marketdata.log"),
    }
