"""Бумажная торговля (ТЗ §35-41). Исполнение по прошедшим Critic гипотезам.

Для каждого события-кандидата: вход open(T+1) (entry_price), стоп = k*vol,
тейк = m*стоп, размер позиции = риск/дистанция до стопа с ограничениями (§36).
Использует готовые mfe/mae/return из событий. Портфель персистится (JSON),
сделки в parquet (§41).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import numpy as np
import polars as pl

from config.settings import PAPER_PORTFOLIO, PAPER_TRADES, load_toml

logger = logging.getLogger(__name__)
_RISK = load_toml("risk.toml")

STATE_FILE = PAPER_PORTFOLIO / "state.json"


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"balance": _RISK["equity_start"], "equity": _RISK["equity_start"],
            "available": _RISK["equity_start"], "used_margin": 0.0,
            "realized_pnl": 0.0, "unrealized_pnl": 0.0, "fees": 0.0, "slippage": 0.0,
            "trade_count": 0, "win_count": 0, "loss_count": 0, "win_rate": 0.0,
            "max_drawdown": 0.0, "peak_equity": _RISK["equity_start"],
            "open_positions": [], "history": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def _position_size(entry: float, stop: float, equity: float) -> float:
    """§36: qty = риск$ / дистанция до стопа, округление вниз по шагу, лимиты."""
    risk_usd = equity * _RISK["risk_per_trade_pct"]
    dist = abs(entry - stop)
    if dist <= 0:
        return 0.0
    qty = np.floor(risk_usd / dist / _RISK["instrument_qty_step"]) * _RISK["instrument_qty_step"]
    if qty < _RISK["instrument_min_lot"] or qty * entry < _RISK["instrument_min_notional"]:
        return 0.0
    return float(qty)


def _outcome(entry: float, side: float, stop: float, take: float,
             h_high: float, h_low: float, close_px: float) -> tuple[str, float]:
    """Консервативно: стоп раньше тейка; иначе закрытие по close интервала."""
    if side > 0:
        if h_low <= stop:
            return "stop", stop
        if h_high >= take:
            return "take", take
    else:
        if h_high >= stop:
            return "stop", stop
        if h_low <= take:
            return "take", take
    return "close", close_px


def paper_run(events: pl.DataFrame, finalist: dict) -> dict:
    """Бумажный прогон по всем событиям гипотезы (обычно OOS-период)."""
    state = load_state()
    h_min = int(finalist["horizon_min"])
    side = 1.0 if finalist["entry_side"] == "long" else -1.0
    slip = _RISK["slippage_bps"] / 10_000
    fee_rt = _RISK["fee_round_trip"]

    trades: list[dict] = []
    for row in events.iter_rows(named=True):
        entry = row.get("entry_price")
        mfe, mae = row.get(f"mfe_{h_min}m"), row.get(f"mae_{h_min}m")
        ret = row.get(f"return_{h_min}m")
        vol = row.get("vol_gk_30d")
        if entry is None or mfe is None or mae is None or ret is None or vol is None:
            continue
        stop_dist = _RISK["stop_k_vol"] * vol * entry
        if stop_dist <= 0:
            continue
        stop = entry - side * stop_dist
        take = entry + side * _RISK["take_k_stop"] * stop_dist
        qty = _position_size(entry, stop, state["balance"])
        if qty == 0:
            continue

        h_high = entry * (1 + mfe)
        h_low = entry * (1 + mae)
        reason, exit_px = _outcome(entry, side, stop, take, h_high, h_low,
                                   entry * (1 + ret))
        fill_entry = entry * (1 + side * slip)
        fill_exit = exit_px * (1 - side * slip)
        gross_pnl = (fill_exit - fill_entry) * side * qty
        fee = fee_rt * fill_entry * qty
        net_pnl = gross_pnl - fee

        state["balance"] += net_pnl
        state["realized_pnl"] += net_pnl
        state["fees"] += fee
        state["slippage"] += abs(fill_entry - entry) * qty + abs(fill_exit - exit_px) * qty
        state["trade_count"] += 1
        if net_pnl > 0:
            state["win_count"] += 1
        else:
            state["loss_count"] += 1
        state["win_rate"] = state["win_count"] / state["trade_count"]
        state["equity"] = state["balance"]
        state["peak_equity"] = max(state["peak_equity"], state["equity"])
        state["max_drawdown"] = min(state["max_drawdown"],
                                    (state["equity"] - state["peak_equity"]) / state["peak_equity"])

        trades.append({
            "trade_id": f"paper_{state['trade_count']:04d}",
            "timestamp_open": str(row["open_time"]),
            "timestamp_close": str(row["open_time"] + timedelta(minutes=h_min)),
            "symbol": row["symbol"], "category": row["category"],
            "direction": finalist["entry_side"], "hypothesis_id": finalist["hypothesis_id"],
            "entry_price": round(fill_entry, 8), "exit_price": round(fill_exit, 8),
            "stop_loss": round(stop, 8), "take_profit": round(take, 8),
            "position_size": qty,
            "risk_amount": round(qty * abs(entry - stop), 4),
            "gross_pnl": round(gross_pnl, 4), "fee": round(fee, 4),
            "net_pnl": round(net_pnl, 4), "reason": reason,
            "win": net_pnl > 0, "loss": net_pnl <= 0,
            "holding_time_min": h_min,
            "balance_after": round(state["balance"], 2),
            "entry_features": {k: row.get(k) for k in
                               ["relative_volume", "relative_range", "hour_utc",
                                "upper_wick", "lower_wick", "volume_zscore"]},
        })
        if state["equity"] <= 0:
            break

    save_state(state)
    path = None
    if trades:
        path = PAPER_TRADES / f"paper_{datetime.utcnow():%Y%m%dT%H%M%SZ}.parquet"
        pl.DataFrame(trades).write_parquet(path)

    net = np.array([t["net_pnl"] for t in trades], dtype=float) if trades else np.array([])
    return {
        "n_trades": len(trades), "net_pnl": float(net.sum()),
        "win_rate": float((net > 0).mean()) if net.size else 0.0,
        "max_drawdown": state["max_drawdown"],
        "sharpe": float(net.mean() / net.std() * np.sqrt(365)) if net.std() > 1e-12 else 0.0,
        "balance_end": state["balance"],
        "trades_file": path.name if path else None,
    }