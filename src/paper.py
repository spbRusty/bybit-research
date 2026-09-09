"""Paper trading executor (ТЗ §35-41). Real-time signal → execution → PnL.

Accepts a signal at time T with data available only at T.
Executes forward from T using actual market data.
No future data leakage: signal contains only pre-T information.

Modes:
  MODE A (validated) — only for hypotheses that passed all gates
  MODE B (shadow) — observation only, not proof of edge
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum

import numpy as np
import polars as pl

from config.settings import PAPER_PORTFOLIO, PAPER_SHADOW, PAPER_TRADES, load_toml

logger = logging.getLogger(__name__)
_RISK = load_toml("risk.toml")

STATE_FILE = PAPER_PORTFOLIO / "state.json"
SHADOW_STATE_FILE = PAPER_SHADOW / "state.json"
TRADES_FILE = PAPER_TRADES / "trades.parquet"


class PaperMode(str, Enum):
    VALIDATED = "VALIDATED"
    SHADOW = "SHADOW"


@dataclass
class PaperSignal:
    hypothesis_id: str
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit: float
    signal_timestamp: str
    horizon_min: int
    mode: PaperMode
    vol_gk_30d: float = 0.0
    config_hash: str = ""
    feature_version: str = ""
    rules_hash: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PaperTrade:
    trade_id: str
    hypothesis_id: str
    symbol: str
    side: str
    mode: str
    signal_timestamp: str
    entry_timestamp: str
    exit_timestamp: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    position_size: float
    entry_fee: float
    exit_fee: float
    slippage_cost: float
    gross_pnl: float
    net_pnl: float
    reason: str
    balance_after: float

    def to_dict(self) -> dict:
        return asdict(self)


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return _initial_state()


def _initial_state() -> dict:
    return {
        "equity_start": _RISK["equity_start"],
        "balance": _RISK["equity_start"],
        "equity": _RISK["equity_start"],
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "total_fees": 0.0,
        "total_slippage": 0.0,
        "trade_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "win_rate": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "profit_factor": 0.0,
        "max_drawdown": 0.0,
        "peak_equity": _RISK["equity_start"],
        "open_positions": [],
        "trades": [],
        "mode": None,
        "hypothesis_id": None,
        "started_at": datetime.now(tz=timezone.utc).isoformat(),
        "provenance": {},
    }


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False, default=str))


def load_shadow_state() -> dict:
    if SHADOW_STATE_FILE.exists():
        return json.loads(SHADOW_STATE_FILE.read_text())
    return _initial_state()


def save_shadow_state(state: dict) -> None:
    SHADOW_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    SHADOW_STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False, default=str))


def _round_qty(qty: float, qty_step: float) -> float:
    if qty_step <= 0:
        return qty
    # Determine decimal places from step (0.001 → 3, 0.01 → 2, 0.1 → 1)
    import math
    decimals = max(0, -int(math.floor(math.log10(qty_step)))) if qty_step > 0 else 8
    return round(float(np.floor(qty / qty_step) * qty_step), decimals)


def _position_size(entry: float, stop: float, equity: float,
                   instrument: dict | None = None) -> float:
    risk_usd = equity * _RISK["risk_per_trade_pct"]
    dist = abs(entry - stop)
    if dist <= 0:
        return 0.0

    qty_step = (instrument or {}).get("qty_step", _RISK["instrument_qty_step"])
    min_lot = (instrument or {}).get("min_order_qty", _RISK["instrument_min_lot"])
    max_lot = (instrument or {}).get("max_order_qty", 1e9)
    min_notional = (instrument or {}).get("min_notional", _RISK["instrument_min_notional"])

    qty = _round_qty(risk_usd / dist, qty_step)
    if qty < min_lot or qty * entry < min_notional:
        return 0.0
    if qty > max_lot:
        qty = _round_qty(max_lot, qty_step)
    return float(qty)


def _calculate_fills(entry: float, exit_px: float, side: float,
                     slip_bps: float) -> tuple[float, float, float]:
    slip = slip_bps / 10_000
    fill_entry = entry * (1 + side * slip)
    fill_exit = exit_px * (1 - side * slip)
    slippage = abs(fill_entry - entry) + abs(fill_exit - exit_px)
    return fill_entry, fill_exit, slippage


def _calculate_fees(fill_entry: float, fill_exit: float, qty: float,
                    fee_rate: float) -> tuple[float, float]:
    entry_fee = fee_rate * fill_entry * qty
    exit_fee = fee_rate * fill_exit * qty
    return entry_fee, exit_fee


def _determine_exit(entry: float, side: float, stop: float, take: float,
                    high: float, low: float, close: float) -> tuple[str, float]:
    if side > 0:
        if low <= stop:
            return "stop", stop
        if high >= take:
            return "take", take
    else:
        if high >= stop:
            return "stop", stop
        if low <= take:
            return "take", take
    return "close", close


def execute_signal(signal: PaperSignal, candle_data: pl.DataFrame,
                   state: dict | None = None,
                   instrument: dict | None = None) -> dict:
    """Execute a paper trade from a signal at time T.

    candle_data must contain candles from T+1 onward (open, high, low, close).
    No future data is used in signal construction — only in exit simulation.
    """
    if state is None:
        state = load_state()

    entry = signal.entry_price
    side = 1.0 if signal.side == "long" else -1.0
    stop = signal.stop_loss
    take = signal.take_profit
    h_min = signal.horizon_min

    qty = _position_size(entry, stop, state["balance"], instrument)
    if qty == 0:
        return {"executed": False, "reason": "position_size_zero"}

    slip_bps = _RISK["slippage_bps"]
    fee_rate = _RISK["fee_round_trip"] / 2

    h_high = entry
    h_low = entry
    exit_px = entry
    exit_reason = "timeout"
    exit_ts = signal.signal_timestamp

    horizon_candles = candle_data.head(h_min)
    last_close = entry
    for row in horizon_candles.iter_rows(named=True):
        h_high = max(h_high, row.get("high", entry))
        h_low = min(h_low, row.get("low", entry))
        exit_ts = str(row.get("open_time", signal.signal_timestamp))
        last_close = row.get("close", entry)

    reason, raw_exit = _determine_exit(entry, side, stop, take, h_high, h_low, last_close)
    exit_reason = reason
    exit_px = raw_exit

    fill_entry, fill_exit, slippage = _calculate_fills(entry, exit_px, side, slip_bps)
    entry_fee, exit_fee = _calculate_fees(fill_entry, fill_exit, qty, fee_rate)
    gross_pnl = (fill_exit - fill_entry) * side * qty
    total_fee = entry_fee + exit_fee
    net_pnl = gross_pnl - total_fee

    state["balance"] += net_pnl
    state["realized_pnl"] += net_pnl
    state["total_fees"] += total_fee
    state["total_slippage"] += slippage * qty
    state["trade_count"] += 1
    if net_pnl > 0:
        state["win_count"] += 1
    else:
        state["loss_count"] += 1
    state["win_rate"] = state["win_count"] / state["trade_count"] if state["trade_count"] > 0 else 0.0

    wins = [t.get("net_pnl", 0) for t in state["trades"] if t.get("net_pnl", 0) > 0]
    losses = [t.get("net_pnl", 0) for t in state["trades"] if t.get("net_pnl", 0) <= 0]
    state["avg_win"] = float(np.mean(wins)) if wins else 0.0
    state["avg_loss"] = float(np.mean(losses)) if losses else 0.0
    gross_wins = sum(w for w in wins)
    gross_losses = abs(sum(l for l in losses))
    state["profit_factor"] = gross_wins / gross_losses if gross_losses > 0 else float("inf") if gross_wins > 0 else 0.0

    state["equity"] = state["balance"]
    state["peak_equity"] = max(state["peak_equity"], state["equity"])
    dd = (state["equity"] - state["peak_equity"]) / state["peak_equity"] if state["peak_equity"] > 0 else 0.0
    state["max_drawdown"] = min(state["max_drawdown"], dd)

    trade = PaperTrade(
        trade_id=f"paper_{state['trade_count']:04d}",
        hypothesis_id=signal.hypothesis_id,
        symbol=signal.symbol,
        side=signal.side,
        mode=signal.mode.value,
        signal_timestamp=signal.signal_timestamp,
        entry_timestamp=signal.signal_timestamp,
        exit_timestamp=exit_ts,
        entry_price=round(fill_entry, 8),
        exit_price=round(fill_exit, 8),
        stop_loss=round(stop, 8),
        take_profit=round(take, 8),
        position_size=qty,
        entry_fee=round(entry_fee, 6),
        exit_fee=round(exit_fee, 6),
        slippage_cost=round(slippage * qty, 6),
        gross_pnl=round(gross_pnl, 4),
        net_pnl=round(net_pnl, 4),
        reason=exit_reason,
        balance_after=round(state["balance"], 2),
    )
    state["trades"].append(trade.to_dict())
    save_state(state)

    return {
        "executed": True,
        "trade": trade.to_dict(),
        "balance": state["balance"],
        "equity": state["equity"],
    }


def get_portfolio_summary(state: dict | None = None) -> dict:
    if state is None:
        state = load_state()
    return {
        "equity_start": state.get("equity_start", _RISK["equity_start"]),
        "balance": state["balance"],
        "realized_pnl": state["realized_pnl"],
        "unrealized_pnl": state.get("unrealized_pnl", 0.0),
        "total_pnl": state["realized_pnl"],
        "total_pnl_pct": (state["realized_pnl"] / state["equity_start"] * 100
                         if state["equity_start"] > 0 else 0.0),
        "trade_count": state["trade_count"],
        "win_rate": state["win_rate"],
        "avg_win": state.get("avg_win", 0.0),
        "avg_loss": state.get("avg_loss", 0.0),
        "profit_factor": state.get("profit_factor", 0.0),
        "max_drawdown": state["max_drawdown"],
        "total_fees": state["total_fees"],
        "total_slippage": state.get("total_slippage", 0.0),
        "open_positions": state.get("open_positions", []),
        "mode": state.get("mode"),
        "hypothesis_id": state.get("hypothesis_id"),
    }


def paper_run_backtest(events: pl.DataFrame, finalist: dict) -> dict:
    """Backward-compatible: run paper on pre-computed events (for validation/OOS only)."""
    state = load_state()
    h_min = int(finalist["horizon_min"])
    side = 1.0 if finalist["entry_side"] == "long" else -1.0
    slip_bps = _RISK["slippage_bps"]
    fee_rate = _RISK["fee_round_trip"] / 2

    trades: list[dict] = []
    for row in events.iter_rows(named=True):
        entry = row.get("entry_price")
        mfe = row.get(f"mfe_{h_min}m")
        mae = row.get(f"mae_{h_min}m")
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
        reason, raw_exit = _determine_exit(entry, side, stop, take, h_high, h_low,
                                           entry * (1 + ret))
        fill_entry, fill_exit, slippage = _calculate_fills(entry, raw_exit, side, slip_bps)
        entry_fee, exit_fee = _calculate_fees(fill_entry, fill_exit, qty, fee_rate)
        gross_pnl = (fill_exit - fill_entry) * side * qty
        total_fee = entry_fee + exit_fee
        net_pnl = gross_pnl - total_fee

        state["balance"] += net_pnl
        state["realized_pnl"] += net_pnl
        state["total_fees"] += total_fee
        state["total_slippage"] += slippage * qty
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
            "symbol": row["symbol"],
            "direction": finalist["entry_side"],
            "hypothesis_id": finalist["hypothesis_id"],
            "entry_price": round(fill_entry, 8),
            "exit_price": round(fill_exit, 8),
            "stop_loss": round(stop, 8),
            "take_profit": round(take, 8),
            "position_size": qty,
            "entry_fee": round(entry_fee, 6),
            "exit_fee": round(exit_fee, 6),
            "slippage_cost": round(slippage * qty, 6),
            "gross_pnl": round(gross_pnl, 4),
            "net_pnl": round(net_pnl, 4),
            "reason": reason,
            "balance_after": round(state["balance"], 2),
        })
        if state["equity"] <= 0:
            break

    save_state(state)
    path = None
    if trades:
        path = PAPER_TRADES / f"paper_{datetime.now(tz=timezone.utc):%Y%m%dT%H%M%SZ}.parquet"
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


def reset_state() -> dict:
    state = _initial_state()
    save_state(state)
    return state


# --------------------------------------------------------------------------
# Shadow Paper (MODE B) — automatic observation of CANDIDATE hypotheses
# --------------------------------------------------------------------------

def shadow_run(events: pl.DataFrame,
               hypotheses: list,
               state: dict | None = None,
               instruments: dict | None = None) -> dict:
    """Execute shadow paper trades for CANDIDATE hypotheses.

    For each event that matches a hypothesis condition, execute a shadow trade
    in MODE B (observation only). No real orders, no proof of edge.
    Shadow state is persisted to SHADOW_STATE_FILE — never touches the
    MODE A paper portfolio (Task 2: shadow does not change paper state).

    Args:
        events: DataFrame with event data (must have hypothesis condition columns)
        hypotheses: List of Hypothesis objects to test
        state: Optional shadow state (loaded from shadow file if None)
        instruments: Optional dict of instrument specs by symbol

    Returns:
        dict with trade count, PnL summary, and per-hypothesis stats
    """
    if state is None:
        state = load_shadow_state()
        state["mode"] = PaperMode.SHADOW.value
    if instruments is None:
        instruments = {}

    trades_executed = 0
    trades_by_hyp: dict[str, int] = {}
    pnl_by_hyp: dict[str, float] = {}

    for hyp in hypotheses:
        hyp_id = hyp.hypothesis_id
        trades_by_hyp[hyp_id] = 0
        pnl_by_hyp[hyp_id] = 0.0

        try:
            cond = eval(hyp.condition, {"pl": pl})
        except Exception:
            continue

        target_col = f"return_{hyp.horizon_min}m"
        mfe_col = f"mfe_{hyp.horizon_min}m"
        mae_col = f"mae_{hyp.horizon_min}m"

        sub = events.filter(cond).filter(
            pl.col(target_col).is_not_null() &
            pl.col("entry_price").is_not_null()
        )

        for row in sub.iter_rows(named=True):
            entry = row.get("entry_price")
            if entry is None or entry <= 0:
                continue

            vol = row.get("vol_gk_30d", 0.0) or 0.0
            stop_dist = _RISK["stop_k_vol"] * vol * entry if vol > 0 else entry * 0.002
            if stop_dist <= 0:
                continue

            side = 1.0 if hyp.entry_side == "long" else -1.0
            stop = entry - side * stop_dist
            take = entry + side * _RISK["take_k_stop"] * stop_dist

            symbol = row.get("symbol", "UNKNOWN")
            inst = instruments.get(symbol)
            qty = _position_size(entry, stop, state["balance"], inst)
            if qty == 0:
                continue

            slip_bps = _RISK["slippage_bps"]
            fee_rate = _RISK["fee_round_trip"] / 2

            h_high = entry * (1 + (row.get(mfe_col, 0) or 0))
            h_low = entry * (1 + (row.get(mae_col, 0) or 0))
            raw_ret = row.get(target_col, 0) or 0

            reason, raw_exit = _determine_exit(entry, side, stop, take, h_high, h_low,
                                               entry * (1 + raw_ret))
            exit_px = raw_exit

            fill_entry, fill_exit, slippage = _calculate_fills(entry, exit_px, side, slip_bps)
            entry_fee, exit_fee = _calculate_fees(fill_entry, fill_exit, qty, fee_rate)
            gross_pnl = (fill_exit - fill_entry) * side * qty
            total_fee = entry_fee + exit_fee
            net_pnl = gross_pnl - total_fee

            state["balance"] += net_pnl
            state["realized_pnl"] += net_pnl
            state["total_fees"] += total_fee
            state["total_slippage"] += slippage * qty
            state["trade_count"] += 1
            if net_pnl > 0:
                state["win_count"] += 1
            else:
                state["loss_count"] += 1
            state["win_rate"] = state["win_count"] / state["trade_count"]
            state["equity"] = state["balance"]
            state["peak_equity"] = max(state["peak_equity"], state["equity"])
            dd = (state["equity"] - state["peak_equity"]) / state["peak_equity"] if state["peak_equity"] > 0 else 0.0
            state["max_drawdown"] = min(state["max_drawdown"], dd)

            trade = PaperTrade(
                trade_id=f"shadow_{state['trade_count']:04d}",
                hypothesis_id=hyp_id,
                symbol=symbol,
                side=hyp.entry_side,
                mode=PaperMode.SHADOW.value,
                signal_timestamp=str(row.get("open_time", "")),
                entry_timestamp=str(row.get("open_time", "")),
                exit_timestamp=str(row.get("open_time", "")),
                entry_price=round(fill_entry, 8),
                exit_price=round(fill_exit, 8),
                stop_loss=round(stop, 8),
                take_profit=round(take, 8),
                position_size=qty,
                entry_fee=round(entry_fee, 6),
                exit_fee=round(exit_fee, 6),
                slippage_cost=round(slippage * qty, 6),
                gross_pnl=round(gross_pnl, 4),
                net_pnl=round(net_pnl, 4),
                reason=reason,
                balance_after=round(state["balance"], 2),
            )
            state["trades"].append(trade.to_dict())
            trades_executed += 1
            trades_by_hyp[hyp_id] += 1
            pnl_by_hyp[hyp_id] += net_pnl

    save_shadow_state(state)

    return {
        "trades_executed": trades_executed,
        "trades_by_hypothesis": trades_by_hyp,
        "pnl_by_hypothesis": pnl_by_hyp,
        "balance": state["balance"],
        "realized_pnl": state["realized_pnl"],
        "trade_count": state["trade_count"],
        "win_rate": state["win_rate"],
        "max_drawdown": state["max_drawdown"],
    }


def get_shadow_summary(state: dict | None = None) -> dict:
    """Summary of shadow paper activity (MODE B, read-only for dashboard)."""
    if state is None:
        state = load_shadow_state()
    shadow_trades = [t for t in state.get("trades", []) if t.get("mode") == "SHADOW"]
    return {
        "shadow_trade_count": len(shadow_trades),
        "shadow_pnl": sum(t.get("net_pnl", 0) for t in shadow_trades),
        "shadow_win_rate": (sum(1 for t in shadow_trades if t.get("net_pnl", 0) > 0) / len(shadow_trades)
                           if shadow_trades else 0.0),
        "balance": state["balance"],
        "realized_pnl": state["realized_pnl"],
    }
