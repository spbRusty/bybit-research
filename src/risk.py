"""Risk layer for paper trading — position limits, exposure, qty guards.

Risk Layer can:
  - reduce position size
  - reject position
  - halt paper trading

Risk Layer cannot:
  - create signals
  - choose hypotheses
  - modify research parameters
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from config.settings import load_toml

logger = logging.getLogger(__name__)
_RISK = load_toml("risk.toml")


@dataclass
class RiskCheck:
    allowed: bool
    adjusted_qty: float = 0.0
    reason: str = ""


def check_position(signal_qty: float, entry_price: float, state: dict,
                   instrument: dict | None = None) -> RiskCheck:
    """Check if a position passes all risk limits. Returns adjusted qty or rejection."""
    inst = instrument or {}
    qty_step = inst.get("qty_step", _RISK["instrument_qty_step"])
    min_lot = inst.get("min_order_qty", _RISK["instrument_min_lot"])
    max_lot = inst.get("max_order_qty", _RISK.get("instrument_max_lot", 1000.0))
    min_notional = inst.get("min_notional", _RISK["instrument_min_notional"])
    max_position_usd = _RISK.get("max_position_size_usd", 100.0)
    max_exposure_usd = _RISK.get("max_exposure_usd", 500.0)
    max_open = _RISK.get("max_open_positions", 5)

    qty = signal_qty

    if qty < min_lot:
        return RiskCheck(False, 0.0, f"qty {qty} < min_lot {min_lot}")

    if qty > max_lot:
        qty = max_lot
        logger.info("Risk: qty capped to max_lot %s", max_lot)

    notional = qty * entry_price
    if notional < min_notional:
        return RiskCheck(False, 0.0, f"notional {notional:.2f} < min_notional {min_notional}")

    position_usd = qty * entry_price
    if position_usd > max_position_usd:
        qty = max_position_usd / entry_price
        qty = _round_to_step(qty, qty_step)
        logger.info("Risk: qty reduced to max_position_size_usd %s", max_position_usd)

    open_positions = state.get("open_positions", [])
    if len(open_positions) >= max_open:
        return RiskCheck(False, 0.0, f"open_positions {len(open_positions)} >= max {max_open}")

    current_exposure = sum(
        p.get("position_size", 0) * p.get("entry_price", 0)
        for p in open_positions
    )
    if current_exposure + qty * entry_price > max_exposure_usd:
        remaining = max_exposure_usd - current_exposure
        if remaining <= 0:
            return RiskCheck(False, 0.0, f"exposure {current_exposure:.2f} >= max {max_exposure_usd}")
        qty = remaining / entry_price
        qty = _round_to_step(qty, qty_step)
        logger.info("Risk: qty reduced to fit max_exposure_usd %s", max_exposure_usd)

    if state.get("balance", 0) <= 0:
        return RiskCheck(False, 0.0, "balance <= 0, trading halted")

    return RiskCheck(True, qty)


def _round_to_step(qty: float, step: float) -> float:
    import numpy as np
    if step <= 0:
        return qty
    return float(np.floor(qty / step) * step)


def can_trade(state: dict) -> bool:
    if state.get("balance", 0) <= 0:
        return False
    open_positions = state.get("open_positions", [])
    if len(open_positions) >= _RISK.get("max_open_positions", 5):
        return False
    return True
