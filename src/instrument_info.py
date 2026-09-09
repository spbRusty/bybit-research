"""Bybit instrument info — tick_size, qty_step, min/max order qty.

Fetches from Bybit REST API and caches locally.
Falls back to risk.toml defaults if API unavailable.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import httpx

from config.settings import load_toml

logger = logging.getLogger(__name__)

_RISK = load_toml("risk.toml")
CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "instruments"
CACHE_FILE = CACHE_DIR / "bybit_linear.json"
CACHE_TTL = 86400

INSTRUMENT_FIELDS = [
    "symbol", "baseCoin", "quoteCoin",
    "tickSize", "qtyStep", "minOrderQty", "maxOrderQty",
    "minNotional", "status",
]


def _ensure_cache_dir():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_instruments() -> dict[str, dict]:
    """Fetch all linear perpetual instruments from Bybit."""
    url = "https://api.bybit.com/v5/market/instruments-info"
    params = {"category": "linear", "limit": 1000}
    instruments = {}
    cursor = ""

    try:
        with httpx.Client(timeout=15) as client:
            while True:
                if cursor:
                    params["cursor"] = cursor
                resp = client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                if data.get("retCode") != 0:
                    logger.warning("Bybit API error: %s", data.get("retMsg"))
                    break
                for item in data.get("result", {}).get("list", []):
                    sym = item.get("symbol", "")
                    instruments[sym] = {
                        "symbol": sym,
                        "base_coin": item.get("baseCoin", ""),
                        "quote_coin": item.get("quoteCoin", ""),
                        "tick_size": float(item.get("tickSize", "0.00001")),
                        "qty_step": float(item.get("qtyStep", "0.001")),
                        "min_order_qty": float(item.get("minOrderQty", "0.001")),
                        "max_order_qty": float(item.get("maxOrderQty", "1000")),
                        "min_notional": float(item.get("minNotional", "5")),
                        "status": item.get("status", ""),
                        "fetched_at": time.time(),
                    }
                cursor = data.get("result", {}).get("cursor", "")
                if not cursor:
                    break
    except Exception as e:
        logger.warning("Failed to fetch Bybit instruments: %s", e)

    return instruments


def save_cache(instruments: dict[str, dict]) -> None:
    _ensure_cache_dir()
    CACHE_FILE.write_text(json.dumps(instruments, indent=2, ensure_ascii=False))


def load_cache() -> dict[str, dict]:
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text())
            if data:
                sample = next(iter(data.values()), {})
                age = time.time() - sample.get("fetched_at", 0)
                if age < CACHE_TTL:
                    return data
        except Exception:
            pass
    return {}


def get_instruments(force_refresh: bool = False) -> dict[str, dict]:
    if not force_refresh:
        cached = load_cache()
        if cached:
            return cached
    instruments = fetch_instruments()
    if instruments:
        save_cache(instruments)
    return instruments


def get_instrument(symbol: str) -> dict:
    instruments = get_instruments()
    if symbol in instruments:
        return instruments[symbol]
    return {
        "symbol": symbol,
        "tick_size": _RISK["instrument_tick_size"],
        "qty_step": _RISK["instrument_qty_step"],
        "min_order_qty": _RISK["instrument_min_lot"],
        "max_order_qty": _RISK["instrument_max_lot"],
        "min_notional": _RISK["instrument_min_notional"],
    }
