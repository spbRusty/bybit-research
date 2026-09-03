"""Внешние источники (§21): рыночная температура и новостные сигналы.

Источники:
- Fear & Greed Index (alternative.me) — температура рынка 0-100, без ключа
- CoinGecko /global — доминанция BTC, без ключа
- CryptoPanic (опционально) — новостные сигналы, требует бесплатный API-ключ

Все данные — дневные или инфрактные, join на минутные свечи через forward-fill.
"""
from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone
from typing import Any

import polars as pl

from src.registry import Feature, register


def _mk(fid: str, name: str, desc: str, formula: str, cost: str = "low") -> Feature:
    return Feature(fid, name, "external", desc, formula, "features_external",
                   "1d", 1, ("external",), False, cost)


register(_mk("fng_value", "Fear & Greed Index", "рыночная температура 0-100",
             "fng(0-100)"))
register(_mk("btc_dominance", "BTC Dominance", "доля BTC в общей рыночной капитализации %",
             "btc_dom(%)"))
register(_mk("total_market_cap", "Total Market Cap", "общая капитализация рынка, USD",
             "mcap(usd)"))


def _fetch_json(url: str, timeout: int = 10) -> dict | None:
    """HTTP GET → JSON. None при любой ошибке (сеть, таймаут, не-200)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "bybit-research/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def fetch_fng(limit: int = 90) -> pl.DataFrame | None:
    """Fear & Greed Index: [date, fng_value]. None если API недоступен."""
    data = _fetch_json(f"https://api.alternative.me/fng/?limit={limit}")
    if not data or "data" not in data:
        return None
    rows = []
    for item in data["data"]:
        ts = int(item.get("timestamp", 0))
        if ts > 0:
            rows.append({
                "date": datetime.fromtimestamp(ts, tz=timezone.utc).date(),
                "fng_value": int(item["value"]),
            })
    if not rows:
        return None
    return pl.DataFrame(rows).unique("date", keep="last").sort("date")


def fetch_coingecko_global() -> dict[str, Any] | None:
    """CoinGecko /global: total_market_cap_usd, btc_dominance. None если недоступен."""
    data = _fetch_json("https://api.coingecko.com/api/v3/global")
    if not data or "data" not in data:
        return None
    g = data["data"]
    btc_dom = (g.get("market_cap_percentage") or {}).get("btc", 0.0)
    total_mcap = (g.get("total_market_cap") or {}).get("usd", 0.0)
    return {
        "btc_dominance": round(btc_dom, 2),
        "total_market_cap": round(total_mcap, 0),
    }


def add_external_features(df: pl.DataFrame, crypto_key: str | None = None) -> pl.DataFrame:
    """Внешние признаки: join Fear&Greed + CoinGecko по дате (forward-fill на минуты).

    crypto_key: если передан, загружает CryptoPanic (пока заглушка).
    """
    if df.height == 0:
        return df

    # 1. Fear & Greed (дневной)
    fng = fetch_fng()
    if fng is not None and fng.height:
        fng = fng.rename({"date": "_ext_date"})
        df = df.with_columns(pl.col("open_time").dt.date().alias("_ext_date"))
        df = df.join(fng, on="_ext_date", how="left").drop("_ext_date")
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Int32).alias("fng_value"))

    # 2. CoinGecko /global (дневной)
    cg = fetch_coingecko_global()
    if cg is not None:
        df = df.with_columns([
            pl.lit(cg["btc_dominance"]).alias("btc_dominance"),
            pl.lit(cg["total_market_cap"]).alias("total_market_cap"),
        ])
    else:
        df = df.with_columns([
            pl.lit(None, dtype=pl.Float64).alias("btc_dominance"),
            pl.lit(None, dtype=pl.Float64).alias("total_market_cap"),
        ])

    # 3. CryptoPanic — заглушка (активируется при наличии ключа)
    # TODO: реализовать при наличии API-ключа
    # if crypto_key: ...

    return df
