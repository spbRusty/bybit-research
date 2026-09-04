"""Рыночные тиковые признаки (ТЗ §12-§15): стакан, order flow, фьючерсные funding/OI.

Тиковые потоки собирает Rust-сервис marketdata в
MARKET_DATA_DIR/{trades,orderbook,futures,liquidation,ratio}/linear/{sym}.parquet
(формат: ts = Int64 миллисекунды, не datetime).

build_market_features агрегирует тики к минутным бакетам и возвращает
DataFrame[open_time (datetime), market_*]. Признак на бакете T использует ТОЛЬКО
тики ts ∈ [T, T+60s) — без look-ahead в будущее.
"""
from __future__ import annotations

import polars as pl

from config.settings import MARKET_DATA_DIR
from src.registry import Feature, register

_MS = 60_000
_TOP = 5  # глубина для depth-признаков стакана


def _mk(fid: str, name: str, desc: str, formula: str, lookback: int = 1,
        data: str = "tick") -> Feature:
    return Feature(fid, name, "market", desc, formula, "features_market",
                   "1m", lookback, (data,), True, "medium")


_MARKET_FEATURES = [
    # §12 — стакан (sparse, top-50 символов)
    _mk("mk_best_bid", "Лучшая заявка на покупку", "bid_px(level=1, посл. non-NaN)", "bid1"),
    _mk("mk_best_ask", "Лучшая заявка на продажу", "ask_px(level=1, посл. non-NaN)", "ask1"),
    _mk("mk_spread_pct", "Относительный спред", "(ask1-bid1)/mid", "(a1-b1)/mid"),
    _mk("mk_imid", "Срединная цена", "(bid1+ask1)/2", "(b1+a1)/2"),
    _mk("mk_imb1", "Дисбаланс объёма 1-го уровня", "(bid_sz1-ask_sz1)/(bid_sz1+ask_sz1)", "(bs1-as1)/(bs1+as1)"),
    _mk("mk_depth_bid5", "Глубина бида по 5 уровням", "sum(bid_sz 1..5)", "Σbid_sz(1-5)"),
    _mk("mk_depth_ask5", "Глубина аска по 5 уровням", "sum(ask_sz 1..5)", "Σask_sz(1-5)"),
    # §13 — order flow
    _mk("mk_buy_vol", "Объём покупок за минуту", "sum(size | is_buy)", "Σsize(Buy)"),
    _mk("mk_sell_vol", "Объём продаж за минуту", "sum(size | ~is_buy)", "Σsize(Sell)"),
    _mk("mk_flow_imb", "Дисбаланс потока сделок", "(buy-sell)/(buy+sell)", "(B-S)/(B+S)"),
    _mk("mk_trade_count", "Число сделок за минуту", "count(rows)", "count"),
    _mk("mk_notional", "Оборот за минуту", "sum(price*size)", "Σp·v"),
    _mk("mk_avg_size", "Средний размер сделки", "sum(size)/count", "Σv/count"),
    _mk("mk_large_buy_vol", "Объём крупных покупок (>5·медианы размера)", "sum(size|is_buy & size>5·median)", "Σsize(big Buy)"),
    # §14 — futures funding/OI (последний non-NaN в минуте)
    _mk("mk_funding_rate", "Ставка финансирования (посл.)", "funding_rate на закрытии минуты", "fr(T)"),
    _mk("mk_oi", "Открытый интерес (посл.)", "oi на закрытии минуты", "oi(T)"),
    _mk("mk_oi_change", "Изменение OI за минуту", "oi(T)-oi(T-1)", "Δoi"),
    _mk("mk_mark_px", "Марк-цена (посл.)", "mark_px на закрытии минуты", "mark(T)"),
    # liquidation (редкие события)
    _mk("mk_liq_sell_vol", "Объём ликвидаций продаж", "sum(size | is_sell)", "Σliq_size(Sell)"),
    # ratio (посл. значение long/short)
    _mk("mk_buy_ratio", "Доля длинных позиций", "buy_ratio на закрытии минуты", "buyRatio(T)"),
]
for _f in _MARKET_FEATURES:
    register(_f)


def _bucket_ms(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(((pl.col("ts") // _MS) * _MS).alias("bucket_ms")).sort("bucket_ms")


def _read(kind: str, sym: str) -> pl.DataFrame | None:
    p = MARKET_DATA_DIR / kind / "linear" / f"{sym}.parquet"
    return pl.read_parquet(p) if p.exists() else None


def _join_min(out: pl.DataFrame, f: pl.DataFrame) -> pl.DataFrame:
    right_cols = [c for c in f.columns if c != "bucket_ms"]
    collide = [c for c in right_cols if c in out.columns]
    f = f.rename({c: f"{c}_r" for c in collide})
    return (out.join(f, on="bucket_ms", how="full", coalesce=True)
            .drop([f"{c}_r" for c in collide])
            .sort("bucket_ms"))


def _trades_features(df: pl.DataFrame) -> pl.DataFrame:
    g = (_bucket_ms(df)
         .with_columns((pl.col("size") * pl.col("is_buy")).alias("bs"),
                       (pl.col("size") * (~pl.col("is_buy")).cast(pl.Int64)).alias("ss"),
                       (pl.col("price") * pl.col("size")).alias("notional"))
         .group_by("bucket_ms")
         .agg(pl.col("bs").sum().alias("mk_buy_vol"),
              pl.col("ss").sum().alias("mk_sell_vol"),
              pl.len().alias("mk_trade_count"),
              pl.col("notional").sum().alias("mk_notional"),
              pl.col("size").mean().alias("mk_avg_size"),
              (pl.col("bs").filter(pl.col("size") > 5 * pl.col("size").median()))
              .sum().alias("mk_large_buy_vol")))
    return g.with_columns(
        pl.when((pl.col("mk_buy_vol") + pl.col("mk_sell_vol")) > 0)
         .then((pl.col("mk_buy_vol") - pl.col("mk_sell_vol"))
               / (pl.col("mk_buy_vol") + pl.col("mk_sell_vol")))
         .otherwise(None).alias("mk_flow_imb"))


def _orderbook_features(df: pl.DataFrame) -> pl.DataFrame:
    lvl = (_bucket_ms(df).sort("bucket_ms", "ts", "seq")
           .group_by(["bucket_ms", "level"])
           .agg(pl.col("bid_px").filter(pl.col("bid_px").is_not_nan()).last(),
                pl.col("bid_sz").filter(pl.col("bid_sz").is_not_nan()).last(),
                pl.col("ask_px").filter(pl.col("ask_px").is_not_nan()).last(),
                pl.col("ask_sz").filter(pl.col("ask_sz").is_not_nan()).last()))
    if lvl.height == 0:
        return lvl
    lvl = (lvl.filter(pl.col("level") <= _TOP)
           .sort("bucket_ms", "level"))
    used = lvl["level"].unique().sort().to_list()
    pivot = lvl.pivot(on="level", index="bucket_ms",
                      values=["bid_px", "bid_sz", "ask_px", "ask_sz"],
                      aggregate_function="last")
    rename = {}
    for i in used:
        for side in ("bid", "ask"):
            for field in ("px", "sz"):
                rename[f"{side}_{field}_{i}"] = f"{side}{i}_{field}"
    pivot = pivot.rename(rename)
    bid_sz_cols = [c for c in pivot.columns if c.startswith("bid") and c.endswith("_sz")]
    ask_sz_cols = [c for c in pivot.columns if c.startswith("ask") and c.endswith("_sz")]
    raw_level_cols = [c for c in pivot.columns
                       if c.startswith(("bid", "ask"))]
    return pivot.with_columns([
        pl.sum_horizontal(bid_sz_cols).alias("mk_depth_bid5"),
        pl.sum_horizontal(ask_sz_cols).alias("mk_depth_ask5"),
        pl.col("bid1_px").alias("mk_best_bid"),
        pl.col("ask1_px").alias("mk_best_ask"),
        pl.col("bid1_sz").alias("mk_bid1_sz"),
        pl.col("ask1_sz").alias("mk_ask1_sz"),
    ]).drop(raw_level_cols)


def _futures_features(df: pl.DataFrame) -> pl.DataFrame:
    g = (_bucket_ms(df).sort("bucket_ms", "ts")
         .group_by("bucket_ms")
         .agg(pl.col("funding_rate").last(), pl.col("oi").last(),
              pl.col("mark_px").last()))
    g = g.rename({"funding_rate": "mk_funding_rate", "oi": "mk_oi",
                  "mark_px": "mk_mark_px"})
    return g.with_columns(
        pl.when(pl.col("mk_funding_rate").is_nan()).then(None)
        .otherwise(pl.col("mk_funding_rate")).alias("mk_funding_rate"))


def _liq_features(df: pl.DataFrame) -> pl.DataFrame:
    return (_bucket_ms(df)
            .with_columns((pl.col("size") * pl.col("is_sell").cast(pl.Int64)).alias("ss"))
            .group_by("bucket_ms").agg(pl.col("ss").sum().alias("mk_liq_sell_vol")))


def _ratio_features(df: pl.DataFrame) -> pl.DataFrame:
    return (_bucket_ms(df).sort("bucket_ms", "ts")
            .group_by("bucket_ms").agg(pl.col("buy_ratio").last().alias("mk_buy_ratio")))


def build_market_features(symbol: str) -> pl.DataFrame:
    """Агрегирует все потоки символа к минутным бакетам -> DataFrame[open_time, market_*]."""
    readers = {
        "trades": _trades_features,
        "orderbook": _orderbook_features,
        "futures": _futures_features,
        "liquidation": _liq_features,
        "ratio": _ratio_features,
    }
    frames = []
    for kind, fn in readers.items():
        df = _read(kind, symbol)
        if df is None or df.height == 0:
            continue
        frames.append(fn(df))
    if not frames:
        return pl.DataFrame({"open_time": pl.Series([], dtype=pl.Datetime("ms"))})
    out = frames[0]
    for f in frames[1:]:
        out = _join_min(out, f)
    # Разреженность: стакан/funding/ratio — «состояния», известные и между поллингами.
    # forward_fill по времени (только назад, без look-ahead в будущее) для медленных величин.
    state_cols = [c for c in ("mk_best_bid", "mk_best_ask", "mk_funding_rate", "mk_oi",
                              "mk_mark_px", "mk_buy_ratio",
                              "mk_depth_bid5", "mk_depth_ask5") if c in out.columns]
    out = out.with_columns([pl.col(c).forward_fill() for c in state_cols])
    if "mk_oi" in out.columns:
        out = out.with_columns(pl.col("mk_oi").diff().alias("mk_oi_change"))
    # spread/imid/imb1 — после forward-fill обеих сторон (иначе sparse-дельта даёт NaN)
    if "mk_best_bid" in out.columns and "mk_best_ask" in out.columns:
        out = out.with_columns([
            pl.when(pl.col("mk_best_bid").is_not_null() & pl.col("mk_best_ask").is_not_null())
             .then(pl.col("mk_best_ask") / pl.col("mk_best_bid") - 1).otherwise(None)
             .alias("mk_spread_pct"),
            pl.when(pl.col("mk_best_bid").is_not_null() & pl.col("mk_best_ask").is_not_null())
             .then((pl.col("mk_best_bid") + pl.col("mk_best_ask")) / 2).otherwise(None)
             .alias("mk_imid"),
        ])
    if "mk_bid1_sz" in out.columns and "mk_ask1_sz" in out.columns:
        out = out.with_columns([
            pl.when((pl.col("mk_bid1_sz") + pl.col("mk_ask1_sz")) > 0)
             .then((pl.col("mk_bid1_sz") - pl.col("mk_ask1_sz"))
                   / (pl.col("mk_bid1_sz") + pl.col("mk_ask1_sz"))).otherwise(None)
             .alias("mk_imb1"),
        ])
    return (out.with_columns(pl.col("bucket_ms").cast(pl.Datetime("ms")).alias("open_time"))
            .select(["open_time"] + [c for c in out.columns if c != "bucket_ms"]))


if __name__ == "__main__":
    import os
    from pathlib import Path
    os.chdir(Path(__file__).resolve().parent.parent)
    out = build_market_features("BTCUSDT")
    print("BTCUSDT market buckets:", out.height)
    print("cols:", [c for c in out.columns])
    cols = [c for c in ["open_time", "mk_best_bid", "mk_best_ask", "mk_spread_pct",
                        "mk_imb1", "mk_buy_vol", "mk_sell_vol", "mk_flow_imb",
                        "mk_funding_rate", "mk_oi", "mk_oi_change", "mk_buy_ratio"]
            if c in out.columns]
    print(out.tail(3).select(cols))
