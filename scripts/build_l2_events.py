"""Build l2_events.parquet — dense 60s-grid OB-feature events for src.l2_screen.

Per symbol: read reconstructed snapshots (collector tree + protected old tree),
minute grid per UTC date of coverage, asof-backward feature join, quality label
mirroring src.orderbook_integration (gap / ok<=120s / stale<=600s / missing),
keep {ok, stale}, inner-join kline targets (src.events._future_metrics), drop
entry-null and interior null-target rows (live edge kept). Output: 117 cols,
row cap enforced by pre-count. Reads only — never touches the protected tree.
"""
from __future__ import annotations

import logging
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import EVENTS_DIR, RAW_KLINES_DIR  # noqa: E402
from src.data import load_validated  # noqa: E402
from src.events import _FEAT, _future_metrics  # noqa: E402
from src.orderbook_features import DEPTH_LEVELS, compute_all_features  # noqa: E402
from src.orderbook_integration import (  # noqa: E402
    MISSING_THRESHOLD_SEC,
    STALE_THRESHOLD_SEC,
    _get_ob_columns,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("src.data").setLevel(logging.ERROR)  # gap-warning flood x882

DUMP = Path("/home/vlad/Документы/bybit_rs/data/orderbook")
OUT = EVENTS_DIR / "l2_events.parquet"
ROOTS = (ROOT / "collector/data/market/orderbook/reconstructed",
         ROOT / "data/market/orderbook/reconstructed")  # read-only, both trees
HORIZONS: list[int] = list(_FEAT["future_horizons_min"])
TGT = [f"{p}_{h}m" for h in HORIZONS for p in ("return", "mfe", "mae")]
OB_COLS = _get_ob_columns()
COLS = (["open_time", "symbol", "category", "event_id", "entry_price"]
        + TGT + OB_COLS
        + ["ob_data_quality", "ob_ts_min_ms", "ob_ts_max_ms"])
SCHEMA: dict[str, object] = {
    "open_time": pl.Datetime("ms"),
    **{c: pl.String for c in ("symbol", "category", "event_id", "ob_data_quality")},
    "entry_price": pl.Float64,
    **{c: pl.Float64 for c in TGT + OB_COLS},
    "ob_ts_min_ms": pl.Int64,
    "ob_ts_max_ms": pl.Int64,
}
MAX_ROWS = 10_000_000
_MAX_LEVEL = max(DEPTH_LEVELS)  # dump depth 10; 11..20 are zero-filled in files
OB_RD = (["timestamp_ms", "gap_detected", "best_bid", "best_ask",
          "mid_price", "spread_bps"]
         + [f"{q}_{k}_{i}" for i in range(1, _MAX_LEVEL + 1)
            for q in ("bid", "ask") for k in ("px", "sz")])


def minute_grid(ts: np.ndarray) -> np.ndarray:
    """60s grid, floor(min)..floor(max) PER UTC date of snapshot coverage."""
    buck = ts // 60_000 * 60_000
    days = buck // 86_400_000
    return np.concatenate([
        np.arange(buck[days == d].min(), buck[days == d].max() + 1,
                  60_000, dtype=np.int64)
        for d in np.unique(days)])


def universe() -> tuple[list[str], dict[str, str], list[str]]:
    """Symbol dirs (union of both trees) -> category, skipped (no klines)."""
    dirs = sorted({p.name for r in ROOTS if r.is_dir()
                   for p in r.iterdir() if p.is_dir()})
    lin = spo = set()
    for cat, sink in (("linear", "lin"), ("spot", "spo")):
        ps = sorted((DUMP / cat).glob("*.parquet"))
        if ps:
            got = set(pl.concat(
                [pl.read_parquet(p, columns=["symbol"]) for p in ps]
            )["symbol"].unique())
            if sink == "lin":
                lin = got
            else:
                spo = got
    cats: dict[str, str] = {}
    skipped: list[str] = []
    for s in dirs:
        if s in lin:
            cats[s] = "linear"
        elif s in spo:
            cats[s] = "spot"
        else:
            cat = next((c for c in ("linear", "spot")
                        if (RAW_KLINES_DIR / c / f"{s}_{c}_1m.parquet").exists()),
                       None)
            if cat is None:
                skipped.append(s)
            else:
                cats[s] = cat
    return [s for s in dirs if s in cats], cats, skipped


def snap_files(sym: str) -> list[Path]:
    return [p for r in ROOTS if (d := r / sym).is_dir()
            for p in sorted(d.glob("*.parquet*"))]


def ts_of(files: list[Path]) -> np.ndarray:
    """Unique, sorted snapshot timestamps (ms) across both trees."""
    ts = pl.concat([pl.read_parquet(f, columns=["timestamp_ms"]) for f in files])
    return (ts["timestamp_ms"].unique().sort().to_numpy().astype(np.int64))


def pre_count(syms: list[str]) -> tuple[int, dict[str, int]]:
    """Upper bound = grid minutes with a snapshot within MISSING threshold.
    Also returns expected rows-per-date (cov600 > 0 dates). Asserts the cap."""
    total = 0
    dates: dict[str, int] = {}
    for i, s in enumerate(syms):
        fs = snap_files(s)
        if not fs:
            continue
        ts = ts_of(fs)
        if ts.size == 0:
            continue
        grid = minute_grid(ts)
        idx = np.searchsorted(ts, grid, side="right") - 1
        hit = (idx >= 0) & (grid - ts[np.clip(idx, 0, ts.size - 1)]
                            <= MISSING_THRESHOLD_SEC * 1000)
        total += int(hit.sum())
        d_uni, d_cnt = np.unique(grid[hit] // 86_400_000, return_counts=True)
        for d, n in zip(d_uni, d_cnt):
            key = str(np.datetime64(int(d), "D"))
            dates[key] = dates.get(key, 0) + int(n)
        if (i + 1) % 200 == 0:
            print(f"pre-count {i + 1}/{len(syms)}: {total} rows", flush=True)
    assert total <= MAX_ROWS, f"pre-count {total} > MAX_ROWS {MAX_ROWS}"
    return total, dates


def build_symbol(sym: str, cat: str, cnt: Counter) -> pl.DataFrame | None:
    fs = snap_files(sym)
    if not fs:
        cnt["no_files"] += 1
        return None
    ob = pl.concat([pl.read_parquet(f, columns=OB_RD) for f in fs])
    ob = ob.unique(subset=["timestamp_ms"], keep="last").sort("timestamp_ms")
    if ob.height == 0:
        cnt["no_files"] += 1
        return None
    ts = ob["timestamp_ms"].to_numpy().astype(np.int64)
    t0, t1 = int(ts.min()), int(ts.max())
    grid = pl.DataFrame({"_ms": minute_grid(ts)})
    feat = compute_all_features(ob).select(
        ["timestamp_ms", "gap_detected", *OB_COLS]).sort("timestamp_ms")
    j = grid.join_asof(feat, left_on="_ms", right_on="timestamp_ms",
                       strategy="backward")
    j = j.with_columns(((pl.col("_ms") - pl.col("timestamp_ms")) / 1000)
                       .alias("_lag"))
    j = j.with_columns(
        pl.when(pl.col("gap_detected").fill_null(False)).then(pl.lit("gap"))
        .when(pl.col("_lag").fill_null(1e9) <= STALE_THRESHOLD_SEC)
        .then(pl.lit("ok"))
        .when(pl.col("_lag").fill_null(1e9) <= MISSING_THRESHOLD_SEC)
        .then(pl.lit("stale"))
        .otherwise(pl.lit("missing"))
        .alias("ob_data_quality"))
    for q, n in j["ob_data_quality"].value_counts().iter_rows():
        cnt[f"q_{q}"] += int(n)
    j = j.filter(pl.col("ob_data_quality").is_in(["ok", "stale"]))
    if j.height == 0:
        cnt["no_quality_rows"] += 1
        return None
    kl = load_validated(sym, cat)
    if kl is None:
        cnt["klines_invalid"] += 1
        return None
    # Window: klines from coverage midnight to +12h (> 240min + 360min span
    # cap, so _future_metrics semantics are identical to the full series).
    ok = kl.filter(
        (pl.col("open_time").cast(pl.Int64) >= (t0 // 86_400_000) * 86_400_000)
        & (pl.col("open_time").cast(pl.Int64) <= t1 + 720 * 60_000))
    met = _future_metrics(ok)
    if "entry_price" not in met.columns:
        cnt["klines_invalid"] += 1
        return None
    j = j.with_columns(pl.col("_ms").cast(pl.Datetime("ms")).alias("open_time"))
    j = j.join(met.select(["open_time", "entry_price", *TGT]),
               on="open_time", how="inner")
    n0 = j.height
    j = j.drop_nulls("entry_price")
    cnt["drop_entry_null"] += n0 - j.height
    kmax = int(met["open_time"].cast(pl.Int64).max())
    interior = (pl.any_horizontal(*[pl.col(c).is_null() for c in TGT])
                & (pl.col("open_time").cast(pl.Int64) <= kmax - 240 * 60_000))
    n1 = j.height
    j = j.filter(~interior)
    cnt["drop_interior_null"] += n1 - j.height
    if j.height == 0:
        cnt["no_rows"] += 1
        return None
    cnt[f"cat_{cat}"] += 1
    return j.with_columns(
        pl.lit(sym).alias("symbol"),
        pl.lit(cat).alias("category"),
        pl.col("open_time").dt.strftime("%Y%m%dT%H%M%SZ").alias("event_id"),
        pl.lit(t0, dtype=pl.Int64).alias("ob_ts_min_ms"),
        pl.lit(t1, dtype=pl.Int64).alias("ob_ts_max_ms"),
    ).select(COLS).sort("open_time")


def main() -> int:
    t_start = time.time()
    syms, cats, skipped = universe()
    print(f"universe: {len(syms)} symbols; skipped no-klines: "
          f"{len(skipped)} {skipped}", flush=True)
    pre, exp_dates = pre_count(syms)
    print(f"pre-count: {pre} rows (cap {MAX_ROWS}); expected dates: "
          f"{sorted(exp_dates)}", flush=True)

    cnt: Counter = Counter()
    writer: pq.ParquetWriter | None = None
    written = 0
    for i, s in enumerate(syms):
        df = build_symbol(s, cats[s], cnt)
        if df is not None:
            tbl = df.cast(SCHEMA).to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(str(OUT), tbl.schema)
            assert tbl.schema == writer.schema, f"schema drift at {s}"
            writer.write_table(tbl)
            written += df.height
        if (i + 1) % 100 == 0:
            print(f"built {i + 1}/{len(syms)} rows={written} "
                  f"({time.time() - t_start:.0f}s)", flush=True)
    assert writer is not None and written > 0, f"nothing written: {dict(cnt)}"
    writer.close()

    # --- smoke: schema, caps, invariants, date coverage ---
    chk = pl.read_parquet(OUT)
    assert chk.columns == COLS, f"schema mismatch: {len(chk.columns)}"
    assert 0 < chk.height == written <= MAX_ROWS, f"rows {chk.height}"
    assert chk["entry_price"].null_count() == 0, "null entry_price"
    assert (chk["open_time"].cast(pl.Int64) % 60_000 == 0).all(), "off-minute"
    assert chk["event_id"].str.contains(r"^\d{8}T\d{6}Z$").all(), "bad event_id"
    q = set(chk["ob_data_quality"].unique().to_list())
    assert q <= {"ok", "stale"}, f"quality leak: {q}"
    assert chk["ob_mid"].null_count() < chk.height, "ob_mid all null"
    assert chk["symbol"].n_unique() >= 5, "too few symbols"
    out_dates = set(chk["open_time"].dt.strftime("%Y-%m-%d").unique().to_list())
    assert out_dates == set(exp_dates), (
        f"date coverage {sorted(out_dates)} != expected {sorted(exp_dates)}")

    print(f"OK rows={chk.height} cols={len(chk.columns)} "
          f"symbols={chk['symbol'].n_unique()} "
          f"dates={min(out_dates)}..{max(out_dates)} "
          f"({time.time() - t_start:.0f}s)")
    per_date = (chk.with_columns(pl.col("open_time").dt.strftime("%Y-%m-%d")
                                 .alias("_d")).group_by("_d").len().sort("_d"))
    print("rows by date:", dict(zip(per_date["_d"], per_date["len"])))
    print("quality (pre-drop):",
          {k: v for k, v in sorted(cnt.items()) if k.startswith("q_")})
    print("drops:", {k: v for k, v in sorted(cnt.items())
                     if k.startswith(("drop_", "no_", "klines_"))})
    print("categories:", {k: v for k, v in sorted(cnt.items())
                          if k.startswith("cat_")})
    return 0


if __name__ == "__main__":
    sys.exit(main())
