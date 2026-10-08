"""Convert bybit_rs L2 dump parquet into the collector reconstructed-orderbook layout.

Input:  bybit_rs/data/orderbook/{linear,spot}/{cat}_*.parquet
        (timestamp, symbol, bid/ask_price/size_1..10 — one row per symbol per flush)
Output: collector/data/market/orderbook/reconstructed/{SYMBOL}/{YYYY-MM-DD-HH}.parquet
        90-col schema matching the reference files exactly (levels 11-20 zero-filled).

Rules: never overwrites existing hour files (per-file skip, collector owns them),
skips spot rows for symbols present in the linear dump, never touches the protected
old tree. Symbol dirs are NOT skipped, so hours the reconstructor missed get gap-filled.
"""
from pathlib import Path

import polars as pl

BASE = Path("/home/vlad/Документы/построение")
RECON = BASE / "collector/data/market/orderbook/reconstructed"
PROTECTED = BASE / "data/market/orderbook/reconstructed"
DUMP = Path("/home/vlad/Документы/bybit_rs/data/orderbook")
PROTECTED_DATES = {"2026-09-07", "2026-09-16"}
REF = RECON / "BTCUSDT" / "2026-10-05-10.parquet"

SCALAR = [
    "timestamp_ms", "update_id", "symbol", "best_bid", "best_ask",
    "spread_bps", "mid_price", "gap_detected", "reconstruction_version", "n_levels",
]
# reference order per level: bid_px, bid_sz, ask_px, ask_sz
LEVELS = [
    f"{q}_{kind}_{i}"
    for i in range(1, 21)
    for q in ("bid", "ask")
    for kind in ("px", "sz")
]
COLS = SCALAR + LEVELS
N_SRC = 10  # dump depth; levels 11..20 zero-filled (reference cols are not-null)


def load_dump() -> tuple[pl.DataFrame, pl.DataFrame]:
    lin = pl.concat([pl.read_parquet(p) for p in sorted((DUMP / "linear").glob("*.parquet"))])
    spo = pl.concat([pl.read_parquet(p) for p in sorted((DUMP / "spot").glob("*.parquet"))])
    return lin, spo


def to_frame(rows: pl.DataFrame) -> pl.DataFrame:
    """Map dump rows to the 90-col reconstructed schema."""
    mid = (pl.col("bid_price_1") + pl.col("ask_price_1")) / 2.0
    out = rows.with_columns(
        pl.col("timestamp").dt.cast_time_unit("ms").cast(pl.Int64).alias("timestamp_ms"),
        pl.lit(0, dtype=pl.Int64).alias("update_id"),
        pl.col("bid_price_1").alias("best_bid"),
        pl.col("ask_price_1").alias("best_ask"),
        mid.alias("mid_price"),
        ((pl.col("ask_price_1") - pl.col("bid_price_1")) / mid * 1e4).alias("spread_bps"),
        pl.lit(False).alias("gap_detected"),
        pl.lit("1.0").alias("reconstruction_version"),
        pl.lit(N_SRC, dtype=pl.Int64).alias("n_levels"),
    ).rename({
        f"{side}_{qty}_{i}": f"{side[:3]}_{q}_{i}"
        for i in range(1, N_SRC + 1)
        for side in ("bid", "ask")
        for qty, q in (("price", "px"), ("size", "sz"))
    })
    for i in range(N_SRC + 1, 21):  # zero-fill absent depth (cols are not-null)
        for q in ("bid", "ask"):
            for kind in ("px", "sz"):
                out = out.with_columns(pl.lit(0.0, dtype=pl.Float64).alias(f"{q}_{kind}_{i}"))
    out = out.select(COLS)
    assert out.null_count().sum_horizontal().item() == 0, "nulls in output frame"
    assert (out["mid_price"] > 0).all(), "non-positive mid"
    return out


def write_symbol(rows: pl.DataFrame) -> int:
    """Bucket rows by (symbol, date-hour) and write; skip existing files. Returns files written."""
    df = to_frame(rows).with_columns(
        pl.col("timestamp_ms")
        .cast(pl.Datetime("ms"))
        .dt.strftime("%Y-%m-%d-%H")
        .alias("_bucket")
    )
    n = 0
    for (symbol, bucket), part in df.partition_by(["symbol", "_bucket"], as_dict=True).items():
        if str(bucket)[:10] in PROTECTED_DATES:
            raise RuntimeError(f"protected date in dump: {bucket}")
        dest = RECON / symbol / f"{bucket}.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            continue
        part.sort("timestamp_ms").select(COLS).write_parquet(dest)
        n += 1
    return n


def main() -> int:
    # output tree must be the reader-visible collector tree, never the protected old tree
    assert "/collector/data/market/orderbook/reconstructed" in str(RECON)
    assert RECON != PROTECTED and PROTECTED.exists()
    prot = lambda: (
        len(list(PROTECTED.rglob("*.parquet*"))),
        max(p.stat().st_mtime for p in PROTECTED.rglob("*.parquet*")),
    )
    prot_before = prot()

    ref = pl.read_parquet(REF, n_rows=1)
    assert ref.columns == COLS, "reference schema drift"

    lin, spo = load_dump()
    lin_syms = set(lin["symbol"].unique())
    # dumps contain flushes with bid/ask = 0 during subscription gaps; drop them at the trust boundary
    def drop_bad(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
        ok = (pl.col("bid_price_1") > 0) & (pl.col("ask_price_1") > 0)
        return df.filter(ok), df.height - df.filter(ok).height
    lin, n_bad_lin = drop_bad(lin)
    spo, n_bad_spo = drop_bad(spo)
    # ponytail: full rescan of all dumps each run; add a processed-files marker if dumps exceed ~1yr
    # spot rows only for symbols absent from the linear dump (collector owns the rest)
    spo_only = set(spo["symbol"].unique()) - lin_syms
    spo_new = spo.filter(pl.col("symbol").is_in(list(spo_only))) if spo_only else spo.head(0)

    n_files = write_symbol(lin) + write_symbol(spo_new)

    assert prot() == prot_before, "protected tree modified"

    sample = sorted(RECON.rglob("*.parquet"), key=lambda p: p.stat().st_mtime)[-1]
    got = pl.read_parquet(sample, n_rows=1)
    assert got.columns == COLS and all(got.schema[c] == ref.schema[c] for c in COLS), "schema mismatch"
    print(
        f"input rows: linear={lin.height} spot={spo.height} | "
        f"dropped bad mids: {n_bad_lin} linear, {n_bad_spo} spot | "
        f"spot-only symbols: {len(spo_only)} | files written: {n_files}"
    )
    return n_files


if __name__ == "__main__":
    written = main()
    print(f"OK files_written={written}")
