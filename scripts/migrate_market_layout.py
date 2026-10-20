"""Одноразовая миграция marketdata: {kind}/linear/{SYM}.parquet ->
{kind}/linear/{SYM}/{YYYY-MM-DD}.parquet (ZSTD).

Запускать при остановленном bybit_marketdata.service.
Legacy-файлы переносятся в collector/data/legacy_flat/{kind}/.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1] / "collector" / "data" / "market"
LEGACY = Path(__file__).resolve().parents[1] / "collector" / "data" / "legacy_flat"


def main() -> int:
    moved = 0
    for kind in ("trades", "futures", "liquidation", "ratio"):
        d = ROOT / kind / "linear"
        if not d.is_dir():
            continue
        for p in sorted(d.glob("*.parquet")):
            sym = p.stem
            df = pl.read_parquet(p)
            n = df.height
            if n == 0:
                continue
            out_dir = d / sym
            out_dir.mkdir(parents=True, exist_ok=True)
            df = df.with_columns(
                pl.from_epoch(pl.col("ts"), time_unit="ms").dt.strftime("%Y-%m-%d").alias("_date"))
            for (date,), part in df.partition_by(["_date"], as_dict=True).items():
                dest = out_dir / f"{date}.parquet"
                part = part.drop("_date").sort("ts")
                if dest.exists():
                    old = pl.read_parquet(dest)
                    part = pl.concat([old, part]).unique(subset=["ts"], keep="last").sort("ts")
                part.write_parquet(dest, compression="zstd",
                                   row_group_size=128_000, statistics=True)
            total = sum(pl.read_parquet(q).height for q in out_dir.glob("*.parquet"))
            assert total >= n, f"{p}: rows {total} < {n}"
            LEGACY.mkdir(parents=True, exist_ok=True)
            (LEGACY / kind).mkdir(parents=True, exist_ok=True)
            shutil.move(str(p), LEGACY / kind / p.name)
            moved += 1
    print(f"OK migrated {moved} files; legacy kept in {LEGACY}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
