"""Одноразовая миграция klines: {SYM}_{cat}_1m.parquet -> {SYM}/{YYYY-MM}.parquet.

Читает legacy-файлы, режет по UTC-месяцам, пишет ZSTD-партиции, проверяет,
что число строк не уменьшилось, и переносит legacy в data/klines_legacy/.
Запускать при остановленном bybit_klines.service.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1] / "data" / "klines"
LEGACY = ROOT.parent / "klines_legacy"


def main() -> int:
    moved = 0
    for cat in ("linear", "spot"):
        cat_dir = ROOT / cat
        if not cat_dir.is_dir():
            continue
        for p in sorted(cat_dir.glob("*_1m.parquet")):
            sym = p.name[: -len(f"_{cat}_1m.parquet")]
            df = pl.read_parquet(p)
            n = df.height
            if n == 0:
                continue
            out_dir = cat_dir / sym
            out_dir.mkdir(parents=True, exist_ok=True)
            df = df.with_columns(pl.col("open_time").dt.strftime("%Y-%m").alias("_month"))
            for (month,), part in df.partition_by(["_month"], as_dict=True).items():
                dest = out_dir / f"{month}.parquet"
                part = part.drop("_month").sort("open_time")
                if dest.exists():
                    old = pl.read_parquet(dest)
                    part = pl.concat([old, part]).unique("open_time", keep="last").sort("open_time")
                part.write_parquet(dest, compression="zstd",
                                   row_group_size=128_000, statistics=True)
            total = sum(pl.read_parquet(q).height for q in out_dir.glob("*.parquet"))
            assert total >= n, f"{p}: rows {total} < {n}"
            LEGACY.mkdir(parents=True, exist_ok=True)
            (LEGACY / cat).mkdir(parents=True, exist_ok=True)
            shutil.move(str(p), LEGACY / cat / p.name)
            moved += 1
            if moved % 100 == 0:
                print(f"migrated {moved} files...", flush=True)
    print(f"OK migrated {moved} legacy files; legacy kept in {LEGACY}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
