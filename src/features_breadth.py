"""Рыночная широта (§17): cross-sectional доля растущих свечей по всем символам.

breadth[t] = (advancing - declining) / total = 2*mean(is_green) - 1 по всем
символам юниверса в минуту t. Источник — уже собранные klines (is_green).
"""
from __future__ import annotations

import polars as pl


def compute_breadth(symbol_data: dict[tuple[str, str], pl.DataFrame]) -> pl.DataFrame:
    """Считает breadth по общей сетке времени из свечей всех символов.

    symbol_data: {(symbol, category): df с колонками open_time, is_green}
    Возвращает DataFrame [open_time, breadth] с одной строкой на минуту.
    Если данных нет — пустой DataFrame теми же колонками.
    """
    frames = []
    for (sym, cat), df in symbol_data.items():
        g = (df.select(["open_time", "is_green"])
                .rename({"is_green": f"g_{sym}_{cat}"}))
        frames.append(g)
    if not frames:
        return pl.DataFrame({"open_time": pl.Series(dtype=pl.Datetime), "breadth": []})
    # полный outer join по времени всех символов
    joined = frames[0]
    for f in frames[1:]:
        joined = joined.join(f, on="open_time", how="full", coalesce=True)
    green_cols = [c for c in joined.columns if c.startswith("g_")]
    pres = pl.sum_horizontal([pl.col(c).is_not_null() for c in green_cols])
    greens = pl.sum_horizontal([pl.col(c).fill_null(False) for c in green_cols])
    breadth = (
        (2 * greens / pres - 1)
        .round(4).fill_null(0).alias("breadth"))
    return joined.select(pl.col("open_time"), breadth)
