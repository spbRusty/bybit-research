"""События (ТЗ §16, §28, §27): предварительный фильтр свечей -> события + будущие доходности.

Свеча проходит предфильтр (relative_volume > X и/или relative_range > Y) ->
становится потенциальным событием. К событию присоединяются признаки на момент
T (close свечи). Будущие доходности считаются от ENTRY = open(T+1) до close(T+1+h)
либо до стопа/тейка (MFE/MAE). Никаких данных после T в признаках нет.
"""
from __future__ import annotations

import numpy as np
import polars as pl
from config.settings import EVENTS_DIR
from config.settings import load_toml

_FEAT = load_toml("features.toml")
_RISK = load_toml("risk.toml")


def suspicious_candles(df: pl.DataFrame,
                       min_rvol: float | None = None,
                       min_rrange: float | None = None) -> pl.DataFrame:
    """Свечи, прошедшие предварительный фильтр (пороги из конфига по умолчанию)."""
    rvol = _FEAT["prefilter_rel_volume"] if min_rvol is None else min_rvol
    rrange = _FEAT["prefilter_rel_range"] if min_rrange is None else min_rrange
    return df.filter((pl.col("relative_volume") > rvol) |
                     (pl.col("relative_range") > rrange))


def _sparse_extreme(vals: np.ndarray, take_max: bool) -> list[np.ndarray]:
    """Sparse table (двоичные подъёмы) для запросов max/min на произвольном отрезке."""
    n = vals.shape[0]
    levels = [vals.astype(np.float64, copy=False)]
    step = 1
    while (step << 1) <= n:
        prev = levels[-1]
        m = n - (step << 1) + 1
        cur = (np.maximum if take_max else np.minimum)(prev[:m], prev[step:step + m])
        levels.append(cur)
        step <<= 1
    return levels


def _range_extreme(levels: list[np.ndarray], starts: np.ndarray, ends: np.ndarray,
                   take_max: bool = True) -> np.ndarray:
    """Экстремум на включительном отрезке [starts[i], ends[i]]; NaN где отрезок пуст."""
    out = np.full(len(starts), np.nan)
    ok = (ends >= starts) & (ends < len(levels[0])) & (starts >= 0)
    if not ok.any():
        return out
    s, e = starts[ok], ends[ok]
    k = np.floor(np.log2((e - s + 1).astype(np.float64))).astype(np.int64)
    pick = np.maximum if take_max else np.minimum
    # два перекрывающихся блока длины 2^k покрывают весь отрезок
    res = np.empty(len(k))
    for lvl in np.unique(k):
        m = k == lvl
        tbl = levels[int(lvl)]
        res[m] = pick(tbl[s[m]], tbl[e[m] - (1 << int(lvl)) + 1])
    out[ok] = res
    return out


def _future_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Будущие доходности и MFE/MAE для каждой свечи (строго вперёд от open(T+1)).

    Горизонт задаётся В МИНУТАХ, а не в строках: выход = первая свеча с
    open_time >= T + h минут. Раньше был shift(-h) по строкам, а сетка баров
    нерегулярна (медиана gap ~2 мин, максимум 174 мин) — реальный горизонт
    плавал в разы и таргеты были подписаны неверно.

    return_{h}m = close(T+h мин) / open(T+1) - 1
    mfe_{h}m   = max(high[T+1..T+h мин]) / open(T+1) - 1
    mae_{h}m   = min(low[T+1..T+h мин]) / open(T+1) - 1
    Окно с пропуском данных (фактический горизонт > 1.5*h) -> null: h минут
    не покрыты свечами, заявлять такой таргет нельзя.
    """
    if df.height == 0:
        return df
    df = df.sort("open_time")
    n = df.height
    out = df.with_columns(pl.col("open").shift(-1).alias("entry_price"))

    t_ms = df["open_time"].dt.epoch("ms").to_numpy().astype(np.int64)
    rows = np.arange(n, dtype=np.int64)
    entry_idx = rows + 1
    entry_price = np.full(n, np.nan)
    entry_price[:-1] = df["open"].to_numpy()[1:].astype(np.float64)
    close = df["close"].to_numpy().astype(np.float64)
    hi = df["high"].to_numpy().astype(np.float64)
    lo = df["low"].to_numpy().astype(np.float64)
    hi_tbl = _sparse_extreme(hi, take_max=True)
    lo_tbl = _sparse_extreme(lo, take_max=False)

    for h in _FEAT["future_horizons_min"]:
        exit_idx = np.searchsorted(t_ms, t_ms + int(h) * 60_000, side="left")
        # фактическое покрытие окна: не более 1.5*h минут, иначе h минут нет данных
        span = np.full(n, np.inf)
        has_exit = exit_idx < n
        span[has_exit] = t_ms[exit_idx[has_exit]] - t_ms[has_exit]
        ok = has_exit & (span <= int(h) * 60_000 * 3 // 2)
        end_c = np.clip(exit_idx, 0, n - 1)
        mfe = _range_extreme(hi_tbl, entry_idx, end_c, take_max=True)
        mae = _range_extreme(lo_tbl, entry_idx, end_c, take_max=False)
        ret = np.full(n, np.nan)
        np.divide(close[end_c], entry_price, out=ret, where=entry_price > 0)
        ret -= 1.0
        out = out.with_columns([
            pl.Series(f"return_{h}m", np.where(ok, ret, np.nan)),
            pl.Series(f"mfe_{h}m", np.where(ok, mfe / entry_price - 1.0, np.nan)),
            pl.Series(f"mae_{h}m", np.where(ok, mae / entry_price - 1.0, np.nan)),
        ])
    return out


def build_events(df: pl.DataFrame, symbol: str, category: str) -> pl.DataFrame:
    """Полный контур: признаки -> будущие доходности -> предфильтр -> события.

    Будущие доходности (return_{h}m / mfe / mae) считаются на ПОЛНОМ временном
    ряду ДО фильтра, иначе окно считалось бы по прореженному df — и таргет
    сдвинулся бы на h строк подозрительных свечей вместо h минут.
    Предфильтр применяется ПОСЛЕ, чтобы выбрать, какие строки становятся событиями.
    """
    full = _future_metrics(df)
    cand = suspicious_candles(full)
    if cand.height == 0:
        return cand
    cand = cand.with_columns([
        pl.lit(symbol).alias("symbol"),
        pl.lit(category).alias("category"),
        pl.concat_str([
            pl.col("open_time").dt.strftime("%Y%m%dT%H%M%SZ"),
            pl.lit(f"_{symbol}"),
        ]).alias("event_id"),
    ])
    # события без полного будущего окна исключаются (null)
    cand = cand.drop_nulls(subset=["entry_price"])
    return cand


def save_events(events: pl.DataFrame, name: str) -> None:
    path = EVENTS_DIR / f"{name}_events.parquet"
    events.write_parquet(path)


def load_events(name: str) -> pl.DataFrame:
    path = EVENTS_DIR / f"{name}_events.parquet"
    return pl.read_parquet(path) if path.exists() else None