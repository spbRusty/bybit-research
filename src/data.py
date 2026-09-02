"""Данные (ТЗ §6-8, §45-46): загрузка сырых свечей, проверка качества, фильтр ликвидности.

Сырые свечи уже скачаны Rust-загрузчиком bybit_rs в parquet
(data/klines/{linear,spot}/*_1m.parquet). Этот модуль их читает, проверяет
и строит юниверс по фильтру ликвидности.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import polars as pl
from pathlib import Path

from config.settings import RAW_KLINES_DIR, CLEAN_CANDLES, DATA
from config.settings import load_toml

logger = logging.getLogger(__name__)

REQUIRED_COLS = ["open_time", "open", "high", "low", "close", "volume", "turnover", "is_green"]
GAP_MIN_MS = 3 * 60_000          # пропуск > 3 минут
STALE_DAYS = 7                   # символ без свечей последние 7 дней — мёртвый

_MKT = load_toml("market.toml")
_LIQ = load_toml("liquidity.toml")


def find_files() -> list[Path]:
    out = []
    for cat in _MKT["categories"]:
        d = RAW_KLINES_DIR / cat
        if d.exists():
            out += sorted(d.glob("*_1m.parquet"))
    return out


def read_raw(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(path)
    keep = [c for c in REQUIRED_COLS if c in df.columns]
    return df.select(keep).sort("open_time")


# --------------------------------------------------------------------------
# Проверка качества (ТЗ §46)
# --------------------------------------------------------------------------

def validate_candles(df: pl.DataFrame, symbol: str) -> tuple[list[str], list[str]]:
    """Детерминированные проверки. (fatal, warnings) — дыры суть warning:
    ремонт данных на паузе, юниверс строится по лучшему доступному."""
    fatal: list[str] = []
    warnings: list[str] = []
    n = df.height
    if n == 0:
        return [f"{symbol}: пустой файл"], []
    # дубли
    dups = df.filter(pl.col("open_time").is_duplicated()).height
    if dups:
        fatal.append(f"{symbol}: дубли open_time={dups}")
    # сортировка
    t = df["open_time"].to_numpy()
    if not (t[:-1] <= t[1:]).all():
        fatal.append(f"{symbol}: нарушена сортировка")
    # negative / high<low / open-close вне диапазона
    if df.filter((pl.col("high") < pl.col("low")) |
                 (pl.col("open") < pl.col("low")) | (pl.col("open") > pl.col("high")) |
                 (pl.col("close") < pl.col("low")) | (pl.col("close") > pl.col("high")) |
                 (pl.col("volume") < 0) | (pl.col("turnover") < 0)).height:
        fatal.append(f"{symbol}: некорректные цены/объёмы")
    # пропуски
    tms = t.astype("datetime64[ms]").astype("int64")
    gaps = tms[1:] - tms[:-1]
    big = int((gaps > GAP_MIN_MS).sum())
    if big:
        warnings.append(f"{symbol}: пропусков > {GAP_MIN_MS/60_000:.0f} мин = {big}")
    # свежесть
    last_dt = datetime.fromtimestamp(tms[-1] / 1000, tz=timezone.utc)
    if (datetime.now(timezone.utc) - last_dt).days > STALE_DAYS:
        warnings.append(f"{symbol}: данные не свежие (последняя свеча старше {STALE_DAYS} дн)")
    return fatal, warnings


def load_validated(symbol: str, category: str) -> pl.DataFrame | None:
    """Файл -> проверенный DataFrame (None только при фатальных проблемах)."""
    path = RAW_KLINES_DIR / category / f"{symbol}_{category}_1m.parquet"
    if not path.exists():
        return None
    df = read_raw(path)
    fatal, warnings = validate_candles(df, f"{symbol}_{category}")
    for w in warnings:
        logger.warning(w)
    if fatal:
        logger.warning(" | ".join(fatal))
        return None
    return df


# --------------------------------------------------------------------------
# Фильтр ликвидности (ТЗ §8)
# --------------------------------------------------------------------------

def turnover_by_day() -> pl.DataFrame:
    """Суточный оборот USD по каждому символу за всё время (для фильтра)."""
    rows = []
    for path in find_files():
        df = pl.read_parquet(path, columns=["open_time", "turnover"])
        sym, cat = _symbol_category(path)
        day = (df.with_columns(pl.col("open_time").dt.date().alias("d"))
                 .group_by("d").agg(pl.col("turnover").sum().alias("turnover"))
                 .with_columns(pl.lit(sym).alias("symbol"), pl.lit(cat).alias("category")))
        rows.append(day)
    if not rows:
        raise RuntimeError("Нет файлов свечей")
    return pl.concat(rows)


def _symbol_category(path: Path) -> tuple[str, str]:
    name = path.name
    for cat in _MKT["categories"]:
        suffix = f"_{cat}_1m.parquet"
        if name.endswith(suffix):
            return name[: -len(suffix)], cat
    return name.removesuffix("_1m.parquet"), ""


def liquidity_universe(verbose: bool = True) -> pl.DataFrame:
    """Символы, прошедшие фильтр ликвидности (§8):
    устойчивость = дни>=порога / всего дней x 100 >= min_stability_pct."""
    window = _LIQ["window_days"]
    thr = _LIQ["threshold_usd"]
    stab = _LIQ["min_stability_pct"]
    min_turn = _LIQ["min_turnover_30d_usd"]

    day = turnover_by_day()
    # только последние window дней относительно конца данных
    ref_end = day["d"].max()
    day30 = day.filter(pl.col("d") > ref_end - pl.duration(days=window))
    stats = (day30.group_by(["symbol", "category"])
                  .agg(pl.len().alias("days_checked"),
                       (pl.col("turnover") >= thr).sum().alias("days_above"),
                       pl.col("turnover").mean().alias("avg_daily"),
                       pl.col("turnover").median().alias("median_daily"),
                       pl.col("turnover").sum().alias("turnover_30d"),
                       pl.col("d").max().alias("last_day")))
    stats = stats.with_columns(
        (pl.col("days_above") / pl.col("days_checked") * 100).alias("stability_pct"))
    ok = stats.filter((pl.col("stability_pct") >= stab) &
                      (pl.col("turnover_30d") >= min_turn))
    # свежесть: мёртвые символы отсекаем (хвост старше STALE_DAYS)
    age_days = (ref_end - pl.col("last_day")).dt.total_days()
    ok = ok.with_columns(age_days.alias("age_days")).filter(pl.col("age_days") <= STALE_DAYS)
    univ = ok.sort("turnover_30d", descending=True)
    if verbose:
        logger.info("Юниверс ликвидности: %d символов из %d "
                    "(окно=%dd порог=$%.0fM устойчивость>=%.0f%%)",
                    univ.height, day.group_by(["symbol", "category"]).len().height,
                    window, thr / 1e6, stab)
    return univ


def load_universe_data(universe: pl.DataFrame) -> dict[tuple[str, str], pl.DataFrame]:
    """Читает и валидирует свечи для символов юниверса."""
    out = {}
    for row in universe.iter_rows(named=True):
        df = load_validated(row["symbol"], row["category"])
        if df is not None:
            out[(row["symbol"], row["category"])] = df
    return out