"""Основной конвейер (ТЗ §43) / Главный контур построения.

ликвидность -> волатильность ГК -> признаки -> подозрительные свечи -> события
-> будущие доходности -> research (БХ) -> Critic -> paper (если прошёл) -> ntfy.

Данные свечей уже скачаны (bybit_rs). Запуск:
  .venv/bin/python -m src.main [--limit N] [--category linear|spot]
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import LOGS_DIR, load_toml
from src import data as data_mod
from src import critic as critic_mod
from src import data as data_mod
from src import events as events_mod
from src import features as features_mod
from src import features_breadth as breadth_mod
from src import hypothesis_generator as hgen_mod
from src import notify as notify_mod
from src import paper as paper_mod
from src import research as research_mod
from src import volatility as vol_mod

logger = logging.getLogger("построение")

_FEAT = load_toml("features.toml")
_R = load_toml("research.toml")
OOS_START = datetime.fromisoformat(
    next(p[1] for p in _R["sample_periods"] if p[0] == "oos"))


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(module)s %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8")])


def load_btc() -> pl.DataFrame | None:
    """BTC — фактор рынка (§25); свечи linear BTCUSDT."""
    try:
        df = data_mod.load_validated(_FEAT["btc_symbol"], "linear")
        if df is None:
            logger.warning("BTCUSDT не найден — BTC-признаки пропущены")
        return df
    except Exception as e:
        logger.warning("Ошибка загрузки BTC: %s", e)
        return None


def load_eth() -> pl.DataFrame | None:
    """ETH — второй фактор рынка (§16); свечи linear ETHUSDT."""
    try:
        df = data_mod.load_validated(_FEAT["eth_symbol"], "linear")
        if df is None:
            logger.warning("ETHUSDT не найден — ETH-признаки пропущены")
        return df
    except Exception as e:
        logger.warning("Ошибка загрузки ETH: %s", e)
        return None


def main(limit: int | None = None, category: str | None = None) -> int:
    setup_logging()
    t0 = datetime.utcnow()
    logger.info("=== ПОСТРОЕНИЕ: конвейер старт (%s) ===", t0.isoformat())

    # 1. Фильтр ликвидности (§8)
    universe = data_mod.liquidity_universe()
    if category:
        universe = universe.filter(pl.col("category") == category)
    if limit:
        universe = universe.head(limit)
    logger.info("Юниверс: %d символов", universe.height)

    # 2. Данные + признаки + волатильность
    btc = load_btc()
    eth = load_eth()
    # грузим свечи юниверса один раз (нужны и для breadth §17)
    univ_dfs = data_mod.load_universe_data(universe)
    breadth = breadth_mod.compute_breadth(univ_dfs)
    all_events = []
    n_loaded = 0
    for row in universe.iter_rows(named=True):
        df = univ_dfs.get((row["symbol"], row["category"]))
        if df is None:
            continue
        df = features_mod.add_features(df, btc, eth, market_symbol=row["symbol"],
                                       breadth=breadth)
        # волатильность Гармана–Класса, 30 дней (§9) — для стопов
        gk = vol_mod.rolling_gk(df, 30).select(["date", "vol_gk_30d"])
        df = df.join(gk, left_on=pl.col("open_time").dt.date(), right_on="date", how="left")
        ev = events_mod.build_events(df, row["symbol"], row["category"])
        if ev.height:
            all_events.append(ev)
        n_loaded += 1
        if n_loaded % 25 == 0:
            logger.info("обработано символов: %d, событий накоплено: %d",
                        n_loaded, sum(e.height for e in all_events))

    if not all_events:
        logger.error("Нет событий — проверьте данные/юниверс")
        return 1
    events = pl.concat(all_events)
    events_mod.save_events(events, "all")
    logger.info("Событий всего: %d", events.height)

    # 3. Исследование (§30-34): baseline H001-H008 + автоматически сгенерированные (§22)
    baseline = research_mod.HYPOTHESES
    generated = hgen_mod.generate_hypotheses(events)
    generated = hgen_mod.filter_by_freq(generated, events, _R["min_events"])
    all_hyp = list(baseline) + generated
    logger.info("Гипотез: baseline=%d + generator=%d = %d",
                len(baseline), len(generated), len(all_hyp))
    result = research_mod.run_research(events, hypotheses=all_hyp)
    rid = research_mod.save_result(result)
    logger.info("RESEARCH %s: кандидаты=%s финалист=%s вердикт=%s",
                rid, result["candidates"], (result.get("finalist") or {}).get("hypothesis_id"),
                result["verdict"])

    # 4. Critic (§32) — реальные проверки по events
    verdict = critic_mod.review(result, events)
    critic_mod.save_report(result, verdict)
    logger.info("CRITIC: %s (%s)", "PASS" if verdict.passed else "REJECT", verdict.fail_reason)

    # 5. Бумажная торговля (§35-41), только если Critic прошёл
    paper = None
    if verdict.passed and result.get("finalist"):
        oos_ms = int(OOS_START.timestamp() * 1000)
        oos_events = events.filter(pl.col("open_time").dt.epoch("ms") >= oos_ms)
        paper = paper_mod.paper_run(oos_events, result["finalist"])
        logger.info("PAPER: сделок=%d net=%.2f DD=%.2f%%",
                    paper["n_trades"], paper["net_pnl"], paper["max_drawdown"] * 100)
        critic_mod.save_report(result, verdict, paper)

    # 6. Уведомления (§42)
    notify_mod.notify_research(result, verdict.passed, paper)

    dt = (datetime.utcnow() - t0).total_seconds()
    logger.info("=== ГОТОВО за %.1f мин: результат=%s critic=%s paper=%s ===",
                dt / 60, result["verdict"],
                "PASS" if verdict.passed else "REJECT",
                f"{paper['n_trades']} сделок" if paper else "—")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Пайплайн построения исследовательской торговой системы")
    ap.add_argument("--limit", type=int, default=None, help="ограничить число символов (для смоука)")
    ap.add_argument("--category", type=str, default=None, choices=["linear", "spot"])
    args = ap.parse_args()
    sys.exit(main(limit=args.limit, category=args.category))