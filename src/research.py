"""Исследовательский контур (ТЗ §30-31, §33-34).

Гипотезы H001..: условие на признаки события + направление + горизонт.
Проверка: mean будущей доходности после издержек, t-статистика (scipy),
поправка Бенджамини–Хохберга при множественном тестировании (§31),
разделение выборок discovery/validation/oos (§33), сетка издержек (§34).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime

import numpy as np
import polars as pl
from scipy import stats

from config.settings import RESULTS_DIR, HYPOTHESES_DIR
from config.settings import load_toml

logger = logging.getLogger(__name__)

_R = load_toml("research.toml")

D_v2 = datetime.fromisoformat


# --------------------------------------------------------------------------
# Гипотезы (§30)
# --------------------------------------------------------------------------

@dataclass
class Hypothesis:
    hypothesis_id: str
    description: str
    condition: str           # polars-выражение над колонками события
    entry_side: str          # long | short
    horizon_min: int
    target_column: str = None  # колонка будущей доходности, по умолчанию return_{h}m
    version: str = "1.0"
    status: str = "CANDIDATE"
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def __post_init__(self):
        if self.target_column is None:
            self.target_column = f"return_{self.horizon_min}m"

    def to_record(self) -> dict:
        return asdict(self)


HYPOTHESES: list[Hypothesis] = [
    Hypothesis("H001", "Всплеск объёма + зелёная свеча -> продолжение вверх",
               "(pl.col('relative_volume') > 3.0) & pl.col('is_green')",
               "long", 5),
    Hypothesis("H002", "Всплеск объёма + красная свеча -> продолжение вниз",
               "(pl.col('relative_volume') > 3.0) & ~pl.col('is_green')",
               "short", 5),
    Hypothesis("H003", "Всплеск диапазона -> импульс продолжается",
               "pl.col('relative_range') > 3.0", "long", 10),
    Hypothesis("H004", "Всплеск диапазона + верхняя тень -> откат вниз",
               "(pl.col('relative_range') > 3.0) & (pl.col('upper_wick') > 0.002)",
               "short", 15),
    Hypothesis("H005", "Всплеск объёма в американскую сессию (14-21 UTC)",
               "(pl.col('relative_volume') > 3.0) & pl.col('hour_utc').is_between(14, 21)",
               "long", 5),
    Hypothesis("H006", "Азия (0-6 UTC): тихий объём -> восстановление",
               "(pl.col('relative_volume') < 0.7) & pl.col('hour_utc').is_between(0, 6)",
               "long", 30),
    Hypothesis("H007", "Всплеск объёма + большая верхняя тень -> разворот вниз",
               "(pl.col('relative_volume') > 2.0) & (pl.col('upper_wick') > 0.003)",
               "short", 10),
    Hypothesis("H008", "Всплеск объёма + большая нижняя тень -> разворот вверх",
               "(pl.col('relative_volume') > 2.0) & (pl.col('lower_wick') > 0.003)",
               "long", 10),
]


# --------------------------------------------------------------------------
# Статистика (§31)
# --------------------------------------------------------------------------

def test_hypothesis(events: pl.DataFrame, hyp: Hypothesis, cost: float) -> dict:
    """События -> подвыборка по условию -> метрики net-доходности.

    net_ret = side * return_{h}m - cost (круговой оборот).
    """
    base = {"n": 0, "n_symbols": 0, "n_months": 0, "t_stat": np.nan,
            "p_value": np.nan, "mean_net": np.nan, "median_net": np.nan,
            "winrate": np.nan, "ev_annualized": np.nan, "error": ""}
    try:
        cond = eval(hyp.condition, {"pl": pl})
    except Exception as e:
        base["error"] = f"condition: {e}"
        return base
    sub = events.filter(cond).filter(
        pl.col(hyp.target_column).is_not_null() &
        pl.col("entry_price").is_not_null())
    n = sub.height
    if n == 0:
        return base
    side = 1.0 if hyp.entry_side == "long" else -1.0
    ret: np.ndarray = sub[hyp.target_column].to_numpy() * side - cost
    t, p = stats.ttest_1samp(ret, 0.0)
    return {**base,
            "n": n,
            "n_symbols": int(sub["symbol"].n_unique()),
            "n_months": int(sub["open_time"].dt.strftime("%Y-%m").n_unique()),
            "t_stat": float(t), "p_value": float(p),
            "mean_net": float(ret.mean()), "median_net": float(np.median(ret)),
            "winrate": float((ret > 0).mean()),
            "ev_annualized": float(ret.mean()) * 1440 * 365 / hyp.horizon_min}


def benjamini_hochberg(p_values: np.ndarray, q: float) -> np.ndarray:
    """Поправка БХ (§31): булев массив значимых при q (контроль FDR)."""
    p = np.asarray(p_values, dtype=float)
    m = p.size
    if m == 0:
        return np.zeros(0, dtype=bool)
    order = np.argsort(p)
    ranked = p[order] * m / (np.arange(1, m + 1))
    k = 0
    for i in range(m - 1, -1, -1):
        if ranked[i] <= q:
            k = i + 1
            break
    sig = np.zeros(m, dtype=bool)
    sig[order[:k]] = True
    return sig


# --------------------------------------------------------------------------
# Прогон (§33-34)
# --------------------------------------------------------------------------

def split_periods(events: pl.DataFrame) -> dict[str, pl.DataFrame]:
    return {
        name: events.filter(
            (pl.col("open_time") >= D_v2(start)) & (pl.col("open_time") < D_v2(end)))
        for name, start, end in _R["sample_periods"]
    }


def run_research(events: pl.DataFrame,
                 q: float | None = None,
                 cost_survival: float | None = None) -> dict:
    """Полный прогон: discovery sweep -> БХ -> validation -> oos -> результат."""
    q = q or _R["bh_q"]
    cost_survival = cost_survival or _R["survival_cost"]
    periods = split_periods(events)
    disc, val, oos = periods["discovery"], periods["validation"], periods["oos"]

    rows = []
    for hyp in HYPOTHESES:
        m = test_hypothesis(disc, hyp, cost_survival)
        m.update({"hypothesis_id": hyp.hypothesis_id, "description": hyp.description,
                  "entry_side": hyp.entry_side, "horizon_min": hyp.horizon_min})
        rows.append(m)
    df = pl.DataFrame(rows).sort("t_stat", descending=True)

    p_arr = np.nan_to_num(df["p_value"].to_numpy(), nan=1.0)
    sig_bh = benjamini_hochberg(p_arr, q)

    mask = (sig_bh &
            (df["n"].to_numpy() >= _R["min_events"]) &
            (df["n_symbols"].to_numpy() >= _R["min_unique_symbols"]) &
            (df["n_months"].to_numpy() >= _R["min_months"]) &
            (df["t_stat"].to_numpy() >= _R["min_t_stat"]) &
            (df["mean_net"].to_numpy() > 0))
    candidates = df.filter(pl.Series(mask))

    out = {
        "created_at": datetime.utcnow().isoformat(),
        "q_bh": q, "cost_survival": cost_survival, "n_hypotheses": len(HYPOTHESES),
        "n_events_total": events.height,
        "n_events": {k: v.height for k, v in periods.items()},
        "discovery_results": df.to_dicts(),
        "candidates": candidates.select("hypothesis_id").to_series().to_list() if candidates.height else [],
        "validation": {}, "oos": {}, "verdict": "NO_CANDIDATE",
    }

    for cid in out["candidates"]:
        hyp = next(h for h in HYPOTHESES if h.hypothesis_id == cid)
        mv = test_hypothesis(val, hyp, cost_survival)
        mo = test_hypothesis(oos, hyp, cost_survival)
        out["validation"][cid] = mv
        out["oos"][cid] = mo
        if mv.get("mean_net", np.nan) > 0 and mo.get("mean_net", np.nan) > 0 \
                and mo.get("n", 0) >= _R["min_events"]:
            out["verdict"] = "CANDIDATE"
            out["finalist"] = {"hypothesis_id": cid, "horizon_min": hyp.horizon_min,
                               "entry_side": hyp.entry_side, "condition": hyp.condition,
                               "description": hyp.description}
    return out


def save_result(result: dict) -> str:
    rid = f"research_{datetime.utcnow():%Y%m%dT%H%M%SZ}"
    (RESULTS_DIR / f"{rid}.json").write_text(json.dumps(result, indent=2, ensure_ascii=False))
    (HYPOTHESES_DIR / "hypotheses_v1.json").write_text(
        json.dumps([h.to_record() for h in HYPOTHESES], indent=2, ensure_ascii=False))
    return rid


def load_latest_result() -> dict | None:
    files = sorted(RESULTS_DIR.glob("research_*.json"))
    return json.loads(files[-1].read_text()) if files else None