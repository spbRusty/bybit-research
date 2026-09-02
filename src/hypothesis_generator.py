"""Ограниченный генератор кандидатов-гипотез (ТЗ §22-24, §38).

Автоматически создаёт feature + condition + direction + horizon.
Управление комбинаторным взрывом (§38): whitelist типов, макс. условий,
мин. частота события, дедупликация. Discovery не объявляет стратегию — только
создаёт кандидатов (передаёт их в research).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict

import polars as pl

from config.settings import load_toml
from src.registry import REGISTRY
from src.research import Hypothesis

logger = logging.getLogger(__name__)

_G = load_toml("hypothesis_generator.toml")

# Направление и горизонт по умолчанию (если не переопределено)
DEFAULT_HORIZONS = (5, 10, 30)
DEFAULT_ENTRY = "long"


@dataclass
class GenRule:
    """Правило генерации: feature + сравнение + направление + max_conditions."""
    feature_id: str
    operator: str            # gt | lt | in_range
    entry_side: str = "long"
    horizons: tuple[int, ...] = DEFAULT_HORIZONS
    max_conditions: int = 1
    min_event_share: float = 0.001   # событие не реже доли всех свечей

    def to_record(self):
        return asdict(self)


# Правила генерации (whitelist §38). Семантически осмысленные признаки.
def default_rules() -> list[GenRule]:
    return [
        GenRule("relative_volume_60", "gt", "long", (5, 10, 30)),
        GenRule("relative_range", "gt", "long", (5, 10)),
        GenRule("volume_zscore", "gt", "long", (5, 10)),
        GenRule("realized_vol_60", "gt", "long", (5, 10)),
        GenRule("volatility_regime", "in_range", "long", (5, 30)),
        GenRule("dist_rolling_high_60", "gt", "short", (5, 10)),
        GenRule("dist_rolling_low_60", "lt", "long", (5, 10)),
        GenRule("breakout_20", "gt", "long", (5, 10)),
        GenRule("roc_20", "gt", "long", (5, 10)),
        GenRule("rsi_14", "gt", "short", (5, 30)),
        GenRule("rsi_14", "lt", "long", (5, 30)),
        GenRule("corr_btc_60", "gt", "long", (5, 10)),
        GenRule("btc_trend_regime", "gt", "long", (10, 30)),
        GenRule("volatility_regime", "in_range", "short", (5, 10)),
    ]


def _threshold(feature_id: str, operator: str, df: pl.DataFrame) -> float:
    """Порог по распределению признака (процентиль, параметризован в конфиге)."""
    cfg = _G.get("thresholds", {})
    pct = cfg.get(feature_id)
    if pct is None:
        # дефолт: 90-й процентиль для gt, 10-й для lt
        pct = 0.9 if operator == "gt" else 0.1
    q = df[feature_id].drop_nulls().quantile(pct)
    return float(q)


# Пороги по умолчанию для "in_range" (режимы — категориальные, отдельно)
_THR_ABS = {
    "relative_volume_60": 2.0,
    "relative_range": 2.0,
    "volume_zscore": 2.0,
    "realized_vol_60": None,   # используется процентиль
    "dist_rolling_high_60": 0.0,
    "dist_rolling_low_60": 0.0,
    "breakout_20": 0.5,
    "roc_20": 0.0,
    "rsi_14": 70.0,
    "corr_btc_60": 0.3,
    "btc_trend_regime": 0.0,
}


def _condition_from_rule(rule: GenRule, df: pl.DataFrame) -> str:
    """Строит polars-выражение условия для правила."""
    fid = rule.feature_id
    if fid not in df.columns:
        return None
    f = pl.col(fid)
    if rule.operator == "gt":
        thr = _THR_ABS.get(fid)
        if thr is None:
            thr = _threshold(fid, "gt", df)
        return f"pl.col('{fid}') > {thr:.6g}"
    if rule.operator == "lt":
        thr = _THR_ABS.get(fid)
        if thr is None:
            thr = _threshold(fid, "lt", df)
        return f"pl.col('{fid}') < {thr:.6g}"
    if rule.operator == "in_range":
        if fid == "volatility_regime":
            # категориальный: в режиме high/very_high
            return "pl.col('volatility_regime').is_in(['high', 'very_high'])"
        # числовой диапазон: 0.1 < x < 0.9 процентиль
        lo = df[fid].drop_nulls().quantile(0.1)
        hi = df[fid].drop_nulls().quantile(0.9)
        return f"(pl.col('{fid}') > {lo:.6g}) & (pl.col('{fid}') < {hi:.6g})"
    return None


def generate_hypotheses(df: pl.DataFrame,
                        rules: list[GenRule] | None = None) -> list[Hypothesis]:
    """Генерирует ограниченное множество кандидатов по правилам (§38).

    Дедупликация: (feature, operator, entry, horizon) уникальны.
    """
    df = df  # df может быть большим; используем только для порогов
    rules = rules or default_rules()
    hyps: list[Hypothesis] = []
    seen: set[tuple] = set()
    idx = 1
    base_id = _G.get("id_prefix", "HG")
    for rule in rules:
        cond = _condition_from_rule(rule, df)
        if not cond:
            logger.debug("Пропуск правила %s: признак отсутствует или порог не вычислим",
                         rule.feature_id)
            continue
        for h in rule.horizons:
            key = (rule.feature_id, rule.operator, rule.entry_side, h)
            if key in seen:
                continue
            seen.add(key)
            hyps.append(Hypothesis(
                hypothesis_id=f"{base_id}{idx:03d}",
                description=f"{rule.feature_id} {rule.operator} -> "
                            f"{rule.entry_side} @ {h}m",
                condition=cond,
                entry_side=rule.entry_side,
                horizon_min=h,
                version="1.0",
            ))
            idx += 1
    return hyps


def filter_by_freq(hyps: list[Hypothesis], df: pl.DataFrame,
                   min_events: int | None = None) -> list[Hypothesis]:
    """Отбрасывает кандидатов с частотой события ниже порога (для cheap filter §38).

    Каждая гипотеза оценивается по discovery-подмножеству: n >= min_events.
    Здесь approximation — по полному df (для экономии). Точный фильтр в research.
    """
    min_ev = min_events or _G.get("min_events", 100)
    out = []
    for hyp in hyps:
        try:
            n = df.filter(eval(hyp.condition, {"pl": pl})).height
        except Exception:
            continue
        if n >= min_ev:
            out.append(hyp)
    return out
