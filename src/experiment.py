"""Experiment — единица работы Research Controller (docs/research_controller_design.md §1).

Неизменяемая запись эксперимента плюс чистые builder'ы, которые превращают
оси поиска (§2) в первоклассные `research.Hypothesis`. Никакого I/O и никакой
зависимости от pipeline — это позволяет тестировать выбор контроллера без данных.

`hypothesis_specs=None` означает «не инжектировать»: pipeline строит гипотезы
сам (baseline + generator + mr). Так делаются семейства `baseline/generator/ob`,
которым для порогов нужны фактические события.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

MODES = (
    "BASELINE",
    "THRESHOLD_SWEEP",
    "HORIZON_SWEEP",
    "REGIME_SWEEP",
    "CONDITIONAL",
    "FAMILY_SWITCH",
)

# Поля Hypothesis, участвующие в config_hash и в персистентной записи спека.
# created_at/status исключены намеренно: иначе хеш и дедупликация недетерминированы.
_SPEC_FIELDS = (
    "hypothesis_id",
    "condition",
    "entry_side",
    "horizon_min",
    "target_column",
    "stop_loss",
    "version",
)


def utc_now_iso(now: datetime | None = None) -> str:
    return (now or datetime.now(tz=timezone.utc)).isoformat()


def spec_record(hyp) -> dict:
    """Детерминированная запись Hypothesis для хеша/персистентности."""
    return {f: getattr(hyp, f) for f in _SPEC_FIELDS}


def compute_experiment_hash(mode: str, parameters: dict,
                            hypothesis_specs: list[dict]) -> str:
    """sha256 от mode + sorted(parameters) + hypothesis_specs, БЕЗ результатов.

    Результаты (discovery/validation/OOS) не участвуют: повторный расчёт того же
    спека даёт тот же хеш, а ledger дедуплицирует конфигурации (§4). OOS в выборе
    следующего эксперимента не участвует (инвариант 6/7).
    """
    payload = {
        "mode": mode,
        "parameters": parameters,
        "hypothesis_specs": hypothesis_specs,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def make_experiment_id(existing_ids, now: datetime | None = None) -> str:
    ts = (now or datetime.now(tz=timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    base = f"EXP_{ts}"
    if base not in existing_ids:
        return base
    i = 1
    while f"{base}_{i}" in existing_ids:
        i += 1
    return f"{base}_{i}"


# --------------------------------------------------------------------------
# Experiment record (§1)
# --------------------------------------------------------------------------

@dataclass
class Experiment:
    experiment_id: str
    parent_experiment_id: str | None
    cycle_id: str
    config_hash: str
    mode: str
    parameters: dict
    hypothesis_specs: list[dict]
    created_at_utc: str
    dataset_boundary: dict
    status: str = "RUNNING"          # RUNNING|DONE|FAILED|SKIPPED
    discovery_result: dict | None = None
    validation_result: dict | None = None
    oos_result: dict | None = None
    critic_result: dict | None = None
    final_verdict: str | None = None
    reject_diagnosis: dict | None = None
    acceptance_report: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "Experiment":
        fields = {f for f in Experiment.__dataclass_fields__}
        return Experiment(**{k: v for k, v in d.items() if k in fields})


# --------------------------------------------------------------------------
# Оси поиска -> Hypothesis (§2)
# --------------------------------------------------------------------------

def condition_for(column: str, operator: str, threshold: float) -> str:
    """Условие признака (НЕ приёмки). Поддерживаются gt/lt из дискретных сеток."""
    if operator == "gt":
        return f"pl.col('{column}') > {threshold:.6g}"
    if operator == "lt":
        return f"pl.col('{column}') < {threshold:.6g}"
    raise ValueError(f"Unsupported operator for sweep: {operator}")


def mr_hypotheses(thresholds: list[float], horizons: list[int]) -> list:
    """MR-семья: close/SMA120-1 < thr + green -> long (единый источник с orchestrator)."""
    from src.research import Hypothesis

    hyps = []
    for thr in thresholds:
        for h in horizons:
            hyps.append(Hypothesis(
                hypothesis_id=f"MR_SMA120_t{abs(thr) * 100:.0f}p_h{h}",
                description=f"MR: close/SMA120-1 < {thr} + green -> long {h}m",
                condition=f"(pl.col('mr_sma120') < {thr}) & pl.col('is_green')",
                entry_side="long",
                horizon_min=h,
            ))
    return hyps


def _slug(text: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in text).strip("_")


def build_hypothesis_specs(mode: str, parameters: dict) -> list | None:
    """Строит список Hypothesis для режима/осей. None => отдать pipeline штатную генерацию.

    Sweep-режимы тестируют объявленную сетку по одному признаку (условие), не
    трогая приёмочные пороги. FAMILY_SWITCH=mr строится явно (данные не нужны),
    остальные семейства собирает pipeline (им нужны фактические события для порогов).
    """
    if mode == "BASELINE":
        return None
    if mode == "FAMILY_SWITCH":
        if parameters.get("family") == "mr":
            return mr_hypotheses(parameters.get("thresholds", []),
                                 parameters.get("horizons", [5, 10, 30]))
        return None

    from src.research import Hypothesis

    column = parameters["column"]
    cond = condition_for(column, parameters["operator"], parameters["threshold"])
    side = parameters["entry_side"]

    if mode == "REGIME_SWEEP":
        cond = f"({cond}) & ({parameters['regime_condition']})"
    elif mode == "CONDITIONAL":
        cond = f"({cond}) & ({parameters['second_condition']})"
    elif mode not in ("THRESHOLD_SWEEP", "HORIZON_SWEEP"):
        raise ValueError(f"Unknown mode: {mode}")

    if mode == "HORIZON_SWEEP":
        horizons = [parameters["horizon"]]
    else:
        horizons = list(parameters["horizons"])

    tag = _slug(mode)
    hyps = []
    for h in horizons:
        hyps.append(Hypothesis(
            hypothesis_id=f"RC_{tag}_{_slug(column)}_{h}",
            description=f"[{mode}] {cond} -> {side} @ {h}m",
            condition=cond,
            entry_side=side,
            horizon_min=h,
        ))
    return hyps
