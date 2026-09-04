"""Structured pipeline stages (StageResult).

Каждый этап pipeline возвращает StageResult — машинночитаемый результат
с status, metrics, errors, warnings. Это основа для:
  - формального data validation gate (STOP)
  - feature validation (QUARANTINE)
  - parameter freeze (explicit frozen marker)
  - registry lifecycle
  - structured acceptance report
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

import numpy as np
import polars as pl

from config.settings import load_toml

logger = logging.getLogger(__name__)


class StageStatus(str, Enum):
    PASS = "PASS"
    REJECT = "REJECT"
    STOP = "STOP"
    ERROR = "ERROR"


@dataclass
class StageResult:
    stage: str
    status: StageStatus
    run_id: str
    config_hash: str = ""
    data_version: str = ""
    input_version: str = ""
    output_version: str = ""
    metrics: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "stage": self.stage,
            "status": self.status.value,
            "run_id": self.run_id,
            "config_hash": self.config_hash,
            "metrics": self.metrics,
            "errors": self.errors,
            "warnings": self.warnings,
            "artifacts": self.artifacts,
        }

    @property
    def passed(self) -> bool:
        return self.status == StageStatus.PASS


def compute_config_hash(config_path: Path | None = None) -> str:
    """SHA256 hash of config file, first 12 chars."""
    p = config_path or (Path(__file__).resolve().parent.parent / "config" / "research.toml")
    return hashlib.sha256(p.read_bytes()).hexdigest()[:12]


def _make_run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


# --------------------------------------------------------------------------
# Stage: Data Validation
# --------------------------------------------------------------------------

def stage_data_validation(
    events: pl.DataFrame,
    universe: pl.DataFrame,
    config: dict | None = None,
) -> StageResult:
    """Формальная валидация данных. STOP при недостаточных данных."""
    cfg = config or load_toml("research.toml")
    run_id = _make_run_id()
    errors: list[str] = []
    warnings: list[str] = []
    metrics: dict = {}

    metrics["n_events"] = events.height
    metrics["n_symbols"] = int(events["symbol"].n_unique()) if events.height > 0 else 0

    # 1. Пустые данные
    if events.height == 0:
        errors.append("No events")
        return StageResult(
            stage="data_validation", status=StageStatus.STOP, run_id=run_id,
            config_hash=compute_config_hash(), metrics=metrics, errors=errors,
        )

    # 2. Минимальное число событий
    min_ev = cfg.get("min_events", 100)
    if events.height < min_ev:
        errors.append(f"Events {events.height} < min_events {min_ev}")
        return StageResult(
            stage="data_validation", status=StageStatus.STOP, run_id=run_id,
            config_hash=compute_config_hash(), metrics=metrics, errors=errors,
        )

    # 3. Минимальное число символов
    min_sym = cfg.get("min_unique_symbols", 5)
    n_sym = events["symbol"].n_unique()
    if n_sym < min_sym:
        errors.append(f"Symbols {n_sym} < min_unique_symbols {min_sym}")
        return StageResult(
            stage="data_validation", status=StageStatus.STOP, run_id=run_id,
            config_hash=compute_config_hash(), metrics=metrics, errors=errors,
        )

    # 4. Необходимые колонки
    required = ["open_time", "symbol", "entry_price"]
    for col in required:
        if col not in events.columns:
            errors.append(f"Missing required column: {col}")

    if errors:
        return StageResult(
            stage="data_validation", status=StageStatus.ERROR, run_id=run_id,
            config_hash=compute_config_hash(), metrics=metrics, errors=errors,
        )

    # 5. Временной диапазон
    periods = cfg.get("sample_periods", [])
    if periods and "open_time" in events.columns:
        tmin = events["open_time"].min()
        tmax = events["open_time"].max()
        metrics["date_min"] = str(tmin)
        metrics["date_max"] = str(tmax)

    # 6. Null rate
    n_rows = events.height
    if n_rows > 0:
        null_counts = events.select([pl.col(c).null_count().alias(c) for c in events.columns])
        total_cells = n_rows * len(events.columns)
        total_nulls = sum(null_counts.row(0))
        metrics["null_rate"] = round(total_nulls / total_cells, 4) if total_cells else 0.0
    else:
        metrics["null_rate"] = 0.0

    return StageResult(
        stage="data_validation", status=StageStatus.PASS, run_id=run_id,
        config_hash=compute_config_hash(), metrics=metrics,
    )


# --------------------------------------------------------------------------
# Stage: Feature Validation
# --------------------------------------------------------------------------

def stage_feature_validation(
    events: pl.DataFrame,
    required_cols: list[str] | None = None,
) -> StageResult:
    """Проверка признаков: null rates, constancy, existence."""
    run_id = _make_run_id()
    errors: list[str] = []
    warnings: list[str] = []
    metrics: dict = {}
    feature_metrics: dict = {}

    cols = required_cols or [c for c in events.columns if c not in
                            ("open_time", "symbol", "category", "entry_price")]

    for col in cols:
        if col not in events.columns:
            errors.append(f"Missing feature: {col}")
            continue
        s = events[col]
        null_pct = float(s.is_null().mean() * 100) if len(s) > 0 else 0.0
        is_const = s.n_unique() <= 1
        feature_metrics[col] = {"null_pct": round(null_pct, 2), "is_constant": is_const}
        if null_pct > 50:
            warnings.append(f"{col}: null rate {null_pct:.1f}% > 50%")
        if is_const:
            warnings.append(f"{col}: constant (unique={s.n_unique()})")

    metrics["n_features_checked"] = len(cols)
    metrics["n_features_missing"] = len(errors)
    metrics["n_features_warning"] = len(warnings)
    metrics["features"] = feature_metrics

    if errors:
        return StageResult(
            stage="feature_validation", status=StageStatus.ERROR, run_id=run_id,
            config_hash=compute_config_hash(), metrics=metrics, errors=errors,
        )

    if warnings:
        # WARN: предупреждения (высокий null rate), но не блокируют pipeline.
        # STOP зарезервирован для реальных остановок (data_validation).
        return StageResult(
            stage="feature_validation", status=StageStatus.PASS, run_id=run_id,
            config_hash=compute_config_hash(), metrics=metrics, warnings=warnings,
        )

    return StageResult(
        stage="feature_validation", status=StageStatus.PASS, run_id=run_id,
        config_hash=compute_config_hash(), metrics=metrics,
    )


# --------------------------------------------------------------------------
# Stage: Validation Gate
# --------------------------------------------------------------------------

def stage_validation_gate(
    val_result: dict,
    config: dict | None = None,
) -> StageResult:
    """Formal validation gate: mean_net > 0 AND n >= min_events AND t >= min_t_stat."""
    cfg = config or load_toml("research.toml")
    run_id = _make_run_id()
    errors: list[str] = []
    metrics: dict = {}

    n = val_result.get("n", 0)
    mean_net = val_result.get("mean_net", float("nan"))
    t_stat = val_result.get("t_stat", float("nan"))

    min_ev = cfg.get("min_events", 100)
    min_t = cfg.get("min_t_stat", 2.0)

    metrics["n"] = n
    metrics["mean_net"] = mean_net
    metrics["t_stat"] = t_stat
    metrics["min_events"] = min_ev
    metrics["min_t_stat"] = min_t

    if n < min_ev:
        errors.append(f"n={n} < min_events={min_ev}")
    if mean_net <= 0:
        errors.append(f"mean_net={mean_net} <= 0")
    if np.isnan(t_stat) or t_stat < min_t:
        errors.append(f"t_stat={t_stat} < min_t_stat={min_t}")

    status = StageStatus.PASS if not errors else StageStatus.REJECT
    return StageResult(
        stage="validation_gate", status=status, run_id=run_id,
        config_hash=compute_config_hash(), metrics=metrics, errors=errors,
    )


# --------------------------------------------------------------------------
# Stage: OOS Gate
# --------------------------------------------------------------------------

def stage_oos_gate(
    oos_result: dict,
    config: dict | None = None,
) -> StageResult:
    """Formal OOS gate: mean_net > 0 AND n >= min_events AND t >= min_t_stat."""
    cfg = config or load_toml("research.toml")
    run_id = _make_run_id()
    errors: list[str] = []
    metrics: dict = {}

    n = oos_result.get("n", 0)
    mean_net = oos_result.get("mean_net", float("nan"))
    t_stat = oos_result.get("t_stat", float("nan"))

    min_ev = cfg.get("min_events", 100)
    min_t = cfg.get("min_t_stat", 2.0)

    metrics["n"] = n
    metrics["mean_net"] = mean_net
    metrics["t_stat"] = t_stat
    metrics["min_events"] = min_ev
    metrics["min_t_stat"] = min_t

    if n < min_ev:
        errors.append(f"n={n} < min_events={min_ev}")
    if mean_net <= 0:
        errors.append(f"mean_net={mean_net} <= 0")
    if np.isnan(t_stat) or t_stat < min_t:
        errors.append(f"t_stat={t_stat} < min_t_stat={min_t}")

    status = StageStatus.PASS if not errors else StageStatus.REJECT
    return StageResult(
        stage="oos_gate", status=status, run_id=run_id,
        config_hash=compute_config_hash(), metrics=metrics, errors=errors,
    )


# --------------------------------------------------------------------------
# Stage: Critic (wraps existing critic.review)
# --------------------------------------------------------------------------

def stage_critic(
    result: dict,
    events: pl.DataFrame,
) -> StageResult:
    """Wraps existing critic.review(). Returns PASS or REJECT."""
    run_id = _make_run_id()
    try:
        from src.critic import review
        verdict = review(result, events)
        status = StageStatus.PASS if verdict.passed else StageStatus.REJECT
        metrics = {"passed": verdict.passed, "results": verdict.results}
        errors = [] if verdict.passed else [verdict.fail_reason or "Critic REJECT"]
        return StageResult(
            stage="critic", status=status, run_id=run_id,
            config_hash=compute_config_hash(), metrics=metrics, errors=errors,
        )
    except Exception as e:
        return StageResult(
            stage="critic", status=StageStatus.ERROR, run_id=run_id,
            config_hash=compute_config_hash(), errors=[str(e)],
        )


# --------------------------------------------------------------------------
# Stage: Parameter Freeze
# --------------------------------------------------------------------------

def freeze_finalist(result: dict) -> dict:
    """Snapshot finalist params after discovery. Returns frozen config."""
    finalist = result.get("finalist")
    if not finalist:
        return {}
    frozen = {
        "hypothesis_id": finalist["hypothesis_id"],
        "condition": finalist["condition"],
        "entry_side": finalist["entry_side"],
        "horizon_min": finalist["horizon_min"],
        "description": finalist.get("description", ""),
        "frozen_hash": hashlib.sha256(
            json.dumps(finalist, sort_keys=True, default=str).encode()
        ).hexdigest()[:12],
    }
    return frozen


def stage_parameter_freeze(
    result: dict,
    frozen: dict,
) -> StageResult:
    """Verify that validation/OOS used identical finalist params as discovery."""
    run_id = _make_run_id()
    errors: list[str] = []
    metrics: dict = {}

    finalist = result.get("finalist")
    if not finalist:
        return StageResult(
            stage="parameter_freeze", status=StageStatus.PASS, run_id=run_id,
            config_hash=compute_config_hash(),
            metrics={"skipped": True, "reason": "no finalist"},
        )

    current_hash = hashlib.sha256(
        json.dumps(finalist, sort_keys=True, default=str).encode()
    ).hexdigest()[:12]
    frozen_hash = frozen.get("frozen_hash", "")

    metrics["frozen_hash"] = frozen_hash
    metrics["current_hash"] = current_hash
    metrics["match"] = current_hash == frozen_hash

    if current_hash != frozen_hash:
        errors.append(
            f"Parameter drift: frozen={frozen_hash} current={current_hash}"
        )

    status = StageStatus.PASS if not errors else StageStatus.REJECT
    return StageResult(
        stage="parameter_freeze", status=status, run_id=run_id,
        config_hash=compute_config_hash(), metrics=metrics, errors=errors,
    )


# --------------------------------------------------------------------------
# Structured acceptance report
# --------------------------------------------------------------------------

def build_acceptance_report(
    stages: list[StageResult],
    result: dict,
    run_id: str,
) -> dict:
    verdict = "PASS"
    reject_reasons = []
    for s in stages:
        if not s.passed:
            verdict = s.status.value
            reject_reasons.extend(s.errors)

    return {
        "run_id": run_id,
        "timestamp": datetime.utcnow().isoformat(),
        "verdict": verdict,
        "config_hash": compute_config_hash(),
        "provenance": result.get("provenance", {}),
        "stages": [s.to_dict() for s in stages],
        "candidates": result.get("candidates", []),
        "finalist": result.get("finalist"),
        "reject_reasons": reject_reasons,
        "n_hypotheses": result.get("n_hypotheses", 0),
        "n_events_total": result.get("n_events_total", 0),
    }
