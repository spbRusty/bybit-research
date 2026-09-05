"""Candle trigger: оценивает признаки свечи и создаёт триггеры для захвата orderbook.

Приложение A (ТЗ §3): candle trigger → candidate → orderbook capture.
Каждый триггер записывается в JSON-файл в watches-директорию, которую
наблюдает Rust-collector. Файл содержит event_id, symbol, параметры триггера,
длительность захвата — всю необходимую информацию для полной провенанс-цепочки.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from config.settings import load_toml, MARKET_DATA_DIR

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

_CFG = load_toml("orderbook_capture.toml")
_TRIGGERS_DIR = MARKET_DATA_DIR.parent / "triggers"
_TRIGGERS_DIR.mkdir(parents=True, exist_ok=True)


def _config_hash() -> str:
    """SHA-256 хеш файла конфигурации триггера (версионирование)."""
    raw = json.dumps(_CFG, sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()[:12]


CONFIG_VERSION = _CFG.get("trigger_version", "1.0")
CONFIG_HASH = _config_hash()


# ---------------------------------------------------------------------------
# Trigger rule evaluation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TriggerRule:
    """Одно правило candle trigger (из [[rules]] в orderbook_capture.toml)."""
    feature: str
    operator: str        # gt, lt, ge, le, eq
    threshold: float
    horizons: list[int] = field(default_factory=list)

    def evaluate(self, value: float) -> bool:
        match self.operator:
            case "gt": return value > self.threshold
            case "lt": return value < self.threshold
            case "ge": return value >= self.threshold
            case "le": return value <= self.threshold
            case "eq": return abs(value - self.threshold) < 1e-9
            case _: return False


def _load_rules() -> list[TriggerRule]:
    """Загрузка правил из конфига."""
    rules = []
    for r in _CFG.get("rules", []):
        rules.append(TriggerRule(
            feature=r["feature"],
            operator=r.get("operator", "gt"),
            threshold=r.get("threshold", 3.0),
            horizons=r.get("horizons", []),
        ))
    return rules


RULES = _load_rules()


# ---------------------------------------------------------------------------
# Cooldown tracking
# ---------------------------------------------------------------------------

class CooldownTracker:
    """Отслеживает cooldown между триггерами для одного символа.

    Cooldown хранится в памяти (не в файле) — при рестарте процесса
    cooldown сбрасывается. Это допустимо для research-системы.
    """
    def __init__(self, cooldown_sec: int | None = None):
        self._cooldown = cooldown_sec or _CFG.get("cooldown_sec", 300)
        self._last_trigger: dict[str, float] = {}

    def can_trigger(self, symbol: str) -> bool:
        last = self._last_trigger.get(symbol, 0)
        return (time.time() - last) >= self._cooldown

    def record(self, symbol: str) -> None:
        self._last_trigger[symbol] = time.time()


# ---------------------------------------------------------------------------
# Trigger file creation
# ---------------------------------------------------------------------------

def _make_event_id(symbol: str, ts: datetime) -> str:
    """Event ID: YYYYMMDDTHHmmssZ_{SYMBOL}_{config_hash}."""
    ts_str = ts.strftime("%Y%m%dT%H%M%SZ")
    short_hash = CONFIG_HASH[:6]
    return f"{ts_str}_{symbol}_{short_hash}"


@dataclass
class TriggerFile:
    """Содержимое JSON-файла триггера для Rust-collector."""
    event_id: str
    symbol: str
    category: str
    trigger_type: str = "candle_features"
    trigger_version: str = CONFIG_VERSION
    trigger_config_hash: str = CONFIG_HASH
    trigger_params: dict = field(default_factory=dict)
    horizons: list[int] = field(default_factory=list)
    capture_duration_sec: int = 1200
    created_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())

    def write(self, triggers_dir: Path | None = None) -> Path:
        """Записать JSON-файл триггера в watches-директорию."""
        d = triggers_dir or _TRIGGERS_DIR
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{self.event_id}.json"
        path.write_text(json.dumps(asdict(self), indent=2, ensure_ascii=False))
        return path


def evaluate_trigger(
    df: pl.DataFrame,
    symbol: str,
    category: str,
    cooldown: CooldownTracker | None = None,
) -> TriggerFile | None:
    """Оценить признаки свечи и создать триггер если условия выполнены.

    Args:
        df: DataFrame с вычисленными признаками (relative_volume, relative_range и т.д.)
            Должен содержать хотя бы одну строку (последняя свеча).
        symbol: Торговый символ (e.g. "BTCUSDT")
        category: Категория (e.g. "linear")
        cooldown: Трекер cooldown (создаётся автоматически если None)

    Returns:
        TriggerFile если триггер сработал, None если нет.
    """
    if df.height == 0:
        return None

    cd = cooldown or CooldownTracker()
    if not cd.can_trigger(symbol):
        return None

    # Берём последнюю строку (самую свежую свечу)
    last = df.tail(1)
    if last.height == 0:
        return None

    # Оцениваем каждое правило
    matched_rules = []
    trigger_params = {}
    for rule in RULES:
        if rule.feature not in last.columns:
            continue
        val = last[rule.feature][0]
        if val is None:
            continue
        if rule.evaluate(val):
            matched_rules.append(rule)
            trigger_params[rule.feature] = {
                "value": float(val),
                "operator": rule.operator,
                "threshold": rule.threshold,
            }

    if not matched_rules:
        return None

    # Собираем горизонты из всех сработавших правил
    all_horizons = set()
    for r in matched_rules:
        all_horizons.update(r.horizons)
    horizons = sorted(all_horizons)

    # Создаём триггер
    now = datetime.now(tz=timezone.utc)
    event_id = _make_event_id(symbol, now)

    trigger = TriggerFile(
        event_id=event_id,
        symbol=symbol,
        category=category,
        trigger_params=trigger_params,
        horizons=horizons,
        capture_duration_sec=_CFG.get("capture", {}).get("duration_sec", 1200),
    )

    # Записываем файл
    path = trigger.write()

    # Фиксируем cooldown
    cd.record(symbol)

    return trigger


def evaluate_all_triggers(
    events: pl.DataFrame,
) -> list[TriggerFile]:
    """Оценить триггеры для всех событий (свечей, прошедших предфильтр).

    Args:
        events: DataFrame с events (symbol, category, relative_volume, relative_range)

    Returns:
        Список созданных триггеров.
    """
    if events.height == 0:
        return []

    cd = CooldownTracker()
    triggers = []

    # Группируем по символу — cooldown работает per-symbol
    for symbol in events["symbol"].unique().to_list():
        cat_rows = events.filter(pl.col("symbol") == symbol)
        cat = cat_rows["category"][0] if "category" in cat_rows.columns else "linear"
        t = evaluate_trigger(cat_rows, symbol, cat, cooldown=cd)
        if t is not None:
            triggers.append(t)

    return triggers


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def list_pending_triggers(triggers_dir: Path | None = None) -> list[Path]:
    """Список необработанных JSON-файлов триггеров."""
    d = triggers_dir or _TRIGGERS_DIR
    if not d.exists():
        return []
    return sorted(d.glob("*.json"))


def cleanup_processed_trigger(event_id: str, triggers_dir: Path | None = None) -> None:
    """Удалить JSON-файл триггера после обработки collector-ом."""
    d = triggers_dir or _TRIGGERS_DIR
    path = d / f"{event_id}.json"
    if path.exists():
        path.unlink()
