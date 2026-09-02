"""Feature Registry (§4) + Provenance (§32).

Единая система регистрации признаков. Каждый признак имеет строго определённое
время доступности — информация, ставшая известной после формирования события,
не может участвовать в условии входа.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime


@dataclass(frozen=True)
class Feature:
    id: str
    name: str
    category: str          # candle, volume, volatility, momentum, structure, cross, regime, context
    description: str
    formula: str           # human-readable
    source: str            # module path
    timeframe: str         # 1m, 5m, 1h, 4h
    lookback: int          # candles needed before this feature is valid
    data_required: tuple[str, ...]  # ohlcv, btc, eth
    realtime_available: bool
    cost: str              # low, medium, high
    version: str = "1.0"


class FeatureRegistry:
    def __init__(self):
        self._features: dict[str, Feature] = {}

    def register(self, feature: Feature):
        self._features[feature.id] = feature

    def get(self, fid: str) -> Feature | None:
        return self._features.get(fid)

    def by_category(self, cat: str) -> list[Feature]:
        return [f for f in self._features.values() if f.category == cat]

    def by_data(self, data: str) -> list[Feature]:
        return [f for f in self._features.values() if data in f.data_required]

    def all_ids(self) -> list[str]:
        return sorted(self._features.keys())

    def all_columns(self) -> list[str]:
        return self.all_ids()

    def to_json(self) -> str:
        return json.dumps([asdict(f) for f in self._features.values()],
                          indent=2, ensure_ascii=False)

    def __len__(self):
        return len(self._features)

    def __repr__(self):
        return f"FeatureRegistry({len(self)} features)"


# Глобальный реестр — импортируют и регистрируют через register()
REGISTRY = FeatureRegistry()


def register(feature: Feature):
    REGISTRY.register(feature)


# --------------------------------------------------------------------------
# Provenance (§32)
# --------------------------------------------------------------------------

@dataclass
class Provenance:
    data_version: str = ""
    feature_version: str = "1.0"
    hypothesis_version: str = "1.0"
    config_version: str = "1.0"
    code_version: str = ""
    dataset_period: tuple[str, str] = ("", "")
    cost_assumptions: list[float] = field(default_factory=list)
    random_seed: int = 42
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict:
        d = asdict(self)
        d["dataset_period"] = list(d["dataset_period"])
        return d

    def fingerprint(self) -> str:
        raw = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:12]
