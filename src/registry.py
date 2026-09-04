"""Feature Registry (§4) + Provenance (§32) + Hypothesis Lifecycle.

Единая система регистрации признаков. Каждый признак имеет строго определённое
время доступности — информация, ставшая известной после формирования события,
не может участвовать в условии входа.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


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
    timestamp: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())

    def to_dict(self) -> dict:
        d = asdict(self)
        d["dataset_period"] = list(d["dataset_period"])
        return d

    def fingerprint(self) -> str:
        raw = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:12]


# --------------------------------------------------------------------------
# Hypothesis Lifecycle
# --------------------------------------------------------------------------

class HypothesisStatus(str, Enum):
    CANDIDATE = "CANDIDATE"
    VALIDATED = "VALIDATED"
    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"
    ARCHIVED = "ARCHIVED"


class HypothesisLifecycle:
    VALID_TRANSITIONS = {
        HypothesisStatus.CANDIDATE: [HypothesisStatus.VALIDATED, HypothesisStatus.DEPRECATED],
        HypothesisStatus.VALIDATED: [HypothesisStatus.ACTIVE, HypothesisStatus.DEPRECATED],
        HypothesisStatus.ACTIVE: [HypothesisStatus.DEPRECATED],
        HypothesisStatus.DEPRECATED: [HypothesisStatus.ARCHIVED],
        HypothesisStatus.ARCHIVED: [],
    }

    def __init__(self, registry_path: Path | None = None):
        self._path = registry_path or (Path(__file__).resolve().parent.parent / "data" / "research" / "registry.json")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: dict[str, dict] = self._load()

    def _load(self) -> dict:
        if self._path.exists():
            return json.loads(self._path.read_text())
        return {}

    def _save(self) -> None:
        self._path.write_text(json.dumps(self._entries, indent=2, ensure_ascii=False, default=str))

    def transition(self, hypothesis_id: str, new_status: HypothesisStatus, reason: str = "") -> bool:
        entry = self._entries.get(hypothesis_id)
        if entry is None:
            if new_status == HypothesisStatus.CANDIDATE:
                self.register(hypothesis_id, new_status, {"reason": reason})
                return True
            return False
        current = HypothesisStatus(entry["status"])
        if new_status not in self.VALID_TRANSITIONS.get(current, []):
            return False
        entry["status"] = new_status.value
        entry["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        entry["history"].append({
            "status": new_status.value,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "reason": reason,
        })
        self._save()
        return True

    def get_status(self, hypothesis_id: str) -> HypothesisStatus:
        entry = self._entries.get(hypothesis_id)
        return HypothesisStatus(entry["status"]) if entry else HypothesisStatus.CANDIDATE

    def get_entry(self, hypothesis_id: str) -> dict | None:
        return self._entries.get(hypothesis_id)

    def register(self, hypothesis_id: str, initial: HypothesisStatus, metadata: dict | None = None) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._entries[hypothesis_id] = {
            "status": initial.value,
            "created_at": now,
            "updated_at": now,
            "metadata": metadata or {},
            "history": [{"status": initial.value, "timestamp": now, "reason": "initial"}],
        }
        self._save()

    def list_by_status(self, status: HypothesisStatus) -> list[str]:
        return [hid for hid, e in self._entries.items() if e["status"] == status.value]

    def to_dict(self) -> dict:
        return dict(self._entries)


def compute_config_hash(config_path: Path | None = None) -> str:
    p = config_path or (Path(__file__).resolve().parent.parent / "config" / "research.toml")
    return hashlib.sha256(p.read_bytes()).hexdigest()[:12]


def get_git_commit(repo_path: Path | None = None) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_path or Path(__file__).resolve().parent.parent,
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""
