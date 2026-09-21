"""Data-ready gate: скан данных и проверка готовности к research-run.

Запускается перед research (таймером или вручную --gated). Идемпотентен:
состояние — data/research/last_run.json + flock data/research/research.lock.

Решение о готовности (все критерии):
    * прирост новых свечей с прошлого рана (мин и строки)
    * свежесть: последняя свеча не старше max_data_age_minutes
    * покрытие universe: свежих символов >= research.toml min_unique_symbols
    * OB coverage: >= min_ob_symbols валидных символов (is_valid, обновлены)
    * нет коoldown-паузы между ранами
    * конфиг research.toml не менялся с прошлого рана
"""
from __future__ import annotations

import fcntl
import json
import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from config.settings import MARKET_DATA_DIR, RAW_KLINES_DIR, RESEARCH_DIR, load_toml
from src.data import STALE_DAYS

logger = logging.getLogger("data_ready")

LOCK_PATH = RESEARCH_DIR / "research.lock"
STATE_PATH = RESEARCH_DIR / "last_run.json"
OB_METRICS = MARKET_DATA_DIR / "orderbook" / "reconstructed" / "_metrics.jsonl"
OB_COVERAGE_WINDOW_MIN = 30   # «обновлён» = запись OB не старше 30 мин

_G = load_toml("auto_research.toml")["gate"]

# Frozen boundary: снапшот данных, зафиксированный Research Runner на время
# цикла. Действует ТОЛЬКО внутри цикла: за пределами FROZEN_TTL не применяется,
# чтобы сбой/перезагрузка не оставили вечный обход обычного data-ready gate.
FROZEN_TTL = timedelta(hours=24)


class Lock:
    """flock-лок от параллельного запуска: авто-освобождается ядром."""

    def __init__(self, path: Path = LOCK_PATH):
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            os.close(self._fd)
            self._fd = None
            return False

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None


# --- Состояние -----------------------------------------------------------

def load_last_run() -> dict | None:
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return None


def save_last_run(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str))
    tmp.replace(STATE_PATH)


def git_head() -> str:
    try:
        r = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                           capture_output=True, text=True, timeout=5)
        return r.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def config_hash() -> str:
    """SHA256 конфигов research: изменение конфига триггерит новый ран."""
    import hashlib
    from pathlib import Path as P
    h = hashlib.sha256()
    for name in ("research.toml", "auto_research.toml"):
        p = P(__file__).resolve().parent.parent / "config" / name
        h.update(p.read_bytes())
    return h.hexdigest()[:12]


# --- Скан klines ---------------------------------------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def scan_klines(since: datetime | None = None) -> dict:
    """max(open_time), прирост и живой universe по сырым klines.

    Сканируются файлы с mtime >= порога свежести; прирост (new_rows)
    считается только по строкам с open_time > since (предыдущий ран).
    """
    files = sorted(RAW_KLINES_DIR.rglob("*.parquet"))
    if not files:
        return {"klines_max": None, "new_rows": 0, "n_files": 0,
                "fresh_symbols": 0, "stale_symbols": 0}

    now = now_utc()
    min_mtime = (now - timedelta(minutes=_G["max_data_age_minutes"])).timestamp()

    fresh_symbols: set[str] = set()
    stale_symbols: set[str] = set()
    klines_max: datetime | None = None
    new_rows = 0
    n_files = 0

    for f in files:
        mtime = f.stat().st_mtime
        if mtime < min_mtime:
            if mtime < (now - timedelta(days=STALE_DAYS)).timestamp():
                stale_symbols.add(f.stem.split("_")[0])
            continue
        n_files += 1
        sym = f.stem.split("_")[0]
        try:
            lf = pl.scan_parquet(f).select(pl.col("open_time"))
            mx = lf.select(pl.col("open_time").max()).collect().item()
        except Exception:
            continue
        if mx is None:
            continue
        fresh_symbols.add(sym)
        if klines_max is None or mx > klines_max:
            klines_max = mx
        if since is not None:
            new_rows += lf.filter(pl.col("open_time") > since) \
                          .select(pl.len()).collect().item()

    return {
        "klines_max": klines_max,
        "new_rows": new_rows,
        "n_files": n_files,
        "fresh_symbols": len(fresh_symbols),
        "stale_symbols": len(stale_symbols),
    }


# --- OB coverage ---------------------------------------------------------

def scan_ob() -> dict:
    """Уникальные символы с валидной записью реконструированного OB за окно."""
    if not OB_METRICS.exists():
        return {"ob_valid_symbols": 0, "ob_unique_symbols": 0, "ob_rows": 0}
    cutoff = (now_utc() - timedelta(minutes=OB_COVERAGE_WINDOW_MIN)).isoformat()
    latest: dict[str, dict] = {}
    rows = 0
    with open(OB_METRICS) as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            rows += 1
            latest[r.get("symbol", "?")] = r
    valid = [s for s, r in latest.items()
             if r.get("is_valid") is True
             and r.get("timestamp", "") >= cutoff
             and (r.get("update_id_jumps") or 0) <= _G["max_ob_jumps"]]
    return {"ob_valid_symbols": len(valid), "ob_unique_symbols": len(latest),
            "ob_rows": rows}


# --- Frozen boundary (Research Runner) -----------------------------------

def frozen_boundary_path() -> Path:
    return RESEARCH_DIR / "frozen_boundary.json"


def load_frozen_boundary() -> dict | None:
    """Снапшот замороженного boundary, если он существует и не протух.

    Протухший (старше FROZEN_TTL) или повреждённый снапшот игнорируется —
    обычный data-ready gate снова работает в полном объёме.
    """
    p = frozen_boundary_path()
    if not p.exists():
        return None
    try:
        snap = json.loads(p.read_text())
    except (ValueError, OSError):
        logger.warning("frozen_boundary.json corrupt, ignoring")
        return None
    km_raw = snap.get("klines_max")
    if not km_raw:
        return None
    try:
        km = datetime.fromisoformat(str(km_raw))
        if km.tzinfo is not None:
            km = km.astimezone(timezone.utc).replace(tzinfo=None)
    except ValueError:
        logger.warning("frozen_boundary.json bad klines_max, ignoring")
        return None
    snap["_klines_max"] = km
    frozen_at = snap.get("frozen_at_utc")
    if frozen_at:
        try:
            ft = datetime.fromisoformat(str(frozen_at))
            if ft.tzinfo is not None:
                ft = ft.astimezone(timezone.utc).replace(tzinfo=None)
            age = now_utc() - ft
            if age > FROZEN_TTL:
                logger.warning("frozen boundary stale (age=%s), ignoring", age)
                return None
        except ValueError:
            pass
    return snap


def save_frozen_boundary(snapshot: dict) -> None:
    snap = dict(snapshot)
    snap.pop("_klines_max", None)
    snap["frozen_at_utc"] = now_utc().isoformat()
    p = frozen_boundary_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(snap, ensure_ascii=False, indent=2, default=str))
    tmp.replace(p)


def clear_frozen_boundary() -> None:
    try:
        frozen_boundary_path().unlink()
    except FileNotFoundError:
        pass


# --- Решение -------------------------------------------------------------

def check_ready() -> tuple[bool, list[str], dict]:
    """(ready, причины отказа, метрики). ready=True без last_run.json
    (первый запуск) — gate не должен блокировать инициализацию."""
    reasons: list[str] = []

    frozen = load_frozen_boundary()
    if frozen is not None:
        klines_max = frozen["_klines_max"]
        logger.info("Frozen boundary %s: data-ready gate skipped",
                    klines_max.isoformat())
        metrics = {
            "klines": {
                "klines_max": klines_max,
                "new_rows": 0,
                "n_files": int(frozen.get("n_files", 0)),
                "fresh_symbols": int(frozen.get("fresh_symbols", 0)),
                "stale_symbols": 0,
            },
            "ob": {
                "ob_valid_symbols": int(frozen.get("ob_valid_symbols", 0)),
                "ob_unique_symbols": int(frozen.get("ob_unique_symbols", 0)),
                "ob_rows": int(frozen.get("ob_rows", 0)),
            },
            "config_hash": str(frozen.get("config_hash", "")),
            "git_head": str(frozen.get("git_head", "")),
            "data_version": str(frozen.get("data_version", "1.0")),
        }
        return True, [], metrics

    last = load_last_run()
    since = datetime.fromisoformat(last["klines_max"]) if last and last.get("klines_max") else None
    metrics = {"klines": scan_klines(since=since), "ob": scan_ob(),
               "config_hash": config_hash(), "git_head": git_head()}

    if last is None:
        return True, [], metrics

    # Коoldown между ранами
    finished = datetime.fromisoformat(last.get("finished_at_utc"))
    if finished.tzinfo is not None:
        finished = finished.astimezone(timezone.utc).replace(tzinfo=None)
    cooldown = timedelta(minutes=_G["cooldown_minutes"])
    if now_utc() - finished < cooldown:
        reasons.append(f"cooldown until {finished + cooldown:%Y-%m-%d %H:%M} UTC")

    # Конфиг изменился — новый ран легитимен даже без прироста
    if last.get("config_hash") != metrics["config_hash"]:
        return True, reasons, metrics

    k = metrics["klines"]
    if k["klines_max"] is None or k["n_files"] == 0:
        reasons.append("no klines data")
        return False, reasons, metrics

    # Свежесть
    age = now_utc() - k["klines_max"]
    if age > timedelta(minutes=_G["max_data_age_minutes"]):
        reasons.append(f"klines stale: {age:.0f} min")

    # Прирост с прошлого рана
    last_max = datetime.fromisoformat(last.get("klines_max"))
    if last_max.tzinfo is not None:
        last_max = last_max.astimezone(timezone.utc).replace(tzinfo=None)
    growth_min = (k["klines_max"] - last_max).total_seconds() / 60
    if growth_min < _G["min_new_data_minutes"]:
        reasons.append(f"new data span too small: {growth_min:.0f} min")
    if k["new_rows"] < _G["min_new_rows"]:
        reasons.append(f"new rows {k['new_rows']} < {_G['min_new_rows']}")

    # Покрытие universe (берём из research.toml, не дублируем)
    rcfg = load_toml("research.toml")
    if k["fresh_symbols"] < rcfg.get("min_unique_symbols", 5):
        reasons.append(f"fresh symbols {k['fresh_symbols']} < "
                       f"{rcfg.get('min_unique_symbols', 5)}")

    # OB coverage
    if metrics["ob"]["ob_valid_symbols"] < _G["min_ob_symbols"]:
        reasons.append(f"OB valid symbols {metrics['ob']['ob_valid_symbols']}"
                       f" < {_G['min_ob_symbols']}")

    return not reasons, reasons, metrics