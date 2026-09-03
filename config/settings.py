"""Пути, окружение, секреты. Минимальный загрузчик .env (stdlib, без зависимостей).

Секреты — только из .env, никогда в коде и логах (ТЗ §48).
"""
import os
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_env(path: Path):
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())


_load_env(ROOT / ".env")

# --- Сырые свечи Bybit (готовы, скачаны Rust-загрузчиком bybit_rs) ---
RAW_KLINES_DIR = Path(os.environ.get(
    "RAW_KLINES_DIR",
    str(Path.home() / "Документы/bybit_rs/data/klines")))

# --- Тиковые рыночные потоки (Rust marketdata-сервис, §12-§15) ---
MARKET_DATA_DIR = Path(os.environ.get(
    "MARKET_DATA_DIR",
    str(Path.home() / "Документы/построение/collector/data/market")))

# --- Выходы (ТЗ §4) ---
DATA = ROOT / "data"
CLEAN_CANDLES = DATA / "clean" / "candles"
FEATURES_DIR = DATA / "features"
EVENTS_DIR = DATA / "events" / "signal_events"
RESEARCH_DIR = DATA / "research"
HYPOTHESES_DIR = RESEARCH_DIR / "hypotheses"
RESULTS_DIR = RESEARCH_DIR / "results"
REPORTS_DIR = RESEARCH_DIR / "reports"
PAPER_DIR = DATA / "paper"
PAPER_ORDERS = PAPER_DIR / "orders"
PAPER_TRADES = PAPER_DIR / "trades"
PAPER_PORTFOLIO = PAPER_DIR / "portfolio"
LOGS_DIR = ROOT / "logs"

for _d in (CLEAN_CANDLES, FEATURES_DIR, EVENTS_DIR, HYPOTHESES_DIR, RESULTS_DIR,
           REPORTS_DIR, PAPER_ORDERS, PAPER_TRADES, PAPER_PORTFOLIO, LOGS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# --- Секреты ---
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "")
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh")


def load_toml(name: str) -> dict:
    with open(ROOT / "config" / name, "rb") as f:
        return tomllib.load(f)