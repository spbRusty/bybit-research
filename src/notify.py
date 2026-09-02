"""Уведомления ntfy (ТЗ §42). Топик — конфигурационный параметр (из .env).

Никаких секретов в логах. Stdlib urllib, без зависимостей.
"""
from __future__ import annotations

import json
import logging
import urllib.request

from config.settings import NTFY_TOPIC, NTFY_SERVER

logger = logging.getLogger(__name__)


def notify(title: str, message: str, tags: str = "") -> bool:
    """Отправка в ntfy (JSON-тело: UTF-8, без заголовков). True при успехе."""
    if not NTFY_TOPIC:
        logger.info("[ntfy отключён: NTFY_TOPIC не задан] %s: %s", title, message)
        return False
    payload = json.dumps({"topic": NTFY_TOPIC, "title": title[:200],
                          "message": message, "tags": tags.split()}).encode("utf-8")
    req = urllib.request.Request(
        f"{NTFY_SERVER.rstrip('/')}/", data=payload, method="POST",
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        logger.warning("ntfy error: %s", e)
        return False


def notify_research(result: dict, verdict_pass: bool, paper: dict | None = None) -> None:
    """Сводка по исследованию (§42: после завершения цикла)."""
    fin = result.get("finalist") or {}
    lines = [
        f"Исследование: {result.get('n_hypotheses')} гипотез, событий {result.get('n_events_total'):,}",
        f"Кандидаты: {result.get('candidates') or '—'}",
        f"Финалист: {fin.get('hypothesis_id', '—')} ({fin.get('entry_side', '—')})",
        f"Critic: {'ПРОШЁЛ' if verdict_pass else 'ОТКЛОНЁН'}",
    ]
    if paper:
        lines += [
            f"Paper: сделок={paper['n_trades']}, win={paper['win_rate']:.0%}, "
            f"PnL={paper['net_pnl']:+.2f}$, DD={paper['max_drawdown']:.2%}",
        ]
    notify("RESEARCH SUMMARY", "\n".join(lines), tags="bar_chart")
    if paper and paper.get("n_trades"):
        notify_trade(paper)


def notify_trade(paper: dict) -> None:
    """Отправка итога бумажной сделки (в сводке по контуру §42)."""
    msg = (f"Paper-сделок: {paper['n_trades']}; win_rate {paper['win_rate']:.1%}; "
           f"net {paper['net_pnl']:+.2f} USDT; просадка {paper['max_drawdown']:.2%}; "
           f"баланс {paper['balance_end']:.2f} USDT")
    notify("PAPER TRADE CLOSED", msg, tags="chart")