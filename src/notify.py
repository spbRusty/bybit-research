"""Уведомления ntfy (ТЗ §42). Топик — конфигурационный параметр (из .env).

Никаких секретов в логах. Stdlib urllib, без зависимостей.
"""
from __future__ import annotations

import json
import logging
import urllib.request
from collections import Counter

from config.settings import NTFY_TOPIC, NTFY_SERVER

logger = logging.getLogger(__name__)

_VERDICT_RU = {"ACCEPT": "ПРИНЯТА", "REJECT": "ОТКЛОНЕНА", "FAILED": "ОШИБКА"}
_DIAGNOSIS_RU = {
    "no_signal": "нет сигнала",
    "cost_sensitive": "чувствительно к комиссиям",
    "concentrated": "концентрация сделок",
    "no_edge": "нет преимущества",
    "unstable": "нестабильно",
}
_MODE_RU = {
    "BASELINE": "базовая линия",
    "REGIME_SWEEP": "обзор режимов",
    "HORIZON_SWEEP": "обзор горизонтов",
    "THRESHOLD_SWEEP": "обзор порогов",
    "CONDITIONAL": "условные входы",
    "FAMILY_SWITCH": "смена семейства",
}


def _ru(counts: Counter, table: dict[str, str]) -> str:
    """'нет сигнала 8; чувствительно к комиссиям 3' — с переводом и fallback."""
    parts = [f"{table.get(k, k)} {v}" for k, v in counts.most_common()]
    return "; ".join(parts) if parts else "—"


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
        f"Критик: {'ПРОШЁЛ' if verdict_pass else 'ОТКЛОНЁН'}",
    ]
    if paper:
        lines += [
            f"Бумага: сделок={paper['n_trades']}, винрейт={paper['win_rate']:.0%}, "
            f"PnL={paper['net_pnl']:+.2f}$, просадка={paper['max_drawdown']:.2%}",
        ]
    notify("СВОДКА ИССЛЕДОВАНИЯ", "\n".join(lines), tags="bar_chart")
    if paper and paper.get("n_trades"):
        notify_trade(paper)


def notify_trade(paper: dict) -> None:
    """Отправка итога бумажной сделки (в сводке по контуру §42)."""
    msg = (f"Бумажных сделок: {paper['n_trades']}; винрейт {paper['win_rate']:.1%}; "
           f"итог {paper['net_pnl']:+.2f} USDT; просадка {paper['max_drawdown']:.2%}; "
           f"баланс {paper['balance_end']:.2f} USDT")
    notify("БУМАЖНАЯ СДЕЛКА ЗАКРЫТА", msg, tags="chart")


def notify_hypothesis_candidate(hyp_id: str, description: str) -> None:
    """Уведомление о новом кандидате-гипотезе."""
    notify("КАНДИДАТ-ГИПОТЕЗА", f"{hyp_id}: {description}", tags="thought_balloon")


def notify_hypothesis_validated(hyp_id: str, description: str) -> None:
    """Уведомление о валидации гипотезы (все gates пройдены)."""
    notify("ГИПОТЕЗА ПОДТВЕРЖДЕНА", f"{hyp_id}: {description} — все проверки пройдены",
           tags="white_check_mark")


def notify_paper_started(mode: str, hyp_id: str) -> None:
    """Уведомление о запуске paper trading."""
    notify("БУМАЖНАЯ ТОРГОВЛЯ ЗАПУЩЕНА", f"Режим: {mode}, гипотеза: {hyp_id}",
           tags="rocket")


def notify_paper_stopped(reason: str) -> None:
    """Уведомление о остановке paper trading."""
    notify("БУМАЖНАЯ ТОРГОВЛЯ ОСТАНОВЛЕНА", f"Причина: {reason}", tags="octagonal_sign")


def notify_shadow_summary(trades: int, pnl: float, balance: float) -> None:
    """Сводка по shadow paper за цикл."""
    msg = (f"Сделок в shadow: {trades}; PnL: {pnl:+.4f} USDT; "
           f"баланс: {balance:.2f} USDT")
    notify("СВОДКА ПО SHADOW", msg, tags="bar_chart")


def notify_cycle_report(experiments: list[dict], state: dict) -> None:
    """Итог прогона контроллера — батч экспериментов на frozen boundary.

    experiments: сырые записи журнала цикла (RUNNING+DONE дублируются по
    experiment_id, здесь дедуплицируются). state: controller_state.json.
    """
    done: dict[str, dict] = {}
    for e in experiments:
        if e.get("status") == "DONE":
            done[e["experiment_id"]] = e
    if not done:
        notify("ИТОГ ЦИКЛА: ЭКСПЕРИМЕНТЫ", "Завершённых экспериментов нет",
               tags="grey_question")
        return

    budget = int(state.get("budget_used", 0) or 0)
    tests = int(state.get("n_tests_cumulative", 0) or 0)
    lines = [
        f"Цикл: {state.get('cycle_id', '—')}",
        f"Экспериментов завершено: {len(done)}"
        + (f" (бюджет {budget})" if budget else ""),
        f"Гипотез оценено за цикл нарастающим итогом: {tests}"
        if tests else "Гипотез оценено: —",
        f"Вердикты: {_ru(Counter(e.get('final_verdict') for e in done.values()), _VERDICT_RU)}",
        f"Диагнозы: {_ru(Counter(e.get('diagnosis') for e in done.values()), _DIAGNOSIS_RU)}",
        f"Режимы: {_ru(Counter(e.get('mode') for e in done.values()), _MODE_RU)}",
    ]
    accepted = sum(1 for e in done.values()
                   if e.get("final_verdict") == "ACCEPT")
    lines.append("Итог: " + ("есть принятые кандидаты" if accepted
                              else "новых кандидатов нет"))
    notify("ИТОГ ЦИКЛА: ЭКСПЕРИМЕНТЫ", "\n".join(lines),
           tags="tada" if accepted else "bar_chart")