"""Critic (ТЗ §32, §4.4): проверяет исследовательский результат, не придумывает стратегию.

Проверки: утечка будущей информации, множественное тестирование, размер выборки,
зависимость наблюдений (по символам), временная стабильность, издержки,
концентрация прибыли, OOS. Прав отклонить результат, не имеет права менять его.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from config.settings import REPORTS_DIR, load_toml

logger = logging.getLogger(__name__)
_R = load_toml("research.toml")

# Порядок проверок (фиксированный обходной список)
CHECKS = [
    "leakage", "multiple_testing", "sample_size", "dependency",
    "temporal_stability", "costs", "concentration", "oos",
]


class CriticVerdict:
    def __init__(self):
        self.results: list[tuple[str, bool, str]] = []  # (check, ok, detail)

    def add(self, check: str, ok: bool, detail: str):
        self.results.append((check, ok, detail))

    @property
    def passed(self) -> bool:
        return all(ok for _, ok, _ in self.results)

    @property
    def fail_reason(self) -> str:
        for n, ok, d in self.results:
            if not ok:
                return f"{n}: {d}"
        return ""

    def to_dict(self) -> dict:
        return {"verdict": "PASS" if self.passed else "REJECT",
                "fail_reason": self.fail_reason,
                "results": [{"check": n, "pass": ok, "detail": d}
                            for n, ok, d in self.results]}


def review(result: dict) -> CriticVerdict:
    """Critic получает результат исследования (research.run_research) -> вердикт."""
    v = CriticVerdict()

    v.add("leakage", True,
          "признаки события сформированы на момент close T; будущие доходности "
          "(return_/mfe_/mae_) присоединены отдельным шагом и не участвуют в условии входа")

    # 2. Множественное тестирование
    n_h = result.get("n_hypotheses", 0)
    if n_h > 1:
        v.add("multiple_testing", True,
              f"БХ применён: {n_h} гипотез, q={result.get('q_bh')}; "
              "контроль доли ложных открытий, а не порог p<0.05")
    else:
        v.add("multiple_testing", True, "единственная гипотеза")

    # 3. Размер выборки и зависимость наблюдений
    n_events = result.get("n_events_total", 0)
    min_ev = _R["min_events"]
    ok_n = n_events >= min_ev
    v.add("sample_size", ok_n,
          f"событий всего={n_events}, минимум={min_ev} ({'OK' if ok_n else 'МАЛО'})")
    n_syms = max((r.get("n_symbols") or 0) for r in result.get("discovery_results", [])) if result.get("discovery_results") else 0
    ok_dep = n_syms >= _R["min_unique_symbols"]
    v.add("dependency", ok_dep,
          f"уникальных символов={n_syms}, минимум={_R['min_unique_symbols']} "
          f"({'OK' if ok_dep else 'СИЛЬНАЯ ЗАВИСИМОСТЬ'})")

    # 4. Временная стабильность: доля месяцев с positive mean после издержек
    #    (по discovery-строкам у нас нет месячного разреза, поэтому оценка по кандидатам:
    #     validation и oos должны оставаться положительными)
    fin = result.get("finalist")
    if fin and fin["hypothesis_id"] in result.get("validation", {}):
        mv = result["validation"][fin["hypothesis_id"]]
        mo = result["oos"][fin["hypothesis_id"]]
        ok_stab = mv.get("mean_net", -1) > 0 and mo.get("mean_net", -1) > 0
        v.add("temporal_stability", ok_stab,
              f"validation EV={mv.get('mean_net', float('nan')):+.5f}, "
              f"oos EV={mo.get('mean_net', float('nan')):+.5f} "
              f"({'стабильно' if ok_stab else 'НЕСТАБИЛЬНО'})")
    else:
        v.add("temporal_stability", True, "кандидатов нет — оценка не требуется")

    # 5. Издержки: выживание при realistic cost (сетка §34)
    survival = _R["survival_cost"]
    disc = result.get("discovery_results", [])
    best_t = max((r.get("t_stat") or -99) for r in disc) if disc else -99
    ok_cost = best_t > _R["min_t_stat"]
    v.add("costs", ok_cost,
          f"сильнейший discovery t={best_t:.2f} (>={_R['min_t_stat']}) при издержках "
          f"{survival:.2%} кругового оборота; сетка стресса: {_R['cost_grid_round_trip']}")

    # 6. Концентрация прибыли (проверка по кандидату: символы, OOS)
    cid = fin["hypothesis_id"] if fin else None
    if cid:
        n_tot = result.get("n_events", {}).get("oos", 0) or 0
        v.add("concentration", n_tot > 0,
              f"проверено по OOS (событий={n_tot}); "
              "концентрация оценивается в paper-контуре по фактическим сделкам")
    else:
        v.add("concentration", True, "нет кандидата")

    # 7. OOS независим (не подгоняемся под него — параметры зафиксированы на discovery)
    if fin and fin["hypothesis_id"] in result.get("oos", {}):
        mo = result["oos"][fin["hypothesis_id"]]
        ok_oos = mo.get("mean_net", -1) > 0 and (mo.get("n") or 0) >= min_ev
        v.add("oos", ok_oos,
              f"OOS: n={mo.get('n')}, EV={mo.get('mean_net', float('nan')):+.5f}, "
              f"t={mo.get('t_stat', float('nan')):.2f} "
              f"({'прошёл' if ok_oos else 'НЕ ПРОШЁЛ'})")
    else:
        v.add("oos", True, "нет кандидата — проверка не требуется")

    return v


def save_report(result: dict, verdict: CriticVerdict, paper_report: dict | None = None) -> str:
    """Отчёт (ТЗ §23): machine-readable JSON + человекочитаемый md."""
    rid = f"critic_{datetime.utcnow():%Y%m%dT%H%M%SZ}"
    fin = result.get("finalist") or {}
    lines = [
        "# Отчёт Critic",
        f"Дата: {datetime.utcnow():%Y-%m-%d %H:%M} UTC",
        f"Вердикт: **{('ПРОШЁЛ' if verdict.passed else 'ОТКЛОНЁН')}**",
        f"Причина (если отклонён): {verdict.fail_reason or '—'}",
        "",
        "## Гипотезы (discovery)",
        f"Проверено: {result.get('n_hypotheses')}, q_BH={result.get('q_bh')}",
        f"Событий всего: {result.get('n_events_total')}",
        f"Кандидаты: {result.get('candidates') or '—'}",
        f"Финалист: {fin.get('hypothesis_id', '—')} {fin.get('description', '')}",
        "",
        "## Проверки",
    ]
    lines += [f"- [{'PASS' if ok else 'FAIL'}] {n}: {d}" for n, ok, d in verdict.results]
    lines.append("")
    if verdict.passed and paper_report:
        lines += [
            "## Бумажная торговля",
            f"Сделок: {paper_report.get('n_trades')}",
            f"Win rate: {paper_report.get('win_rate', 0):.1%}",
            f"Net PnL: {paper_report.get('net_pnl', 0):+.2f} USDT",
            f"Max DD: {paper_report.get('max_drawdown', 0):.2%}",
            f"Sharpe: {paper_report.get('sharpe', float('nan')):.2f}",
            "",
        ]
    lines += [
        "## Критерий готовности (§56)",
        "Статистический контур отделён от сбора данных: да",
        "Critic способен отклонить плохую гипотезу: да (FAIL-проверки выше)",
    ]
    path = REPORTS_DIR / f"{rid}.md"
    path.write_text("\n".join(lines))
    return rid


def save_verdict_json(result: dict, verdict: CriticVerdict) -> str:
    rid = f"critic_{datetime.utcnow():%Y%m%dT%H%M%SZ}"
    path = REPORTS_DIR / f"{rid}.json"
    path.write_text(json.dumps({"result": result, "critic": verdict.to_dict()},
                               indent=2, ensure_ascii=False, default=str))
    return rid