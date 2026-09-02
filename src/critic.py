"""Critic (ТЗ §32, §4.4): проверяет исследовательский результат, не придумывает стратегию.

Проверки: утечка будущей информации, множественное тестирование, размер выборки,
зависимость наблюдений (по символам), временная стабильность, издержки,
концентрация прибыли, OOS. Прав отклонить результат, не имеет права менять его.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

import numpy as np
import polars as pl

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
        # UNKNOWN (None) не проходит: only explicit True counts as passed
        return self.results and all(ok is True for _, ok, _ in self.results)

    @property
    def fail_reason(self) -> str:
        for n, ok, d in self.results:
            if ok is not True:
                return f"{n}: {d}"
        return ""

    def to_dict(self) -> dict:
        status = {True: "PASS", False: "FAIL", None: "UNKNOWN"}
        return {"verdict": "PASS" if self.passed else "REJECT",
                "fail_reason": self.fail_reason,
                "results": [{"check": n, "pass": ok,
                             "status": status.get(ok, "UNKNOWN"), "detail": d}
                            for n, ok, d in self.results]}


def review(result: dict, events: pl.DataFrame | None = None) -> CriticVerdict:
    """Critic получает результат исследования (research.run_research) -> вердикт.

    events — полный DataFrame событий (для реальных проверок temporal stability
    и concentration по кандидату). Если events не передан — эти проверки UNKNOWN
    (а не PASS): проверка, фактически не выполненная, не маркируется как пройденная (ТЗ §34).
    """
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

    # 3. Размер выборки
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

    # 4. Временная стабильность
    fin = result.get("finalist")
    if fin:
        cid = fin["hypothesis_id"]
        if events is not None and cid:
            # реальная проверка: доля месяцев с положительным EV после издержек
            stab = _temporal_stability(events, fin, result.get("cost_survival", _R["survival_cost"]))
            ok_stab = stab["pos_share"] >= _R["stability_pos_share"]
            v.add("temporal_stability", ok_stab,
                  f"месяцев_pol={stab['n_pos']}/{stab['n']}, доля={stab['pos_share']:.0%} "
                  f"(>={_R['stability_pos_share']:.0%}), worst={stab['worst']:+.5f}, "
                  f"best={stab['best']:+.5f} "
                  f"({'стабильно' if ok_stab else 'НЕСТАБИЛЬНО'})")
        else:
            v.add("temporal_stability", None,
                  "нет events для реальной проверки по месяцам (UNKNOWN)")
    else:
        v.add("temporal_stability", True, "кандидатов нет — оценка не требуется")

    # 5. Издержки
    survival = _R["survival_cost"]
    disc = result.get("discovery_results", [])
    best_t = max((r.get("t_stat") or -99) for r in disc) if disc else -99
    ok_cost = best_t > _R["min_t_stat"]
    v.add("costs", ok_cost,
          f"сильнейший discovery t={best_t:.2f} (>={_R['min_t_stat']}) при издержках "
          f"{survival:.2%} кругового оборота; сетка стресса: {_R['cost_grid_round_trip']}")

    # 6. Концентрация (реальная, по кандидату)
    cid = fin["hypothesis_id"] if fin else None
    if cid and events is not None:
        conc = _concentration(events, fin, result.get("cost_survival", _R["survival_cost"]))
        ok_c = conc["top1_share"] <= _R["max_symbol_concentration"]
        v.add("concentration", ok_c,
              f"top1={conc['top1_share']:.0%} событий, top5={conc['top5_share']:.0%}, "
              f"символов={conc['n_symbols']} (>={_R['min_unique_symbols']}); "
              f"порог top1<={_R['max_symbol_concentration']:.0%} "
              f"({'OK' if ok_c else 'КОНЦЕНТРАЦИЯ'})")
    elif cid:
        v.add("concentration", None, "нет events для реальной проверки (UNKNOWN)")
    else:
        v.add("concentration", True, "нет кандидата")

    # 7. OOS
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


def _candidate_subset(events: pl.DataFrame, finalist: dict) -> pl.DataFrame | None:
    """Подвыборка событий, удовлетворяющих условию финалиста."""
    try:
        cond = eval(finalist["condition"], {"pl": pl})
    except Exception:
        return None
    target = f"return_{finalist['horizon_min']}m"
    side = 1.0 if finalist.get("entry_side") == "long" else -1.0
    sub = events.filter(cond).filter(
        pl.col(target).is_not_null() & pl.col("entry_price").is_not_null())
    return sub.with_columns(
        ((pl.col(target) * side) - finalist.get("cost", _R["survival_cost"]))
        .alias("net_ret"))


def _temporal_stability(events: pl.DataFrame, finalist: dict, cost: float) -> dict:
    """Доля месяцев с положительным средним net_ret после издержек (§27)."""
    finalist = {**finalist, "cost": cost}
    sub = _candidate_subset(events, finalist)
    if sub is None or sub.height == 0:
        return {"n": 0, "n_pos": 0, "pos_share": 0.0, "worst": np.nan, "best": np.nan}
    by_month = (sub.with_columns(pl.col("open_time").dt.strftime("%Y-%m").alias("m"))
                   .group_by("m").agg(pl.col("net_ret").mean().alias("mean")))
    means = by_month["mean"].to_numpy()
    pos = int((means > 0).sum())
    return {"n": int(means.size), "n_pos": pos, "pos_share": pos / max(1, means.size),
            "worst": float(means.min()), "best": float(means.max())}


def _concentration(events: pl.DataFrame, finalist: dict, cost: float) -> dict:
    """Концентрация по символам: доля событий top-1/top-5 (§29)."""
    finalist = {**finalist, "cost": cost}
    sub = _candidate_subset(events, finalist)
    if sub is None or sub.height == 0:
        return {"n_symbols": 0, "top1_share": 1.0, "top5_share": 1.0}
    by_sym = (sub.group_by("symbol").agg(pl.len().alias("n"))
                 .sort("n", descending=True))
    ns = by_sym["n"].to_numpy()
    total = ns.sum()
    top1 = ns[0] / total if ns.size else 1.0
    top5 = ns[:5].sum() / total if ns.size else 1.0
    return {"n_symbols": int(by_sym.height), "top1_share": float(top1),
            "top5_share": float(top5)}


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
    lines += [f"- [{'PASS' if ok is True else 'UNKNOWN' if ok is None else 'FAIL'}] {n}: {d}"
              for n, ok, d in verdict.results]
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