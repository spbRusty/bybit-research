"""RSI14 < 25 LONG — исследовательский эксперимент 3 horizons x 3 costs.

FEATURE:  rsi_14 < 25
ENTRY:    open(T+1), LONG
HORIZONS: 60m, 120m, 240m
COSTS:    0.10% / 0.15% / 0.20% round-trip (net)

Критично:
- return_60m / return_240m из существующего events pipeline — РЕТРО-фичи
  (add_multi_timeframe_returns: close/close(-n)-1), НЕ используются как target.
- return_120m вообще отсутствует.
- Forward target формируется заново по канонической формуле events._future_metrics:
      entry_price = open(T+1)
      return_{h}m = close(T+h) / entry_price - 1
  на ПОЛНОМ ряду ДО prefilter (иначе shift(-h) сдвигал бы по прореженным строкам).
- Признаки только <= T: rsi_14 и relative_volume/relative_range без look-ahead.

Методология (2 правки):
1. Temporal split: при формировании discovery/validation/OOS выборок события,
   чьё forward-окно (open_time + h) выходит за конец периода, исключаются
   (буфер = horizon_min минут, критично для h=240). sample_periods не меняются,
   исходный полный events не модифицируется.
2. Multiple testing: 3 cost (0.10/0.15/0.20%) — сценарии стоимости ОДНОГО
   сигнала, не независимые гипотезы. БХ применяется ОДИН раз на семью из
   3 горизонтов при ref_cost = survival_cost (0.20%); 9 комбинаций остаются
   в таблице, но stat-verdict один на сигнал, а cost-строки читаются как
   sensitivity к издержкам.

Standalone-скрипт по образцу mr_research.py: никаких изменений production-кода,
config, critic, paper, controller.
"""
from __future__ import annotations

import sys, json, warnings, logging
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.WARNING)

sys.path.insert(0, "/home/vlad/Документы/построение")

import numpy as np
import polars as pl
from datetime import datetime, timedelta, timezone
from scipy import stats as sp_stats

from src import data as data_mod
from src.research import (test_hypothesis, benjamini_hochberg, split_periods,
                          Hypothesis)
from src.critic import review, _temporal_stability, _concentration
from src.features_momentum import _rsi
from src.features_candle import add_candle_features
from src.features_volume import add_volume_features
from src.events import suspicious_candles
from config.settings import load_toml, RESULTS_DIR

_R = load_toml("research.toml")

HORIZONS = (60, 120, 240)
COSTS = (0.001, 0.0015, 0.002)   # 0.10% / 0.15% / 0.20% round-trip
RSI_THRESHOLD = 25.0
MIN_DF_ROWS = 660                # rsi warmup + 240-мин таргет + запас

COND = f"pl.col('rsi_14') < {RSI_THRESHOLD}"


# ---------------------------------------------------------------------------
# Self-check: forward target формула идентична events._future_metrics
# ---------------------------------------------------------------------------

def _forward_targets(df: pl.DataFrame, h: int) -> pl.DataFrame:
    """Каноническая формула events._future_metrics для одного горизонта.

    entry_price = open(T+1)
    return_{h}m = close(T+h) / entry_price - 1
    """
    return df.with_columns([
        pl.col("open").shift(-1).alias("entry_price"),
        (pl.col("close").shift(-h) / pl.col("open").shift(-1) - 1)
        .alias(f"return_{h}m"),
    ])


def _selfcheck() -> None:
    """Сверка формулы на синтетике против событий events._future_metrics."""
    from src.events import _future_metrics as _fm
    n = 30
    df = pl.DataFrame({
        "open_time": pl.datetime_range(
            datetime(2026, 1, 1), datetime(2026, 1, 1, 0, n - 1),
            interval="1m", eager=True),
        "open": pl.Series(range(1, n + 1)).cast(pl.Float64),
        "high": pl.Series(range(1, n + 1)).cast(pl.Float64) + 0.5,
        "low": pl.Series(range(1, n + 1)).cast(pl.Float64) - 0.5,
        "close": pl.Series(range(1, n + 1)).cast(pl.Float64) + 0.25,
        "volume": pl.Series([1.0] * n),
        "turnover": pl.Series([1.0] * n),
        "is_green": pl.Series([True] * n),
    })
    mine = _forward_targets(df, 3).select(["open_time", "entry_price", "return_3m"])
    ref = _fm(df).select(["open_time", "entry_price", "return_3m"])
    assert mine.equals(ref), "forward target formula diverges from events._future_metrics"
    # явная проверка чисел: last 3 rows должны иметь null-таргет (нет окна)
    tails = mine["return_3m"].tail(3).to_list()
    assert all(t is None for t in tails), f"ожидались null на хвосте, got {tails}"
    print("  self-check OK: формула forward target == events._future_metrics, "
          "хвост null (нет полного окна)")


# ---------------------------------------------------------------------------
# 1a. TEMPORAL SPLIT
# ---------------------------------------------------------------------------

def split_periods_buffered(events: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """split_periods + исключение событий, чьё forward-окно выходит за конец периода.

    Для каждого периода и каждого horizon: событие с open_time=T остаётся в
    выборке ТОЛЬКО если T + h < end периода (таргет close(T+h) ещё внутри).
    Критично для h=240. sample_periods НЕ меняются; полный events не меняется
    (фильтрация только при формировании выборок).
    """
    raw = split_periods(events)
    out: dict[str, pl.DataFrame] = {}
    for h in HORIZONS:
        buf = timedelta(minutes=h)
        for name, start_s, end_s in _R["sample_periods"]:
            start_dt = datetime.fromisoformat(start_s)
            end_dt = datetime.fromisoformat(end_s)
            key = f"{h}m_{name}" if name != "discovery" else f"{h}m_disc"
            out[key] = raw[name].filter(
                (pl.col("open_time") >= start_dt) &
                (pl.col("open_time") < end_dt - buf))
    return out


# ---------------------------------------------------------------------------
# 1. LOAD DATA + BUILD EVENTS (тот же контур, что build_events)
# ---------------------------------------------------------------------------

def build_experiment_events(verbose: bool = True) -> pl.DataFrame:
    """События: признаки (полный ряд) -> forward targets (полный ряд) -> prefilter."""
    universe = data_mod.liquidity_universe()
    if verbose:
        print(f"Universe: {universe.height} symbols")

    chunks = []
    n_loaded = 0
    for row in universe.iter_rows(named=True):
        df = data_mod.load_validated(row["symbol"], row["category"])
        if df is None or df.height < MIN_DF_ROWS:
            continue

        # Признаки (<= T, без look-ahead), те же функции, что в pipeline:
        # relative_range (candle) + relative_volume (volume) — для prefilter
        full = add_volume_features(add_candle_features(df))
        full = full.with_columns(_rsi(pl.col("close"), 14))

        # Forward targets на ПОЛНОМ ряду ДО prefilter (events.py:55-58)
        for h in HORIZONS:
            full = _forward_targets(full, h)

        # Существующий prefilter событий (rvol > 3.0 ИЛИ rrange > 3.0 из конфига)
        cand = suspicious_candles(full)

        chunk = cand.select([
            "open_time", "rsi_14", "entry_price",
            *[f"return_{h}m" for h in HORIZONS],
        ]).with_columns(pl.lit(row["symbol"]).alias("symbol"))
        chunks.append(chunk)
        n_loaded += 1
        if verbose and n_loaded % 25 == 0:
            print(f"  loaded {n_loaded} symbols...")

    if verbose:
        print(f"Loaded {n_loaded} symbols")
    events = pl.concat(chunks)
    # события без входа (хвост ряда) исключаются, как в build_events
    events = events.drop_nulls(subset=["entry_price"])
    return events


# ---------------------------------------------------------------------------
# 2. МЕТРИКИ: gross (до издержек) + net (после) — раздельно
# ---------------------------------------------------------------------------

def gross_stats(events: pl.DataFrame, hyp: Hypothesis, cost: float) -> dict:
    """Gross-метрики ДО издержек (справочно; edge требует net > 0 по pipeline).

    Подвыборка — идентична test_hypothesis (тот же фильтр cond + не-null).
    """
    cond = eval(hyp.condition, {"pl": pl})
    sub = events.filter(cond).filter(
        pl.col(hyp.target_column).is_not_null() &
        pl.col("entry_price").is_not_null())
    n = sub.height
    if n == 0:
        return {"n": 0, "mean_gross": np.nan, "median_gross": np.nan,
                "t_gross": np.nan, "winrate_gross": np.nan}
    side = 1.0 if hyp.entry_side == "long" else -1.0
    gross = sub[hyp.target_column].to_numpy() * side
    t, _ = sp_stats.ttest_1samp(gross, 0.0)
    return {"n": n, "mean_gross": float(gross.mean()),
            "median_gross": float(np.median(gross)), "t_gross": float(t),
            "winrate_gross": float((gross > 0).mean())}


def _fmt_ci(ci) -> str:
    try:
        return f"[{ci[0]:+.5f}, {ci[1]:+.5f}]"
    except Exception:
        return "n/a"


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("RSI14 < 25 LONG RESEARCH  (3 horizons x 3 costs)")
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print("=" * 70)

    _selfcheck()

    events = build_experiment_events()
    print(f"Total events: {events.height}")
    print(f"Event columns: {events.columns}")

    # ---- split periods (существующие, НЕ меняются) ----
    periods = split_periods(events)
    print(f"\nDiscovery:  {periods['discovery'].height} events "
          f"({_R['sample_periods'][0][1]}..{_R['sample_periods'][0][2]})")
    print(f"Validation: {periods['validation'].height} events")
    print(f"OOS:        {periods['oos'].height} events")

    # Буфер forward-окна при формировании выборок (см. split_periods_buffered)
    buffered = split_periods_buffered(events)
    for h in HORIZONS:
        print(f"  (h={h:>3}m) buffered: disc={buffered[f'{h}m_disc'].height}, "
              f"val={buffered[f'{h}m_validation'].height}, "
              f"oos={buffered[f'{h}m_oos'].height}")

    # ---- гипотезы: 3 horizons, одна сигнальная фича ----
    hypotheses = []
    for h in HORIZONS:
        hypotheses.append(Hypothesis(
            hypothesis_id=f"RSI14_low_h{h}",
            description=f"rsi_14 < 25 → long {h}m",
            condition=COND,
            entry_side="long",
            horizon_min=h,
            target_column=f"return_{h}m",
        ))

    # ---- discovery: 3 horizons x 3 costs = 9 комбинаций ----
    # Costs — сценарии стоимости ОДНОГО сигнала, не независимые гипотезы.
    # Statistical discovery: БХ ОДИН раз на семье 3 горизонтов при ref-cost.
    # Cost-строки остаются в таблице как sensitivity, verdict один на сигнал.
    q = _R["bh_q"]
    ref_cost = _R["survival_cost"]   # 0.0020 — якорь статистического discovery
    rows: list[dict] = []
    for h in HORIZONS:
        hyp = next(x for x in hypotheses if x.horizon_min == h)
        disc_h = buffered[f"{h}m_disc"]   # выборка без событий с неполным окном
        for cost in COSTS:
            m = test_hypothesis(disc_h, hyp, cost)
            g = gross_stats(disc_h, hyp, cost)
            assert m["n"] == g["n"], "net/gross подвыборки разошлись"
            # temporal stability / concentration по комбинации (диагностика)
            fin = {"hypothesis_id": hyp.hypothesis_id, "horizon_min": h,
                   "entry_side": "long", "condition": hyp.condition,
                   "cost": cost}
            stab = _temporal_stability(disc_h, fin, cost)
            conc = _concentration(disc_h, fin, cost)
            rows.append({
                "horizon_min": h, "cost": cost, "n": m["n"],
                "n_symbols": m["n_symbols"], "n_months": m["n_months"],
                "mean_gross": g["mean_gross"], "t_gross": g["t_gross"],
                "winrate_gross": g["winrate_gross"],
                "mean_net": m["mean_net"], "median_net": m["median_net"],
                "t_stat": m["t_stat"], "p_value": m["p_value"],
                "winrate": m["winrate"], "ev_annualized": m["ev_annualized"],
                "bootstrap_ci": m["bootstrap_ci"], "cluster_ci": m["cluster_ci"],
                "t_hac": m["t_hac"],
                "stab_pos_share": stab["pos_share"], "stab_n": stab["n"],
                "conc_top1": conc["top1_share"], "conc_n_symbols": conc["n_symbols"],
                "verdict": "", "failed_gates": [],
            })

    # ---- statistical discovery: БХ один раз (3 горизонта) при ref_cost ----
    ref_idx = [i for i, r in enumerate(rows) if abs(r["cost"] - ref_cost) < 1e-12]
    p_arr = np.nan_to_num([rows[i]["p_value"] for i in ref_idx], nan=1.0)
    sig_bh = benjamini_hochberg(p_arr, q)
    signal_verdict: dict[int, str] = {}
    for hor_pos, i in enumerate(ref_idx):
        r = rows[i]
        fails = []
        if not sig_bh[hor_pos]:
            fails.append(f"BH(p={r['p_value']:.3f})")
        if r["n"] < _R["min_events"]:
            fails.append(f"n={r['n']}<{_R['min_events']}")
        if r["n_symbols"] < _R["min_unique_symbols"]:
            fails.append(f"sym={r['n_symbols']}<{_R['min_unique_symbols']}")
        if r["n_months"] < _R["min_months"]:
            fails.append(f"mo={r['n_months']}<{_R['min_months']}")
        if r["t_stat"] < _R["min_t_stat"]:
            fails.append(f"t={r['t_stat']:.2f}<{_R['min_t_stat']}")
        if r["mean_net"] <= 0:
            fails.append(f"net={r['mean_net']:.6f}<=0")
        signal_verdict[r["horizon_min"]] = "CANDIDATE" if not fails else "FAIL"
        rows[i]["failed_gates"] = fails

    # verdict сигнала распространяется на все 3 cost-строки его горизонта
    for r in rows:
        r["verdict"] = signal_verdict[r["horizon_min"]]

    candidates = [h for h, v in signal_verdict.items() if v == "CANDIDATE"]
    n_cand = len(candidates)

    # ---- сводная таблица 9 комбинаций ----
    print("\n" + "=" * 70)
    print(f"DISCOVERY: 9 COMBINATIONS (3 horizons x 3 costs; "
          f"BH once at ref_cost={ref_cost})")
    print("=" * 70)
    hdr = (f"  {'h':>4} {'cost':>6} {'n':>7} {'sym':>4} {'mo':>3} "
           f"{'gross':>9} {'net':>9} {'t_net':>6} {'t_hac':>6} {'wr':>6} "
           f"{'boot_ci':>20} {'clust_ci':>20} {'stab':>6} {'top1':>6} verdict")
    print(hdr)
    for r in rows:
        print(
            f"  {r['horizon_min']:>4} {r['cost']:>6.3f} {r['n']:>7} "
            f"{r['n_symbols']:>4} {r['n_months']:>3} "
            f"{r['mean_gross']:>+9.5f} {r['mean_net']:>+9.5f} "
            f"{r['t_stat']:>6.2f} {r['t_hac']:>6.2f} {r['winrate']:>6.1%} "
            f"{_fmt_ci(r['bootstrap_ci']):>20} {_fmt_ci(r['cluster_ci']):>20} "
            f"{r['stab_pos_share']:>6.0%} {r['conc_top1']:>6.0%} "
            f"{r['verdict']}")
    print(f"\nDiscovery candidates (signal-level): {n_cand}")
    print("NB: mean_gross — ДО издержек, справочно. Положительный gross НЕ "
          "является торговым edge: edge = net > 0 после применимой комиссии.")
    print("NB: verdict — статистический discovery сигнала (БХ по 3 горизонтам "
          "при ref_cost). Cost-строки — sensitivity к издержкам, не доп. гипотезы.")

    if n_cand == 0:
        print("\nGate failures (per-horizon, ref_cost):")
        for i in ref_idx:
            r = rows[i]
            if r["failed_gates"]:
                print(f"  h{r['horizon_min']:>3}: " + ", ".join(r["failed_gates"]))

    # ---- границы периодов для 240m (методологическая заметка) ----
    print("\n" + "=" * 70)
    print("PERIOD BOUNDARY NOTE (horizon 240m)")
    print("=" * 70)
    boundary_notes = []
    for name, start_s, end_s in _R["sample_periods"]:
        if name == "oos":
            continue
        end_dt = datetime.fromisoformat(end_s)
        # события, исключённые буфером split_periods_buffered
        # (вход <= 240 мин до границы периода: форвард-окно выходит за конец)
        excluded = events.filter(
            (pl.col("open_time") >= end_dt - timedelta(minutes=240)) &
            (pl.col("open_time") < end_dt)).height
        msg = (f"  {name}->: {excluded} событий исключены буфером форвард-окна "
               f"(вход в последние 240 мин периода, граница {end_s}).")
        print(msg)
        boundary_notes.append({"period": name, "end": end_s,
                               "n_excluded_240m": int(excluded)})
    n_tail = events.filter(pl.col("return_240m").is_null()).height
    print(f"  Всего событий без полного 240m-окна (хвост данных, исключены "
          f"буфером): {n_tail}")
    boundary_notes.append({"period": "tail", "n_no_240m_window": int(n_tail)})

    # ---- кандидаты: validation + OOS + critic ----
    out = {
        "experiment": "rsi14_low_long_60_120_240m",
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "condition": COND, "entry_side": "long",
        "horizons": list(HORIZONS), "costs": list(COSTS),
        "q_bh": q, "n_hypotheses": len(hypotheses),
        "n_events_total": events.height,
        "n_events": {k: v.height for k, v in periods.items()},
        "discovery_results": rows,
        "candidates": [], "validation": {}, "oos": {}, "critic": {},
        "boundary_notes": boundary_notes,
        "note_gross_not_edge": ("gross — до издержек, справочно; торговый edge "
                                "только при net>0 по применяемой комиссии"),
    }

    for h in candidates:
        hid = f"RSI14_low_h{h}"
        hyp = next(x for x in hypotheses if x.hypothesis_id == hid)
        cost = ref_cost
        mv = test_hypothesis(buffered[f"{h}m_validation"], hyp, cost)
        mo = test_hypothesis(buffered[f"{h}m_oos"], hyp, cost)
        out["candidates"].append(hid)
        out["validation"][hid] = mv
        out["oos"][hid] = mo

        result = {
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
            "q_bh": q, "cost_survival": cost, "n_hypotheses": len(hypotheses),
            "n_events_total": events.height,
            "n_events": {k: v.height for k, v in periods.items()},
            "discovery_results": [dict(x) for x in rows
                                  if abs(x["cost"] - cost) < 1e-12],
            "candidates": [hid],
            "validation": {hid: mv}, "oos": {hid: mo},
            "finalist": {"hypothesis_id": hid, "horizon_min": h,
                         "entry_side": "long", "condition": COND,
                         "description": hyp.description},
            "verdict": "CANDIDATE",
        }
        verdict = review(result, events)
        out["critic"][hid] = verdict.to_dict()

        print("\n" + "=" * 70)
        print(f"CANDIDATE {hid}  ref_cost={cost:.3f}")
        print("=" * 70)
        mv_str = (f"n={mv['n']} net={mv['mean_net']:+.5f} t={mv['t_stat']:.2f} "
                  f"wr={mv['winrate']:.1%}")
        mo_str = (f"n={mo['n']} net={mo['mean_net']:+.5f} t={mo['t_stat']:.2f} "
                  f"wr={mo['winrate']:.1%}")
        print(f"  Validation: {mv_str}")
        print(f"  OOS:        {mo_str}")
        for name_c, ok_c, detail in verdict.results:
            status = "PASS" if ok_c is True else "FAIL" if ok_c is False else "UNKNOWN"
            print(f"  [{status}] {name_c}: {detail}")
        print(f"  VERDICT: {'PASS' if verdict.passed else 'REJECT'}"
              + (f" (reason: {verdict.fail_reason})" if not verdict.passed else ""))

    # ---- сохранение: уникальное имя, ничего не перезаписывает ----
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RESULTS_DIR / f"rsi_horizon_research_{ts}.json"
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    print(f"\nSaved: {path}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()