# Research Controller — архитектурный дизайн

Статус: **design only, код не реализован**.
Цель: `research → diagnose → choose experiment → run → validate → OOS → Critic → compare → next experiment`.
Это контроллер **исследований**, не генератор стратегии и не auto-trader.

---

## 0. Найденная текущая архитектура

### Точки входа (`src/orchestrator.py`)
- `run_pipeline(limit, category, mr_control)` — полный прогон, возвращает acceptance report.
- `gated_research_once(limit, category, mr_control)` — data-ready gate → `run_pipeline` → reviewer hook; идемпотентен (`flock` + `last_run.json`); SKIP при нехватке данных.
- `auto_research_loop(interval_sec, limit)` — бесконечный цикл (сейчас не используется).
- CLI: `--gated`, `--auto`, `--limit`, `--category`, `--mr-control`.

### Этапы pipeline
1. Universe (`data.liquidity_universe`, фильтры `category`/`limit`).
2. Data + features + events.
3. `stage_data_validation` — STOP.
4. `stage_feature_validation` — PASS/ERROR (warnings).
5. `stage_ob_feature_validation` — per-feature coverage; INSUFFICIENT_DATA; **NON_BLOCKING** (`pipeline.NON_BLOCKING_STAGES`).
6. Candle triggers → OB capture.
7. **Генерация гипотез**: baseline `research.HYPOTHESES` (H001–H008) + `hgen.generate_hypotheses(default_rules + ob_rules)` + опц. `_mr_hypotheses()` (MR_SMA120_*). OB-непригодные гипотезы отсеиваются по `ineligible_features`.
8. `research.run_research` — discovery sweep → BH(FDR) → validation → OOS. Кандидаты: BH ∧ `n≥min_events` ∧ `n_symbols≥min_unique_symbols` ∧ `n_months≥min_months` ∧ `t_stat≥min_t_stat` ∧ `mean_net>0`.
9. `stage_validation_gate` (по кандидату).
10. `stage_oos_gate` (по прошедшему validation).
11. Finalist = первый прошедший OOS.
12. `freeze_finalist` + `stage_parameter_freeze` (защита от parameter drift).
13. `stage_critic` → `critic.review` (10 проверок; `all(ok is True)`).
14. **Registry update только при Critic PASS** (`registry.update_from_research`).
15. Acceptance report → `data/research/results/acceptance_{run_id}.json`.
16. Shadow paper (MODE B, CANDIDATE) — §13.
17. **Auto trigger MODE A (§14)** — `auto_paper_trigger`, запускает PAPER при любом непустом `eligible_a` (**риск: не зависит от вердикта**).

### Доменные типы и модули
- `research.Hypothesis` (dataclass): `hypothesis_id, description, condition(polars-str), entry_side, horizon_min, target_column, stop_loss, mae_column, version, status`.
- `research.test_hypothesis(events, hyp, cost)` — net_ret = `side*return_h − cost`; t/p; dependency stats (HAC, bootstrap).
- `research.benjamini_hochberg` — FDR внутри прогона.
- `research.split_periods` — discovery/validation/oos; `oos.end` расширяется до `klines_max` (вариант 2).
- `critic.review` — проверки: leakage, ob_temporal_leakage, multiple_testing, sample_size, dependency, temporal_stability, costs, concentration, validation, oos. `fail_reason` = первая не-True проверка.
- `registry.HypothesisLifecycle` — статусы CANDIDATE→VALIDATED→ACTIVE→DEPRECATED→ARCHIVED; `update_from_research` (переход в VALIDATED только если discovery+validation+OOS пройдены, пороги захардкожены: `mean_net>0, n≥100, t≥2.0`).
- `data_ready.check_ready` — gate: cooldown, `config_hash`, свежесть (30 мин), прирост (1440 мин / 1000 строк), fresh symbols, OB coverage. `Lock` (flock `research.lock`), `load/save_last_run`.

### Конфиги
- `research.toml` — sample_periods, cost_grid `[0.001,0.0015,0.002,0.003]`, survival_cost 0.002, bh_q 0.05, min_events 100, min_unique_symbols 5, min_months 3, min_t_stat 2.0, stability_pos_share 0.6, max_symbol_concentration 0.5.
- `hypothesis_generator.toml` — id_prefix, min_events, thresholds, max_conditions_per_hyp=2, max_features_per_hyp=2, dedup_by.
- `auto_research.toml[gate]` — min_new_data_minutes=1440, max_data_age_minutes=30, min_new_rows=1000, min_ob_symbols=20, max_ob_jumps=5, cooldown_minutes=720.
- `ob_hypothesis_rules.toml` — OB-whitelist, min_feature_coverage=0.5, min_feature_valid_events=100.
- `config_hash` (data_ready) = sha256(`research.toml`+`auto_research.toml`)[:12]; `pipeline.compute_config_hash` = sha256(`research.toml`)[:12].

### State-файлы
- `data/research/last_run.json` — run_id, verdict, config_hash, git_head, klines_max, ob_valid_symbols.
- `data/research/registry.json` — жизненный цикл гипотез.
- `data/research/research.lock` — flock.
- `data/research/results/*.json` — acceptance + research отчёты.

### Что уже есть против overfitting
BH-контроль FDR внутри прогона; фиксированная cost-grid; фиксированные периоды; OOS-гейт; Critic; parameter freeze; обязательный data-ready gate.
**Чего нет**: межиспытательного (межexperiment) учёта множественного тестирования, бюджета экспериментов, дедупликации конфигураций, истории экспериментов, автоматического диагноза REJECT.

---

## 1. Experiment

Единица работы контроллера. Каждый запуск создаёт неизменяемую запись.

```python
@dataclass
class Experiment:
    experiment_id: str            # EXP_{YYYYMMDDTHHMMSSZ}
    parent_experiment_id: str|None
    cycle_id: str                 # привязка к dataset boundary (см. §4)
    config_hash: str              # hash экспериментального спека (см. §4), НЕ research.toml
    mode: str                     # BASELINE|THRESHOLD_SWEEP|HORIZON_SWEEP|REGIME_SWEEP|CONDITIONAL|FAMILY_SWITCH
    parameters: dict              # оси поиска этого эксперимента (family, threshold, horizon, regime, combination, universe)
    hypothesis_specs: list[dict]  # сериализованные Hypothesis
    created_at_utc: str
    dataset_boundary: dict        # klines_max, data_version, n_events по периодам
    status: str                   # RUNNING|DONE|FAILED|SKIPPED
    discovery_result: dict|None   # сводка discovery_results (top-N + агрегаты)
    validation_result: dict|None
    oos_result: dict|None
    critic_result: dict|None      # verdict + fail_reason + results
    final_verdict: str|None       # PASS|REJECT|NO_CANDIDATE|STOP|ERROR|SKIP|INSUFFICIENT_DATA
    reject_diagnosis: dict|None   # см. §3
    acceptance_report: str|None   # путь к acceptance_*.json
```

**Хранение**
- Журнал (append-only, JSONL): `data/research/experiments.jsonl` — одна строка на эксперимент, дописывается дважды: при старте (`status=RUNNING`) и при завершении (`DONE/FAILED`).
- Полные артефакты: `data/research/experiments/{experiment_id}.json` (полная запись + указатели на research/acceptance).
- Текущее состояние контроллера: `data/research/controller_state.json`.

---

## 2. Разрешённые оси поиска

Контроллер меняет **только** перечисленное. Приёмка (`research.toml`: `min_t_stat`, `bh_q`, `min_events`, cost-grid и т.д.) **не меняется** — иначе нарушение инварианта 11.

| Ось | Что меняется | Безопасный диапазон (дискретный) | Источник |
|---|---|---|---|
| **hypothesis family** | набор правил | `baseline` (H001–H008) · `generator` (`default_rules`) · `ob` (`ob_rules`) · `mr` (`_mr_hypotheses`) | `research.HYPOTHESES`, `hgen.default_rules/ob_rules`, orchestrator `_mr_hypotheses` |
| **threshold** | порог **условия признака** (не приёмки!) | по правилу: `relative_volume{2,3,4}`, `relative_range{2,3}`, `rsi_14{a:60..75, b:25..40}`, `mr_sma120{−0.01,−0.02,−0.03,−0.05}`, `corr_btc_60{0.3,0.5}`, `breakout_20{0.5,1.0}`, `upper_wick/lower_wick{0.002,0.003}` | `hgen._THR_ABS`, `orchestrator._mr_hypotheses` |
| **horizon** | `horizon_min` | `{5, 10, 30}` (только существующие `return_{h}m`/`mfe_`/`mae_` из `features.toml future_horizons_min`) | `features.toml` |
| **regime filter** | `AND` с режимом | `volatility_regime ∈ {low,normal,high,very_high}`, `btc_trend_regime {>0,<0}`, `trend_regime {trend,range}`, `liquidity_regime`, `volume_regime` | `features_regime.py`, `features_volatility.py` |
| **condition combination** | `AND` 2 условий | не более `max_conditions_per_hyp=2`, `max_features_per_hyp=2`; только whitelisted признаки; только из уже существующих условий | `hypothesis_generator.toml` |
| **universe filter** | подмножество символов | `category ∈ {linear, spot}`, symbol whitelist из `liquidity_universe`; **не** сужать до < `min_unique_symbols=5` | `liquidity.toml`, `data.liquidity_universe` |

Запрещено менять: приёмочные gates/Critic, trading execution, `paper.py`, `cost_grid`, `sample_periods`, добавлять ML.

---

## 3. Режимы и стратегии переключения

| Режим | Что делает | Когда выбирается |
|---|---|---|
| `BASELINE` | текущий стандартный research (baseline+generator+mr), без изменений | нет предыдущего эксперимента / после явного сброса |
| `THRESHOLD_SWEEP` | один признак, соседние значения порога из дискретной сетки | REJECT по `costs`/discovery при **положительном, но слабом** t у части гипотез; либо кандидат прошёл discovery, но провалил validation |
| `HORIZON_SWEEP` | тот же condition, соседний горизонт | REJECT, где t растёт/меняется с горизонтом; провал validation/OOS при прошедшем discovery |
| `REGIME_SWEEP` | `AND` с режимным фильтром | провал `temporal_stability` / `concentration` / `dependency`; нестабильность по месяцам |
| `CONDITIONAL` | комбинация двух существующих условий (≤2) | одиночные гипотезы близки к порогу, но не проходят; нужен более селективный вход |
| `FAMILY_SWITCH` | переход к другой семье гипотез | «нет кандидатов вообще», все t отрицательные, или семья исчерпана бюджетом |

### Диагноз REJECT → режим
Контроллер классифицирует причину из acceptance report (`reject_reasons`) и Critic (`fail_reason`):

| Наблюдаемая причина | Диагноз | Следующий режим |
|---|---|---|
| `costs: сильнейший discovery t ≤ min_t_stat`, все t ≤ 0 | `no_signal` | `FAMILY_SWITCH` |
| Часть гипотез t>0, но < порога; кандидатов нет | `weak_signal` | `THRESHOLD_SWEEP` |
| Кандидат есть, `validation_gate` REJECT | `validation_fail` | `THRESHOLD_SWEEP` / `REGIME_SWEEP` |
| Validation PASS, `oos_gate` REJECT | `oos_fail` | `HORIZON_SWEEP` / `FAMILY_SWITCH` (OOS **не** используется для подбора значений) |
| Critic: `temporal_stability` FAIL | `unstable` | `REGIME_SWEEP` |
| Critic: `concentration`/`dependency` FAIL | `concentrated` | `CONDITIONAL` / universe filter |
| Critic: `costs` FAIL при t>0 | `cost_sensitive` | `REGIME_SWEEP` / `CONDITIONAL` |
| `NO_CANDIDATE` | `no_candidates` | другая ветвь той же оси, затем `FAMILY_SWITCH` |
| `ERROR`/`STOP` | `infra` | не выбирать эксперимент; вернуться в `READY`/`WAIT_DATA` |

**Правило выбора**: режим выбирается *по провалившейся проверке*, а **не** по максимуму t-статистики. Внутри режима выбирается первый допустимый (не повторявшийся, в рамках бюджета) вариант из заранее объявленной сетки. Никогда не выбирать конфигурацию по результату OOS.

---

## 4. Anti-overfitting

- **Лимит экспериментов на dataset boundary**: `max_experiments_per_boundary` (напр. 12). `cycle_id = f"{data_version}:{klines_max.date()}:{gate_config_hash}"`. При смене boundary счётчик сбрасывается, история сохраняется.
- **Запрет повторять конфигурацию**: `config_hash` эксперимента (sha256 от `mode + sorted(parameters) + hypothesis_specs`, БЕЗ OOS) хранится в ledger; повторный запуск отклоняется (`SELECT_NEXT` берёт следующий вариант).
- **Experiment history**: append-only `experiments.jsonl`; все решения и результаты сохраняются.
- **Multiple-testing accounting**: в ledger ведётся кумулятивный `n_tests_cumulative` (число оценённых гипотез за cycle). BH внутри прогона сохраняется; при исчерпании бюджета тестов контроллер останавливается (`STOPPED`), а не продолжает перебор. (Опционально: ужесточение `q` пропорционально бюджету — только с отдельным разрешением, т.к. это приёмка.)
- **Независимый OOS**: OOS-окно формируется штатно (`split_periods`, вариант 2), OOS-метрики **не участвуют** в выборе следующего эксперимента (инвариант 6/7). Выбор использует только discovery/validation.
- **Фиксированные cost assumptions**: `cost_grid_round_trip` + `survival_cost` не меняются.
- **Останов при исчерпании search budget**: `STOPPED` + отчёт.
- **Не гнаться за t-stat**: selection policy — rule-based по диагнозу (§3), с дедупликацией и бюджетом. Никакого аргмакса по t.

---

## 5. State machine

```
                ┌─────────┐
                │ STOPPED │◄──────────── manual stop / budget exhausted
                └────▲────┘
                     │
   ┌───────┐  gate   ├──────────┐
   │ READY ├───────► │ WAIT_DATA│
   └───┬───┘  fail   └────┬─────┘
       │ gate ok            │ next check
       ▼                    └──────► READY
   ┌─────────┐
   │ RUNNING │  (experiment RUNNING в журнале, lock held)
   └────┬────┘
        │ acceptance report
        ├── full PASS ─────────────► ┌──────────────────┐
        │                            │ PASS_PENDING_PAPER│
        ├── REJECT ───► ┌──────────────────┐            │
        │               │ REJECT_DIAGNOSIS │            │
        │               └────────┬─────────┘            │
        │                        │ budget ok            │ manual authorize
        │                        ▼                      ▼
        │               ┌──────────────┐          (PAPER — вне контроллера)
        │               │ SELECT_NEXT  │                │
        │               └──────┬───────┘                │
        │                      │ есть вариант           │
        │                      └──────► RUNNING         │
        │                                              │
        └── ERROR/STOP/infra ──────────────────────────┘
```

| Состояние | Смысл | Выходы |
|---|---|---|
| `READY` | контроллер свободен, можно проверять gate | → `WAIT_DATA`, → `RUNNING` |
| `RUNNING` | pipeline выполняется (журнал `RUNNING`, lock) | → `PASS_PENDING_PAPER`, `REJECT_DIAGNOSIS`, `STOPPED` |
| `REJECT_DIAGNOSIS` | разбор acceptance/Critic | → `SELECT_NEXT`, `STOPPED` |
| `SELECT_NEXT` | выбор режима+параметров (§3), проверка бюджета/дедупа | → `RUNNING`, `STOPPED` |
| `WAIT_DATA` | gate вернул SKIP | → `READY` |
| `PASS_PENDING_PAPER` | **полный PASS текущего цикла**; PAPER НЕ запущен | → manual authorize (вне контроллера), `STOPPED` |
| `STOPPED` | бюджет/ручная остановка | терминальное |

**PAPER** — отдельное **ручное** разрешение после `PASS_PENDING_PAPER`. Контроллер никогда не вызывает PAPER; auto-trigger MODE A (§14 `orchestrator`) должен быть отключён (см. §8).

---

## 6. Recovery

- **Падение процесса**: эксперимент остаётся в журнале как `RUNNING`. При следующем старте контроллер находит «висящий» `RUNNING` без завершающей записи → помечает `FAILED` (`interrupted`). Его `config_hash` остаётся в ledger → **нельзя повторно считать тот же experiment новым** (инвариант «не re-count»).
- **История не теряется**: `experiments.jsonl` append-only; `controller_state.json` пишется атомарно (tmp+replace, как `save_last_run`). При отсутствии/повреждении `controller_state.json` состояние восстанавливается из хвоста журнала.
- **Lock от параллельных запусков**: отдельный `data/research/controller.lock` (flock, по образцу `data_ready.Lock`). Плюс существующий `research.lock` защищает сам research-прогон. Двойная защита: controller.lock (selection/state) + research.lock (gate/run).
- **Идемпотентность**: повторный старт с тем же `experiment_id`/`config_hash` не создаёт дубликат (проверка ledger до RUNNING).

---

## 7. Dashboard / report

**На эксперимент** (`experiments/{id}.json` + `experiments.jsonl`) сохраняется:
- почему **предыдущий** experiment отклонён (`reject_diagnosis`: категория, конкретные `reject_reasons`, прошедшие/проваленные гейты);
- какой режим выбран и почему (mapping §3);
- какие параметры изменены относительно родителя (`parameters` diff vs `parent_experiment_id`);
- результат (discovery summary, validation, OOS, Critic verdict/fail_reason, final_verdict);
- почему выбран **следующий** эксперимент (selection rationale + бюджет).

**Dashboard** (`src/dashboard/*`, read-only) добавляет блок «Research Controller»:
текущее состояние, cycle_id, бюджет (использовано/лимит, `n_tests_cumulative`), последние N экспериментов (mode, changed params, verdict, reject_diagnosis), указатель на `PASS_PENDING_PAPER`.

Дашборд только читает state-файлы; исполнение не трогает.

---

## 8. Интеграция и изменяемые файлы

### Принцип (минимизация)
Контроллер — тонкий слой **над** существующим pipeline. Он конструирует `Hypothesis` (уже первоклассный тип) и вызывает `run_pipeline`; вся статистика/гейты/Critic/registry не меняются.

### Точки интеграции
1. **Инъекция гипотез** — `run_pipeline` сейчас сам строит гипотезы (§6, строки 259–272). Добавить необязательный параметр `hypothesis_specs: list[Hypothesis] | None` (и/или `experiment`); при задании он заменяет блок генерации. Остальной pipeline без изменений.
2. **Отключение auto-PAPER** — §14 (`auto_paper_trigger`) срабатывает при непустом `eligible_a` и **не зависит от вердикта**. Добавить явный флаг `auto_paper: bool` (Controller передаёт `False`); CLI сохраняет текущее поведение. Это одновременно закрывает инварианты 3/4/5.
3. **Acceptance report** — уже отдаёт `verdict`, `reject_reasons`, `stages`, `candidates`, `finalist`; этого достаточно для диагноза. При желании добавить `experiment_id`/`parent_experiment_id`/`cycle_id` в report.
4. **Universe filter** — уже поддержан (`category`/`limit`); при необходимости расширить symbol whitelist.

### Новые файлы
- `src/research_controller.py` — state machine, диагноз, `SELECT_NEXT`, бюджет/дедуп, journal/lock, recovery.
- `src/experiment.py` (или внутри контроллера) — `Experiment` dataclass, построение `hypothesis_specs` из осей (§2), расчёт `config_hash`.
- `config/research_controller.toml` — `max_experiments_per_boundary`, `max_experiments_per_family`, безопасные сетки осей, mapping диагноз→режим.
- `data/research/experiments.jsonl`, `data/research/experiments/`, `data/research/controller_state.json`, `data/research/controller.lock` — создаются в рантайме.
- `tests/test_research_controller.py` — state machine, диагноз, дедуп, бюджет, recovery, инвариант «OOS не участвует в выборе».

### Изменяемые существующие файлы
| Файл | Изменение |
|---|---|
| `src/orchestrator.py` | опц. `hypothesis_specs` в `run_pipeline`; флаг `auto_paper`; проброс через `gated_research_once`; опц. поля `experiment_id/cycle_id` в report |
| `src/dashboard/server.py` (+ static) | read-only блок Research Controller |
| `tests/test_pipeline.py` | регрессия на инъекцию гипотез и `auto_paper=False` |

### Файлы, которые **не** меняются
`src/research.py`, `src/critic.py`, `src/pipeline.py` (гейты/приёмка), `src/paper.py` (execution), `src/registry.py`, `config/research.toml`, `config/ob_hypothesis_rules.toml`, `config/auto_research.toml`.

### Проверка инвариантов
| # | Инвариант | Как соблюдается |
|---|---|---|
| 1 | Live→Research разрешён | контроллер вызывает только research |
| 2 | Live→Strategy запрещён | не трогает execution/strategy |
| 3 | PAPER не авто после REJECT | `auto_paper=False` + PAPER вне контроллера |
| 4 | PAPER только после полного PASS текущего цикла | `PASS_PENDING_PAPER` требует свежий PASS текущего experiment |
| 5 | Старые VALIDATED ≠ подтверждение цикла | per-experiment переходы; не использовать глобальный `get_eligible_for_paper` |
| 6/7 | OOS не используется для выбора | selection только по discovery/validation |
| 8 | Cost stress и gates обязательны | не меняются, вызываются штатно |
| 9 | Нет auto commit/push | вне контроллера |
| 10 | Trading execution не меняется | `paper.py` не трогается |
| 11 | Приёмка не меняется | меняются только пороги *условий гипотез*, не gates |
| 12 | Нет ML | не добавляется |
| 13 | Пространство поиска ограничено | дискретные сетки + бюджет + дедуп |

---

## Открытые вопросы (требуют решения перед реализацией)
1. **Кумулятивный multiple-testing**: достаточно ли фиксированного `bh_q` + бюджета тестов, или ужесточать `q` по мере расхода бюджета? (последнее — изменение приёмки, нужно отдельное разрешение).
2. **Значения бюджета**: `max_experiments_per_boundary` (предлагается 12) и `max_experiments_per_family`.
3. **`cycle_id`**: привязка к `klines_max.date()` или к смене `data_ready.config_hash`? Предлагается оба (boundary = смена любого).
4. **Universe filter**: разрешать только `linear/spot`, или symbol whitelist тоже?
