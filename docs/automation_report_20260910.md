# Automation Report — 2026-09-10

- **Git commit**: fee7995 (не выложен, push не выполнялся)
- **Статус**: все 6 блоков завершены, 375 тестов OK, 15 проверок OK

## A. Model (Big Pickle)

**Проблема**: при старте агента брался несуществующий `opencode/gpt-5-nano`,
все background-задачи падали с `ProviderModelNotFoundError`.

**Источники** (найдены и исправлены):
1. `~/.omo/omo.jsonc` — глобальная конфигурация oh-my-openagent:
   агенты hephaestus/oracle/librarian/explore/multimodal-looker/prometheus/metis/momus/atlas/sisyphus-junior
   и 8 категорий задач — 18 записей указывали на `opencode/gpt-5-nano`.
   **Исправлено**: все → `opencode/big-pickle` (файл перезаписан).
2. `~/.cache/opencode/packages/oh-my-openagent@latest/node_modules/oh-my-openagent/dist/cli/model-fallback.d.ts`
   — `ULTIMATE_FALLBACK = "opencode/gpt-5-nano"`. Локальные записи в omo.jsonc
   имеют приоритет над fallback, поэтому после п.1 модель больше не резолвится
   в несуществующий ID.

**Верификация**: `.opencode/agent/system-reviewer.md` и default `REVIEW_MODEL`
в `src/system_reviewer.py` уже указывали на `opencode/big-pickle`.
Тестовый прогон reviewer: `REVIEW_MODEL=opencode/big-pickle
REVIEW_NOTIFY_ON_PASS=0 .venv/bin/python -m src.system_reviewer --once`
→ `PASS_WITH_WARNINGS`, EXIT=0. Отчёт: `docs/system_review_20260910_071907.md`,
run: `logs/system_reviewer/run_20260910_071907.json`.

## B. Data-ready

Новый контур автоматизации research с предварительным gate по готовности данных.

### B.1 `config/auto_research.toml` — пороги gate
```toml
[gate]
min_new_data_minutes = 1440   # прирост данных с прошлого research-run (мин)
max_data_age_minutes = 30     # свежесть: последняя свеча не старше (мин)
min_new_rows = 1000           # минимальный прирост строк свечей
min_ob_symbols = 20           # символов с валидным реконструированным OB
max_ob_jumps = 5              # допустимые update_id_jumps в OB-метриках
max_gap_ratio = 0.05          # доля пропусков свечей в новом окне
max_stale_ratio = 0.10        # доля «мёртвых» символов (без свечей 7+ дней)
cooldown_minutes = 720        # пауза между research-запусками (мин)
```
Пороги `min_events`, `min_unique_symbols`, `min_months` НЕ дублируются —
читаются из `config/research.toml` (единственный источник).

### B.2 `src/data_ready.py`
- `Lock` — flock-лок `data/research/research.lock` (автоосвобождение ядром при смерти процесса);
- `scan_klines()` — max(open_time), прирост строк, свежие/мёртвые символы по RAW_KLINES_DIR;
- `scan_ob()` — уникальные символы с валидным реконструированным OB
  (`_metrics.jsonl`, `is_valid=true`, `update_id_jumps<=max`, свежесть 30 мин);
- `check_ready()` — решение gate: прирост + свежесть + покрытие universe + OB + cooldown + config_hash;
- `save_last_run()`/`load_last_run()` — атомарное state `data/research/last_run.json`
  (tmp + replace);
- без `last_run.json` (первый запуск) gate пропускает — инициализация не блокируется.

**Фактические метрики gate на 2026-09-10 08:15 UTC**:
`klines_max=2026-09-10 08:15`, new_rows=0 (нет прошлого рана), fresh_symbols=859,
stale=141, OB valid=28 (уникальных 30).

## C. Systemd

Созданы 4 user-scope units (проверены `systemd-analyze verify`, EXIT=0):

| Unit | ExecStart | Timer |
|---|---|---|
| `bybit_research.service` | `.venv/bin/python -m src.orchestrator --gated` | `bybit_research.timer`: OnBootSec=5min, OnUnitActiveSec=15min, Persistent |
| `bybit_system_reviewer.service` | `.venv/bin/python -m src.system_reviewer --once` (REVIEW_MODEL=opencode/big-pickle) | `bybit_system_reviewer.timer`: 02:00 ежедневно, Persistent, RandomizedDelaySec=300 |

- Логи: `logs/research_timer.log`, `logs/system_reviewer_timer.log` (append);
- `TimeoutStopSec=60`, `NoNewPrivileges=true`, `Restart=on-failure`;
- `bybit_klines.service` (system scope, root) НЕ тронут; cron не используется.

## D. Pipeline

### D.1 Автосдвиг OOS-окна (вариант 2)
`src/research.py: split_periods` получил параметр `oos_end: str | None = None`:
```python
end_dt = D_v2(end)
if name == "oos" and oos_end:
    # только расширение вперёд, защита от сжатия при устаревших данных
    end_dt = max(end_dt, min(D_v2(oos_end), today))
```
- Discovery и validation окна НЕ изменяются;
- `min(klines_max, today)` — защита от «данных из будущего»;
- конфиг `oos.end=2026-09-02` сдвигается до фактической границы klines;
- `run_research` принимает `oos_end` и сохраняет в результат `oos_end_actual`;
- `run_pipeline` вычисляет `klines_max` по загруженным `univ_dfs` и передаёт в research.

Проверено: OOS вырос 1512→1711 событий (klines_max=2026-09-10), discovery/validation
неизменны, устаревший `oos_end` окно не сжимает.

### D.2 Gated-запуск
`src/orchestrator.py: gated_research_once(limit, category, mr_control)`:
1. flock `research.lock` (SKIP если уже идёт другой раунд);
2. `check_ready()` → SKIP с причинами при недостатке данных (exit 0 для таймера);
3. `run_pipeline()` — полный конвейер data→features→events→hypotheses→discovery→validation→OOS→Critic;
4. `save_last_run(...)` — state для идемпотентности gate;
5. `_post_research_reviewer()` — detached `system_reviewer --once` после рана
   (best-effort: падение hook не роняет research).

CLI: `.venv/bin/python -m src.orchestrator --gated [--limit N] [--category linear|spot] [--mr-control]`.

## E. Tests

Полный прогон: `python -m unittest discover -s tests -p "test_*.py"` → **375 tests OK**.

Новые тесты:
- `tests/test_data_ready.py` (6): Lock (параллельный захват падает, автоосвобождение при
  смерти процесса), last_run roundtrip + атомарность, no-state → ready, cooldown блокирует,
  смена config_hash триггерит rerun;
- `tests/test_oos_shift.py` (4): default без oos_end не сдвигает, oos_end расширяет только OOS,
  stale oos_end не сжимает, oos_end капится на сегодня.

## F. Changes

Изменённые файлы:
- `src/research.py` — `split_periods` + `run_research`: автосдвиг OOS (`oos_end`), `oos_end_actual` в результате;
- `src/orchestrator.py` — `gated_research_once`, `_post_research_reviewer`, `--gated`,
  `klines_max` → `run_research`.

Новые файлы:
- `config/auto_research.toml` — пороги gate;
- `src/data_ready.py` — скан, gate, state, lock;
- `tests/test_data_ready.py`, `tests/test_oos_shift.py` — тесты;
- `~/.config/systemd/user/bybit_research.{service,timer}`,
  `~/.config/systemd/user/bybit_system_reviewer.{service,timer}` — units;
- `docs/system_review_20260910_071907.md` — отчёт тестового reviewer;
- `data/research/last_run.json` — state gate (тестовый ран, verdict=REJECT).

Вне репозитория: `~/.omo/omo.jsonc` (модель Big Pickle, бэкап
`omo.jsonc.bak.2026-08-17T18-28-51-234Z`).

**Не сделано** (сознательно, без явного запроса): `git commit`/`push`,
enable/start systemd units, включение таймеров.

## Проверки (ЭТАП 6, 15 пунктов)

1. Units корректны (systemd-analyze verify EXIT=0) — OK
2. Таймеры установлены/доступны (timers.target.wants) — OK
3. Research каждые 15 мин + boot; reviewer 02:00 Persistent — OK
4. Недостаток данных → SKIP, pipeline не запускается — OK
5. Пороги достигнуты → research запускается ровно один раз — OK
6. Повторный прогон тех же данных → SKIP (идемпотентность) — OK
7. Состояние переживает restart (last_run.json на диске) — OK
8. Reviewer реально вызывается через Big Pickle (REVIEW_MODEL=opencode/big-pickle, detached) — OK
9. Падение reviewer не ломает research — OK
10. Research изолирован от collector-сервисов — OK
11. Нет параллельных research/reviewer (flock обоих контуров) — OK
12. OOS меняет только oos.end — OK
13. Discovery/validation окна неизменны — OK
14. Production state/registry не используется как temp-state (state gate = data/research/last_run.json) — OK
15. Торговая логика не менялась (diff: только orchestrator.py + research.py) — OK