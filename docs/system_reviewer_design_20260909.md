# System Reviewer — Design

Дата: 2026-09-09. Репозиторий: `/home/vlad/Документы/построение` (bybit-research).

## Цель

Периодический независимый аудит исследовательской системы через OpenCode в режиме **AUDIT ONLY**.
Reviewer НЕ меняет код, НЕ трогает pipeline, НЕ эволюционирует систему.

## Аудит текущей инфраструктуры (факты)

| Компонент | Статус | Как запускается |
|---|---|---|
| Collector marketdata (Rust) | active | systemd user `bybit_marketdata.service` (Restart=on-failure) |
| OB reconstructor (Rust) | active, 28/30 sym, jumps=0 | systemd user `bybit_ob_reconstructor.service` |
| Klines backfill | active | systemd system `bybit_klines.service` (отдельный репо bybit_rs) |
| Dashboard (Python) | работает PID 1533, :8420 | вручную, НЕТ systemd, НЕТ логов (stdout→socket) |
| Orchestrator | разовый цикл, exit 0 вчера 11:30 | вручную `python -m src.orchestrator` |
| ntfy | NTFY_TOPIC=openagent_trade, server ntfy.sh | `src/notify.py` (stdlib urllib, retry нет) |
| Логи | `logs/orchestrator.log`, `collector/logs/*.log` | периодические (коллектор), разовые (orchestrator) |
| Промежуточные результаты | `data/research/results/research_*.json`, `acceptance_*.json`, `registry.json`, `data/paper/portfolio/state.json` | каждый research cycle |

- OpenCode v1.18.29 в `~/.opencode/bin/opencode`. Доступные модели: `opencode/big-pickle` (рабочая), free-модели. Модель `opencode/gpt-5-nano` НЕ существует — это ломало запуски; reviewer явно передаёт `-m opencode/big-pickle`.
- OpenCode конфигурация: создаём `.opencode/agent/system-reviewer.md` (permission-based AUDIT ONLY).
- Тесты: `python -m pytest tests/` (unittest-стиль, без conftest, 338 тестов). Часть тестов мутирует `registry.json`/`state.json` — reviewer тесты не запускает, только читает результаты.

## Выбранный подход

### Схема

```
[запуск: python -m src.system_reviewer [--once] | systemd timer | cron | вручную]
        ↓
1. flock-лок (data/system_reviewer/system_reviewer.lock) — защита от двойного запуска, авто-освобождение при смерти процесса
2. Сбор детерминированных фактов (context.json): git commit, systemd-статусы, df/free,
   рост parquet, последние research/acceptance verdicts, registry, state.json, хвосты логов
3. Запуск: opencode run --agent system-reviewer -m opencode/big-pickle <context+инструкция>
   (вывод в temp-файл, start_new_session, killpg по таймауту — паттерн advisor.py)
4. Парсинг структурированного вердикта из вывода (JSON с конца — паттерн extract_verdict)
5. Сборка отчёта docs/system_review_YYYYMMDD_HHMMSS.md
   (FACT из контекста + WARNING/RECOMMENDATION из вердикта opencode)
6. Лог запуска в logs/system_reviewer/system_reviewer.log (+ raw вывод в .jsonl)
7. ntfy при FAIL/critical — через существующий src/notify.py; PASS — по REVIEW_NOTIFY_ON_PASS
```

### Ключевые решения

1. **Размещение**: `src/system_reviewer.py`, запуск `python -m src.system_reviewer`.
   - `--once`: один аудит (для systemd timer / cron / ручного запуска).
   - без аргументов: демон-цикл с периодом `REVIEW_INTERVAL` (env, default 21600 сек).
2. **Независимость**: reviewer не импортирует и не вызывает orchestrator/paper/collector;
   только читает их выходы (JSON, логи, файлы) и запускает read-only команды.
3. **Lock от двойного запуска**: `fcntl.flock(LOCK_EX | LOCK_NB)` на файл-лок. При занятости —
   log + exit. flock авто-освобождается ядром при смерти процесса.
4. **Timeout**: `REVIEW_TIMEOUT` (env, default 1800 сек). По таймауту — killpg(SIGKILL) всего
   дерева opencode, запуск считается FAIL (отчёт + ntfy).
5. **AUDIT ONLY гарантия** (два уровня):
   - `.opencode/agent/system-reviewer.md`: permission `edit: deny`, `bash`: только read-only
     команды (git status/log/diff, systemctl status/is-active, df, free, tail, du, ls, find,
     python -m pytest как read-проверка), `write: deny`.
   - Промпт: жёсткая инструкция — НЕ менять ничего, только анализировать и вернуть JSON-вердикт.
   - файл отчёта создаёт scheduler (reviewer), opencode отдаёт только вердикт.
6. **Контекст для OpenCode**: компактный JSON, БЕЗ датасета: git HEAD, статусы systemd,
   df/free/swap, число и рост parquet, последние 5 research/acceptance verdicts, registry,
   paper state (баланс/трейды), хвосты логов. OpenCode сам решает, что читать глубже.
7. **Отчёт**: `docs/system_review_YYYYMMDD_HHMMSS.md` по структуре ТЗ (15 секций);
   разделение FACT (детерминированные измерения) / WARNING (подозрения из вердикта) /
   RECOMMENDATION (рекомендации). Raw verdict и контекст сохраняются в
   `logs/system_reviewer/*.jsonl` для аудита прошлых запусков.
8. **ntfy**: переиспользуется `src/notify.py` (существующий механизм, stdlib urllib, топик из
   .env). FAIL/critical → notify. PASS → только если `REVIEW_NOTIFY_ON_PASS=1`.
9. **Производственный запуск**: systemd unit + timer (user scope), файлы в `deploy/` —
   рассматривается как рекомендуемый вариант; primary — `python -m src.system_reviewer --once`
   из cron/timer (lock защищает от наложений).

### Что НЕ делается (ограничения)

- НЕ меняются: methodology, hypotheses, targets, events, discovery, validation, OOS, BH,
  Critic, gates, transaction costs, risk model, orderbook features, collector architecture,
  universe, Paper execution.
- Reviewer не запускает live trading, не правит Registry, не применяет исправления.
- Цепочка: обнаружение → отчёт → уведомление → решение человека → отдельный implementation run.

## Тесты (детерминированные, без зависимостей от сети/opencode)

- интервал (REVIEW_INTERVAL разбор);
- lock: второй процесс не стартует, пока первый держит flock;
- timeout: открытие subprocess без реального opencode (подмена bin), killpg;
- crash opencode: ненулевой exit → FAIL-отчёт;
- формирование отчёта: структура 15 секций, FACT/WARNING/RECOMMENDATION;
- ntfy failure handling: notify при выключенном NTFY_TOPIC не падает;
- изоляция: reviewer не импортирует src.orchestrator/paper/collector (импорт-тест);
- повторный запуск после ошибки: lock освобождается, второй запуск успешен.