# Об диагностика — 2026-09-10 (диагностика только, без изменений)

Прогон: run_id=20260910T101215Z, verdict=REJECT, 37 гипотез, 4 663 516 events.
Reject-причина: OB-фичи coverage=0.09% (valid_events=4311) < min(50%, 100).

---

## A. OB pipeline: почему 4311 из 4.66M (0.09%)

Цепочка (код):
1. `orchestrator.py` L197: `events = join_ob_features(events)` → `src/orderbook_integration.py`.
2. `_process_symbol` читает `collector/data/market/orderbook/reconstructed/{symbol}/{date}.parquet`
   (`MARKET_DATA_DIR` из `config/settings.py` L31–33 → `collector/data/market`).
3. Нет файлов → все события символа получают `ob_data_quality="missing"` → OB-колонки NULL.
4. Есть файлы → `compute_all_features` → `join_asof(backward)`; качество:
   `lag ≤ 120s → ok`, `≤ 600s → stale`, `gap_detected → gap`, иначе `missing`.

**Корневая причина 0.09% — OB-данные физически покрывают 28 символов × 5 дней × часы-окна, а events — 8.5 месяцев × вся вселенная (743 файла linear.txt).**

Факты (измерено):
- `collector/data/market/orderbook/reconstructed/`: **28 символов**, 140 parquet (28×5),
  даты **09-05, 09-07, 09-08, 09-09, 09-10** (09-06 отсутствует у всех), `_metrics.jsonl` 16.4 МБ, mtime сегодня 15:13–15:14.
- Каждый daily-parquet покрывает **только часы подписки**, а не сутки:
  - BTCUSDT 09-05: 22:40:35–23:47:21 (~1 ч); 09-07: 00:06–01:11 (~1 ч);
    09-08: 01:52–08:30; 09-09: 02:46–16:40; 09-10: 04:06–11:41 (7.5 ч).
- Events: `sample_periods` из `research.toml` = discovery 2025-12-25…2026-04-30,
  validation 05-01…06-30, oos 07-01…(расширен до klines_max 09-10 10:10).
  → события охватывают ~260 дней; OB-окно (~5 дней × ~28 символов × часы) = крошечная доля.

Арифметика: 4311 / 4 663 516 = **0.0924% ≈ 0.09%** — сходится в точности с reject-сообщением.
Итого valid_events=4311 = 2160 ok + 2151 stale (lag≤600s); остальные 4.66M — "missing".

**Вывод: это не поломка join/фич — это нехватка самих OB-данных.**
Коллектор подписывается на orderbook поток только при candle-триггерах
(в этом прогоне: `Candle triggers fired: 64 (orderbook captures queued)`),
поэтому reconstructed содержит лишь окна захвата.

### Почему 09-06 пуст
- `captures/` имеет 10 captures-файлов за 2026-09-06 (parquet), но **ни одного**
  `reconstructed/…/2026-09-06.parquet` и в `raw/` нет 09-06 jsonl (raw BTCUSDT начинается с 09-07).
- 09-06: 20 файлов в captures (10 parquet + 10 meta) — захват был, реконструкция не дала дня.
  Вероятно: семпл 09-06 был тривиально мал/не прошёл порог или raw не записывался в тот день.
  **Требует проверки ob_reconstructor.rs — вторичная проблема (P2).**

---

## B. Почему dashboard и research показывают разные OB-цифры

**Разные объекты и знаменатели.**

| Источник | Что считает | Результат |
|---|---|---|
| Research (`orderbook_integration.py`, `stage_ob_feature_validation`) | **Events** с непустой OB-фичей (ok/stale) / все events | 4311 / 4 663 516 → 0.09% |
| Dashboard `get_data_quality` | **Строки `_metrics.jsonl`** (per-symbol per-flush): `is_valid → ok`, `!is_valid → gap`, `invalid_state>60 → stale` | ok 53 564, gap 3 826, stale 0 |
| Dashboard `get_ob_collector` | Сумма **последней** записи `_metrics.jsonl` по символу (updates/rows/reconnects/errors) | 2 431 804 updates, rows 2 431 802, errors 0 |

Строки `_metrics.jsonl` (58 470 на момент проверки) — это логи здоровье-флагов коллектора
по символам (по строке в час), НЕ события research. Поэтому числа несовместимы по построению.
`stale 0` на dashboard — порог `invalid_state_duration_secs > 60` (все = 0), тогда как
research `stale` = 120s < lag ≤ 600s join'а. **Разные определения stale.**

`Instrument Info: NOT AVAILABLE` — не сбой: `get_instrument_info()` (collectors.py L189–204)
жёстко возвращает `available=False` с reason «Collector does NOT save per-symbol specs
(minOrderQty, qtyStep, tickSize). Only symbol list is preserved». `linear.txt` есть (743 символа).

---

## C. Parquet «противоречие» — два параллельных дерева

Существуют **ДВА** reconstructed-дерева:

| Дерево | Файлы | Свежесть | Кто читает |
|---|---|---|---|
| `collector/data/market/orderbook/reconstructed/` | 140 parquet, 28 сим. | **свежее** (mtime 15:13–15:14 сегодня) | research, dashboard, system_reviewer (L54) |
| `data/market/orderbook/reconstructed/` | 747 parquet, 748 сим. | **заморожено 09-07 17:16**, по 1 файлу `2026-09-07.parquet` на символ | только `ob_daily_report.py` (default-аргумент) |

Отсюда «per-symbol parquet after 09-07 absent» — это **legacy-дерево** (748 символов, все на 09-07),
которое pipeline не использует. Рабочее дерево (collector) свежее и содержит 09-10.parquet.
`ob_reconstructed_parquet=140` (reviewer) — корректно (28×5).
**Противоречия нет; есть забытое legacy-дерево (P2 — удалить/задокументировать).**

---

## D. config_hash mismatch — две разные функции, а не ошибка данных

| Файл | Hash | Функция | Входы |
|---|---|---|---|
| `data/research/last_run.json` | `25a8d0fabd90` | `data_ready.config_hash()` | `research.toml` + `auto_research.toml` |
| `…/acceptance_20260910T101215Z.json` | `22babfc21527` | `pipeline.compute_config_hash()` | **только** `research.toml` |

Проверено вычислением: `sha256(research.toml)` = 22babfc21527; `sha256(research.toml+auto_research.toml)` = 25a8d0fabd90. ✓
Мismatch — **по умолчанию двух разных реализаций**: last_run пишет hash из data_ready-метрик
(orchestrator L503 `metrics["config_hash"]`), acceptance — из pipeline-стадий (L179, L281…).
Не баг целостности, но **двузначность того, что такое config_hash** (P2 — унифицировать:
одна функция + фиксация входов в поле `config_hash_inputs`).

---

## E. Пустой provenance — поля просто не заполняются

`src/research.py` L212–216 строит `Provenance(data_version, config_version, cost_assumptions, random_seed)`
— и **никогда не заполняет** `code_version`, `dataset_period`, `feature_version`,
`hypothesis_version`, `ob_*` (реестр-дефолты пустые строки, `registry.py` L85–94).

`src/orderbook_integration.get_ob_provenance()` **существует и возвращает всё заполненным**
(INTEGRATION_VERSION="2.0", FEATURE_VERSION, RECONSTRUCTION_VERSION, hashes) — но **нигде не вызывается**
(pipeline/orchestrator/research). Минимальный фикс (план, без реализации):
- в `research.py run_research`: `code_version=git_head()`, `dataset_period=[min,max open_time]`,
  `feature_version`/`hypothesis_version` из реестра;
- после `join_ob_features` смержить `get_ob_provenance()` в provenance отчёта.

---

## F. Reconnects: dashboard 404 vs reviewer 1868 — разные агрегации

Обе читают `collector/…/reconstructed/_metrics.jsonl`:
- **Dashboard** (`get_ob_collector` L680–695): берёт **последнюю** метрику на символ и суммирует → 420 (30 символов; ~404 в момент снятия пользователем).
- **Reviewer** (`ob_quality` L166–176): считает **число строк** за последние 2000, где `reconnects>0` → 1868.

**Потери данных от reconnects НЕТ:** для каждого символа `updates_received == rows_written`
(BTC 156359=156359, ETH 126124=126124, …), `update_id_jumps=0`, `errors=0` (проверено по всем 30).
Reconnects — штатный переподписка потока; состояние восстанавливается без дыр в последовательности.

---

## G. Candle status — артефакт чтения «последнего файла по имени»

`get_candle_collector` (L755–775) берёт `files[0]` и `files[-1]` **отсортированные по имени**:
- spot: `files[-1]` = `ZTXUSDT_spot_1m.parquet` → **newest 2026-01-13** (этот символ перестал обновляться);
- linear: `files[-1]` = `ZRXUSDT_linear_1m.parquet` → newest 12:49 сегодня.

При этом **реальные максимумы по данным свежие**: spot-файлы имеют новейшие open_time
2026-09-10 12:49 (0GUSDT…), linear — 12:52 (проверено по всем файлам).
«Spot newest 2026-01-13» — **артефакт**: newest берётся из одного (алфавитно последнего) файла,
а не из max по всем данным. Dashboard-баг чтения (P1): `newest` нужно считать как
`max(open_time)` по сканированию, а не `files[-1]`.

---

## H. Приоритеты

**P0 (блокирует полезность research):**
1. **OB-данные покрывают 28 символов × ~5 дней × часы** → 0.09% coverage.
   Без непрерывной записи orderbook (все символы universe, круглосуточно) требование
   ≥50% coverage для events за 8.5 мес принципиально недостижимо.
   Это архитектурный выбор коллектора (подписка только на триггер-окна), а не баг пайплайна.

**P1 (неправильные показания):**
2. Dashboard candle status: newest из `files[-1]` по имени, а не из max(open_time) → «spot 2026-01-13» фантом.
3. Dashboard OB quality считает строки метрик, research — events; единая метрика coverage нужна в одном месте.
4. Provenance не заполняется (см. E) — отчёт невоспроизводим без версий/hash.

**P2 (чистота/однозначность):**
5. `config_hash`: две функции, два входа — унифицировать.
6. Legacy-дерево `data/market/orderbook/reconstructed` (замороженно 09-07, 748 символов) — удалить/документировать.
7. 09-06: captures есть (10), reconstructed — нет; проверить ob_reconstructor.rs.
8. Reconnects: dashboard/reviewer считают по-разному (последняя строка vs число строк) — выровнять семантику.

---

## I. План изменений (ТОЛЬКО план, без реализации)

1. **НЕ трогать** thresholds/trading/hypothesis/risk/architecture (запрет пользователя).
2. **OB coverage (P0)** — только диагностический шаг: посчитать, сколько событий попадает
   в существующие OB-окна (уже сделано: 4311) и какой % событий за 09-05…09-10 по 28 символам.
   Дальнейшие шаги (расширение данных коллектором 24/7, или сужение research-окна до OB-покрытия,
   или порог `min_feature_coverage` под фактическое покрытие) — **на согласование пользователя**,
   т.к. это архитектурные/threshold-решения.
3. **Candle status (P1)**: в `get_candle_collector` заменить `files[0]/files[-1]` на
   `max(open_time)`/`min(open_time)` по сканированию сгруппированных данных.
4. **Provenance (P1)**: вызвать `get_ob_provenance()` + заполнить `code_version=git_head()`,
   `dataset_period`, `feature/hypothesis_version` в `run_research`.
5. **config_hash (P2)**: единая функция хэша (существующий набор: research.toml+auto_research.toml),
   в acceptance добавить поле `config_hash_inputs`.
6. **Legacy-дерево (P2)**: после подтверждения, что pipeline не читает `data/market/…`,
   пометить/удалить (или перенести метрики).
7. **09-06 (P2)**: чтение `collector/src/bin/ob_reconstructor.rs` — почему день не собран
   при наличии captures.

> Диагностика завершена. Внесено только чтение данных и кода; изменения НЕ выполнялись.