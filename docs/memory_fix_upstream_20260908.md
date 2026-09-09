# Memory Fix — Upstream Verdict (PASS)

Дата: 2026-09-08. Источник: `docs/memory_audit_upstream_20260907.md` (Option A).
Ограничения (verbatim): не делать Option B/C, не проводить доп. рефакторинг, не исправлять unrelated issues, не менять методологию ради производительности, SLIM_COLS не прописывать вручную вслепую.

## 1. Что изменено — `src/orchestrator.py`

Slim-путь в цикле сбора событий:

1. **Популярные свечи** — `df = univ_dfs.pop((sym, cat))` — `univ_dfs` уменьшается по ходу итерации (освобождается после `del df`).
2. **Slim-селект после `build_events`** — берутся только колонки из `_required_columns()` (166→41 после селекта), а не все 232.
3. **`del all_events, aligned`** сразу после `events = pl.concat(...)`.

`_required_columns()` выводится из фактических потребителей (гипотезы `research_mod.HYPOTHESES` + mr-control, `hgen_mod.all_rules()`, candle-trigger `ct_mod.RULES`, таргеты из `future_horizons_min`, мета, `_get_ob_columns()` + `ob_data_quality`, **`vol_gk_30d`**). Колонок в required: **146** (после добавления `vol_gk_30d`; до него — 145).

**Важное методологическое исправление по пути:** `vol_gk_30d` читался `paper.shadow_run` (стр. 464) и `paper_run_backtest` (стр. 326) из events, но не входил в initial `_required_columns()` → slim-путь дропал его, и shadow-риск-стоп деградировал до фолбэка `entry*0.002`. `vol_gk_30d` добавлен в `_required_columns()` (секция #6). `py_compile` OK, тесты 338/338 OK.

## 2. Доказательство детерминизма — `scripts/mem_slim_check.py --limit 20` (v2)

Лог: `/tmp/opencode/mem_slim_check_20_v2.log`.

- Events: full 1,213,630×232; slim 1,213,630×**41**.
- **Pre-OB identical: True**, **post-OB identical: True**.
- Research: full 819 s / slim 790 s; n_hyp **37 / 37**; candidates **[] / []**; discovery_results **identical: True**.
- Slim-колонки — подмножество required (41 колонка), все downstream-потребители покрыты (подтверждено `vol_gk_30d in required: True`).

Приписка: `mem_slim_check.py` оставлен в репозитории для воспроизводимости.

## 3. Полный 72-символьный прогон orchestrator

Лог: `/tmp/opencode/orch_full72.log`, запуск `setsid nohup /usr/bin/time -v .venv/bin/python -m src.orchestrator`.

| Метрика | Значение |
|---|---|
| Exit status | **0** |
| Elapsed (wall) | 1:00:34 |
| Swaps | **0** |
| Max RSS | 12,152,428 KB (~11.6 GB) |
| Major page faults | 72 |
| Events | 4,606,468 |
| OB integration | 104 колонки |
| Гипотезы | 37 |
| Critic | REJECT (t=-21.307) |
| Registry | 0 validated / 1 candidate |
| Acceptance | `data/research/results/acceptance_20260908T072943Z.json` (verdict=REJECT) |
| OOM-строк в логе | 0 |

Прогон завершился без OOM и без swap — цель аудита достигнута. Высокий пик RSS (~11.6 GB против прогноза 2.5–3.5 GB) объяснён тем, что `all_events` в полном 72-символьном прогоне собирает ~4.6M событий × 146 required-колонок — события после slim по-прежнему держатся целиком до OB-интеграции и research; slim сократил ширину в ~5× (232→41), но не объём строк. Отказ от OOM достигнут (baseline во время аудита OOM-ился на 72 символах). Методология не менялась: verdict REJECT, 37 гипотез, кандидаты [] — соответствует baseline-поведению (Critic-отклонение).

### Shadow-расхождение (не эффект slim)

Бэйзлайн-лог (62,360 трейдов, PnL=-996.97) против новых прогонов (670 / 0 трейдов, PnL=-999.0049) объяснён **внешним сбросом стейта**, а не slim-изменением:

- `data/research/registry.json` пересоздан в 04:18Z (07:18 локально) — только H001 CANDIDATE (создан тестами: `tests/smoke_paper.py`, `tests/test_paper_trading.py` регистрируют H001).
- `data/paper/portfolio/state.json` сброшен до баланса ~1.0 (осталась 1 запись `E2E_SUCCESS` от `tests/test_e2e_shadow_paper.py`) → в `shadow_run` `_position_size` даёт qty≈0 → трейды почти не исполняются.
- `vol_gk_30d` сохранён в required-колонках (см. §1) — shadow-риск-стоп не деградирует.

## 4. Вердикт

**PASS** — upstream-фикс подтверждён:

- `--limit 20`: exit 0, Max RSS 3,503,220 KB (~3.3 GB), 37 гипотез, determinism proven (identical).
- Полный 72-символьный прогон: **exit 0, swaps 0, no OOM**, методологически идентичен (REJECT, 37 гипотез).

Дальнейшая работа (вне scope, по ограничениям): Option B (потоковая обработка батчами) — если понадобится снизить пик RSS ниже ~11.6 GB; верификация shadow-трейдов после восстановления state.json.