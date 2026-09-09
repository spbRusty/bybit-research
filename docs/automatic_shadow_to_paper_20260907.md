# Automatic Shadow → Paper Pipeline

**Дата**: 2026-09-07
**Статус**: IMPLEMENTED
**Тесты**: 319/319 PASS (18 новых)

## Summary

Автоматический пайплайн от research до paper trading реализован в 16 фазах.
Ключевые компоненты:

1. **Shadow Paper (MODE B)** — автоматическое наблюдение за CANDIDATE гипотезами
2. **Auto Research Loop** — запуск pipeline по расписанию
3. **Registry Lifecycle** — CANDIDATE → VALIDATED только после всех gates
4. **Auto Paper Trigger** — запуск MODE A после VALIDATED
5. **Premature Paper Guard** — блокировка paper без полного pipeline
6. **Notifications** — уведомления по ключевым событиям
7. **Dashboard** — эндпоинт для shadow paper статуса

## Components Modified

| File | Changes |
|------|---------|
| `src/paper.py` | Added `shadow_run()`, `get_shadow_summary()` |
| `src/orchestrator.py` | Added `auto_research_loop()`, `auto_paper_trigger()`, `premature_paper_guard()`, `_load_latest_events()` |
| `src/notify.py` | Added `notify_hypothesis_candidate()`, `notify_hypothesis_validated()`, `notify_paper_started()`, `notify_paper_stopped()`, `notify_shadow_summary()` |
| `src/dashboard/server.py` | Added `/api/shadow-paper` endpoint |
| `src/dashboard/collectors.py` | Added `get_shadow_paper()` |
| `tests/test_auto_shadow_paper.py` | 15 deterministic tests |
| `tests/test_e2e_shadow_paper.py` | 3 end-to-end synthetic tests |

## Pipeline Flow

```
Data → Features → Events → OB Integration → Hypothesis Generation
  ↓
Research (Discovery → BH → Validation → OOS)
  ↓
Registry Update (CANDIDATE → VALIDATED only if ALL gates pass)
  ↓
Shadow Paper (MODE B) — observe CANDIDATE hypotheses
  ↓
Auto Trigger (MODE A) — start paper for VALIDATED hypotheses
```

## Safety Guarantees

- **No live orders**: Paper trading only, no API calls to exchanges
- **No premature paper**: Guard blocks paper without full pipeline completion
- **No future data leakage**: entry_price = open(T+1), enforced in events.py
- **No synthetic data in production**: Synthetic only in tests
- **Temporal safety**: OB features timestamp <= event T

## Test Coverage

| Test Suite | Tests | Status |
|------------|-------|--------|
| Existing tests | 301 | PASS |
| Auto shadow paper | 15 | PASS |
| E2E shadow paper | 3 | PASS |
| **Total** | **319** | **PASS** |

## Usage

```bash
# Single pipeline run
.venv/bin/python -m src.orchestrator

# Auto-loop (hourly)
.venv/bin/python -m src.orchestrator --auto --interval 3600

# Auto-loop with limit
.venv/bin/python -m src.orchestrator --auto --interval 1800 --limit 100
```

## Dashboard

New endpoint: `GET /api/shadow-paper`

Returns shadow paper status, trades by hypothesis, PnL summary.

## Notifications

New notification events:
- `HYPOTHESIS CANDIDATE` — new candidate discovered
- `HYPOTHESIS VALIDATED` — all gates passed
- `PAPER STARTED` — paper trading started
- `PAPER STOPPED` — paper trading stopped
- `SHADOW SUMMARY` — shadow paper cycle summary
