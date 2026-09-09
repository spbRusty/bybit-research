# System Reviewer — Findings Implementation Report — 2026-09-09

Implements the three findings from the system review of 2026-09-09
(`docs/system_review_20260909_060741.md` + task doc
`docs/system_reviewer_implementation_20260909.md`).

Verifier: `python -m unittest discover -s tests` — **355/355 OK**
(was 353 before findings work; +2 new lifecycle tests added to
`tests/test_paper_trading.py` under Finding 1).

---

## Finding 1 — Tests/E2E/smoke mutated production state

**Finding**: `tests/e2e/test_e2e_shadow_paper.py`, `test_audit_cases.py`,
`test_auto_shadow_paper.py` and `smoke_paper.py` wrote to the production
`data/paper/portfolio/state.json` and `data/research/registry.json`
(`E2E_SUCCESS` signature found in both files).

**Fix (state isolation)**:

| File | Change |
|---|---|
| `tests/test_paper_trading.py` | `TestLifecycleIntegration` uses `HypothesisLifecycle(registry_path=Path(tempfile.mktemp(suffix=".json")))` — no production registry |
| `tests/test_e2e_shadow_paper.py` | execute_signal test patches `src.paper.STATE_FILE`; shadow_run test patches `src.paper.SHADOW_STATE_FILE` |
| `tests/test_audit_cases.py` | TestCase F and G patch `SHADOW_STATE_FILE` (shadow path) |
| `tests/test_auto_shadow_paper.py` | 3 shadow_run tests patch `SHADOW_STATE_FILE` |
| `tests/smoke_paper.py` | tmp `STATE_FILE`/`SHADOW_STATE_FILE` via module attr, `registry_path=_TMP/"registry.json"` |

**Production cleanup** (polluted files from earlier runs):

| File | Action | Baseline after cleanup |
|---|---|---|
| `data/paper/portfolio/state.json` | backup → `/tmp/opencode/backup_20260909/state.json.polluted`; restored via `save_state(_initial_state())` (balance 1000, trades []) | md5 `6ec93c1b8ad9be8ef0ff120b0a659908`, mtime `1788943561` |
| `data/research/registry.json` | backup → `/tmp/opencode/backup_20260909/registry.json.polluted`; reset to `{}` | md5 `99914b932bd37a50b983c5e7c90ae93b`, mtime `1788943562` |

**Proof of isolation**: after the full 355-test suite, both production files
were re-hashed — md5 and mtime unchanged. `data/paper/shadow/` contains only
the empty directory (no `state.json`); tests never write to production.

**Smoke**: `tests/smoke_paper.py` → ALL CHECKS PASSED.

---

## Finding 2 — Shadow (MODE B) and Paper (MODE A) shared one `state.json`

**Finding**: `shadow_run` loaded/saved the same state file as `execute_signal`
(MODE A paper portfolio) — shadow could overwrite the paper
balance/trades/wallet/equity/position.

**Fix (state split)**:

| File | Change |
|---|---|
| `config/settings.py` | `PAPER_SHADOW = PAPER_DIR / "shadow"` created in the mkdir loop; `SHADOW_STATE_FILE = PAPER_SHADOW / "state.json"` |
| `src/paper.py` | `PaperMode` enum (VALIDATED/SHADOW); `load_shadow_state()` (state=None → initial); `save_shadow_state()`; `shadow_run(state=None)` loads shadow state and sets `mode=SHADOW`; persists via `save_shadow_state` only; `get_shadow_summary(state=None)` reads shadow file |
| `src/orchestrator.py` | shadow blocks in `run_pipeline` (~L327) and `auto_research_loop` (~L398) import `shadow_run` and call it **without** portfolio state; notify uses `shadow_result["balance"]` |
| `src/dashboard/collectors.py` | `get_shadow_paper()` rewritten: reads `PAPER_SHADOW/state.json` (shadow trades/PnL/win rate) **and** portfolio file (validated trades); response shape preserved; `test_get_shadow_paper_no_data` still green (patches portfolio only; shadow file absent → NO DATA path) |

**Invariants enforced** (script `/tmp/opencode/invariant_check.py`, 8/8 PASS):
- shadow/portfolio state files are distinct paths;
- `shadow_run` body contains `save_shadow_state(state)` and **no**
  `save_state(state)`;
- `get_shadow_summary` reads `load_shadow_state()` only;
- orchestrator shadow calls pass no portfolio state;
- MODE A (`auto_paper_trigger`) untouched: `load_state`/`save_state` on the
  portfolio, `mode=VALIDATED`, only `get_eligible_for_paper` (VALIDATED)
  hypotheses may enter paper.

---

## Finding 3 — OB features in research ~100% NULL

**Finding**: dashboard used OB quality metrics from a different data tree than
the research join; OB rates shown by the dashboard contradicted the acceptance
report (`0.712 null_rate` vs dashboard numbers that implied almost no data).

**Fix (technical cause)** — `src/dashboard/collectors.py`:
- `_ob_metrics_path` now reads `MARKET_DATA_DIR / "orderbook" / "reconstructed" / "_metrics.jsonl"` (live tree the collector writes);
- `get_ob_collector` / `get_data_quality` use the same live path.

Research join logic itself (`join_ob_features`, timestamp `<= T`, response
`> T`, gap/missing → NULL, LEFT JOIN, no leakage, no forward-fill) was **not
changed** — it is already correct (verified by deterministic tests + smoke).

**Honest data-availability finding** (why ~90% of research OB is NULL is real):
Live upper-bound smoke on the live tree, events = every 1m kline
(`bybit_rs` `.../klines/linear/{SYM}_linear_1m.parquet`) in the
2026-09-05..09-09 window for BTCUSDT, ETHUSDT, XRPUSDT, DOTUSDT (worst case —
maximum possible event count, script `/tmp/opencode/task3_smoke.py`):

| Metric | Value |
|---|---|
| Total events | 25 109 |
| joined ok | 1 762 (7.0%) |
| stale (response > T leak guard) | 1 427 (5.7%) |
| gap | 0 |
| missing | 21 920 (87.3%) |
| Representative OB feature columns non-null | ~10.91% |
| OB columns ~100% NULL | ~89% of rows |

`min_events=100` is achievable on the best symbols: ok+stale per symbol
BTCUSDT 848, ETHUSDT 865, XRPUSDT 791, DOTUSDT 685. So the acceptance-time
finding stands: at acceptance run (2026-09-08 08:29 UTC) the OB parquet for
2026-09-08 was written at 09:09:58 UTC — after the run — and only
09-05/09-07 windows existed (≈1-1.3 h of data per symbol). The dashboard's
earlier claim of near-total data was the wrong-tree artifact; the research
join is technically correct and the NULLs are genuine data absence for the
event range.

---

## Security invariants

Checked via `/tmp/opencode/invariant_check.py` — **8/8 PASS**:

1. shadow/portfolio state files distinct;
2. `shadow_run` writes only `SHADOW_STATE_FILE`;
3. `get_shadow_summary` reads shadow file only;
4. orchestrator shadow blocks pass no portfolio state;
5. MODE A: VALIDATED only, via portfolio state;
6. production `state.json`/`registry.json` md5s unchanged (baseline
   `6ec93c1b…` / `99914b93…`);
7. all 38 test write-call sites (reset_state/execute_signal/shadow_run) run
   under tmp patches;
8. methodology files (`src/research.py`, `features.py`, `events.py`,
   `generator.py`, `discovery.py`, `validators.py`, `oos.py`) — **no diff,
   untouched**.

No changes to: discovery, hypothesis generator, BH/FDR, validation/OOS,
Critic, gates, event selection, cost model, risk limits, registry lifecycle,
collector, OB reconstruction semantics, existing research results.
No new strategies, no live trading.

## Constraints honored

- Task 1: production `state.json`/`registry.json` untouched by tests (hash
  proof); save semantics unchanged (restart/recovery tests still rely on
  `execute_signal` persistence).
- Task 2: shadow never changes paper balance/trades/wallet/equity/position;
  MODE A = only VALIDATED; all shadow tests patch `SHADOW_STATE_FILE`.
- Task 3: only the technical cause fixed; data absence recorded honestly with
  live-tree upper-bound evidence.

## Known limitations

- Dashboard server (PID 1533, `127.0.0.1:8420`) picks up the collectors.py
  changes only after restart — out of scope, noted for the operator.
- Shadow dashboard counters (`by_hypothesis`) now show the shadow file's
  per-run numbers; historical shadow state before the split is not migrated
  (was empty — no data lost).
- Smoke scripts live in `/tmp/opencode/` (outside repo).

## Verdict

**PASS** — all three findings implemented; full suite 355/355;
production files byte-identical after the run; security invariants 8/8;
methodology untouched; Task 3 root cause fixed and residual NULLs proven to be
genuine data absence (with `min_events=100` still achievable on the best
symbols for future runs).