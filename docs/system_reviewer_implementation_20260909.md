# System Reviewer — Implementation Report — 2026-09-09

## What was built

Periodic system reviewer for bybit-research (`src/system_reviewer.py`), AUDIT ONLY.
No Orchestrator call inside, no new notification mechanism, no changes to
production code, methodology, hypotheses, gates, BH, cost model, risk limits,
collector logic, or registry logic. Nothing was modified to make tests pass.

Deliverables:

| Artifact | Path |
|---|---|
| Reviewer module | `src/system_reviewer.py` |
| Tests (17) | `tests/test_system_reviewer.py` |
| Audit agent (AUDIT ONLY) | `.opencode/agent/system-reviewer.md` |
| Design doc | `docs/system_reviewer_design_20260909.md` |
| First report (degenerate run) | `docs/system_review_20260909_055724.md` |
| Second report (real audit) | `docs/system_review_20260909_060741.md` |
| Run artifacts (context + raw output) | `logs/system_reviewer/run_*.json` |

## How it runs

```
python -m src.system_reviewer --once      # one audit (systemd timer / cron)
python -m src.system_reviewer             # daemon loop, REVIEW_INTERVAL (default 21600 s = 6 h)
```

Env: `REVIEW_INTERVAL`, `REVIEW_TIMEOUT` (1800 s), `REVIEW_MODEL`
(`opencode/big-pickle`), `REVIEW_OPENCODE_BIN`, `REVIEW_NOTIFY_ON_PASS`.

Flow per run:
1. `flock` lock (`data/system_reviewer/system_reviewer.lock`) prevents double runs.
2. `gather_context()` — read-only snapshot (services, data growth, OB quality
   from `_metrics.jsonl`, research/registry/paper state, disk/mem/swap,
   dashboard HTTP, log tails). No datasets are read in full.
3. `opencode run --agent system-reviewer -m opencode/big-pickle --dir ROOT`
   — audit agent with **edit/write deny**, read-only bash whitelist, `"*" deny`.
   Output goes to a temp file (not a pipe). Timeout kills the whole process
   group (`start_new_session=True`, `killpg(SIGKILL)`).
4. JSON verdict extracted from the tail of the output (ANSI-stripped).
   If the output contains "Falling back to default agent" → run is rejected as
   `AGENT_NOT_PRIMARY_OR_MISSING` (default agent has no AUDIT restrictions).
5. `docs/system_review_*.md` report (13 sections: collector, data quality,
   research pipeline, hypotheses, orderbook, shadow/paper, infrastructure,
   statistical integrity, anomalies, severity, recommended actions, tests
   executed, findings FACT/WARNING/RECOMMENDATION).
6. ntfy via existing `src/notify.py` on FAIL or critical; on PASS only if
   `REVIEW_NOTIFY_ON_PASS=1`. Notifications land on the existing
   `openagent_trade` topic — no new mechanism.

## Test results

- `python -m unittest tests.test_system_reviewer` — **17/17 OK** (lock, interval,
  timeout/killpg, crash, verdict extraction incl. ANSI, fallback rejection,
  report structure, notify failure handling, isolation from pipeline imports,
  retry after failure).
- Full suite `python -m unittest discover -s tests` — **353/353 OK**.
  Existing tests were not modified.

## Run results (2026-09-09)

Run 1 (05:57 UTC): degenerate — the agent file had `mode: subagent`, so
`opencode run --agent` fell back to the default agent. The fallback was
detected and the guard was added; agent switched to `mode: primary`.
Run 1 wrote no files (mtime window 08:55–09:21 local: only lock/report/logs
plus `state.json`/`registry.json` touched at 08:55:28–33 by the full test
suite run, which predates both reviewer runs and is a known test-mutation
issue).

Run 2 (06:07 UTC): **full real audit, verdict FAIL, severity critical**.
The reviewer read code and data, cross-checked findings against test files
and logs, and produced evidence-backed findings:

- **FACT** production `state.json` contains `paper_0001` from
  `tests/test_e2e_shadow_paper.py` (E2E_SUCCESS, VALIDATED, fake
  2026-01-01 timestamps) — invariant `paper wallet = 1000 USDT` broken.
- **FACT** Shadow MODE B writes into the same production wallet via
  `save_state` (`src/paper.py:538`): 62 360 trades, PnL −996.97 (09-07),
  drained to ~0.995 USDT.
- **FACT** `registry.json` overwritten by `tests/test_paper_trading.py`
  (production default path) — candidate history lost.
- **FACT** OB integration into research is effectively absent: OB features
  99.97% null (4605170/4606468 missing), provenance OB fields empty;
  HG* hypotheses on OB features untestable in the current pipeline.
- **WARNING** liquidation WebSocket reconnect loop (60 s backoff);
  dashboard HTTP 000 (manual process, no systemd, no logs);
  inter-cycle n_events incomparability (540K → 4.7M → 1.2M…, universe changes).
- **FACT** gates work correctly: 5/5 acceptance REJECT, BH q=0.05, honest
  cost grid, leakage checks PASS.

Notifications confirmed on topic `openagent_trade` (ntfy): both FAIL runs
arrived, `src/notify.notify()` returns True.

## Constraints honored

- No architecture changes to the research pipeline.
- No `opencode` call inside `orchestrator.py`.
- Interval configurable (`REVIEW_INTERVAL=21600`).
- Reuses existing notification mechanism (`src/notify.py`).
- No existing tests changed to force a PASS.
- Chain discovery → report → notification → our decision → separate
  implementation run. Findings above are recommendations; nothing was fixed
  as part of this task.
- Read-only verification: no production file (src/, config/, collector/,
  data/ except pre-existing test mutation) was written by the reviewer runs.

## Known limitations

- Audit depth depends on the agent model reading the snapshot + repo; the
  report is a point-in-time picture, not a continuous monitor.
- `registry_state()`/`paper_state()` read JSON directly; if the format
  changes, fields degrade gracefully to empty.
- ntfy topic is shared with the existing pipeline notifications (by design).

## Verdict

Reviewer works end-to-end (lock → context → audit → verdict → report →
ntfy). Run 2 delivered a real, evidence-backed FAIL report with actionable
recommendations. Fixing those findings is a separate implementation run,
pending human decision.