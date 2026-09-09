# Memory Audit — Upstream Research Pipeline

**Date:** 2026-09-07
**Scope:** Execution path in `src/orchestrator.py::run_pipeline` from universe load through OB integration
**Status:** AUDIT COMPLETE — SAFE FIX IDENTIFIED (Option A: free intermediates + slim events to required columns)

---

## 1. Executive Summary

The 72-symbol full-universe run fails **not because of a memory leak**, and **not because of the OB integration**. It fails because the pipeline **architecturally accumulates every symbol's full-width event frame in RAM simultaneously**.

- OB integration (`join_asof`) is fast and light (verified previously: 318K events in 4.2 s / 2.7 GB).
- The killer is the events frame itself: **232 columns × ~4.5M rows ≈ 7.7 GB**, plus retained candle dicts and allocator high-water mark.
- Measured RSS: 3 sym → 1.23 GB; 5 → 1.33 GB; 10 → 1.61 GB; 20 → 3.36 GB; 44 → 6.5 GB; extrapolated 72 → **~10-11 GB → OOM**.
- **Leak test (decisive):** running the same 8-symbol feature loop twice in one process yields run1=1701 MB, run2=1700 MB. No growth → **not a leak**, allocator reuses freed buffers.
- **Methodology check (decisive):** research on full 232-col events vs a 44-col slim subset gives **byte-identical discovery results** on the same data. The other 188 columns are never read by research, critic, or shadow.

**Verdict: SAFE FIX IDENTIFIED.** Freeing intermediates (candles, `all_events`, `aligned`) earlier **and** building the events frame from only the columns required downstream cuts peak RSS ~5× with zero methodological impact.

---

## 2. Exact Root Cause

In `src/orchestrator.py::run_pipeline`, between data load and OB integration:

```python
univ_dfs = data_mod.load_universe_data(universe)   # ALL 72 candle frames in a dict
breadth  = breadth_mod.compute_breadth(univ_dfs)   # outer-join of all is_green
...
for row in universe.iter_rows(named=True):
    df = univ_dfs.get(...)
    df = features_mod.add_features(df, ...)        # widens to ~232 cols
    ev = events_mod.build_events(df, ...)
    if ev.height:
        all_events.append(ev)                      # ACCUMULATES all events
...
aligned = [...]                                     # duplicate list
events  = pl.concat(aligned)                        # full 232×4.5M frame
events  = join_ob_features(events)                  # +OB cols; held through all stages
```

Three objects are held for the **entire function** (until line 299, after shadow):
1. `univ_dfs` — all 72 symbols' candles (only ~1 GB, but never freed)
2. `all_events` + `aligned` — never freed after `events = pl.concat(aligned)`
3. `events` — the 232×4.5M frame, held through research/critic/shadow though research reads only 44 columns

The events frame is the dominant term. Measured `events.estimated_size()` at 438K rows × 232 cols = 0.73 GB, so 4.5M rows ≈ 7.7 GB.

## 3. Memory Profile (measured)

| Stage | RSS @3 sym | RSS @5 sym | RSS @10 sym | RSS @20 sym | RSS @44 sym* |
|-------|-----------|-----------|------------|------------|-------------|
| baseline | 0.15 GB | 0.15 GB | 0.15 GB | 0.15 GB | — |
| candles loaded | 0.30 GB | 0.34 GB | 0.36 GB | 0.59 GB | 1.07 GB |
| features done | 1.23 GB | 1.33 GB | 1.61 GB | 3.36 GB | 6.47 GB |
| events concat | 1.23 GB | 1.33 GB | 1.61 GB | 3.36 GB | — |
| after freeing upstream | 1.22 GB | 1.32 GB | 1.59 GB | 3.36 GB | — |

\* 44 sym / 2.65M events stopped at safety threshold (≈6.5 GB), extrapolated 72 sym ≈ 10-11 GB.

RSS is a **high-water mark**: after `del` + `gc`, RSS does not drop because Polars keeps an allocator arena; but the arena is reused (leak test). So RSS ≈ peak working set, not a growing leak.

**Per-symbol feature transient:** `add_features` produces a wide (~232-col) df transiently per symbol; it is the same-size event. The per-symbol peak is ~0.3-0.6 GB but since the arena reuses freed pages, the *incremental* cost of each symbol is mostly the accumulated events (+80-120 MB/symbol), not the transient.

## 4. Object Lifetime Trace

| Object | Created | Held until | Needed downstream? |
|--------|---------|-----------|---------------------|
| `btc`, `eth` candle frames | line 97-98 | function end (line 299) | only during feature loop |
| `univ_dfs` (all 72 candles) | line 99 | function end | only during feature loop; `pop` per symbol would suffice |
| `breadth` | line 100 | function end | only during feature loop |
| per-symbol `df` (wide) | loop | next iteration | no (transient) |
| per-symbol `ev` (events) | loop | into `all_events` | yes, but slimmed |
| `all_events` list | loop | function end (copied into `aligned`) | no after concat |
| `aligned` list | line 127 | function end | no after concat |
| `events` (232×4.5M) | line 133 | function end (research/critic/shadow) | only 44 columns |

**Per-symbol memory balance:** after `ev = build_events(df)`, the wide `df` is released locally, but `univ_dfs[symbol]` (the candle) is **retained** in the dict. Freeing it (`univ_dfs.pop`) saves ~15% of RSS at 44 symbols (measured: Mode B 5.3 GB vs Mode A 6.1 GB at 2.48M events).

## 5. Why 20 symbols pass but 72 fail

- Events accumulate linearly: each symbol adds ~40-90K rows × 232 cols.
- 20 sym → 1.21M events → 3.36 GB (passes easily with 40 GB RAM).
- 72 sym → ~4.5M events → ~7.7 GB events + 1 GB candles + 1 GB allocator overhead ≈ **10-11 GB RSS**.
- The system has 40 GB free, but swap was **100% full (8/8 GB)** during the failing runs because 3-4 prior OOM-killed processes (each peaking at 10+ GB) and the production collector had wedged swap. With swap full, the kernel OOM-kills instead of swapping.
- **Not a resource "shortage" per se** — the pipeline simply scales its working set with universe size, and at 72 symbols the working set exceeds the comfortable envelope given the full swap.

## 6. Leak vs Accumulation vs Peak

| Category | Verdict | Evidence |
|----------|---------|----------|
| Memory leak | **No** | Identical 8-sym runs: 1701 → 1700 MB. No growth. |
| Peak memory (high but bounded) | **Yes** | RSS ≈ 10-11 GB at 72 sym, then drops after run |
| Nenужное накопление all symbols/events | **Yes** | `univ_dfs` full dict + `all_events` + `aligned` all retained to function end |
| Polars memory behaviour | **Yes** | Allocator arena not returned to OS; RSS is a high-water mark, but reusable |
| Allocator fragmentation | **Minor** | Not measured directly; increments are clean per symbol |

The classification is: **architectural accumulation of all symbols' full-width events + Polars arena high-water mark.** Both are addressable.

## 7. Benchmark Table

Measured with `scripts/mem_bench_upstream.py`, `scripts/mem_probe_driver.py`, `scripts/mem_probe_free.py`, `scripts/mem_leak_test.py`, `scripts/mem_slim_check.py` (kept in repo for reproducibility).

| Universe | Events | RSS (features done) | Per-symbol time | Events est. size |
|----------|--------|--------------------|-----------------|------------------|
| 3 | 173,640 | 1.23 GB | ~7 s | 0.28 GB |
| 5 | 269,371 | 1.33 GB | ~6.6 s | 0.45 GB |
| 10 | 550,361 | 1.61 GB | ~7.3 s | 0.92 GB |
| 20 | 1,212,601 | 3.36 GB | ~6.6 s | 2.0 GB |
| 44 (safe stop) | 2,653,857 | 6.47 GB | — | 4.4 GB |
| 72 (extrapolated) | ~4.5M | ~10-11 GB | — | ~7.7 GB |

**Slim vs full events** (5 sym, identical data): full 232 cols = 0.45 GB; slim 44 cols = 0.09 GB — **5.0× smaller**.

## 8. Options

### Option A — Minimal fix: free intermediates + build slim events (RECOMMENDED)

Change the orchestrator data loop to:

1. **Pop candles per symbol** (`df = univ_dfs.pop((sym, cat))`) so the candle dict shrinks as we go.
2. **After `build_events`, select only required columns** before appending:
   ```python
   ev = build_events(df, ...).select(SLIM_COLS)   # 44 cols instead of 232
   ```
   where `SLIM_COLS` = metadata + hypothesis-condition features + OB columns + targets.
3. **`del all_events, aligned` after `events = pl.concat(...)`.**

- **RAM reduction:** ~5× on the dominant term. 72 sym → ~2-3 GB instead of 10-11 GB (events slim 7.7 → ~1.6 GB).
- **Speed:** slightly faster (less memory traffic, fewer columns in concat/join); same CPU work in feature computation.
- **Complexity:** low — ~30 lines in `orchestrator.py`; no new infra.
- **Behavioral risk:** none — proven deterministic (research identical). `SLIM_COLS` must include every column referenced in any hypothesis condition, all targets (`return_*`/`mfe_*`/`mae_*`), `entry_price`, `open_time`, `symbol`, `category`, `event_id`, and `ob_*` columns. OB integration adds its columns itself.
- **Methodology:** identical research inputs preserved (see §10).

### Option B — Per-symbol streaming / batch processing

Process symbols in batches (e.g. 20-30), run OB integration + research per batch, and aggregate only the summary results (candidate metrics) rather than holding all events.

- **RAM reduction:** bounded by batch size; could cap at ~3 GB regardless of universe.
- **Speed:** slower if research needs the full event set (critic checks concentration across symbols); needs per-batch accounting.
- **Complexity:** high — research/validation/OOS/critic semantics assume a single dataset; batching changes how `n_symbols`, `n_months`, concentration, and BH are computed. **High risk of altering methodology** unless extreme care taken.
- **Methodology:** **likely changes** discovery/BH/validation semantics → **NOT safe without redesign**.

### Option C — Lazy/streaming Polars

Use `scan_parquet` + `.streaming()` + `sink_parquet` for the concatenation/join, or lazy chaining of sorting/filtering.

- **RAM reduction:** can spill intermediates to disk; useful for the concat step.
- **Speed:** slower for in-memory-sized data (I/O bound); streaming benefits only truly larger-than-RAM datasets.
- **Complexity:** medium-high; Polars streaming has caveats (some expressions not supported in streaming).
- **Methodology:** none if the collected result is identical; but does not solve the accumulation of the *final* events frame (still materialized for research).
- **Verdict:** complements Option A but doesn't replace it.

## 9. Recommendation

**Option A**, with `SLIM_COLS` derived from the actual consumer set. It is the smallest change, provably methodology-identical, and cuts the dominant memory term 5×.

Complementary (cheap, optional): `del univ_dfs/breadth/btc/eth/all_events/aligned` after concat (free ~1 GB of objects for reuse by research).

## 10. Methodological Risks

| Concern | Status |
|---------|--------|
| Event selection | Identical — `build_events` unchanged; only projection after it. |
| Target (`return_*`,`mfe_*`,`mae_*`) | Identical — in slim set. |
| Predictor boundary (no future leakage) | Unchanged — no look-ahead in projection. |
| Discovery / BH / Validation / OOS | Identical — proven: `discovery_results identical: True` on 5 sym. |
| Hypothesis rules / thresholds | Identical — all `feature_id` columns in slim set. |
| Critic / gates / cost model | Unchanged — operate on result dict + `ob_data_quality`. |
| Orderbook semantics | Unchanged — `ob_*` columns pass through `join_ob_features`. |
| Registry semantics | Unchanged — reads registry JSON, not events. |

**The only risk in Option A is incomplete `SLIM_COLS`** — if a future hypothesis references a column not in the slim set, research breaks. Mitigation: derive `SLIM_COLS` from `hgen_mod.all_rules()` + `research.HYPOTHESES` condition columns + all `ob_*` + all targets at load time, and assert every referenced column is present.

## 11. Implementation Plan (if approved)

1. Define `_required_columns()` helper in `orchestrator.py` (or `events.py`): union of hypothesis condition columns (parsed from rule configs + baseline H001-H008), `return_*/mfe_*/mae_*` (from `features.toml future_horizons_min`), metadata (`open_time,symbol,category,event_id,entry_price`), and `ob_*`/`ob_data_quality`.
2. In the feature loop, after `build_events`, add `.select(slim_cols)` before append.
3. After `pl.concat`, `del all_events, aligned`; also `del univ_dfs, breadth, btc, eth` before research.
4. (Optional) `univ_dfs.pop()` per symbol.
5. Run deterministic regression: full-vs-slim research comparison test (script exists as `scripts/mem_slim_check.py`).
6. Re-run full test suite (338 tests must stay green).
7. Run `--limit 20` orchestrator cycle; then (approved) full 72-symbol cycle, watching RSS.

## 12. Expected RAM After Fix

| Universe | Current RSS (events phase) | After Option A |
|----------|---------------------------|----------------|
| 20 sym | 3.36 GB | ~0.8-1.0 GB |
| 72 sym | ~10-11 GB (OOM) | **~2.5-3.5 GB** |

(Events 7.7→~1.6 GB + candles ~1 GB + arena overhead ~0.5-1 GB.)

## 13. Regression Strategy

1. **Deterministic research equivalence test** (already built, `scripts/mem_slim_check.py`): full-width vs slim events must yield byte-identical `discovery_results`, identical `candidates`, identical `n_hypotheses`. Currently passes on 5 symbols; extend to 20.
2. **Full test suite**: all 338 existing tests must pass.
3. **Orchestrator smoke**: `--limit 20` full cycle must complete with exit 0.
4. **Then 72-symbol full run** (only after approval), verifying RSS < 6 GB and completion.
5. Keep benchmark scripts in `scripts/` for future regression.

---

## Appendices

### A. Files touched in this audit
- `scripts/mem_bench_upstream.py` (new, benchmark)
- `scripts/mem_probe_driver.py` (new, growth driver)
- `scripts/mem_probe_free.py` (new, candle-free comparison)
- `scripts/mem_leak_test.py` (new, leak classification)
- `scripts/mem_slim_check.py` (new, full-vs-slim determinism)
- `docs/memory_audit_upstream_20260907.md` (this report)

No production source files were modified in this audit.

### B. Definitive evidence commands
```
python scripts/mem_bench_upstream.py --limit {3,5,10,20}
python scripts/mem_probe_driver.py          # per-symbol growth
python scripts/mem_probe_free.py            # Mode A vs B (free candles)
python scripts/mem_leak_test.py             # leak classification
python scripts/mem_slim_check.py            # research determinism check
```