# Orderbook Integration Performance Audit — 2026-09-07

## Verdict: PASS — OB Integration production ready

The O(n²) temporal-join bottleneck in `src/orderbook_integration.py` is fixed.
The same semantic join is now computed in O(E + S) via `polars.join_asof`.
All 338 tests pass; the full 20-symbol orchestrator research cycle (**exit 0**)
runs end-to-end through OB integration, research, critic, registry, shadow.
Collector, storage, universe, OB features, and research methodology untouched.

---

## 1. Original problem

`src/orderbook_integration.py::_process_symbol()` (lines 113-115):

```python
for event in events:                       # O(E) events
    features.filter(pl.col("timestamp_ms") <= event)  # O(S) scan per event
```

For 50K events × 200K snapshots per symbol => ~10 billion comparisons per symbol.
At 17 OB symbols the production run took hours or died. This was the reported
killer of the research pipeline.

## 2. Root cause

Scalar-per-event loop over a columnar DataFrame: every event triggered a full
columnar filter + sort of the snapshot table. No vectorization, no sorted merge,
no index.

## 3. Fix (algorithm only)

Replaced the loop with a single vectorized as-of join (backward = last snapshot
≤ event timestamp), matching the exact prior semantics:

```python
events_keyed = _events.sort("_event_ts_ms")          # O(E log E)
merged = features_ob.join_asof(
    events_keyed, on="_event_ts_ms", by="_symbol",
    strategy="backward",                        # last snapshot <= event
)
```

plus the existing `_as_of_transform` for feature columns and the existing
`gap_detected` handling. **No change** to: feature definitions, event selection,
targets, gates, cost model, hypothesis generation, Critic, Registry.

Schema fix (same algorithm, correctness): the "no frames" path now emits
`pl.lit(None, dtype=pl.Float64)` for `ob_columns` so `pl.concat()` with the
`join_asof` Float64 columns does not raise `type Float64 != Null`.

### Downstream fix (unblocked by OB, bug not methodology)

With OB features now actually flowing into research, `hypothesis_generator.py`
crashed `float(None)` when an all-NULL OB feature column (no valid snapshots for
that event set) hit a percentile threshold. `_threshold` and the `in_range`
branch now return `None` when the quantile is uncomputable, so
`generate_hypotheses` skips the rule — exactly the documented intent at the
existing `if not cond: continue` path. No threshold values, rules, or semantics
changed.

## 4. Benchmark (old vs new)

| Case | Events | Old O(n²) | New join_asof | Speedup |
|------|--------|-----------|---------------|---------|
| 3 symbols | 173K | hours/die | **3.6 s** (48K rows/s, 28.7 MB peak) | >1000× |
| 5 symbols | 268K | ~die | **3.7 s** (71K rows/s) | >1000× |
| 3 heaviest OB symbols | 318K | — | **4.2 s** (2.7 GB RSS) | — |
| 20 symbols (orchestrator) | 1.21M | — | **PASS, no OB bottleneck** | — |

## 5. Semantic preservation — deterministic tests

`tests/test_ob_integration_deterministic.py` — 10 tests over Cases A–H:

- **A** exact timestamp match → snapshot features at T
- **B** last snapshot ≤ T (backward as-of)
- **C** snapshot after T rejected (no future data)
- **D** gap semantics preserved (`gap_detected`, NULL features)
- **E** all-NULL when no prior snapshot (quality "missing")
- **F** stale/old snapshot handling
- **G** multiple events between two snapshots → each maps to same last snapshot
- **H** symbol isolation (no cross-symbol leak)

All PASS. Row preservation asserted (`events == result` on real data too:
318,821 events → 318,821 rows).

## 6. Real-data smoke

- 3 symbols: 173K events, quality distribution computed, rows preserved.
- 5 symbols: 268K events, quality distribution computed.
- 3 heaviest OB symbols (BTC/ETH/XRP): 318K events in 4.2 s.
- 20-symbol orchestrator full run: OB columns integrated for all 17 OB symbols.

## 7. Test suite

- Full suite: **338 tests, OK** (5.4 s) — includes the 10 new deterministic tests.
- 42 pre-existing OB tests: unchanged semantics, still pass.
- Total OB-related: **52/52**.

## 8. Resources

- OB join itself is light: 2.7 GB RSS at 318K events on 3 heavy symbols.
- The 72-symbol full-universe run OOM-kills **earlier**, in upstream feature
  computation (accumulates all events: 9.9 GB RSS at 70/72 before OB is ever
  reached). This is a **pre-existing, OB-independent** limitation; the system
  has 40 GB free but swap is full (8/8 GB) and other services run. It is not
  caused by, and not addressed by, this OB optimization (and is outside the
  "change only the OB join" scope).

## 9. Production safety

- Collector (PID 12152) running/untouched. Storage format (parquet) unchanged.
- Universe (72 symbols) unchanged. OB features (`compute_all_features`) unchanged.
- No methodology, validation/OOS, Discovery, BH, Critic, gates, cost model,
  Registry, or hypothesis-generation logic changed.
- Only 2 files changed: `src/orderbook_integration.py` (join) and
  `src/hypothesis_generator.py` (NULL-threshold bug). One test file added.

## 10. Full orchestrator production cycle (20 symbols)

`python -m src.orchestrator --limit 20` → **exit 0**:

```
Events: 1,209,258 (candle features + OB integrated)
STAGE data_validation: PASS
STAGE feature_validation: PASS
Research: 37 hypotheses evaluated -> candidates=[]
STAGE parameter_freeze: SKIPPED
STAGE critic: REJECT (strongest discovery t=-13.34 <= 2.0; costs 0.20%; stress grid)
Registry: 0 validated (MODE A), 1 candidates (MODE B)
Shadow paper: 62,360 trades, PnL=-996.97 (observation only)
```

- OB integration completed on production volume (1.2M events, 17 OB symbols).
- All hypotheses correctly rejected (strongly negative t) — **expected**, no
  artificial VALIDATED. MODE A correctly not run (no VALIDATED transitions).
- Shadow ran observation-only. No real orders created.

## 11. What changed / what didn't

**Changed:**
1. `_process_symbol`: O(n²) scalar loop → O(E + S) `join_asof` (same semantics).
2. "no frames" path dtype fix (`pl.lit(None, dtype=pl.Float64)`).
3. `hypothesis_generator`: `_threshold`/`in_range` return `None` on uncomputable
   quantile instead of `float(None)` crash (bug fix; skips rule as documented).

**Did not change:** collector, storage, universe, sampling, OB feature
definitions, event selection, target, predictor boundary, validation/OOS,
Discovery, BH, Critic, gates, cost model, hypothesis-generation rules/thresholds,
Registry, live trading.

---

**Conclusion:** OB integration is production-ready. The temporal join that
killed the pipeline now runs in seconds. The only remaining full-universe (72
symbol) limitation is upstream memory pressure in feature computation, which is
independent of this fix.
