# PHASE 0 AUDIT REPORT

**Date**: 2026-09-07
**Repository**: `/home/vlad/Документы/построение`
**Scope**: Full codebase audit before paper trading implementation

---

## 1. Hypotheses That Exist

### Baseline (H001-H008) — defined in `research.py:59-83`

| ID | Description | Direction | Horizon |
|----|-------------|-----------|---------|
| H001 | Volume spike + green candle → continuation up | long | 5m |
| H002 | Volume spike + red candle → continuation down | short | 5m |
| H003 | Range spike → impulse continues | long | 10m |
| H004 | Range spike + upper wick → reversal down | short | 15m |
| H005 | Volume spike in US session (14-21 UTC) | long | 5m |
| H006 | Asia quiet volume → recovery | long | 30m |
| H007 | Volume spike + large upper wick → reversal down | short | 10m |
| H008 | Volume spike + large lower wick → reversal up | long | 10m |

### Generated (hypothesis_generator.py) — 29 candidates from candle features
- Candle rules: 14 (relative_volume, relative_range, volume_zscore, realized_vol, etc.)
- OB rules: 16 (ob_spread_bps, ob_imbalance, ob_top1_share, ob_depth_change, ob_spread_change)

### MR Control — `orchestrator.py:228-242`
- 8 MR hypotheses: close/SMA120-1 < threshold + green → long
- Thresholds: -0.01, -0.02, -0.03, -0.05
- Horizons: 10m, 30m

### Total hypotheses tested: 37 (8 baseline + 29 generated)

---

## 2. Discovery Results

**Latest run** (`research_20260904T191533Z.json`):
- **37 hypotheses tested, 0 candidates**
- **ALL hypotheses have NEGATIVE t-stat and NEGATIVE mean_net**
- Discovery period: 2025-12-25 to 2026-04-30
- Events: 2,123,150 (discovery), 1,155,078 (validation), 1,387,415 (OOS)

**MR Control run** (`research_20260904T052815Z.json`):
- Candidates found: MR_SMA120_t5p_h30, MR_SMA120_t3p_h30, MR_SMA120_t5p_h10
- **OOS FAILED**: t=0.062 (needs ≥2.0), mean_net=+0.00003 (essentially zero)
- MR_SMA120_t5p_h30: validation t=1.091 → **failed validation gate**

---

## 3. Validation Results

| Hypothesis | Discovery t | Discovery mean_net | Validation t | Validation mean_net | OOS t | OOS mean_net |
|-----------|------------|-------------------|-------------|-------------------|-------|-------------|
| MR_SMA120_t5p_h30 | 14.15 | +0.0085 | 1.09 | +0.0008 | 0.06 | +0.00003 |
| MR_SMA120_t5p_h10 | 6.95 | +0.0014 | — | — | — | — |
| All 37 baseline+generated | negative | negative | — | — | — | — |

---

## 4. OOS Results

- **MR_SMA120_t5p_h30**: n=10,685, mean_net=+0.00003, t=0.062 → **FAIL** (needs t≥2.0)
- No other hypotheses reached OOS stage

---

## 5. Critic Gate Results

**Latest Critic** (`critic_20260904T054141Z.md`):
- [PASS] leakage
- [PASS] multiple_testing (BH applied)
- [PASS] sample_size (n=21,881,895)
- [PASS] dependency (47+ symbols)
- [PASS] temporal_stability (7/10 months positive)
- [PASS] costs (best t=14.15)
- [PASS] concentration (top1=22%)
- **[FAIL] oos**: OOS t=0.08 < 2.0

---

## 6. Lifecycle Status

| Status | Hypotheses | Notes |
|--------|-----------|-------|
| CANDIDATE | All 37 + MR | Default status, never transitioned |
| VALIDATED | 0 | No hypothesis passed all gates |
| ACTIVE | 0 | — |
| DEPRECATED | 0 | — |
| ARCHIVED | 0 | — |

**Registry file**: DOES NOT EXIST (`data/research/registry.json` not found)
- `HypothesisLifecycle` class exists in `registry.py` but has never been instantiated

---

## 7. Why Paper Trading Was Not Connected

1. **No eligible hypotheses** — all 37 have negative mean_net; MR control failed OOS
2. **No registry state** — HypothesisLifecycle never instantiated
3. **Paper module exists** (`paper.py`) but uses pre-computed mfe/mae/return from events
4. **No real-time signal → execution flow** — paper.py is a backtest runner, not a live executor
5. **Risk config incomplete** — no max qty, no per-instrument limits

---

## 8. Position Sizing / Max Quantity

**In risk.toml**:
- `risk_per_trade_pct = 0.01` (1% of equity)
- `instrument_min_lot = 0.001` (default, not from Bybit API)
- `instrument_qty_step = 0.001`
- `instrument_min_notional = 5.0`

**MISSING**:
- `instrument_max_qty` — **NOT PRESENT**
- `max_position_size` — **NOT PRESENT**
- `max_exposure` — **NOT PRESENT**
- `max_open_positions` — **NOT PRESENT**
- Per-instrument limits — **NOT PRESENT**

---

## 9. Fee / Slippage / PnL Calculation

**In paper.py**:
- Entry fill: `entry * (1 + side * slip)` where `slip = slippage_bps / 10_000`
- Exit fill: `exit_px * (1 - side * slip)`
- Fee: `fee_rt * fill_entry * qty` (round-trip fee on entry notional)
- Net PnL: `(fill_exit - fill_entry) * side * qty - fee`

**Issues**:
- Fee calculated on entry notional only (should be on both entry and exit)
- No per-instrument fee tiers
- Slippage is fixed bps, not dynamic based on book depth

---

## 10. Future Data Leakage Check

**PASSED** — No temporal leakage found:
- Events built from candle close with `shift(-1)` for entry_price
- Targets (return_m, mfe_m, mae_m) computed on full timeseries BEFORE prefilter
- OB features use only `timestamp_ms <= event_t_ms` (enforced in `orderbook_integration.py:115`)
- Critic checks `ob_temporal_leakage` — all passed
- `test_leakage.py` exists with temporal boundary tests

---

## 11. Paper Trading ≠ Live Strategy

**Current state**: Paper trading module exists but has never been executed.
- No API trading code found
- No live strategy mode
- No real order submission
- The invariant `Live → Research allowed; Live → Strategy forbidden` holds

---

## 12. Orderbook Data State

| Component | Status | Details |
|-----------|--------|---------|
| Raw JSONL | ✅ Present | 752 symbol dirs, 612MB total in `data/market/orderbook/raw/` |
| Reconstructed (main) | ❌ Empty | `data/market/orderbook/reconstructed/` has 0 files |
| Reconstructed (burn test 6h) | ✅ Present | 747 symbols, 252MB in `data/burn_test_20260906_6h/orderbook/` |
| Parquet format | ✅ Verified | 90 columns (10 metadata + 80 levels), readable |
| Data integrity | ✅ Verified | No duplicates, timestamps span 6h |

**Critical**: The main reconstructed directory is empty. The reconstructor writes to a separate data root per test. No reconstructed data exists in the path that `orderbook_integration.py` reads from (`data/market/orderbook/reconstructed/`).

---

## 13. Existing Tests

| Suite | Count | Status |
|-------|-------|--------|
| Python tests | 272 | ALL PASS |
| Rust tests | 19 | ALL PASS |
| Orderbook integration tests | 39 | ALL PASS |
| Leakage tests | Present | ALL PASS |
| Critic gate tests | Present | ALL PASS |
| Pipeline tests | Present | ALL PASS |

---

## 14. Summary of Findings

### Critical Blockers
1. **No hypothesis has statistical edge** — all 37 have negative mean_net
2. **MR control failed OOS** — t=0.062, essentially zero
3. **No registry state** — lifecycle tracking never used
4. **Paper trading never executed** — no state, no trades
5. **Reconstructed OB data not in expected path** — main dir is empty
6. **No Bybit instrument info** — tick_size, qty_step, min/max_qty not from API

### Architecture Issues
7. **paper.py is a backtest runner** — uses pre-computed mfe/mae, not real-time signals
8. **No max qty guard** in risk layer
9. **Fee calculation incomplete** — only on entry notional
10. **risk.toml equity_start=10,000** (user wants 1,000)

### What Works
- Statistical framework (BH, validation, OOS, Critic) is correctly implemented
- Temporal leakage checks are thorough
- Test suite is comprehensive (272 Python + 19 Rust)
- Hypothesis generator is properly constrained
- Orderbook feature extraction is correct
- Collector is stable (6h burn test passed)

---

## 15. Recommendations

### For PHASE 1 (Hypothesis Verification)
- Run existing pipeline on available data — results will show NO_CANDIDATE
- This is the correct outcome — the system is working as designed
- Do NOT lower thresholds to force candidates

### For Paper Trading
- Paper trading cannot begin until at least one hypothesis passes all gates
- Currently: **ZERO hypotheses eligible for MODE A (VALIDATED PAPER)**
- MODE B (SHADOW/RESEARCH PAPER) could run for observation only
- But: paper.py needs significant refactoring to work as real-time executor

### For Orderbook Integration
- Reconstructed data needs to be in `data/market/orderbook/reconstructed/`
- The collector currently writes to test-specific directories
- Need to either: (a) run collector to fill main dir, or (b) symlink/copy

### For Instrument Info
- Need to fetch Bybit instrument info via API or hardcode from docs
- tick_size, qty_step, min_order_qty, max_order_qty per symbol

---

## VERDICT

**PAPER BLOCKED — NO ELIGIBLE HYPOTHESES**

No hypothesis has passed discovery, validation, OOS, and Critic gates. Paper trading cannot proceed in MODE A. MODE B (shadow) is available for observation only.
