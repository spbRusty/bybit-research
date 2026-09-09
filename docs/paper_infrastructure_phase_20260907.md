# Paper Trading Infrastructure — Phase 2 Implementation

**Date**: 2026-09-07  
**Status**: Complete  
**Tests**: 301/301 pass (including 29 new paper trading tests)

---

## Summary

Implemented paper trading infrastructure across 13 phases. All phases complete. System now supports real-time signal execution, shadow paper trading (MODE B), risk limits, and Bybit instrument info.

---

## Changes Made

### Phase 1: Collector Config
- **File**: `collector/src/bin/ob_reconstructor.rs` (no change needed)
- `DEFAULT_DATA_ROOT` already points to `data/market/orderbook/reconstructed`
- Action: Run collector without `--data-root` override

### Phase 2: Registry Wiring
- **File**: `src/registry.py` — Added:
  - `update_from_research(results, registry)` — updates lifecycle states from research results
  - `get_eligible_for_paper(registry)` — returns hypotheses in PAPER status
  - `get_shadow_eligible(registry)` — returns CANDIDATE+ status for shadow mode
- **File**: `src/orchestrator.py` — Added:
  - Registry imports
  - Step 6b: registry update after research step

### Phase 3-6: Paper Executor Refactor
- **File**: `src/paper.py` — Complete rewrite:
  - `PaperSignal` dataclass — signal at time T with only pre-T data
  - `PaperTrade` dataclass — trade record with entry/exit fees
  - `PaperMode` enum — `VALIDATED` (MODE A) and `SHADOW` (MODE B)
  - `execute_signal(signal, candle_data, state)` — real-time execution
  - `_determine_exit()` — stop/take/close logic using horizon candles
  - `_calculate_fills()` — entry/exit with slippage
  - `_calculate_fees()` — entry_fee + exit_fee (not just entry)
  - `_position_size()` — qty_step rounding, min/max enforcement
  - `paper_run_backtest()` — backward-compatible for validation/OOS
  - `get_portfolio_summary()` — full metrics
  - `reset_state()` — clean restart
- **Fee fix**: Now charges `entry_fee + exit_fee` (was: only entry notional)
- **Wallet**: `equity_start = 1_000 USDT` in risk.toml

### Phase 7: Instrument Info
- **File**: `src/instrument_info.py` — New module:
  - `get_instruments()` — fetch all linear perpetuals from Bybit REST API
  - `get_instrument(symbol)` — lookup single instrument
  - Local cache with 24h TTL
  - Fallback to risk.toml defaults if API unavailable

### Phase 8: Risk Layer
- **File**: `src/risk.py` — New module:
  - `check_position(qty, entry, state, instrument)` — returns `RiskCheck` (allowed/adjusted_qty/reason)
  - `can_trade(state)` — simple boolean check
  - Limits: `max_position_size_usd=100`, `max_exposure_usd=500`, `max_open_positions=5`
- **File**: `config/risk.toml` — Updated:
  - `equity_start = 1_000.0` (was 10_000)
  - Added: `max_position_size_usd`, `max_exposure_usd`, `max_open_positions`

### Phase 9: Temporal Tests
- **File**: `tests/test_paper_trading.py` — 29 tests:
  - Signal timestamp correctness
  - No future price leakage
  - Fee = entry + exit
  - Slippage direction (long/short)
  - Quantity rounding to step
  - Min/max quantity enforcement
  - Exposure limits
  - Max open positions
  - Duplicate signal handling
  - State persistence/recovery
  - Provenance immutability
  - Portfolio balance
  - Risk halt on zero balance
  - Exit reasons (stop/take/close)
  - Lifecycle integration

### Phase 10: Live Trading Audit
- **Result**: No live trading code paths found
- Grep for `exchange|ccxt|order_id|place.*order|submit.*order` — 0 matches

### Phase 11: Regression
- **Result**: 301/301 tests pass (272 original + 29 new)

### Phase 12: Smoke Test
- **File**: `tests/smoke_paper.py` — End-to-end verification
- Result: All 8 checks pass

---

## Architecture

```
Signal (T) → execute_signal() → PaperTrade → state.json
                              ↓
                           risk.py → RiskCheck (qty limits, exposure)
                              ↓
                           instrument_info.py → Bybit API / cache
```

**Data Flow**:
1. Hypothesis generates signal at time T
2. Signal contains only pre-T data (no leakage)
3. execute_signal() simulates forward using horizon candles
4. Risk layer checks position limits
5. State persisted to `data/paper/state.json`
6. Trades logged to parquet

---

## Files Modified
- `src/paper.py` — Complete rewrite
- `src/registry.py` — Added lifecycle functions
- `src/orchestrator.py` — Added registry wiring
- `config/risk.toml` — Updated equity, added limits

## Files Created
- `src/instrument_info.py` — Bybit instrument info
- `src/risk.py` — Risk layer
- `tests/test_paper_trading.py` — 29 tests
- `tests/smoke_paper.py` — Smoke test

---

## Next Steps
1. Collect production OB data to `data/market/orderbook/reconstructed/`
2. Run research pipeline to generate hypotheses
3. MODE B will activate automatically for CANDIDATE+ hypotheses
4. MODE A blocked until hypothesis passes all gates (currently 0 eligible)
