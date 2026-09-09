# Order Book Collector — Implementation Report

**Date**: 2026-09-05
**Status**: ✅ READY FOR 24H BURN TEST

---

## Implemented

| Feature | File | Status |
|---------|------|--------|
| Bybit `u` semantics (relaxed) | `book_state.rs` | ✅ |
| Graceful shutdown (SIGTERM/SIGINT) | `ob_reconstructor.rs` | ✅ |
| WebSocket ping/pong + timeout | `ob_reconstructor.rs` | ✅ |
| Exponential backoff reconnect | `ob_reconstructor.rs` | ✅ |
| Disk monitoring (10/5/1 GB) | `ob_reconstructor.rs` | ✅ |
| JSONL metrics export | `ob_reconstructor.rs` | ✅ |
| Parquet atomic writes (tmp→rename) | `ob_reconstructor.rs` | ✅ |
| Daily universe refresh | `ob_reconstructor.rs` | ✅ |
| Raw WS data retention (7 days) | `ob_reconstructor.rs` | ✅ |
| Python daily quality report | `src/ob_daily_report.py` | ✅ |

---

## Reliability

### WebSocket Reconnect
- Exponential backoff: 1→2→4→8→16→32→60s (max)
- Fresh snapshot required after reconnect
- Book state reset before new snapshot

### Ping/Pong
- Client sends ping every 30s
- Activity timeout: 90s (no messages → reconnect)
- Handles both server-initiated and client-initiated pings

### Snapshot Recovery
- State becomes valid only after snapshot arrival
- Deltas applied only when state is valid
- `u` jumps tracked as metrics, NOT as gap indicators

### Graceful Shutdown
- SIGTERM and SIGINT handlers
- Flush all pending data before exit
- Clean WebSocket close
- Metrics saved on shutdown

### Crash Recovery
- Atomic Parquet writes (tmp→rename)
- Corrupted tmp files don't affect existing data
- Collector recovers subscriptions on restart

---

## Data Semantics

### Bybit `u` Field
- Update ID, NOT guaranteed sequential
- `u=1` means snapshot due to service restart
- Official pybit SDK does NOT check `u` sequence
- Our collector tracks `u` jumps as monitoring metrics

### Snapshot vs Delta
- Snapshot: full book state, resets local state
- Delta: incremental changes, applied only when state is valid
- State becomes invalid: waiting for snapshot

### Recording
- Every WebSocket update recorded (full tick-level)
- `freq` controls flush-to-disk timing, NOT sampling frequency
- Average 0.4 updates/sec/symbol (based on Phase 0 data)

---

## Storage

### Parquet Format
- Atomic writes via tmp→rename
- Schema: timestamp_ms, update_id, symbol, best_bid/ask, spread_bps, mid_price, gap_detected, 20 levels (bid_px/sz, ask_px/sz)
- Compression: Snappy

### Raw WS Data
- Location: `data/market/orderbook/raw/{SYMBOL}/{YYYY-MM-DD}.jsonl`
- Retention: 7 days (automatic cleanup)
- Format: one JSON line per update

### Metrics
- Location: `data/market/orderbook/reconstructed/_metrics.jsonl`
- Format: JSON lines with per-symbol statistics
- Update interval: 60 seconds

### Disk Usage (28 symbols)
- Reconstructed: ~1.7 GB/month
- Raw (7-day retention): ~600 GB/week
- Total: ~600 GB peak

---

## Metrics

Each metrics entry contains:
- `timestamp`: ISO 8601
- `symbol`: trading pair
- `updates_received`: total WS messages received
- `updates_written`: total reconstructed snapshots written
- `snapshots`: total snapshots from Bybit
- `reconnects`: total reconnect events
- `update_id_jumps`: non-sequential `u` observations
- `invalid_state_secs`: time waiting for snapshot
- `is_valid`: current book state validity
- `rows_written`: total rows in Parquet files
- `disk_free_gb`: available disk space
- `errors`: write errors count

---

## Tests

### Rust (16 tests)
- `book_state`: 9 tests (snapshot, delta, u-jumps, reset, empty book)
- `ob_reconstructor`: 4 tests (parse snapshot/delta/non-ob, disk free)
- `marketdata`: 6 tests (existing)
- `bybit_rs`: 0 tests (existing)

### Python (272 tests)
- All existing tests pass
- No regression in research pipeline

---

## Burn Test

### NOT RUN

24-hour burn test has not been performed. Before declaring readiness for long-term collection:

1. Run collector with current universe for 24 hours
2. Verify:
   - Continuous operation without crashes
   - Reconnects handled gracefully
   - Storage grows linearly
   - Disk usage within bounds
   - Parquet files readable
   - Metrics collected correctly
   - No invalid-state corruption
   - Shutdown/restart works

### How to Run

```bash
cd /home/vlad/Документы/построение

# Build
cargo build --manifest-path collector/Cargo.toml --release

# Run with current universe
./collector/target/release/ob_reconstructor \
  --universe data/market/symbols/linear.txt \
  --freq 5 \
  --data-root data/market/orderbook/reconstructed

# Monitor metrics
tail -f data/market/orderbook/reconstructed/_metrics.jsonl

# Generate daily report
python src/ob_daily_report.py --date $(date +%Y-%m-%d)
```

---

## Known Limitations

1. **No append-only Parquet**: Current implementation rewrites entire file on flush. For 24h+ collection, this causes O(n²) I/O growth. Acceptable for MVP, upgrade later if needed.

2. **Universe refresh**: New symbols subscribed, removed symbols filtered out. No explicit unsubscribe sent to Bybit (idempotent, not harmful).

3. **No historical gap fill**: If collector misses updates during downtime, those updates are lost. Raw WS data helps debugging but doesn't fill gaps.

---

## Operational Procedure

### Start
```bash
./ob_reconstructor --universe data/market/symbols/linear.txt --freq 5
```

### Monitor
```bash
# Watch logs
tail -f /dev/stderr  # stderr shows stats every 5000 msgs

# Check metrics
cat data/market/orderbook/reconstructed/_metrics.jsonl | jq .

# Daily report
python src/ob_daily_report.py --date 2026-09-05
```

### Stop
```bash
kill -TERM <PID>  # or Ctrl+C
```

### Verify
```bash
# Check Parquet integrity
python -c "
import polars as pl
df = pl.read_parquet('data/market/orderbook/reconstructed/BTCUSDT/2026-09-05.parquet')
print(f'Rows: {len(df)}')
print(f'Symbols: {df[\"symbol\"].unique().to_list()}')
print(df.head())
"
```
