# Memory Leak Fix — 2026-09-06

## Root Cause

`SymState::flush()` in `ob_reconstructor.rs` accumulated `ReconstructedSnapshot` objects in `by_date: HashMap<String, Vec<ReconstructedSnapshot>>` but **never cleared it after writing to parquet**. Every flush cycle:

1. Drained `pending` into `by_date` (new snapshots added)
2. Wrote ALL accumulated snapshots per date to parquet
3. `by_date` retained every snapshot from the start

With 750 symbols at ~185 rows/sec, memory grew ~29 MB/min linearly. Projected 44.5 GB at 24h on a 62 GB system. Would OOM at ~33h.

## Burn Test Evidence

24h burn test (`data/burn_test_20260905/`):

| Time (min) | RSS (GB) | Rows | Connections | Rate |
|---|---|---|---|---|
| 0 | 5.5 | 0 | 0 | - |
| 30 | 6.4 | 228K | 750 | ~30 MB/min |
| 60 | 7.3 | 450K | 750 | ~30 MB/min |
| 120 | 9.2 | 887K | 850 | ~30 MB/min |

Growth was linear and independent of connection count (connections plateaued at 750 but RSS kept growing).

## Fix (2 changes in `collector/src/bin/ob_reconstructor.rs`)

### 1. Clear `by_date` after parquet write (line ~417)

```rust
self.total_flushed += self.by_date.values().map(|v| v.len()).sum::<usize>();
self.by_date.clear();  // <-- added
self.last_write = std::time::Instant::now();
```

### 2. Parquet append instead of overwrite (line ~76-105)

`write_parquet_simple` (overwrote entire file) replaced with `write_parquet_append`:
- If parquet exists: reads existing rows via `ParquetRecordBatchReaderBuilder`, combines with new rows
- Writes combined data back via atomic tmp+rename

This preserves the full-day parquet format while clearing the in-memory buffer.

### 3. `total_flushed` counter for metrics

`metrics_entry.rows_written` and stats line now use a monotonically increasing `total_flushed` counter instead of reading `by_date` (which is now empty between flushes).

## Known Secondary Leak (not fixed)

`Box::leak` in `snapshots_to_table` leaks 80 string allocations (~800 bytes) per call. At 750 symbols * 12 flushes/min = ~345 MB/hour cumulative. This is a small fraction of the 1.8 GB/hour main leak and the strings are effectively static column names. Flagged with `ponytail:` comment for future cleanup if needed.

## Verification

- `cargo test`: 19/19 PASS (9 lib + 6 marketdata + 4 ob_reconstructor)
- `cargo build --release`: OK
- `.venv/bin/python -m unittest discover -s tests -p "test_*.py"`: 272/272 PASS

## Next Step

Re-run 24h burn test to verify RSS stability.
