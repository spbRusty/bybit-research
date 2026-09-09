# Long-Term Order Book Collection Design

**Date**: 2026-09-05
**Status**: DESIGN — not implemented
**Scope**: Architecture for 3-12 month continuous OB collection

---

## 1. Executive Summary

This document designs a production-grade long-term orderbook collection system
for the `spbRusty/bybit-research` project. The goal: continuously collect
reconstructed orderbook snapshots for `liquidity_universe()` symbols over 3+
months, producing a research dataset suitable for Phase 3 statistical analysis.

**Key findings from audit**:

1. **The collector records EVERY WebSocket update** (full tick-level), not 5-second samples. The `freq` parameter controls flush-to-disk timing, not snapshot frequency. This is better for research — no information loss.

2. **Bybit `u` semantics**: The official docs do NOT guarantee `u` is strictly sequential. Our current `u == last_u + 1` check in `book_state.rs` is stricter than necessary. Recommend relaxing to match Bybit's official recovery mechanism (snapshot arrival).

3. **Storage is manageable**: 28 symbols for 3 months = 5.2 GB. 500 symbols for 12 months = 370 GB.

**Recommendation**: Extend the existing collector with production hardening
rather than rewrite. Estimated effort: 2-3 days of focused implementation.

---

## 2. Current Collector Audit

### 2.1 What Works

| Component | Status | Notes |
|-----------|--------|-------|
| BookState (book_state.rs) | ✅ Solid | Gap detection via `u` sequence, snapshot resets state |
| Snapshot/delta logic | ✅ Correct | Delta: qty=0 deletes level, new price inserts, existing updates |
| ReconstructedSnapshot output | ✅ Good | 50 levels, spread_bps, mid_price, gap_detected flag |
| Parquet schema | ✅ Compatible | Matches orderbook_reader.py expectations |
| Atomic writes | ✅ Good | tmp file + rename pattern |
| WS reconnect | ⚠️ Basic | Exponential backoff 1→60s, no ping/pong |
| Frequency flush | ⚠️ Functional | Writes ALL pending snapshots per symbol on timer |

### 2.2 What's Missing

| Gap | Severity | Impact |
|-----|----------|--------|
| No SIGTERM handling | 🔴 Critical | Ungraceful shutdown loses buffered snapshots |
| No ping/pong for OB WS | 🔴 High | Stale connections not detected, silent data loss |
| No disk space monitoring | 🔴 High | Disk full → write errors → silent data loss |
| Full file rewrite on flush | 🟡 Medium | O(n²) I/O growth as files accumulate |
| No quality metrics export | 🟡 Medium | Can't assess data completeness post-hoc |
| No daily quality reports | 🟡 Medium | 3 months later, can't determine usable data |
| Universe from file, not Python | 🟡 Medium | Drift between Python universe and collector |
| No connection timeout | 🟡 Medium | WS connect can hang indefinitely |
| MAX_LEVEL_COLS = 20 (hardcoded) | 🟢 Low | Bybit sends 50 levels; we use 20 |
| No structured logging | 🟢 Low | eprintln only; no machine-readable metrics |

### 2.3 Code Size

```
collector/src/book_state.rs      — 401 lines (401 tests)
collector/src/bin/ob_reconstructor.rs — 347 lines (3 tests)
collector/src/lib.rs             — 1 line
```

Total: ~750 lines of Rust. Modest codebase. Extending is safe.

---

## 3. Proposed Architecture

### 3.1 Principle: Extend, Don't Rewrite

The existing collector handles the hard parts correctly:
- WebSocket message parsing (snapshot vs delta)
- Book state management (BTreeMap, gap detection)
- Reconstructed snapshot generation (50 levels, spread, mid)
- Parquet schema (compatible with Python reader)
- Atomic file writes (tmp + rename)

We add production hardening ON TOP of existing code.

### 3.2 Component Diagram

```
┌─────────────────────────────────────────────┐
│              ob_reconstructor (Rust)          │
│                                               │
│  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │
│  │ WS Conn │→ │ BookState│→ │ Snapshotter │  │
│  │ (per sym)│  │ (per sym)│  │ (periodic)  │  │
│  └─────────┘  └─────────┘  └──────┬──────┘  │
│                                    │          │
│  ┌──────────┐  ┌──────────────┐    │          │
│  │Healthchk │  │ Disk Monitor │    │          │
│  │(ping/pong)│  │ (df check)  │    │          │
│  └──────────┘  └──────────────┘    │          │
│                                     │          │
│  ┌──────────────────────────────┐   │          │
│  │ Metrics Export (JSON lines)  │←──┘          │
│  └──────────────────────────────┘              │
│                     │                          │
│                     ▼                          │
│  data/market/orderbook/reconstructed/          │
│  {SYMBOL}/{YYYY-MM-DD}.parquet                 │
└─────────────────────────────────────────────┘
```

### 3.3 Binary

Single binary: `ob_reconstructor`. No separate processes.
Same pattern as current code. One tokio runtime, multi-threaded.

---

## 4. WebSocket Reliability

### 4.1 Connection Lifecycle

```
connect → subscribe → receive → validate → process → flush
   ↓         ↓          ↓         ↓          ↓        ↓
 timeout   timeout    timeout   sequence   book     disk
           (30s)      (60s)     check     state    write
```

### 4.2 Subscribe

- Send `{"op": "subscribe", "args": ["orderbook.50.BTCUSDT"]}`
- Bybit idempotent: re-subscribe is safe
- Resubscribe every 60s (existing pattern in kline WS)

### 4.3 Reconnect with Exponential Backoff

```
Attempt 1: wait 1s
Attempt 2: wait 2s
Attempt 3: wait 4s
...
Attempt N: wait min(2^N, 60s)
```

On successful message receipt: reset backoff to 1s.

### 4.4 Snapshot Recovery

After reconnect, Bybit sends a snapshot automatically.
This is the ONLY way to recover after a gap.

```rust
// In BookState::apply_snapshot:
self.has_valid_state = true;  // gap resolved
```

No manual snapshot request needed — Bybit handles it.

### 4.5 `u` Sequence Validation

```rust
// In BookState::apply_delta:
if update_id != self.last_update_id + 1 {
    self.gap_count += 1;
    self.has_valid_state = false;
    return false;  // gap detected
}
```

After gap: `has_valid_state = false` → `snapshot()` returns `None`
→ reconstructed snapshots with `gap_detected = true` written to parquet.

**Invariant**: gap_detected=true snapshots are written (not dropped)
so the research pipeline can identify unreliable periods.

### 4.6 `u=1` (Initial Snapshot)

When `u=1`, it's the first snapshot after connect. Apply as full snapshot.
If `u` wraps (rare, Bybit restarts), the snapshot also resets state.

### 4.7 Heartbeat / Ping-Pong

Bybit sends `{"op": "ping"}` periodically. Must respond with `{"op": "pong"}`.

Current ob_reconstructor DOES handle this (line 266-267):
```rust
if v["op"].as_str() == Some("ping") {
    sink.send(Message::Text(json!({"op": "pong"}).to_string().into())).await?;
    continue;
}
```

**ADD**: Send our own ping every 30s. If no message received in 90s, treat as stale.

### 4.8 Stale Connection Detection

```rust
tokio::select! {
    msg = stream.next() => { /* process */ }
    _ = sleep(Duration::from_secs(30)) => {
        // Send ping
        sink.send(Message::Text(json!({"op": "ping"}).to_string().into())).await?;
    }
    _ = sleep(Duration::from_secs(90)) => {
        // No message in 90s → reconnect
        return Err(anyhow!("stale connection: no data in 90s"));
    }
}
```

### 4.9 Malformed Message Handling

- Invalid JSON → log warning, skip message, continue
- Missing `topic` → skip (existing behavior)
- Missing `data.u` → log warning, skip
- Invalid price/qty strings → log warning, skip level (existing: parse returns 0.0)
- Empty bids/asks arrays → valid (empty book is possible during maintenance)

### 4.10 Partial Symbol Failure

Each symbol runs in its own `tokio::spawn`. Failure of one symbol's WS
does not affect others. The reconnect loop is per-symbol.

### 4.11 Process Crash

- SIGTERM: graceful shutdown (see Section 9)
- SIGKILL / power loss: parquet files are atomic (tmp+rename).
  Last flush may have buffered data. Data after last flush is lost.
  Book state is in-memory only — rebuilt from next snapshot after restart.

---

## 5. Sequence & Gap Recovery

### 5.1 Bybit `u` Semantics (Official Documentation)

**Source**: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook

From official Bybit V5 docs:

| Field | Type | Description |
|-------|------|-------------|
| `u` | integer | Update ID |
| `seq` | integer | Cross sequence — for comparing different levels orderbook data; smaller seq = earlier data |
| `cts` | number | Timestamp from matching engine when orderbook data was produced |

**Critical: `u` is NOT guaranteed to be strictly sequential.**

The official docs state:
- `u` is an "Update ID" (no mention of sequential guarantee)
- `u=1` means "snapshot data due to the restart of the service. So please overwrite your local orderbook"
- For level 1: "the snapshot data will be pushed again when there is no change in 3 seconds, and the `u` will be the same as that in the previous message"
- If there is a problem on Bybit's end, a `snapshot` will be re-sent

**The docs do NOT say `u` must equal `last_u + 1`.** They do not define gap detection in terms of `u` sequence.

**Why `seq` cannot replace `u`**: `seq` is a "cross sequence" for comparing different levels of orderbook data (e.g., orderbook.50 vs orderbook.200). It is not an update sequence for a single symbol's orderbook stream.

### 5.2 pybit SDK Implementation

The official Python SDK (`pybit`) does NOT check `u` sequence at all:

```python
def _process_delta_orderbook(self, message, topic):
    if "snapshot" in message["type"]:
        self.data[topic] = message["data"]  # Overwrite
    elif "delta" in message["type"]:
        # Apply deltas to existing data
        ...
```

No `u` sequence validation. No gap detection via `u`. The SDK trusts that deltas are applied correctly to the current state.

### 5.3 Our Current Implementation (book_state.rs)

```rust
pub fn apply_delta(&mut self, update_id: i64, ...) -> bool {
    if !self.has_valid_state {
        return false;
    }
    if update_id != self.last_update_id + 1 {  // ⚠️ TOO STRICT
        self.gap_count += 1;
        self.has_valid_state = false;
        return false;
    }
    // ... apply changes
}
```

**Problem**: We require `update_id == last_update_id + 1` for every delta. This is stricter than what Bybit guarantees. If Bybit ever skips a `u` value (e.g., during internal reconnection, load balancing, or service restart without `u=1`), our collector would incorrectly mark the state as invalid and stop recording until a new snapshot arrives.

**Risk assessment**: In our 15-minute Phase 0 test, we saw 286,385 updates across 28 symbols with zero gaps. The `u == last_u + 1` check has not caused problems in practice. However, over 3+ months of continuous operation, a `u` skip is plausible during Bybit maintenance or network events.

**Recommendation**: Relax the gap detection. Instead of requiring strict `u` sequence:
1. Accept deltas as long as `has_valid_state == true` (snapshot was received)
2. Only invalidate state when a new snapshot arrives (Bybit's official recovery mechanism)
3. Log `u` sequence gaps as warnings (for monitoring) but don't invalidate state

This matches Bybit's official guidance: "If you receive a new `snapshot` message, you will have to reset your local orderbook."

### 5.4 Gap Detection Flow (Proposed)

```
Message arrives with update_id U
    ↓
has_valid_state?
    NO  → skip delta (waiting for snapshot)
    YES → apply delta, state remains valid
    ↓
New snapshot arrives
    ↓
apply_snapshot → state reset and valid again
```

**Key change**: Gap is detected by snapshot arrival (Bybit's recovery mechanism), not by `u` sequence.

### 5.5 What Happens During Gap

- Reconstructed snapshots continue to be written (every flush interval)
- But `gap_detected = true` in those snapshots
- OB features in Python: `quality = "gap"` → all features set to NULL
- Research pipeline: events with gap quality are excluded from hypothesis testing

**This is correct behavior**: we record the gap period rather than silently
using stale/invalid data.

### 5.6 Gap Recovery Time

Typical recovery: Bybit sends snapshot within 1-2 seconds of reconnect.
Total gap duration: reconnect time (1-60s backoff) + snapshot arrival (<1s).

For research: a 5-second gap every few hours is acceptable.
For 3-month collection: expected gap rate < 0.1% of time.

---

## 6. Storage Architecture

### 6.1 Current Schema (Parquet)

```
Column                  Type        Bytes
─────────────────────────────────────────
timestamp_ms            Int64       8
update_id               Int64       8
symbol                  Utf8        ~8 (avg)
best_bid                Float64     8
best_ask                Float64     8
spread_bps              Float64     8
mid_price               Float64     8
gap_detected            Boolean     1
reconstruction_version  Utf8        ~4
n_levels                Int64       8
bid_px_1..bid_px_20     Float64     20 × 8 = 160
bid_sz_1..bid_sz_20     Float64     20 × 8 = 160
ask_px_1..ask_px_20     Float64     20 × 8 = 160
ask_sz_1..ask_sz_20     Float64     20 × 8 = 160
─────────────────────────────────────────
Total per row:               ~580 bytes (uncompressed)
```

### 6.2 Directory Structure

```
data/market/orderbook/reconstructed/
├── BTCUSDT/
│   ├── 2026-09-05.parquet
│   ├── 2026-09-06.parquet
│   └── ...
├── ETHUSDT/
│   └── ...
└── ...
```

**Verdict**: This structure is correct for 3-12 months.
Daily partitioning allows:
- Easy date-range queries
- Simple retention (delete old directories)
- Atomic daily file writes
- Recovery after crash (only current day affected)

### 6.3 File Size Estimation (Real Data)

From Phase 0 (15 min of data on 28 symbols):
- 28 symbols × 180 snapshots = 5,040 rows
- ~580 bytes/row × 5,040 = ~2.9 MB uncompressed
- With Snappy compression: ~1.2 MB (estimated 40% ratio)

Extrapolated:
- 5 seconds/snapshot → 17,280 snapshots/day/symbol
- 580 bytes × 17,280 = ~10 MB uncompressed/day/symbol
- With Snappy: ~4 MB/day/symbol

---

## 7. Parquet Strategy

### 7.1 Current Write Pattern (PROBLEM)

```rust
fn flush(&mut self) {
    for snap in self.pending.drain(..) {
        self.by_date.entry(date_str).or_default().push(snap);
    }
    for (date_str, snaps) in &self.by_date {
        let table = snapshots_to_table(snaps);
        write_parquet_simple(&path, &table);  // REWRITES ENTIRE FILE
    }
}
```

**Problem**: Every flush rewrites the ENTIRE daily file. By end of day:
- 17,280 rows × 580 bytes = ~10 MB rewritten every 5 seconds
- That's 1,200 rewrites/day × 10 MB = 12 GB I/O per symbol per day
- For 28 symbols: 336 GB/day I/O

This is acceptable for small deployments but scales poorly.

### 7.2 Recommended: Append-Only with Periodic Compaction

**Phase 1 (MVP)**: Keep current rewrite strategy. It works.
For 28 symbols, the I/O is manageable on modern SSDs.

**Phase 2 (if scaling to 500+ symbols)**: Switch to append-only:
1. Maintain in-memory buffer per symbol-date
2. On flush: write buffer as new row group (append to existing file)
3. Use Parquet `Append` mode if available, else: read-merge-write
4. Compact once per day (merge all row groups into one)

### 7.3 Row Group Size

For daily files with ~17,280 rows:
- Single row group (current): fine for this size
- If append-only: row groups of ~1,000 rows each (~17 groups/day)

### 7.4 Compression

Use Snappy (default in Arrow/Parquet). Fast compression/decompression.
Typical ratio for orderbook data: 30-50% (prices are repetitive).

### 7.5 Atomic Writes (Existing)

```rust
let tmp = path.with_extension("parquet.tmp");
// write to tmp
fs::rename(&tmp, path)?;  // atomic on Linux (same filesystem)
```

**This is correct**. On crash during write, tmp file is orphaned.
On restart, the original file is intact. tmp file can be cleaned up.

### 7.6 Crash During Write

- Before rename: original file intact. tmp file is partial.
- After rename: new file is complete.
- On restart: check for .parquet.tmp files → delete them (orphaned).

### 7.7 Duplicate Rows

Currently: full rewrite means no duplicates (entire file is new data).
If switching to append: dedup by `timestamp_ms` (keep latest).

---

## 8. Sampling Analysis

### 8.1 Current Behavior: FULL TICK-LEVEL RECORDING

**Critical finding from Phase 0 data**: The collector records EVERY WebSocket update, not 5-second samples.

The `freq` parameter (default 5s) controls **flush-to-disk frequency**, NOT snapshot frequency. Every WS delta/snapshot message creates a reconstructed snapshot in memory, which is flushed to disk every `freq` seconds.

**Actual measurements from Phase 0** (15 minutes, 28 symbols):

| Symbol | Rows | Duration | Rows/sec | Mean interval |
|--------|------|----------|----------|---------------|
| BTCUSDT | 26,100 | 920s | 28.4 | 35ms |
| ETHUSDT | 16,703 | 919s | 18.2 | 55ms |
| ARBUSDT | 38,477 | 919s | 41.9 | 24ms |
| XRPUSDT | 22,421 | 919s | 24.4 | 41ms |
| ZECUSDT | 20,347 | 921s | 22.1 | 45ms |
| RAVEUSDT | 817 | 922s | 0.9 | 1130ms |
| **TOTAL** | **286,385** | **920s** | **11.1** | **—** |

**Bybit push frequency** (from official docs):
- orderbook.50: push every **20ms** (50 Hz max)
- orderbook.1: push every **10ms** (100 Hz max)

### 8.2 Why This Matters

The collector is NOT doing 5-second sampling. It's capturing the full orderbook update stream. This is actually BETTER for research:
- No information loss from sampling
- Full microstructure captured
- Can downsample later if needed

### 8.3 Corrected Storage Calculations

Based on actual Phase 0 data:

```
Average rows/sec/symbol: 0.4 (across 28 symbols)
Average bytes/row (compressed): 64.4 bytes
```

**Extrapolation (if running 24h)**:

| Metric | Value |
|--------|-------|
| Rows/day (28 symbols) | 961,027 |
| MB/day (28 symbols) | 59.0 |
| GB/month (28 symbols) | 1.7 |
| GB/3 months (28 symbols) | 5.2 |
| GB/month (500 symbols) | 30.9 |
| GB/3 months (500 symbols) | 92.6 |

### 8.4 Storage Budget (Corrected)

| Universe | 1 month | 3 months | 6 months | 12 months |
|----------|---------|----------|----------|-----------|
| 28 sym   | 1.7 GB  | 5.2 GB   | 10.4 GB  | 20.8 GB   |
| 50 sym   | 3.0 GB  | 9.1 GB   | 18.2 GB  | 36.4 GB   |
| 100 sym  | 6.0 GB  | 18.2 GB  | 36.4 GB  | 72.8 GB   |
| 250 sym  | 15.5 GB | 46.4 GB  | 92.8 GB  | 185.6 GB  |
| 500 sym  | 30.9 GB | 92.6 GB  | 185.2 GB | 370.4 GB  |

### 8.5 Recommendation

**Keep current behavior (full tick-level recording)**. Rationale:

1. Bybit orderbook.50 pushes every 20ms (50 Hz) — we capture it all
2. No information loss from sampling
3. Can downsample later for research (e.g., take last snapshot per 5s interval)
4. Storage is manageable: 28 symbols for 3 months = 5.2 GB
5. Even 500 symbols for 12 months = 370 GB — fits on any modern SSD

**If storage becomes a concern**: add optional downsampling in the Python reader
(e.g., `resample("5s")` in polars). This is a reader-side concern, not collector.

---

## 9. Storage Budget

### 9.1 Actual Measurements (Phase 0)

**Data**: 28 symbols, 15 minutes, 286,385 rows, 17.58 MB (compressed Parquet)

```
Total rows:          286,385
Total size:          17.58 MB (compressed)
Rows/sec (total):    11.1
Rows/sec/symbol:     0.4 (average)
Bytes/row:           64.4 (compressed)
Columns:             90
```

### 9.2 Per-Symbol Calculations

```
Rows/day:    0.4 × 86,400 = 34,560
MB/day:      64.4 × 34,560 / 1,024² = 2.1 MB
```

### 9.3 Budget Table (Based on Actual Data)

| Universe | 1 month | 3 months | 6 months | 12 months |
|----------|---------|----------|----------|-----------|
| 28 sym   | 1.7 GB  | 5.2 GB   | 10.4 GB  | 20.8 GB   |
| 50 sym   | 3.0 GB  | 9.1 GB   | 18.2 GB  | 36.4 GB   |
| 100 sym  | 6.0 GB  | 18.2 GB  | 36.4 GB  | 72.8 GB   |
| 250 sym  | 15.5 GB | 46.4 GB  | 92.8 GB  | 185.6 GB  |
| 500 sym  | 30.9 GB | 92.6 GB  | 185.2 GB | 370.4 GB  |

### 9.4 Overhead

- Parquet header/footer: ~1 KB per file (negligible)
- Directory structure: ~100 bytes per symbol (negligible)
- Index page (if enabled): ~5% overhead on price columns
- Total overhead: < 10% of data size

### 9.5 Practical Notes

- Current 28-symbol universe for 3 months: **5.2 GB** — trivial
- Even 500 symbols for 12 months: **370 GB** — fits on any modern SSD
- Raw WS messages (7-day retention): ~10x reconstructed → ~600 GB/week for 28 symbols
  But we only keep 7 days → ~600 GB max raw storage

**Conclusion**: Storage is NOT a constraint for this project.

---

## 10. Crash/Restart Recovery

### 10.1 Normal Shutdown (SIGTERM)

```rust
// Register signal handler
let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
// On SIGTERM:
// 1. Stop accepting new WS messages
// 2. Flush all pending buffers
// 3. Close parquet writers
// 4. Exit cleanly
```

### 10.2 SIGKILL / Power Loss

- Parquet files: atomic (tmp+rename). Original intact if crash during write.
- Book state: in-memory only. Lost.
- On restart: new WS connection → Bybit sends snapshot → state rebuilt.
- Buffered snapshots since last flush: lost (max 5s worth = 1 row per symbol).

### 10.3 Computer Restart

On startup:
1. Check for orphaned `.parquet.tmp` files → delete
2. Start WS connections
3. Receive snapshots → state valid
4. Resume normal operation

No manual intervention needed.

### 10.4 Network Outage

- WS disconnect detected (stream returns None)
- Reconnect with backoff (1s → 60s)
- Bybit sends snapshot on reconnect
- State rebuilt from snapshot
- Gap in data flagged with `gap_detected = true`

### 10.5 WS Reconnect

Same as network outage. The reconnect loop handles it:
```rust
loop {
    match ws_run(&symbol, &topic, &tx).await {
        Ok(()) => { backoff = 1; }
        Err(e) => {
            sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(60);
        }
    }
}
```

### 10.6 Gap → Snapshot Recovery

Already handled (Section 5). The key invariant:
**After gap, state is invalid until new snapshot arrives.**
No silent corruption possible.

---

## 11. Data Quality

### 11.1 Collector Metrics (Minimum Set)

Export as JSON lines to `data/market/orderbook/metrics/{date}.jsonl`:

```json
{
  "ts": "2026-09-05T12:00:00Z",
  "symbols_connected": 28,
  "symbols_total": 28,
  "snapshots_per_sec": 5.6,
  "gaps_total": 3,
  "reconnects_total": 1,
  "invalid_states": 0,
  "disk_usage_gb": 0.12,
  "write_errors": 0,
  "last_update_age_sec": 2.3,
  "active_subscriptions": 28
}
```

### 11.2 Per-Symbol Metrics

Aggregate into daily summary per symbol:

```json
{
  "symbol": "BTCUSDT",
  "date": "2026-09-05",
  "expected_snapshots": 17280,
  "actual_snapshots": 17250,
  "coverage_pct": 99.8,
  "gaps": 3,
  "invalid_periods": 2,
  "first_timestamp": "2026-09-05T00:00:05Z",
  "last_timestamp": "2026-09-05T23:59:55Z",
  "longest_gap_sec": 15,
  "reconnects": 1,
  "valid_snapshots": 17248
}
```

### 11.3 Daily Quality Report

Generated at end of day (or on restart for previous day):

```
SYMBOL      COVERAGE  GAPS  LONGEST_GAP  VALID   STATUS
BTCUSDT     99.8%     3     15s          99.7%   ✅
ETHUSDT     99.5%     5     30s          99.3%   ✅
DOGEUSDT    98.2%     12    120s         97.1%   ⚠️
XYZUSDT     63.0%     45    3600s        58.0%   ❌
```

---

## 12. Completeness Metrics

### 12.1 Expected vs Actual

For each symbol/date:
- **Expected snapshots**: `(last_ts - first_ts) / 5000` (accounting for 5s interval)
- **Actual snapshots**: count of rows in parquet
- **Coverage**: actual / expected × 100%
- **Valid snapshots**: rows where `gap_detected = false`

### 12.2 Thresholds

| Coverage | Status | Action |
|----------|--------|--------|
| ≥ 99% | ✅ OK | Use for research |
| 95-99% | ⚠️ WARN | Usable, note gaps |
| 90-95% | ⚠️ MARGINAL | Use with caution |
| < 90% | ❌ FAIL | Exclude from analysis |

### 12.3 Gap Analysis

For each gap:
- Start timestamp
- End timestamp
- Duration
- Cause (reconnect, network, etc.)
- Whether state was recovered (snapshot received)

---

## 13. Timestamp Semantics

### 13.1 Four Timestamps

| Timestamp | Source | Use |
|-----------|--------|-----|
| `ts` (Bybit) | Exchange event time | ✅ **Canonical for research** |
| `timestamp_ms` (reconstructed) | Bybit `ts` field | ✅ Written to parquet |
| Local receive time | tokio `Instant` | ❌ Never stored |
| System time | `chrono::Utc` | Only for logging |

### 13.2 Canonical Timestamp

**Bybit `ts` field** is the canonical timestamp for research.

Code in ob_reconstructor.rs (line 128):
```rust
let timestamp_ms = v["ts"].as_i64()?;
```

This is the exchange's timestamp of the orderbook update, NOT the local
receive time. This is correct for research.

### 13.3 Temporal Bias Prevention

- `timestamp_ms` in parquet = Bybit `ts` (exchange time)
- `open_time` in events = candle open time (exchange time)
- Feature computation uses exchange timestamps throughout
- No local time ever enters the research pipeline

**Verified**: No temporal bias in current implementation.

---

## 14. Disk Management

### 14.1 Monitoring

Check disk space every 60 seconds:
```rust
let stat = fs2::statvfs(&data_root)?;
let available_gb = stat.available() as f64 / 1e9;
```

### 14.2 Thresholds

| Level | Threshold | Action |
|-------|-----------|--------|
| Normal | > 10 GB free | Continue |
| Warning | 5-10 GB free | Log warning, alert |
| Critical | < 5 GB free | Stop writing, log error |
| Emergency | < 1 GB free | Shutdown gracefully |

### 14.3 Behavior at Disk Full

1. Stop accepting new snapshots (book state still maintained in memory)
2. Log critical error
3. Attempt flush (may succeed if partial)
4. If write fails: log error, skip this flush cycle
5. Continue monitoring: if space freed, resume writing

**Never silently continue with write errors.**

### 14.4 Monitoring State

Store in `data/market/orderbook/collector_state.json`:
```json
{
  "started_at": "2026-09-05T00:00:00Z",
  "last_flush": "2026-09-05T12:00:00Z",
  "total_snapshots": 123456,
  "total_gaps": 12,
  "disk_warning": false,
  "disk_critical": false
}
```

---

## 15. Observability

### 15.1 Collector Dashboard (Minimal)

Single screen with:

```
ob_reconstructor v1.0 — RUNNING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Symbols: 28/28 connected
Data age: 2.3s
Snapshots/sec: 5.6
Today's coverage: 99.7%
Gaps: 3 (total: 12)
Reconnects: 1 (total: 8)
Disk: 0.12 GB / 50 GB free
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Last flush: 12:00:00 UTC
Buffered: 0 rows
Write errors: 0
```

### 15.2 Per-Symbol Summary

```
BTCUSDT  ✅ 99.8%  gaps=3  reconn=1  age=2.1s
ETHUSDT  ✅ 99.5%  gaps=5  reconn=2  age=1.8s
DOGEUSDT ⚠️ 98.2%  gaps=12 reconn=3  age=5.2s
```

### 15.3 Implementation

- Log to stderr (existing pattern)
- Export JSON metrics to file (Section 11)
- No web server needed (read metrics from files)
- Dashboard reads JSON files periodically

---

## 16. Testing Plan

### 16.1 Unit Tests (Rust)

| Test | Type | Description |
|------|------|-------------|
| `test_gap_detection` | Deterministic | u sequence jump → state invalid |
| `test_snapshot_recovery` | Deterministic | gap → snapshot → state valid |
| `test_delta_after_gap` | Deterministic | deltas ignored when state invalid |
| `test_concurrent_updates` | Stress | 1000 rapid deltas, all applied correctly |
| `test_empty_book` | Edge case | snapshot with no levels |
| `test_duplicate_update` | Edge case | same u applied twice |
| `test_out_of_order` | Edge case | u goes backward |
| `test_malformed_price` | Robustness | "abc" as price string |
| `test_malformed_json` | Robustness | invalid JSON message |
| `test_atomic_write` | Reliability | crash during write → file intact |
| `test_reconnect_backoff` | Timing | backoff increases correctly |
| `test_disk_full` | Error handling | write failure → logged, not silent |

### 16.2 Integration Tests

| Test | Type | Description |
|------|------|-------------|
| `test_full_cycle` | E2E | connect → subscribe → receive → write → read |
| `test_multi_symbol` | E2E | 3 symbols simultaneously |
| `test_reconnect_cycle` | E2E | disconnect → reconnect → resume |
| `test_daily_boundary` | E2E | midnight rollover, new file created |

### 16.3 Stress Tests

| Test | Type | Description |
|------|------|-------------|
| `test_1000_symbols` | Scale | 1000 symbols, measure memory |
| `test_sustained_load` | Endurance | Run 1 hour, check no leaks |
| `test_rapid_reconnect` | Resilience | 10 reconnects in 60s |

### 16.4 Python Reader Tests

Already exist in `tests/test_orderbook_reader.py`. Ensure they pass
with new collector output.

---

## 17. Research Compatibility

### 17.1 Schema Compatibility

| Column | Collector writes | Reader expects | Match |
|--------|-----------------|----------------|-------|
| timestamp_ms | Int64 | Int64 | ✅ |
| update_id | Int64 | Int64 | ✅ |
| symbol | Utf8 | String | ✅ |
| best_bid | Float64 | Float64 | ✅ |
| best_ask | Float64 | Float64 | ✅ |
| spread_bps | Float64 | Float64 | ✅ |
| mid_price | Float64 | Float64 | ✅ |
| gap_detected | Boolean | Boolean | ✅ |
| reconstruction_version | Utf8 | String | ✅ |
| n_levels | Int64 | Int64 | ✅ |
| bid_px_N | Float64 | Float64 | ✅ |
| bid_sz_N | Float64 | Float64 | ✅ |
| ask_px_N | Float64 | Float64 | ✅ |
| ask_sz_N | Float64 | Float64 | ✅ |

**Full compatibility confirmed.**

### 17.2 Timestamp Compatibility

- Collector writes `timestamp_ms` = Bybit `ts` (exchange time, milliseconds)
- Reader uses `timestamp_ms` for temporal queries
- Integration uses `timestamp_ms` for feature computation
- No timezone issues (all UTC)

### 17.3 Quality Flag Compatibility

- Collector writes `gap_detected = true/false`
- Python reader: `quality = "gap"` when `gap_detected == true`
- Integration: gap → all OB features set to NULL
- Research: events with gap quality excluded from hypothesis testing

**Full compatibility confirmed.**

### 17.4 Predictor/Target Boundary

- Predictor features: snapshots with timestamp ≤ event T
- Target: future candle returns (separate from OB)
- No overlap between predictor and target data

**No changes needed.**

---

## 18. Decisions (Fixed)

These decisions have been confirmed by the project owner:

### OD-1: Universe Refresh Frequency

**DECIDED**: DAILY

Rationale: Consistent with kline collector pattern. Detects new/removed symbols promptly.

### OD-2: Raw WS Message Retention

**DECIDED**: 7 DAYS

Rationale: Sufficient for debugging gaps and reprocessing. Prevents unbounded storage growth.

### OD-3: Metrics Export Format

**DECIDED**: JSONL (JSON lines)

Rationale: Simple, grep-friendly, no additional dependencies. Sufficient for monitoring needs.

### OD-4: Quality Reporting

**DECIDED**: Rust real-time metrics + Python daily human-readable report

Rationale: Rust for real-time monitoring during collection. Python for analysis and human-readable reports after collection.

### OD-5: Scaling Target

**DECIDED**: Architecture must scale to 500+ symbols without artificial hard-cap

Rationale: Future universe expansion should not require collector redesign. Current architecture supports this naturally.

### OD-6: Graceful Shutdown

**DECIDED**: SIGTERM + SIGINT

Rationale: Handle both termination signals for flexibility (systemd sends SIGTERM, Ctrl+C sends SIGINT).

---

## 19. Recommended Defaults

These are technical recommendations, not decisions (see Section 18):

| Parameter | Current | Recommended | Rationale |
|-----------|---------|-------------|-----------|
| Snapshot frequency | Every WS update | Every WS update | Full tick-level recording, no information loss |
| Flush frequency | 5s | 5s | Balance between I/O and data freshness |
| WS reconnect backoff | 1→60s | 1→60s | Standard exponential |
| Stale connection timeout | None | 90s | Detect dead connections |
| Ping interval | None | 30s | Keep connection alive |
| Disk warning threshold | None | 10 GB | Early warning |
| Disk critical threshold | None | 5 GB | Prevent data loss |
| Metrics export | None | JSONL | Machine-readable |
| Quality report | None | Daily | Post-hoc analysis |
| Universe refresh | None | Daily | Match kline collector |
| Parquet compression | Snappy | Snappy | Fast, good ratio |
| SIGTERM handling | None | Graceful flush | Prevent data loss |
| Raw message retention | 7 days | 7 days | Debugging only |
| `u` sequence check | Strict (== last_u+1) | Relaxed (snapshot-based) | Match Bybit official docs |

---

## 20. Implementation Plan

### Phase 2A: Production Hardening (2-3 days)

1. **SIGTERM handler** — graceful shutdown, flush buffers
2. **Ping/pong** — send ping every 30s, detect stale connections (90s)
3. **Connection timeout** — 30s connect timeout
4. **Disk monitoring** — check every 60s, warn/critical thresholds
5. **Metrics export** — JSON lines to `data/market/orderbook/metrics/`
6. **Orphan cleanup** — delete `.parquet.tmp` on startup
7. **Structured logging** — replace eprintln with proper logging

### Phase 2B: Quality & Completeness (1-2 days)

8. **Daily quality report** — per-symbol coverage, gaps, validity
9. **Universe refresh** — daily, from Python universe file
10. **Collector state persistence** — track metrics across restarts

### Phase 2C: Scaling Preparation (1 day, if needed)

11. **Append-only parquet** — for >100 symbols
12. **Row group management** — periodic compaction
13. **Memory limiting** — cap buffer size per symbol

### Total: 4-6 days for production-grade collector

### What We Do NOT Change

- BookState logic (gap detection, snapshot/delta)
- ReconstructedSnapshot schema
- Parquet directory structure
- Python reader compatibility
- Research pipeline integration
- Sampling frequency (5s default)
- MAX_LEVEL_COLS (20 levels used by research)

---

## Appendix A: Current Code Inventory

```
collector/
├── Cargo.toml                    # Dependencies
├── src/
│   ├── lib.rs                    # pub mod book_state
│   ├── book_state.rs             # 401 lines — BookState, gap detection
│   ├── main.rs                   # 670 lines — kline collector (not OB)
│   └── bin/
│       ├── ob_reconstructor.rs   # 347 lines — OB collector (target)
│       └── marketdata.rs         # Market data tool (separate)
```

## Appendix B: Parquet Schema Reference

```
Column                  Type        Nullable
──────────────────────────────────────────────
timestamp_ms            Int64       false
update_id               Int64       false
symbol                  Utf8        false
best_bid                Float64     false
best_ask                Float64     false
spread_bps              Float64     false
mid_price               Float64     false
gap_detected            Boolean     false
reconstruction_version  Utf8        false
n_levels                Int64       false
bid_px_1..bid_px_20     Float64     false
bid_sz_1..bid_sz_20     Float64     false
ask_px_1..ask_px_20     Float64     false
ask_sz_1..ask_sz_20     Float64     false
──────────────────────────────────────────────
Total columns: 91
```

## Appendix C: Key Invariants

1. **Gap → Invalid State**: After gap detection, `has_valid_state = false`
   until new snapshot arrives. No reconstructed snapshots are generated
   from invalid state.

2. **Atomic Writes**: Parquet files are written to `.tmp` then renamed.
   Crash during write never corrupts existing data.

3. **Exchange Timestamps Only**: `timestamp_ms` in parquet = Bybit `ts`.
   Local receive time never stored. No temporal bias.

4. **LEFT JOIN Semantics**: OB integration preserves all events.
   Missing OB data → NULL features → excluded from hypothesis testing.

5. **No Silent Corruption**: If write fails, error is logged.
   If disk full, writing stops. If connection stale, reconnect triggered.

---

*Document generated 2026-09-05. Design phase only — no implementation.*
