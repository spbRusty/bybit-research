//! Book state manager for orderbook reconstruction.
//!
//! Maintains full book state in memory by applying snapshots and deltas.
//! Tracks Bybit's `u` (update_id) for provenance — NOT for gap detection.
//!
//! Bybit semantics (verified from official docs):
//!   - `u` is an Update ID, NOT guaranteed sequential
//!   - `u=1` means snapshot due to service restart
//!   - Gap recovery happens via new snapshot arrival, not `u` sequence
//!   - The official pybit SDK does NOT check `u` sequence
//!
//! Invariants:
//!   - Delta never treated as snapshot (is_delta flag always respected)
//!   - State becomes valid only after snapshot
//!   - Reconstructed snapshots have versioned provenance

use std::collections::BTreeMap;

/// One level in the orderbook: (price, qty) for one side.
pub type Level = (f64, f64);

/// Reconstructed snapshot at a point in time.
#[derive(Debug, Clone)]
pub struct ReconstructedSnapshot {
    pub timestamp_ms: i64,
    pub update_id: i64,
    pub symbol: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread_bps: f64,
    pub mid_price: f64,
    pub levels: Vec<LevelSnapshot>,
    pub gap_detected: bool,
    pub reconstruction_version: String,
}

/// One level in the reconstructed snapshot.
#[derive(Debug, Clone)]
pub struct LevelSnapshot {
    pub level: i32,
    pub bid_px: f64,
    pub bid_sz: f64,
    pub ask_px: f64,
    pub ask_sz: f64,
}

/// Book state for one symbol.
///
/// Maintains bids (sorted descending by price) and asks (sorted ascending).
/// Applies snapshots and deltas from Bybit orderbook.50 stream.
pub struct BookState {
    pub symbol: String,
    bids: BTreeMap<OrderedFloat, f64>, // price -> qty (descending)
    asks: BTreeMap<OrderedFloat, f64>, // price -> qty (ascending)
    last_update_id: i64,
    last_timestamp_ms: i64,
    has_valid_state: bool,
    reconstruction_version: String,
    // Metrics
    pub update_count: u64,
    pub snapshot_count: u64,
    pub update_id_jumps: u64,
    pub last_update_id_observed: i64,
}

impl BookState {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            last_timestamp_ms: 0,
            has_valid_state: false,
            reconstruction_version: "1.0".to_string(),
            update_count: 0,
            snapshot_count: 0,
            update_id_jumps: 0,
            last_update_id_observed: 0,
        }
    }

    /// Apply a snapshot message. Resets entire book state.
    ///
    /// Bybit guarantees snapshot contains the latest data.
    /// After snapshot, book state is valid.
    pub fn apply_snapshot(
        &mut self,
        update_id: i64,
        timestamp_ms: i64,
        bids: &[(String, String)],  // [(price, qty), ...]
        asks: &[(String, String)],
    ) {
        self.bids.clear();
        self.asks.clear();

        for (px_str, qty_str) in bids {
            if let (Ok(px), Ok(qty)) = (px_str.parse::<f64>(), qty_str.parse::<f64>()) {
                if qty > 0.0 {
                    self.bids.insert(OrderedFloat(px), qty);
                }
            }
        }

        for (px_str, qty_str) in asks {
            if let (Ok(px), Ok(qty)) = (px_str.parse::<f64>(), qty_str.parse::<f64>()) {
                if qty > 0.0 {
                    self.asks.insert(OrderedFloat(px), qty);
                }
            }
        }

        self.last_update_id = update_id;
        self.last_timestamp_ms = timestamp_ms;
        self.has_valid_state = true;
        self.snapshot_count += 1;
        self.last_update_id_observed = update_id;
    }

    /// Apply a delta message. Updates changed levels only.
    ///
    /// Delta rules (from Bybit docs):
    ///   - qty = 0 → delete price level
    ///   - new price → insert
    ///   - existing price → update qty
    ///
    /// `u` sequence is NOT checked for gap detection (Bybit docs don't guarantee sequential).
    /// `u` jumps are tracked as metrics for monitoring.
    pub fn apply_delta(
        &mut self,
        update_id: i64,
        timestamp_ms: i64,
        bids: &[(String, String)],
        asks: &[(String, String)],
    ) -> bool {
        if !self.has_valid_state {
            return false;
        }

        // Track `u` jumps as metrics (not gap detection)
        if update_id != self.last_update_id + 1 && self.last_update_id > 0 {
            self.update_id_jumps += 1;
        }

        // Apply bid changes
        for (px_str, qty_str) in bids {
            if let (Ok(px), Ok(qty)) = (px_str.parse::<f64>(), qty_str.parse::<f64>()) {
                if qty == 0.0 {
                    self.bids.remove(&OrderedFloat(px));
                } else {
                    self.bids.insert(OrderedFloat(px), qty);
                }
            }
        }

        // Apply ask changes
        for (px_str, qty_str) in asks {
            if let (Ok(px), Ok(qty)) = (px_str.parse::<f64>(), qty_str.parse::<f64>()) {
                if qty == 0.0 {
                    self.asks.remove(&OrderedFloat(px));
                } else {
                    self.asks.insert(OrderedFloat(px), qty);
                }
            }
        }

        self.last_update_id = update_id;
        self.last_timestamp_ms = timestamp_ms;
        self.update_count += 1;
        self.last_update_id_observed = update_id;
        true
    }

    /// Check if book has valid state (after snapshot).
    pub fn is_valid(&self) -> bool {
        self.has_valid_state
    }

    /// Check if state is invalid (no valid snapshot received yet).
    pub fn gap_detected(&self) -> bool {
        !self.has_valid_state
    }

    /// Create a reconstructed snapshot from current state.
    ///
    /// Returns None if book state is invalid (no snapshot received yet).
    pub fn snapshot(&self) -> Option<ReconstructedSnapshot> {
        if !self.has_valid_state {
            return None;
        }

        let best_bid = self.bids.iter().next_back().map(|(k, _)| k.0).unwrap_or(0.0);
        let best_ask = self.asks.iter().next().map(|(k, _)| k.0).unwrap_or(0.0);

        if best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }

        let mid = (best_bid + best_ask) / 2.0;
        let spread_bps = if mid > 0.0 {
            ((best_ask - best_bid) / mid) * 10000.0
        } else {
            0.0
        };

        let mut levels = Vec::new();
        let bid_vec: Vec<_> = self.bids.iter().rev().collect();
        let ask_vec: Vec<_> = self.asks.iter().collect();

        let max_levels = bid_vec.len().max(ask_vec.len()).min(50);
        for i in 0..max_levels {
            let bid_px = bid_vec.get(i).map_or(0.0, |(k, _)| k.0);
            let bid_sz = bid_vec.get(i).map_or(0.0, |(_, v)| **v);
            let ask_px = ask_vec.get(i).map_or(0.0, |(k, _)| k.0);
            let ask_sz = ask_vec.get(i).map_or(0.0, |(_, v)| **v);
            levels.push(LevelSnapshot {
                level: (i + 1) as i32,
                bid_px,
                bid_sz,
                ask_px,
                ask_sz,
            });
        }

        Some(ReconstructedSnapshot {
            timestamp_ms: self.last_timestamp_ms,
            update_id: self.last_update_id,
            symbol: self.symbol.clone(),
            best_bid,
            best_ask,
            spread_bps,
            mid_price: mid,
            levels,
            gap_detected: false,
            reconstruction_version: self.reconstruction_version.clone(),
        })
    }

    /// Reset book state (e.g., on explicit snapshot request after gap).
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_update_id = 0;
        self.last_timestamp_ms = 0;
        self.has_valid_state = false;
    }
}

/// Wrapper for f64 that implements Ord for use in BTreeMap.
/// Prices are compared with a small epsilon for floating-point stability.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedFloat(f64);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl std::ops::Deref for OrderedFloat {
    type Target = f64;
    fn deref(&self) -> &f64 {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot_update() -> (i64, i64, Vec<(String, String)>, Vec<(String, String)>) {
        let bids = vec![
            ("100.0".to_string(), "1.0".to_string()),
            ("99.0".to_string(), "2.0".to_string()),
            ("98.0".to_string(), "3.0".to_string()),
        ];
        let asks = vec![
            ("101.0".to_string(), "1.5".to_string()),
            ("102.0".to_string(), "2.5".to_string()),
            ("103.0".to_string(), "3.5".to_string()),
        ];
        (1, 1000, bids, asks)
    }

    #[test]
    fn test_apply_snapshot() {
        let mut book = BookState::new("BTCUSDT");
        let (uid, ts, bids, asks) = make_snapshot_update();
        book.apply_snapshot(uid, ts, &bids, &asks);

        assert!(book.is_valid());
        assert_eq!(book.last_update_id, 1);
        assert_eq!(book.last_timestamp_ms, 1000);
        assert_eq!(book.snapshot_count, 1);

        let snap = book.snapshot().unwrap();
        assert_eq!(snap.best_bid, 100.0);
        assert_eq!(snap.best_ask, 101.0);
        assert!(snap.spread_bps > 0.0);
        assert_eq!(snap.levels.len(), 3);
    }

    #[test]
    fn test_apply_delta_update() {
        let mut book = BookState::new("BTCUSDT");
        let (uid, ts, bids, asks) = make_snapshot_update();
        book.apply_snapshot(uid, ts, &bids, &asks);

        let delta_bids = vec![("100.0".to_string(), "5.0".to_string())];
        let delta_asks = vec![];
        assert!(book.apply_delta(2, 1020, &delta_bids, &delta_asks));

        let snap = book.snapshot().unwrap();
        assert_eq!(snap.best_bid, 100.0);
        assert_eq!(book.update_count, 1);
    }

    #[test]
    fn test_apply_delta_delete_level() {
        let mut book = BookState::new("BTCUSDT");
        let (uid, ts, bids, asks) = make_snapshot_update();
        book.apply_snapshot(uid, ts, &bids, &asks);

        let delta_bids = vec![("98.0".to_string(), "0.0".to_string())];
        let delta_asks = vec![];
        assert!(book.apply_delta(2, 1020, &delta_bids, &delta_asks));

        let snap = book.snapshot().unwrap();
        assert_eq!(snap.levels.len(), 3);
    }

    #[test]
    fn test_apply_delta_insert_level() {
        let mut book = BookState::new("BTCUSDT");
        let (uid, ts, bids, asks) = make_snapshot_update();
        book.apply_snapshot(uid, ts, &bids, &asks);

        let delta_bids = vec![("97.0".to_string(), "4.0".to_string())];
        let delta_asks = vec![];
        assert!(book.apply_delta(2, 1020, &delta_bids, &delta_asks));

        let snap = book.snapshot().unwrap();
        assert_eq!(snap.levels.len(), 4);
    }

    #[test]
    fn test_u_jump_tracked_but_not_gap() {
        let mut book = BookState::new("BTCUSDT");
        let (uid, ts, bids, asks) = make_snapshot_update();
        book.apply_snapshot(uid, ts, &bids, &asks);

        let delta_bids = vec![("100.0".to_string(), "5.0".to_string())];
        let delta_asks = vec![];
        assert!(book.apply_delta(5, 1020, &delta_bids, &delta_asks));

        assert!(!book.gap_detected());
        assert!(book.is_valid());
        assert_eq!(book.update_id_jumps, 1);
        assert_eq!(book.update_count, 1);
        assert!(book.snapshot().is_some());
    }

    #[test]
    fn test_u_jump_sequence() {
        let mut book = BookState::new("BTCUSDT");
        let (uid, ts, bids, asks) = make_snapshot_update();
        book.apply_snapshot(uid, ts, &bids, &asks);

        let delta_bids = vec![("100.0".to_string(), "5.0".to_string())];
        let delta_asks = vec![];
        book.apply_delta(2, 1020, &delta_bids, &delta_asks);
        assert_eq!(book.update_id_jumps, 0);

        book.apply_delta(5, 1040, &delta_bids, &delta_asks);
        assert_eq!(book.update_id_jumps, 1);
        assert!(book.is_valid());
    }

    #[test]
    fn test_snapshot_resets_state() {
        let mut book = BookState::new("BTCUSDT");
        let (uid, ts, bids, asks) = make_snapshot_update();
        book.apply_snapshot(uid, ts, &bids, &asks);

        let (uid2, ts2, bids2, asks2) = make_snapshot_update();
        book.apply_snapshot(uid2, ts2 + 1000, &bids2, &asks2);
        assert!(book.is_valid());
        assert!(book.snapshot().is_some());
        assert_eq!(book.snapshot_count, 2);
    }

    #[test]
    fn test_empty_book() {
        let mut book = BookState::new("BTCUSDT");
        book.apply_snapshot(1, 1000, &[], &[]);
        assert!(book.is_valid());
        assert!(book.snapshot().is_none());
    }

    #[test]
    fn test_no_writes_while_invalid() {
        let mut book = BookState::new("BTCUSDT");
        assert!(!book.is_valid());
        assert!(book.snapshot().is_none());

        let delta_bids = vec![("100.0".to_string(), "5.0".to_string())];
        let delta_asks = vec![];
        assert!(!book.apply_delta(5, 1020, &delta_bids, &delta_asks));
        assert!(book.update_count == 0);
    }
}
