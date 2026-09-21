use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;

use collector_lib::book_state::{BookState, LevelSnapshot, ReconstructedSnapshot};

const WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const SUB_CHUNK: usize = 100; // символов на одно WS-соединение, как в marketdata.rs
const DEFAULT_FREQ: u64 = 5;
const DEFAULT_DATA_ROOT: &str = "data/market/orderbook/reconstructed";
const PING_INTERVAL: u64 = 30;
const WS_ACTIVITY_TIMEOUT: u64 = 90;
const MAX_BACKOFF: u64 = 60;
const DISK_WARNING_GB: u64 = 10;
const DISK_CRITICAL_GB: u64 = 5;
const DISK_EMERGENCY_GB: u64 = 1;
const UNIVERSE_REFRESH_INTERVAL: u64 = 86400;
const WRITER_QUEUE_CAP: usize = 4096;
const N_SHARDS: usize = 8;
const CHECKPOINT_SECS: u64 = 60;
const RAW_FLUSH_SECS: u64 = 3;
const RAW_FLUSH_BYTES: usize = 65536;
static WQUEUE_FULL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
enum Cell { I(i64), F(f64), B(bool), S(String) }
type Col = Vec<Cell>;
type Table = Vec<(&'static str, Col)>;

fn table_len(t: &Table) -> usize { t.first().map(|(_, c)| c.len()).unwrap_or(0) }

fn table_to_batch(t: &Table) -> Result<(arrow::datatypes::SchemaRef, arrow::record_batch::RecordBatch)> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let n = table_len(t);
    if n == 0 { return Err(anyhow::anyhow!("empty table")); }
    let mut fields = Vec::with_capacity(t.len());
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(t.len());
    for (name, col) in t {
        match &col[0] {
            Cell::I(_) => {
                fields.push(Field::new(*name, DataType::Int64, false));
                let v: Vec<i64> = col.iter().map(|c| match c { Cell::I(i) => *i, _ => 0 }).collect();
                arrays.push(Arc::new(Int64Array::from(v)));
            }
            Cell::F(_) => {
                fields.push(Field::new(*name, DataType::Float64, false));
                let v: Vec<f64> = col.iter().map(|c| match c { Cell::F(f) => *f, _ => 0.0 }).collect();
                arrays.push(Arc::new(Float64Array::from(v)));
            }
            Cell::B(_) => {
                fields.push(Field::new(*name, DataType::Boolean, false));
                let v: Vec<bool> = col.iter().map(|c| match c { Cell::B(b) => *b, _ => false }).collect();
                arrays.push(Arc::new(BooleanArray::from(v)));
            }
            Cell::S(_) => {
                fields.push(Field::new(*name, DataType::Utf8, false));
                let v: Vec<&str> = col.iter().map(|c| match c { Cell::S(s) => s.as_str(), _ => "" }).collect();
                arrays.push(Arc::new(StringArray::from(v)));
            }
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok((schema, batch))
}

/// Пишет свежий часовой parquet из памяти: temp + fs::rename, без read-modify-write.
fn write_parquet_new(path: &Path, snaps: &[ReconstructedSnapshot]) -> Result<()> {
    use parquet::arrow::arrow_writer::ArrowWriter;

    let (schema, batch) = table_to_batch(&snapshots_to_table(snaps))?;
    let tmp = path.with_extension("parquet.tmp");
    if let Some(parent) = tmp.parent() { fs::create_dir_all(parent)?; }
    let file = fs::File::create(&tmp)?;
    let mut w = ArrowWriter::try_new(file, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn downcast_array<T: arrow::array::Array + 'static>(b: &arrow::record_batch::RecordBatch, i: usize) -> Result<&T> {
    b.column(i).as_any().downcast_ref::<T>().ok_or_else(|| anyhow::anyhow!("column {i}: unexpected arrow type"))
}

/// Единственное разрешённое чтение: текущий часовой файл при старте шарда (seed).
fn read_snaps_from_parquet(path: &Path) -> Result<Vec<ReconstructedSnapshot>> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut snaps = Vec::new();
    for batch in reader {
        let b = batch?;
        let ts = downcast_array::<Int64Array>(&b, 0)?;
        let uid = downcast_array::<Int64Array>(&b, 1)?;
        let sym = downcast_array::<StringArray>(&b, 2)?;
        let bb = downcast_array::<Float64Array>(&b, 3)?;
        let ba = downcast_array::<Float64Array>(&b, 4)?;
        let sp = downcast_array::<Float64Array>(&b, 5)?;
        let mid = downcast_array::<Float64Array>(&b, 6)?;
        let gap = downcast_array::<BooleanArray>(&b, 7)?;
        let ver = downcast_array::<StringArray>(&b, 8)?;
        let nl = downcast_array::<Int64Array>(&b, 9)?;
        for r in 0..b.num_rows() {
            let n_levels = nl.value(r).max(0) as usize;
            let mut levels = Vec::with_capacity(n_levels.min(MAX_LEVEL_COLS));
            for i in 0..n_levels.min(MAX_LEVEL_COLS) {
                let base = 10 + i * 4;
                levels.push(LevelSnapshot {
                    level: (i + 1) as i32,
                    bid_px: downcast_array::<Float64Array>(&b, base)?.value(r),
                    bid_sz: downcast_array::<Float64Array>(&b, base + 1)?.value(r),
                    ask_px: downcast_array::<Float64Array>(&b, base + 2)?.value(r),
                    ask_sz: downcast_array::<Float64Array>(&b, base + 3)?.value(r),
                });
            }
            snaps.push(ReconstructedSnapshot {
                timestamp_ms: ts.value(r),
                update_id: uid.value(r),
                symbol: sym.value(r).to_string(),
                best_bid: bb.value(r),
                best_ask: ba.value(r),
                spread_bps: sp.value(r),
                mid_price: mid.value(r),
                levels,
                gap_detected: gap.value(r),
                reconstruction_version: ver.value(r).to_string(),
            });
        }
    }
    Ok(snaps)
}

/// Имя часового файла: {YYYY-MM-DD}-{HH}.parquet, UTC, HH — zero-padded 2 digits.
fn hour_filename(hour: i64) -> String {
    chrono::DateTime::from_timestamp_millis(hour * 3_600_000)
        .unwrap_or_default()
        .format("%Y-%m-%d-%H")
        .to_string()
        + ".parquet"
}

/// Стабильный хэш символа -> шард (FNV-1a, детерминированный, без зависимостей).
fn shard_for_symbol(symbol: &str) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in symbol.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) % N_SHARDS
}

struct Config {
    symbols: Vec<String>,
    universe_path: Option<PathBuf>,
    freq: u64,
    data_root: PathBuf,
    raw_data_root: PathBuf,
    no_raw: bool,
}

impl Config {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut symbols = Vec::new();
        let mut universe_path = None;
        let mut freq = DEFAULT_FREQ;
        let mut data_root = PathBuf::from(DEFAULT_DATA_ROOT);
        let mut raw_data_root = PathBuf::from("data/market/orderbook/raw");
        let mut no_raw = false;
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--symbols" => { i += 1; if let Some(s) = args.get(i) { symbols = s.split(',').map(|s| s.trim().to_string()).collect(); } }
                "--universe" => {
                    i += 1;
                    if let Some(path) = args.get(i) {
                        universe_path = Some(PathBuf::from(path));
                        if let Ok(c) = fs::read_to_string(path) {
                            symbols = c.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect();
                        }
                    }
                }
                "--freq" => { i += 1; if let Some(f) = args.get(i) { freq = f.parse().unwrap_or(DEFAULT_FREQ); } }
                "--data-root" => { i += 1; if let Some(d) = args.get(i) { data_root = PathBuf::from(d); } }
                "--raw-data-root" => { i += 1; if let Some(d) = args.get(i) { raw_data_root = PathBuf::from(d); } }
                "--no-raw" => { no_raw = true; }
                _ => {}
            }
            i += 1;
        }
        if symbols.is_empty() {
            for p in &["data/market/symbols/linear.txt", "data/symbols/linear.txt"] {
                if let Ok(c) = fs::read_to_string(p) {
                    symbols = c.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect();
                    if !symbols.is_empty() {
                        universe_path = Some(PathBuf::from(p));
                        eprintln!("[config] loaded {} symbols from {}", symbols.len(), p);
                        break;
                    }
                }
            }
        }
        if symbols.is_empty() { symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]; }
        Self { symbols, universe_path, freq, data_root, raw_data_root, no_raw }
    }

    fn load_universe(&self) -> Vec<String> {
        match &self.universe_path {
            Some(path) => {
                if let Ok(c) = fs::read_to_string(path) {
                    c.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect()
                } else {
                    self.symbols.clone()
                }
            }
            None => self.symbols.clone(),
        }
    }
}

#[derive(Debug, Clone)]
enum ObMsg { Ob(ObData), SnapshotRequest(String) }

#[derive(Debug, Clone)]
struct ObData { symbol: String, update_id: i64, timestamp_ms: i64, is_snapshot: bool, bids: Vec<(String, String)>, asks: Vec<(String, String)> }

fn parse_msg(v: &Value) -> Option<ObData> {
    let topic = v["topic"].as_str()?;
    if !topic.starts_with("orderbook.50.") { return None; }
    let symbol = topic.strip_prefix("orderbook.50.")?.to_string();
    let is_snapshot = v["type"].as_str() == Some("snapshot");
    let data = v.get("data")?;
    let update_id = data["u"].as_i64()?;
    let timestamp_ms = v["ts"].as_i64()?;
    let mut bids = Vec::new();
    if let Some(arr) = data["b"].as_array() {
        for level in arr { if let Some(a) = level.as_array() { if a.len() >= 2 { bids.push((a[0].as_str().unwrap_or("0").to_string(), a[1].as_str().unwrap_or("0").to_string())); } } }
    }
    let mut asks = Vec::new();
    if let Some(arr) = data["a"].as_array() {
        for level in arr { if let Some(a) = level.as_array() { if a.len() >= 2 { asks.push((a[0].as_str().unwrap_or("0").to_string(), a[1].as_str().unwrap_or("0").to_string())); } } }
    }
    Some(ObData { symbol, update_id, timestamp_ms, is_snapshot, bids, asks })
}

const MAX_LEVEL_COLS: usize = 20;

/// Батч из главного цикла в шард: снапшоты + сырые строки (по датам) + счётчик ошибок символа.
struct FlushBatch {
    symbol: String,
    snaps: Vec<ReconstructedSnapshot>,
    raw: HashMap<String, Vec<String>>,
    errors: Arc<AtomicU64>,
}

const LEVEL_COL_NAMES: [(&str, &str, &str, &str); MAX_LEVEL_COLS] = [
    ("bid_px_1", "bid_sz_1", "ask_px_1", "ask_sz_1"),
    ("bid_px_2", "bid_sz_2", "ask_px_2", "ask_sz_2"),
    ("bid_px_3", "bid_sz_3", "ask_px_3", "ask_sz_3"),
    ("bid_px_4", "bid_sz_4", "ask_px_4", "ask_sz_4"),
    ("bid_px_5", "bid_sz_5", "ask_px_5", "ask_sz_5"),
    ("bid_px_6", "bid_sz_6", "ask_px_6", "ask_sz_6"),
    ("bid_px_7", "bid_sz_7", "ask_px_7", "ask_sz_7"),
    ("bid_px_8", "bid_sz_8", "ask_px_8", "ask_sz_8"),
    ("bid_px_9", "bid_sz_9", "ask_px_9", "ask_sz_9"),
    ("bid_px_10", "bid_sz_10", "ask_px_10", "ask_sz_10"),
    ("bid_px_11", "bid_sz_11", "ask_px_11", "ask_sz_11"),
    ("bid_px_12", "bid_sz_12", "ask_px_12", "ask_sz_12"),
    ("bid_px_13", "bid_sz_13", "ask_px_13", "ask_sz_13"),
    ("bid_px_14", "bid_sz_14", "ask_px_14", "ask_sz_14"),
    ("bid_px_15", "bid_sz_15", "ask_px_15", "ask_sz_15"),
    ("bid_px_16", "bid_sz_16", "ask_px_16", "ask_sz_16"),
    ("bid_px_17", "bid_sz_17", "ask_px_17", "ask_sz_17"),
    ("bid_px_18", "bid_sz_18", "ask_px_18", "ask_sz_18"),
    ("bid_px_19", "bid_sz_19", "ask_px_19", "ask_sz_19"),
    ("bid_px_20", "bid_sz_20", "ask_px_20", "ask_sz_20"),
];

fn snapshots_to_table(snaps: &[ReconstructedSnapshot]) -> Table {
    let mut t: Table = vec![
        ("timestamp_ms", Col::new()), ("update_id", Col::new()), ("symbol", Col::new()),
        ("best_bid", Col::new()), ("best_ask", Col::new()), ("spread_bps", Col::new()),
        ("mid_price", Col::new()), ("gap_detected", Col::new()),
        ("reconstruction_version", Col::new()), ("n_levels", Col::new()),
    ];
    for &(bp, bs, ap, as_) in &LEVEL_COL_NAMES {
        t.push((bp, Col::new()));
        t.push((bs, Col::new()));
        t.push((ap, Col::new()));
        t.push((as_, Col::new()));
    }
    for snap in snaps {
        t[0].1.push(Cell::I(snap.timestamp_ms));
        t[1].1.push(Cell::I(snap.update_id));
        t[2].1.push(Cell::S(snap.symbol.clone()));
        t[3].1.push(Cell::F(snap.best_bid));
        t[4].1.push(Cell::F(snap.best_ask));
        t[5].1.push(Cell::F(snap.spread_bps));
        t[6].1.push(Cell::F(snap.mid_price));
        t[7].1.push(Cell::B(snap.gap_detected));
        t[8].1.push(Cell::S(snap.reconstruction_version.clone()));
        t[9].1.push(Cell::I(snap.levels.len() as i64));
        for (i, lvl) in snap.levels.iter().enumerate().take(MAX_LEVEL_COLS) {
            let base = 10 + i * 4;
            t[base].1.push(Cell::F(lvl.bid_px));
            t[base + 1].1.push(Cell::F(lvl.bid_sz));
            t[base + 2].1.push(Cell::F(lvl.ask_px));
            t[base + 3].1.push(Cell::F(lvl.ask_sz));
        }
        for i in snap.levels.len()..MAX_LEVEL_COLS {
            let base = 10 + i * 4;
            t[base].1.push(Cell::F(0.0));
            t[base + 1].1.push(Cell::F(0.0));
            t[base + 2].1.push(Cell::F(0.0));
            t[base + 3].1.push(Cell::F(0.0));
        }
    }
    t
}

#[derive(Serialize)]
struct MetricsEntry {
    timestamp: String,
    symbol: String,
    updates_received: u64,
    updates_written: u64,
    snapshots: u64,
    reconnects: u64,
    update_id_jumps: u64,
    invalid_state_duration_secs: f64,
    is_valid: bool,
    rows_written: usize,
    disk_free_gb: f64,
    errors: u64,
}

struct SymState {
    book: BookState,
    pending: Vec<ReconstructedSnapshot>,
    total_flushed: usize,
    reconnect_count: u64,
    last_write: std::time::Instant,
    last_snap_req: std::time::Instant,
    no_raw: bool,
    freq: Duration,
    updates_received: u64,
    updates_written: u64,
    errors: Arc<AtomicU64>,
    invalid_state_since: Option<std::time::Instant>,
    raw_buf: HashMap<String, Vec<String>>,
    raw_buf_bytes: usize,
    raw_first_line: Option<std::time::Instant>,
}

impl SymState {
    fn new(symbol: &str, no_raw: bool, freq: Duration) -> Self {
        Self {
            book: BookState::new(symbol),
            pending: Vec::new(),
            total_flushed: 0,
            reconnect_count: 0,
            last_write: std::time::Instant::now(),
            last_snap_req: std::time::Instant::now(),
            no_raw,
            freq,
            updates_received: 0,
            updates_written: 0,
            errors: Arc::new(AtomicU64::new(0)),
            invalid_state_since: None,
            raw_buf: HashMap::new(),
            raw_buf_bytes: 0,
            raw_first_line: None,
        }
    }

    fn process(&mut self, msg: &ObData) -> bool {
        self.updates_received += 1;
        if msg.is_snapshot {
            self.book.apply_snapshot(msg.update_id, msg.timestamp_ms, &msg.bids, &msg.asks);
            self.reconnect_count += 1;
            self.invalid_state_since = None;
            false
        } else {
            let was_valid = self.book.is_valid();
            self.book.apply_delta(msg.update_id, msg.timestamp_ms, &msg.bids, &msg.asks);
            if was_valid && !self.book.is_valid() {
                self.invalid_state_since = Some(std::time::Instant::now());
            }
            if !self.book.is_valid() && self.last_snap_req.elapsed() >= Duration::from_secs(5) {
                self.last_snap_req = std::time::Instant::now();
                return true;
            }
            false
        }
    }

    fn maybe_snapshot(&mut self) {
        if let Some(snap) = self.book.snapshot() { self.pending.push(snap); }
    }

    /// Буферизует сырую строку в памяти (по дате), без дискового I/O.
    fn buffer_raw(&mut self, msg: &ObData) {
        if self.no_raw { return; }
        let dt = chrono::DateTime::from_timestamp_millis(msg.timestamp_ms).unwrap_or_default();
        let date_str = dt.format("%Y-%m-%d").to_string();
        let line = serde_json::json!({
            "ts": msg.timestamp_ms,
            "u": msg.update_id,
            "type": if msg.is_snapshot { "snapshot" } else { "delta" },
            "b": msg.bids,
            "a": msg.asks,
        })
        .to_string();
        let entry = self.raw_buf.entry(date_str).or_default();
        if entry.is_empty() { self.raw_first_line = Some(std::time::Instant::now()); }
        self.raw_buf_bytes += line.len() + 1;
        entry.push(line);
    }

    fn raw_due(&self) -> bool {
        if self.no_raw || self.raw_buf.is_empty() { return false; }
        if self.raw_buf_bytes >= RAW_FLUSH_BYTES { return true; }
        self.raw_first_line.is_some_and(|t| t.elapsed() >= Duration::from_secs(RAW_FLUSH_SECS))
    }

    /// Чисто in-memory: дренит pending/raw по триггерам, возвращает батч для отправки в шард.
    fn maybe_flush(&mut self) -> Option<FlushBatch> {
        let snap_due = self.last_write.elapsed() >= self.freq && !self.pending.is_empty();
        let raw_due = self.raw_due();
        if !snap_due && !raw_due { return None; }
        let snaps = if snap_due {
            self.updates_written += self.pending.len() as u64;
            self.total_flushed += self.pending.len();
            self.last_write = std::time::Instant::now();
            std::mem::take(&mut self.pending)
        } else { Vec::new() };
        let raw = if snap_due || raw_due {
            self.raw_buf_bytes = 0;
            self.raw_first_line = None;
            std::mem::take(&mut self.raw_buf)
        } else { HashMap::new() };
        Some(FlushBatch { symbol: self.book.symbol.clone(), snaps, raw, errors: self.errors.clone() })
    }

    /// Принудительный сброс всего (только на shutdown).
    fn force_flush(&mut self) -> Option<FlushBatch> {
        if self.pending.is_empty() && self.raw_buf.is_empty() { return None; }
        self.updates_written += self.pending.len() as u64;
        self.total_flushed += self.pending.len();
        let snaps = std::mem::take(&mut self.pending);
        let raw = std::mem::take(&mut self.raw_buf);
        self.raw_buf_bytes = 0;
        self.raw_first_line = None;
        Some(FlushBatch { symbol: self.book.symbol.clone(), snaps, raw, errors: self.errors.clone() })
    }

    fn metrics_entry(&self, disk_free_gb: f64) -> MetricsEntry {
        let invalid_secs = self.invalid_state_since
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let rows_written: usize = self.total_flushed;
        MetricsEntry {
            timestamp: Utc::now().to_rfc3339(),
            symbol: self.book.symbol.clone(),
            updates_received: self.updates_received,
            updates_written: self.updates_written,
            snapshots: self.book.snapshot_count,
            reconnects: self.reconnect_count,
            update_id_jumps: self.book.update_id_jumps,
            invalid_state_duration_secs: invalid_secs,
            is_valid: self.book.is_valid(),
            rows_written,
            disk_free_gb,
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

fn get_disk_free_gb(path: &Path) -> f64 {
    match fs2::available_space(path) {
        Ok(bytes) => bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        Err(_) => -1.0,
    }
}

async fn ws_batch(symbols: Vec<String>, tx: mpsc::Sender<ObMsg>, shutdown: Arc<AtomicBool>) {
    let label = format!("{}", symbols.join(","));
    let mut backoff = 1u64;
    while !shutdown.load(Ordering::Relaxed) {
        match ws_run(&symbols, &tx, &shutdown).await {
            Ok(()) => { backoff = 1; }
            Err(e) => {
                if shutdown.load(Ordering::Relaxed) { break; }
                eprintln!("[ws {}..] {e:#}; reconnect in {backoff}s", symbols.first().map_or("", |s| s.as_str()));
                sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn ws_run(symbols: &[String], tx: &mpsc::Sender<ObMsg>, shutdown: &Arc<AtomicBool>) -> Result<()> {
    let topics: Vec<String> = symbols.iter().map(|s| format!("orderbook.50.{s}")).collect();
    let (ws, _) = tokio_tungstenite::connect_async(WS_URL).await?;
    let (mut sink, mut stream) = ws.split();
    for chunk in topics.chunks(SUB_CHUNK) {
        let sub = serde_json::json!({"op": "subscribe", "args": chunk});
        sink.send(Message::Text(sub.to_string().into())).await?;
    }
    eprintln!("[ws {}..] subscribed {} topics", symbols.first().map_or("", |s| s.as_str()), topics.len());

    let mut last_activity = std::time::Instant::now();
    let mut ping_interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL));
    ping_interval.tick().await;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            let _ = sink.close().await;
            break Ok(());
        }
        tokio::select! {
            msg = stream.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    last_activity = std::time::Instant::now();
                    let v: Value = serde_json::from_str(t.as_str())?;
                    if v["op"].as_str() == Some("ping") {
                        sink.send(Message::Text(serde_json::json!({"op": "pong"}).to_string().into())).await?;
                        continue;
                    }
                    if v.get("topic").is_none() { continue; }
                    if let Some(ob_msg) = parse_msg(&v) {
                        let _ = tx.send(ObMsg::Ob(ob_msg)).await;
                    }
                }
                Some(Ok(Message::Pong(_))) => {
                    last_activity = std::time::Instant::now();
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => break Ok(()),
            },
            _ = ping_interval.tick() => {
                if last_activity.elapsed() > Duration::from_secs(WS_ACTIVITY_TIMEOUT) {
                    return Err(anyhow::anyhow!("activity timeout {}s", WS_ACTIVITY_TIMEOUT));
                }
                if sink.send(Message::Ping(vec![].into())).await.is_err() {
                    return Err(anyhow::anyhow!("ping send failed"));
                }
            }
        }
    }
}

async fn flush_metrics_loop(metrics_rx: &mut mpsc::Receiver<MetricsEntry>, metrics_path: &Path) {
    use std::io::Write;
    let mut file = match fs::OpenOptions::new().create(true).append(true).open(metrics_path) {
        Ok(f) => f,
        Err(e) => { eprintln!("[metrics] cannot open {metrics_path:?}: {e:#}"); return; }
    };
    while let Some(entry) = metrics_rx.recv().await {
        if let Ok(json) = serde_json::to_string(&entry) {
            let _ = writeln!(file, "{json}");
        }
    }
}

async fn cleanup_raw_data(raw_data_root: &Path, shutdown: Arc<AtomicBool>) {
    let retention = Duration::from_secs(7 * 86400);
    loop {
        if shutdown.load(Ordering::Relaxed) { break; }
        sleep(Duration::from_secs(3600)).await;
        if let Ok(entries) = fs::read_dir(raw_data_root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Ok(sym_entries) = fs::read_dir(&path) {
                        for sym_entry in sym_entries.flatten() {
                            let file_path = sym_entry.path();
                            if file_path.extension().map_or(false, |e| e == "jsonl") {
                                if let Ok(meta) = file_path.metadata() {
                                    if let Ok(modified) = meta.modified() {
                                        if modified.elapsed().unwrap_or_default() > retention {
                                            let _ = fs::remove_file(&file_path);
                                            eprintln!("[cleanup] deleted raw: {}", file_path.display());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn universe_refresh_loop(
    config: Arc<Config>,
    states: Arc<tokio::sync::RwLock<HashMap<String, SymState>>>,
    tx: mpsc::Sender<ObMsg>,
    shutdown: Arc<AtomicBool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(UNIVERSE_REFRESH_INTERVAL));
    interval.tick().await;
    loop {
        if shutdown.load(Ordering::Relaxed) { break; }
        interval.tick().await;
        let new_symbols = config.load_universe();
        let current: Vec<String> = states.read().await.keys().cloned().collect();
        let current_set: std::collections::HashSet<&str> = current.iter().map(|s| s.as_str()).collect();
        let new_set: std::collections::HashSet<&str> = new_symbols.iter().map(|s| s.as_str()).collect();
        let mut added = Vec::new();
        for sym in &new_symbols {
            if !current_set.contains(sym.as_str()) {
                added.push(sym.clone());
            }
        }
        if !added.is_empty() {
            eprintln!("[universe] adding {} new symbols", added.len());
            {
                let mut states = states.write().await;
                for sym in &added {
                    let state = SymState::new(sym, config.no_raw, Duration::from_secs(config.freq));
                    states.insert(sym.clone(), state);
                }
            }
            for chunk in added.chunks(SUB_CHUNK) {
                let tx = tx.clone();
                let chunk = chunk.to_vec();
                let shutdown = shutdown.clone();
                tokio::spawn(async move { ws_batch(chunk, tx, shutdown).await; });
            }
        }
        let removed: Vec<String> = current.iter().filter(|s| !new_set.contains(s.as_str())).cloned().collect();
        if !removed.is_empty() {
            eprintln!("[universe] removing {} symbols: {:?}", removed.len(), removed);
            let mut states = states.write().await;
            for sym in &removed {
                states.remove(sym);
            }
        }
    }
}

/// Буфер одного символа внутри шарда: снапшоты текущего часа + счётчик роста с последней записи.
struct HourBuffer {
    hour: i64,
    snaps: Vec<ReconstructedSnapshot>,
    rows_since_write: usize,
    errors: Arc<AtomicU64>,
}

struct ShardState {
    buffers: HashMap<String, HourBuffer>,
    data_root: PathBuf,
    raw_data_root: PathBuf,
    no_raw: bool,
}

/// Группировка снапшотов по часовому бакету (timestamp_ms / 3_600_000).
fn group_snaps_by_hour(snaps: Vec<ReconstructedSnapshot>) -> BTreeMap<i64, Vec<ReconstructedSnapshot>> {
    let mut m: BTreeMap<i64, Vec<ReconstructedSnapshot>> = BTreeMap::new();
    for s in snaps {
        m.entry(s.timestamp_ms / 3_600_000).or_default().push(s);
    }
    m
}

/// Слияние батча в буфер; возвращает финализированные (час, снапшоты) пары при ролловере часа.
fn merge_into_buffer(buf: &mut HourBuffer, by_hour: BTreeMap<i64, Vec<ReconstructedSnapshot>>) -> Vec<(i64, Vec<ReconstructedSnapshot>)> {
    let mut finalized = Vec::new();
    for (hour, snaps) in by_hour {
        if buf.snaps.is_empty() {
            buf.hour = hour;
            let n = snaps.len();
            buf.snaps = snaps;
            buf.rows_since_write += n;
        } else if hour == buf.hour {
            let n = snaps.len();
            buf.snaps.extend(snaps);
            buf.rows_since_write += n;
        } else {
            finalized.push((buf.hour, std::mem::take(&mut buf.snaps)));
            buf.hour = hour;
            let n = snaps.len();
            buf.snaps = snaps;
            buf.rows_since_write = n;
        }
    }
    finalized
}

/// Одно open/append/close на (символ, дату).
fn append_raw(raw_data_root: &Path, symbol: &str, raw: &HashMap<String, Vec<String>>) -> Result<()> {
    use std::io::Write;
    for (date_str, lines) in raw {
        let sym_dir = raw_data_root.join(symbol);
        fs::create_dir_all(&sym_dir)?;
        let path = sym_dir.join(format!("{date_str}.jsonl"));
        let mut file = fs::OpenOptions::new().create(true).append(true).open(&path)?;
        for line in lines {
            writeln!(file, "{line}")?;
        }
    }
    Ok(())
}

/// Отправка батча в шард: try_send, при Full — spawn_blocking blocking_send (главный цикл не блокируется).
fn send_to_shard(tx: &mpsc::Sender<FlushBatch>, batch: FlushBatch) {
    let errors = batch.errors.clone();
    let symbol = batch.symbol.clone();
    match tx.try_send(batch) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(b)) => {
            let n = WQUEUE_FULL.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n % 50 == 0 {
                eprintln!("[writer] queue full #{n}: blocking flush of {}", b.symbol);
            }
            let tx = tx.clone();
            let errors = b.errors.clone();
            let symbol = b.symbol.clone();
            tokio::task::spawn_blocking(move || {
                if tx.blocking_send(b).is_err() {
                    eprintln!("[writer] channel closed, lost batch for {symbol}");
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            eprintln!("[writer] channel closed, lost batch for {symbol}");
            errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl ShardState {
    /// Seed: если текущий часовой файл существует, читаем его ОДИН раз в spawn_blocking.
    async fn seed_symbol(&mut self, symbol: &str) {
        let hour = Utc::now().timestamp_millis() / 3_600_000;
        let path = self.data_root.join(symbol).join(hour_filename(hour));
        let path2 = path.clone();
        let res = tokio::task::spawn_blocking(move || {
            if !path2.exists() { return Ok(Vec::new()); }
            read_snaps_from_parquet(&path2)
        })
        .await;
        match res {
            Ok(Ok(snaps)) if !snaps.is_empty() => {
                self.buffers.insert(symbol.to_string(), HourBuffer {
                    hour, snaps, rows_since_write: 0, errors: Arc::new(AtomicU64::new(0)),
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => eprintln!("[writer] {symbol} seed error: {e:#}"),
            Err(e) => eprintln!("[writer] {symbol} seed join error: {e}"),
        }
    }

    async fn handle_batch(&mut self, batch: FlushBatch) {
        let symbol = batch.symbol;
        let errors = batch.errors;
        let raw = batch.raw;

        let mut finalized: Vec<(i64, Vec<ReconstructedSnapshot>)> = Vec::new();
        if !batch.snaps.is_empty() {
            let by_hour = group_snaps_by_hour(batch.snaps);
            let entry = self.buffers.entry(symbol.clone()).or_insert_with(|| HourBuffer {
                hour: 0, snaps: Vec::new(), rows_since_write: 0, errors: Arc::new(AtomicU64::new(0)),
            });
            entry.errors = errors.clone();
            finalized = merge_into_buffer(entry, by_hour);
        }

        if !finalized.is_empty() || !raw.is_empty() {
            let data_root = self.data_root.clone();
            let raw_root = self.raw_data_root.clone();
            let no_raw = self.no_raw;
            let symbol2 = symbol.clone();
            let res = tokio::task::spawn_blocking(move || {
                let mut first_err: Option<anyhow::Error> = None;
                for (hour, snaps) in &finalized {
                    let path = data_root.join(&symbol2).join(hour_filename(*hour));
                    if let Err(e) = write_parquet_new(&path, snaps) {
                        first_err.get_or_insert(e);
                    }
                }
                if !no_raw && !raw.is_empty() && let Err(e) = append_raw(&raw_root, &symbol2, &raw) {
                    first_err.get_or_insert(e);
                }
                first_err
            })
            .await;
            match res {
                Ok(None) => {}
                Ok(Some(e)) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[writer] {symbol} error: {e:#}");
                }
                Err(e) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[writer] {symbol} join error: {e}");
                }
            }
        }
    }

    async fn write_hour_files(&self, writes: Vec<(String, i64, Vec<ReconstructedSnapshot>, Arc<AtomicU64>)>) {
        for (symbol, hour, snaps, errors) in writes {
            let data_root = self.data_root.clone();
            let symbol2 = symbol.clone();
            let res = tokio::task::spawn_blocking(move || {
                let path = data_root.join(&symbol2).join(hour_filename(hour));
                write_parquet_new(&path, &snaps)
            })
            .await;
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[writer] {symbol} error: {e:#}");
                }
                Err(e) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[writer] {symbol} join error: {e}");
                }
            }
        }
    }

    /// Checkpoint: раз в CHECKPOINT_SECS пишем текущий час, если буфер вырос >= 1 строки.
    async fn checkpoint(&mut self) {
        let mut writes = Vec::new();
        for (symbol, buf) in &mut self.buffers {
            if buf.rows_since_write > 0 && !buf.snaps.is_empty() {
                writes.push((symbol.clone(), buf.hour, buf.snaps.clone(), buf.errors.clone()));
                buf.rows_since_write = 0;
            }
        }
        self.write_hour_files(writes).await;
    }

    /// Shutdown: пишем все оставшиеся часовые буферы.
    async fn finalize_all(&mut self) {
        let mut writes = Vec::new();
        for (symbol, buf) in &mut self.buffers {
            if !buf.snaps.is_empty() {
                writes.push((symbol.clone(), buf.hour, std::mem::take(&mut buf.snaps), buf.errors.clone()));
            }
        }
        self.write_hour_files(writes).await;
    }
}

async fn shard_loop(
    mut rx: mpsc::Receiver<FlushBatch>,
    data_root: PathBuf,
    raw_data_root: PathBuf,
    no_raw: bool,
    symbols: Vec<String>,
) {
    let mut shard = ShardState { buffers: HashMap::new(), data_root, raw_data_root, no_raw };
    for symbol in symbols {
        shard.seed_symbol(&symbol).await;
    }
    let mut checkpoint = tokio::time::interval(Duration::from_secs(CHECKPOINT_SECS));
    checkpoint.tick().await;
    loop {
        tokio::select! {
            batch = rx.recv() => match batch {
                Some(b) => shard.handle_batch(b).await,
                None => { shard.finalize_all().await; break; }
            },
            _ = checkpoint.tick() => shard.checkpoint().await,
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let config = Config::from_args();
    eprintln!("[ob_reconstructor] symbols={}, freq={}s, data={}", config.symbols.len(), config.freq, config.data_root.display());
    fs::create_dir_all(&config.data_root)?;
    if !config.no_raw { fs::create_dir_all(&config.raw_data_root)?; }

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
            tokio::select! {
                _ = sigterm.recv() => { eprintln!("[shutdown] SIGTERM received"); }
                _ = sigint.recv() => { eprintln!("[shutdown] SIGINT received"); }
            }
            shutdown.store(true, Ordering::SeqCst);
        });
    }

    let (tx, mut rx) = mpsc::channel::<ObMsg>(1 << 20);
    let config = Arc::new(config);

    let refresh_tx = tx.clone();

    for chunk in config.symbols.chunks(SUB_CHUNK) {
        let tx = tx.clone();
        let chunk = chunk.to_vec();
        let shutdown = shutdown.clone();
        tokio::spawn(async move { ws_batch(chunk, tx, shutdown).await; });
    }
    eprintln!("[ob_reconstructor] spawned {} ws batch(es), chunk={SUB_CHUNK}", config.symbols.len().div_ceil(SUB_CHUNK));
    drop(tx);

    let (metrics_tx, mut metrics_rx) = mpsc::channel::<MetricsEntry>(1024);
    let metrics_path = config.data_root.join("_metrics.jsonl");
    {
        let metrics_path = metrics_path.clone();
        tokio::spawn(async move { flush_metrics_loop(&mut metrics_rx, &metrics_path).await; });
    }

    let mut shard_txs: Vec<mpsc::Sender<FlushBatch>> = Vec::with_capacity(N_SHARDS);
    let mut shard_handles = Vec::with_capacity(N_SHARDS);
    for i in 0..N_SHARDS {
        let (tx, rx) = mpsc::channel::<FlushBatch>(WRITER_QUEUE_CAP);
        let symbols: Vec<String> = config.symbols.iter().filter(|s| shard_for_symbol(s) == i).cloned().collect();
        let data_root = config.data_root.clone();
        let raw_data_root = config.raw_data_root.clone();
        let no_raw = config.no_raw;
        shard_handles.push(tokio::spawn(async move { shard_loop(rx, data_root, raw_data_root, no_raw, symbols).await; }));
        shard_txs.push(tx);
    }

    let freq = Duration::from_secs(config.freq);
    let mut states_map: HashMap<String, SymState> = HashMap::new();
    for sym in &config.symbols {
        states_map.insert(sym.clone(), SymState::new(sym, config.no_raw, freq));
    }
    let states = Arc::new(tokio::sync::RwLock::new(states_map));

    if !config.no_raw {
        let raw_root = config.raw_data_root.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move { cleanup_raw_data(&raw_root, shutdown).await; });
    }

    {
        let config = config.clone();
        let states = states.clone();
        let tx = refresh_tx;
        let shutdown = shutdown.clone();
        tokio::spawn(async move { universe_refresh_loop(config, states, tx, shutdown).await; });
    }

    let start = std::time::Instant::now();
    let mut msg_count: u64 = 0;
    let mut snap_count: u64 = 0;
    let mut last_metrics = std::time::Instant::now();
    eprintln!("[ob_reconstructor] running...");

    while let Some(msg) = rx.recv().await {
        if shutdown.load(Ordering::Relaxed) { break; }

        match msg {
            ObMsg::Ob(ob_msg) => {
                msg_count += 1;
                let batch = {
                    let mut states = states.write().await;
                    states.get_mut(&ob_msg.symbol).and_then(|state| {
                        state.buffer_raw(&ob_msg);
                        let need_snapshot = state.process(&ob_msg);
                        state.maybe_snapshot();
                        let batch = state.maybe_flush();
                        if need_snapshot { eprintln!("[{}] gap detected, requesting snapshot", ob_msg.symbol); }
                        if ob_msg.is_snapshot { snap_count += 1; }
                        batch
                    })
                };
                if let Some(batch) = batch {
                    let shard = shard_for_symbol(&batch.symbol);
                    send_to_shard(&shard_txs[shard], batch);
                }
            }
            ObMsg::SnapshotRequest(_sym) => {}
        }

        if msg_count % 5000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let states_read = states.read().await;
            let active = states_read.values().filter(|s| s.book.is_valid()).count();
            let total_jumps: u64 = states_read.values().map(|s| s.book.update_id_jumps).sum();
            let total_reconn: u64 = states_read.values().map(|s| s.reconnect_count).sum();
            let total_rows: usize = states_read.values().map(|s| s.total_flushed).sum();
            eprintln!("[stats] {elapsed:.0}s: msgs={msg_count} snaps={snap_count} active={active}/{} jumps={total_jumps} reconn={total_reconn} rows={total_rows}", states_read.len());
        }

        if last_metrics.elapsed() >= Duration::from_secs(60) {
            let disk_free = get_disk_free_gb(&config.data_root);
            if disk_free > 0.0 && disk_free < DISK_CRITICAL_GB as f64 {
                eprintln!("[disk] WARNING: {:.1} GB free", disk_free);
            }
            if disk_free > 0.0 && disk_free < DISK_EMERGENCY_GB as f64 {
                eprintln!("[disk] EMERGENCY: {:.1} GB free, shutting down", disk_free);
                shutdown.store(true, Ordering::SeqCst);
                break;
            }
            let states_read = states.read().await;
            for state in states_read.values() {
                let _ = metrics_tx.send(state.metrics_entry(disk_free)).await;
            }
            last_metrics = std::time::Instant::now();
        }
    }

    eprintln!("[ob_reconstructor] shutting down, flushing...");
    {
        let mut states = states.write().await;
        for state in states.values_mut() {
            if let Some(batch) = state.force_flush() {
                let shard = shard_for_symbol(&batch.symbol);
                send_to_shard(&shard_txs[shard], batch);
            }
        }
        let disk_free = get_disk_free_gb(&config.data_root);
        for state in states.values() {
            let _ = metrics_tx.send(state.metrics_entry(disk_free)).await;
        }
    }
    drop(metrics_tx);
    drop(shard_txs);
    for h in shard_handles { let _ = h.await; }

    eprintln!("[ob_reconstructor] shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_test_snap(symbol: &str, ts: i64) -> ReconstructedSnapshot {
        ReconstructedSnapshot {
            timestamp_ms: ts,
            update_id: 1,
            symbol: symbol.to_string(),
            best_bid: 100.0,
            best_ask: 101.0,
            spread_bps: 99.5,
            mid_price: 100.5,
            levels: vec![LevelSnapshot { level: 1, bid_px: 100.0, bid_sz: 1.0, ask_px: 101.0, ask_sz: 1.5 }],
            gap_detected: false,
            reconstruction_version: "1.0".to_string(),
        }
    }

    #[test]
    fn test_parse_snapshot() {
        let msg = json!({"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1672304484978i64,"data":{"s":"BTCUSDT","b":[["100.0","1.0"],["99.0","2.0"]],"a":[["101.0","1.5"],["102.0","2.5"]],"u":18521288i64,"seq":7961638724i64}});
        let p = parse_msg(&msg).unwrap();
        assert_eq!(p.symbol, "BTCUSDT"); assert!(p.is_snapshot); assert_eq!(p.bids.len(), 2);
    }

    #[test]
    fn test_parse_delta() {
        let msg = json!({"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1687940967466i64,"data":{"s":"BTCUSDT","b":[["30247.20","30.028"]],"a":[["30248.70","0"]],"u":177400507i64,"seq":66544703342i64}});
        let p = parse_msg(&msg).unwrap(); assert!(!p.is_snapshot);
    }

    #[test]
    fn test_parse_non_ob() {
        let msg = json!({"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304486868i64,"data":[]});
        assert!(parse_msg(&msg).is_none());
    }

    #[test]
    fn test_disk_free() {
        let gb = get_disk_free_gb(Path::new("/tmp"));
        assert!(gb > 0.0);
    }

    #[test]
    fn test_symbol_chunking() {
        let syms: Vec<String> = (0..250).map(|i| format!("S{i}USDT")).collect();
        let chunks: Vec<Vec<String>> = syms.chunks(SUB_CHUNK).map(|c| c.to_vec()).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 100);
        assert_eq!(chunks[2].len(), 50);
        let flat: Vec<String> = chunks.into_iter().flatten().collect();
        assert_eq!(flat, syms);
    }

    #[test]
    fn test_shard_routing_stable_and_balanced() {
        let symbols: Vec<String> = (0..744).map(|i| format!("SYM{i:04}USDT")).collect();
        for s in &symbols {
            assert_eq!(shard_for_symbol(s), shard_for_symbol(s), "routing must be stable for {s}");
        }
        let mut buckets = [0usize; N_SHARDS];
        for s in &symbols {
            buckets[shard_for_symbol(s)] += 1;
        }
        let target = 744 / N_SHARDS;
        for (i, b) in buckets.iter().enumerate() {
            assert!((*b as i64 - target as i64).abs() <= 2, "shard {i}: {b} rows, target {target}");
        }
    }

    #[test]
    fn test_hour_partitioning_at_boundary() {
        let h15 = 15 * 3_600_000;
        let snaps = vec![
            make_test_snap("BTCUSDT", h15 - 1),
            make_test_snap("BTCUSDT", h15),
            make_test_snap("BTCUSDT", h15 + 1),
        ];
        let by_hour = group_snaps_by_hour(snaps);
        assert_eq!(by_hour.len(), 2);
        assert_eq!(by_hour[&14].len(), 1);
        assert_eq!(by_hour[&15].len(), 2);
    }

    #[test]
    fn test_rollover_finalizes_previous_hour() {
        let h14 = 14 * 3_600_000;
        let h15 = 15 * 3_600_000;
        let mut buf = HourBuffer {
            hour: 14,
            snaps: vec![make_test_snap("BTCUSDT", h14 + 1000)],
            rows_since_write: 1,
            errors: Arc::new(AtomicU64::new(0)),
        };
        let mut by_hour = BTreeMap::new();
        by_hour.insert(15, vec![make_test_snap("BTCUSDT", h15)]);
        let finalized = merge_into_buffer(&mut buf, by_hour);
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].0, 14);
        assert_eq!(finalized[0].1.len(), 1);
        assert_eq!(buf.hour, 15);
        assert_eq!(buf.snaps.len(), 1);
        assert_eq!(buf.rows_since_write, 1);
    }

    #[test]
    fn test_finalize_schema_90_cols() {
        use arrow::datatypes::DataType;
        let snap = make_test_snap("BTCUSDT", 1_700_000_000_000);
        let dir = std::env::temp_dir().join(format!("ob_recon_schema_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("2026-09-15-14.parquet");
        write_parquet_new(&path, &[snap]).unwrap();

        let file = fs::File::open(&path).unwrap();
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema = builder.schema();

        let mut expected: Vec<(&str, DataType)> = vec![
            ("timestamp_ms", DataType::Int64),
            ("update_id", DataType::Int64),
            ("symbol", DataType::Utf8),
            ("best_bid", DataType::Float64),
            ("best_ask", DataType::Float64),
            ("spread_bps", DataType::Float64),
            ("mid_price", DataType::Float64),
            ("gap_detected", DataType::Boolean),
            ("reconstruction_version", DataType::Utf8),
            ("n_levels", DataType::Int64),
        ];
        for &(bp, bs, ap, as_) in &LEVEL_COL_NAMES {
            expected.push((bp, DataType::Float64));
            expected.push((bs, DataType::Float64));
            expected.push((ap, DataType::Float64));
            expected.push((as_, DataType::Float64));
        }
        assert_eq!(expected.len(), 90);
        assert_eq!(schema.fields().len(), 90);
        for (i, f) in schema.fields().iter().enumerate() {
            assert_eq!(f.name(), expected[i].0, "column {i} name");
            assert_eq!(f.data_type(), &expected[i].1, "column {i} type");
            assert!(!f.is_nullable(), "column {i} must be non-nullable");
        }
        assert!(schema.fields().iter().all(|f| f.name() != "is_valid"), "no is_valid column allowed");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_hour_filename_contract() {
        // Reader-контракт: файл обязан заканчиваться на .parquet (glob "*.parquet")
        // и нести {YYYY-MM-DD}-{HH} с zero-padded часом.
        let hour = 1_767_225_600_000i64 / 3_600_000; // 2026-01-01 00:00 UTC
        let name = hour_filename(hour);
        assert!(name.ends_with(".parquet"), "must end with .parquet, got {name}");
        assert_eq!(&name[..13], "2026-01-01-00");
        assert!(name.starts_with("2026-01-01-"), "hour must be zero-padded 2 digits: {name}");
    }

    #[test]
    fn test_checkpoint_does_not_drain_buffer() {
        let dir = std::env::temp_dir().join(format!("ob_recon_cp_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();

        let hour = 1_767_225_600_000i64 / 3_600_000; // 2026-01-01 00:00 UTC
        let mut state = ShardState {
            buffers: HashMap::new(),
            data_root: dir.clone(),
            raw_data_root: dir.clone(),
            no_raw: true,
        };
        let sym = "TESTUSDT";
        let snaps: Vec<ReconstructedSnapshot> = (0..10)
            .map(|i| make_test_snap(sym, hour * 3_600_000 + i * 1000))
            .collect();
        state.buffers.insert(sym.to_string(), HourBuffer {
            hour,
            snaps: snaps.clone(),
            rows_since_write: 10,
            errors: Arc::new(AtomicU64::new(0)),
        });

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(state.checkpoint());

        let buf = state.buffers.get(sym).unwrap();
        assert_eq!(buf.snaps.len(), 10, "checkpoint must not drain the buffer");
        assert_eq!(buf.rows_since_write, 0, "rows_since_write must reset");

        let path = dir.join(sym).join(hour_filename(hour));
        let file = fs::File::open(&path).unwrap();
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();
        let mut total = 0u64;
        for batch in reader { total += batch.unwrap().num_rows() as u64; }
        assert_eq!(total, 10, "written file must contain all buffered rows");

        fs::remove_dir_all(&dir).ok();
    }
}