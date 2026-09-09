use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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

use collector_lib::book_state::BookState;

const WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const DEFAULT_FREQ: u64 = 5;
const DEFAULT_DATA_ROOT: &str = "data/market/orderbook/reconstructed";
const PING_INTERVAL: u64 = 30;
const WS_ACTIVITY_TIMEOUT: u64 = 90;
const MAX_BACKOFF: u64 = 60;
const DISK_WARNING_GB: u64 = 10;
const DISK_CRITICAL_GB: u64 = 5;
const DISK_EMERGENCY_GB: u64 = 1;
const UNIVERSE_REFRESH_INTERVAL: u64 = 86400;

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

fn write_parquet_append(path: &Path, t: &Table) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::arrow_writer::ArrowWriter;

    let (schema, new_batch) = table_to_batch(t)?;
    let mut all_batches = Vec::new();

    if path.exists() {
        let file = fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        for batch in reader {
            let b = batch?;
            if b.num_rows() > 0 {
                all_batches.push(b);
            }
        }
    }
    all_batches.push(new_batch);

    let tmp = path.with_extension("parquet.tmp");
    if let Some(parent) = tmp.parent() { fs::create_dir_all(parent)?; }
    let file = fs::File::create(&tmp)?;
    let mut w = ArrowWriter::try_new(file, schema, None)?;
    for batch in &all_batches {
        w.write(batch)?;
    }
    w.close()?;
    fs::rename(&tmp, path)?;
    Ok(())
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

fn snapshots_to_table(snaps: &[collector_lib::book_state::ReconstructedSnapshot]) -> Table {
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
    pending: Vec<collector_lib::book_state::ReconstructedSnapshot>,
    by_date: HashMap<String, Vec<collector_lib::book_state::ReconstructedSnapshot>>,
    total_flushed: usize,
    reconnect_count: u64,
    last_write: std::time::Instant,
    last_snap_req: std::time::Instant,
    data_root: PathBuf,
    raw_data_root: PathBuf,
    no_raw: bool,
    freq: Duration,
    updates_received: u64,
    updates_written: u64,
    errors: u64,
    invalid_state_since: Option<std::time::Instant>,
}

impl SymState {
    fn new(symbol: &str, data_root: &Path, raw_data_root: &Path, no_raw: bool, freq: Duration) -> Self {
        Self {
            book: BookState::new(symbol),
            pending: Vec::new(),
            by_date: HashMap::new(),
            total_flushed: 0,
            reconnect_count: 0,
            last_write: std::time::Instant::now(),
            last_snap_req: std::time::Instant::now(),
            data_root: data_root.to_path_buf(),
            raw_data_root: raw_data_root.to_path_buf(),
            no_raw,
            freq,
            updates_received: 0,
            updates_written: 0,
            errors: 0,
            invalid_state_since: None,
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

    fn write_raw(&self, msg: &ObData) {
        if self.no_raw { return; }
        let dt = chrono::DateTime::from_timestamp_millis(msg.timestamp_ms).unwrap_or_default();
        let date_str = dt.format("%Y-%m-%d").to_string();
        let sym_dir = self.raw_data_root.join(&self.book.symbol);
        fs::create_dir_all(&sym_dir).ok();
        let path = sym_dir.join(format!("{date_str}.jsonl"));
        let line = serde_json::json!({
            "ts": msg.timestamp_ms,
            "u": msg.update_id,
            "type": if msg.is_snapshot { "snapshot" } else { "delta" },
            "b": msg.bids,
            "a": msg.asks,
        });
        use std::io::Write;
        if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(&path) {
            let _ = writeln!(file, "{}", line);
        }
    }

    fn flush(&mut self) {
        if self.last_write.elapsed() < self.freq || self.pending.is_empty() { return; }

        for snap in self.pending.drain(..) {
            self.updates_written += 1;
            let dt = chrono::DateTime::from_timestamp_millis(snap.timestamp_ms).unwrap_or_default();
            self.by_date.entry(dt.format("%Y-%m-%d").to_string()).or_default().push(snap);
        }

        let sym_dir = self.data_root.join(&self.book.symbol);
        fs::create_dir_all(&sym_dir).ok();

        for (date_str, snaps) in &self.by_date {
            let path = sym_dir.join(format!("{date_str}.parquet"));
            let table = snapshots_to_table(snaps);
            if let Err(e) = write_parquet_append(&path, &table) {
                eprintln!("[flush] {} error: {:#}", self.book.symbol, e);
                self.errors += 1;
            }
        }
        self.total_flushed += self.by_date.values().map(|v| v.len()).sum::<usize>();
        self.by_date.clear();
        self.last_write = std::time::Instant::now();
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
            errors: self.errors,
        }
    }
}

fn get_disk_free_gb(path: &Path) -> f64 {
    match fs2::available_space(path) {
        Ok(bytes) => bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        Err(_) => -1.0,
    }
}

async fn ws_symbol(symbol: String, tx: mpsc::Sender<ObMsg>, shutdown: Arc<AtomicBool>) {
    let topic = format!("orderbook.50.{symbol}");
    let mut backoff = 1u64;
    while !shutdown.load(Ordering::Relaxed) {
        match ws_run(&symbol, &topic, &tx, &shutdown).await {
            Ok(()) => { backoff = 1; }
            Err(e) => {
                if shutdown.load(Ordering::Relaxed) { break; }
                eprintln!("[ws {symbol}] {e:#}; reconnect in {backoff}s");
                sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn ws_run(symbol: &str, topic: &str, tx: &mpsc::Sender<ObMsg>, shutdown: &Arc<AtomicBool>) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(WS_URL).await?;
    let (mut sink, mut stream) = ws.split();
    let sub = serde_json::json!({"op": "subscribe", "args": [topic]});
    sink.send(Message::Text(sub.to_string().into())).await?;
    eprintln!("[ws {symbol}] subscribed");

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
            eprintln!("[universe] adding {} new symbols: {:?}", added.len(), added);
            let mut states = states.write().await;
            for sym in &added {
                let state = SymState::new(sym, &config.data_root, &config.raw_data_root, config.no_raw, Duration::from_secs(config.freq));
                states.insert(sym.clone(), state);
                let tx = tx.clone();
                let sym = sym.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(async move { ws_symbol(sym, tx, shutdown).await; });
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

    let (tx, mut rx) = mpsc::channel::<ObMsg>(16384);
    let config = Arc::new(config);

    for sym in &config.symbols {
        let tx = tx.clone();
        let sym = sym.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move { ws_symbol(sym, tx, shutdown).await; });
    }
    drop(tx);

    let (metrics_tx, mut metrics_rx) = mpsc::channel::<MetricsEntry>(1024);
    let metrics_path = config.data_root.join("_metrics.jsonl");
    {
        let metrics_path = metrics_path.clone();
        tokio::spawn(async move { flush_metrics_loop(&mut metrics_rx, &metrics_path).await; });
    }

    let freq = Duration::from_secs(config.freq);
    let mut states_map: HashMap<String, SymState> = HashMap::new();
    for sym in &config.symbols {
        states_map.insert(sym.clone(), SymState::new(sym, &config.data_root, &config.raw_data_root, config.no_raw, freq));
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
        let tx = mpsc::channel::<ObMsg>(1).0;
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
                let mut states = states.write().await;
                if let Some(state) = states.get_mut(&ob_msg.symbol) {
                    state.write_raw(&ob_msg);
                    let need_snapshot = state.process(&ob_msg);
                    state.maybe_snapshot();
                    state.flush();
                    if need_snapshot { eprintln!("[{}] gap detected, requesting snapshot", ob_msg.symbol); }
                    if ob_msg.is_snapshot { snap_count += 1; }
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
            state.last_write = std::time::Instant::now() - Duration::from_secs(999);
            state.flush();
        }
        let disk_free = get_disk_free_gb(&config.data_root);
        for state in states.values() {
            let _ = metrics_tx.send(state.metrics_entry(disk_free)).await;
        }
    }
    drop(metrics_tx);

    eprintln!("[ob_reconstructor] shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
