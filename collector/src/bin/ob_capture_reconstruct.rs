use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use serde_json::json;

use collector_lib::book_state::BookState;

const DEFAULT_CAPTURES_DIR: &str = "data/market/orderbook/captures";
const DEFAULT_OUT_DIR: &str = "data/market/orderbook/reconstructed_captures";
const DEFAULT_FREQ_SEC: u64 = 5;
const MAX_LEVEL_COLS: usize = 20;
const RESEARCH_DEPTH_LEVELS: usize = 20;

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
                let v: Vec<f64> = col.iter().map(|c| match c { Cell::F(f) => *f, _ => f64::NAN }).collect();
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

fn write_parquet(path: &Path, t: &Table) -> Result<()> {
    use parquet::arrow::arrow_writer::ArrowWriter;

    let (schema, batch) = table_to_batch(t)?;
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let tmp = path.with_extension("parquet.tmp");
    let file = fs::File::create(&tmp)?;
    let mut w = ArrowWriter::try_new(file, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn read_capture(path: &Path) -> Result<Table> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut out: Table = Vec::new();
    for batch in reader {
        let b = batch?;
        if out.is_empty() {
            for f in b.schema().fields() {
                out.push((Box::leak(f.name().clone().into_boxed_str()), Col::new()));
            }
        }
        for (i, (_, col)) in out.iter_mut().enumerate() {
            let a = b.column(i);
            let rows = b.num_rows();
            if let Some(arr) = a.as_any().downcast_ref::<Int64Array>() {
                for j in 0..rows { col.push(Cell::I(arr.value(j))); }
            } else if let Some(arr) = a.as_any().downcast_ref::<Float64Array>() {
                for j in 0..rows { col.push(Cell::F(arr.value(j))); }
            } else if let Some(arr) = a.as_any().downcast_ref::<BooleanArray>() {
                for j in 0..rows { col.push(Cell::B(arr.value(j))); }
            } else if let Some(arr) = a.as_any().downcast_ref::<StringArray>() {
                for j in 0..rows { col.push(Cell::S(arr.value(j).to_string())); }
            }
        }
    }
    Ok(out)
}

fn col_i<'a>(t: &'a Table, name: &'a str) -> Option<&'a Col> {
    t.iter().find(|(n, _)| *n == name).map(|(_, c)| c)
}

fn as_i(c: &Cell) -> i64 { if let Cell::I(v) = c { *v } else { 0 } }
fn as_f(c: &Cell) -> f64 { if let Cell::F(v) = c { *v } else { f64::NAN } }
fn as_b(c: &Cell) -> bool { if let Cell::B(v) = c { *v } else { false } }

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

struct CaptureRow { ts: i64, seq: i64, is_delta: bool, bid_px: f64, bid_sz: f64, ask_px: f64, ask_sz: f64 }

fn load_messages(t: &Table) -> Vec<Vec<CaptureRow>> {
    let (Some(ts_col), Some(seq_col), Some(delta_col), Some(bp), Some(bs), Some(ap), Some(as_)) = (
        col_i(t, "ts"), col_i(t, "seq"), col_i(t, "is_delta"),
        col_i(t, "bid_px"), col_i(t, "bid_sz"), col_i(t, "ask_px"), col_i(t, "ask_sz"),
    ) else { return Vec::new() };
    let n = table_len(t);
    let mut by_key: BTreeMap<(i64, i64), Vec<CaptureRow>> = BTreeMap::new();
    for i in 0..n {
        let row = CaptureRow {
            ts: as_i(&ts_col[i]),
            seq: as_i(&seq_col[i]),
            is_delta: as_b(&delta_col[i]),
            bid_px: as_f(&bp[i]),
            bid_sz: as_f(&bs[i]),
            ask_px: as_f(&ap[i]),
            ask_sz: as_f(&as_[i]),
        };
        by_key.entry((row.ts, row.seq)).or_default().push(row);
    }
    by_key.into_values().collect()
}

/// Реконструкция capture: снапшоты каждые freq_sec, NaN за пределами глубины.
fn reconstruct(symbol: &str, msgs: &[Vec<CaptureRow>], freq_sec: u64) -> (Table, usize) {
    let mut book = BookState::new(symbol);
    let mut snaps: Vec<collector_lib::book_state::ReconstructedSnapshot> = Vec::new();
    let mut last_emit: i64 = i64::MIN;
    let mut max_level_seen: usize = 0;
    let freq_ms = (freq_sec * 1000) as i64;

    for group in msgs {
        let first = &group[0];
        let mut bids: Vec<(String, String)> = Vec::new();
        let mut asks: Vec<(String, String)> = Vec::new();
        for r in group {
            if !r.bid_px.is_nan() {
                bids.push((format!("{}", r.bid_px), format!("{}", r.bid_sz)));
            }
            if !r.ask_px.is_nan() {
                asks.push((format!("{}", r.ask_px), format!("{}", r.ask_sz)));
            }
        }
        max_level_seen = max_level_seen.max(bids.len()).max(asks.len());
        if first.is_delta {
            book.apply_delta(first.seq, first.ts, &bids, &asks);
        } else {
            book.apply_snapshot(first.seq, first.ts, &bids, &asks);
        }
        if book.is_valid() && (last_emit == i64::MIN || first.ts - last_emit >= freq_ms) {
            if let Some(snap) = book.snapshot() {
                last_emit = first.ts;
                snaps.push(snap);
            }
        }
    }

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
    for snap in &snaps {
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
        for (li, lvl) in snap.levels.iter().enumerate().take(MAX_LEVEL_COLS) {
            let base = 10 + li * 4;
            t[base].1.push(Cell::F(lvl.bid_px));
            t[base + 1].1.push(Cell::F(lvl.bid_sz));
            t[base + 2].1.push(Cell::F(lvl.ask_px));
            t[base + 3].1.push(Cell::F(lvl.ask_sz));
        }
        for li in snap.levels.len()..MAX_LEVEL_COLS {
            let base = 10 + li * 4;
            t[base].1.push(Cell::F(f64::NAN));
            t[base + 1].1.push(Cell::F(f64::NAN));
            t[base + 2].1.push(Cell::F(f64::NAN));
            t[base + 3].1.push(Cell::F(f64::NAN));
        }
    }
    (t, max_level_seen)
}

fn capture_symbol(captures_dir: &Path, event_id: &str) -> String {
    let meta_path = captures_dir.join(format!("{event_id}.meta.json"));
    if let Ok(raw) = fs::read_to_string(&meta_path) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
            if let Some(s) = v["symbol"].as_str() {
                return s.to_string();
            }
        }
    }
    event_id.rsplit('_').next().unwrap_or("").to_string()
}

fn process_capture(captures_dir: &Path, out_dir: &Path, event_id: &str, freq_sec: u64) -> Result<()> {
    let capture_path = captures_dir.join(format!("{event_id}.parquet"));
    if !capture_path.exists() {
        eprintln!("[recon] пропуск: нет capture {event_id}");
        return Ok(());
    }
    let meta_out = out_dir.join(format!("{event_id}.meta.json"));
    if meta_out.exists() {
        eprintln!("[recon] пропуск (уже обработан): {event_id}");
        return Ok(());
    }

    let table = read_capture(&capture_path)?;
    let messages = load_messages(&table);
    if messages.is_empty() {
        let meta = json!({
            "event_id": event_id,
            "status": "no_data",
            "n_messages": 0,
            "n_snapshot_msgs": 0,
            "n_rows": 0,
            "first_ts_ms": serde_json::Value::Null,
            "last_ts_ms": serde_json::Value::Null,
            "span_sec": 0.0,
            "max_level_seen": 0,
            "depth_incomplete": true,
            "reconstruction_version": "capture-1.0",
        });
        fs::create_dir_all(out_dir)?;
        fs::write(&meta_out, serde_json::to_string_pretty(&meta)?)?;
        return Ok(());
    }

    let symbol = capture_symbol(&captures_dir, event_id);
    let (t, max_level_seen) = reconstruct(&symbol, &messages, freq_sec);

    let n_rows = table_len(&t);
    let first_ts = messages.first().map(|g| g[0].ts).unwrap_or(0);
    let last_ts = messages.last().map(|g| g[g.len() - 1].ts).unwrap_or(0);
    let span_sec = (last_ts - first_ts) as f64 / 1000.0;
    let n_snapshot_msgs = messages.iter().filter(|g| !g[0].is_delta).count();

    let status = if n_rows > 0 { "ok" } else { "no_snapshot" };
    let meta = json!({
        "event_id": event_id,
        "status": status,
        "n_messages": messages.len(),
        "n_snapshot_msgs": n_snapshot_msgs,
        "n_rows": n_rows,
        "first_ts_ms": first_ts,
        "last_ts_ms": last_ts,
        "span_sec": span_sec,
        "max_level_seen": max_level_seen,
        "depth_incomplete": max_level_seen < RESEARCH_DEPTH_LEVELS,
        "reconstruction_version": "capture-1.0",
        "freq_sec": freq_sec,
    });

    fs::create_dir_all(out_dir)?;
    if n_rows > 0 {
        write_parquet(&out_dir.join(format!("{event_id}.parquet")), &t)?;
    }
    fs::write(&meta_out, serde_json::to_string_pretty(&meta)?)?;

    let depth = if max_level_seen >= RESEARCH_DEPTH_LEVELS { "full" } else { "partial" };
    eprintln!("[recon] {event_id}: status={status} msgs={} snaps={} rows={n_rows} span={span_sec:.0}s depth={max_level_seen}({depth})", messages.len(), n_snapshot_msgs);
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut captures_dir = PathBuf::from(DEFAULT_CAPTURES_DIR);
    let mut out_dir = PathBuf::from(DEFAULT_OUT_DIR);
    let mut event_id: Option<String> = None;
    let mut freq_sec = DEFAULT_FREQ_SEC;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--captures-dir" => { i += 1; if let Some(v) = args.get(i) { captures_dir = PathBuf::from(v); } }
            "--out-dir" => { i += 1; if let Some(v) = args.get(i) { out_dir = PathBuf::from(v); } }
            "--event-id" => { i += 1; event_id = args.get(i).cloned(); }
            "--freq-sec" => { i += 1; if let Some(v) = args.get(i) { freq_sec = v.parse().unwrap_or(DEFAULT_FREQ_SEC); } }
            _ => {}
        }
        i += 1;
    }

    fs::create_dir_all(&out_dir)?;
    match event_id {
        Some(id) => process_capture(&captures_dir, &out_dir, &id, freq_sec)?,
        None => {
            let mut ids: Vec<String> = Vec::new();
            for entry in fs::read_dir(&captures_dir)? {
                let p = entry?.path();
                if p.extension().map_or(false, |e| e == "parquet") {
                    if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                        ids.push(stem.to_string());
                    }
                }
            }
            ids.sort();
            eprintln!("[recon] найдено captures: {}", ids.len());
            for id in ids {
                process_capture(&captures_dir, &out_dir, &id, freq_sec)?;
            }
        }
    }
    Ok(())
}