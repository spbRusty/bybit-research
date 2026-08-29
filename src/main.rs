//! bybit_rs: дофилл 1m-свечек в существующие parquet-файлы + реалтайм-дозапись.
//!
//! Фаза 1 (дофилл): для каждого `data/klines/{linear,spot}/*_1m.parquet` читает последнюю
//! свечу и пагинационно докачивает REST-ом до текущего момента (лимит Bybit 600 req/min,
//! глобальный темп ~9 req/s). Фаза 2 (реалтайм): подписка WebSocket kline.1.{symbol},
//! буфер в памяти, сброс в файлы раз в 30 секунд. Перезапуск безопасен: курсор всегда
//! берётся из последней свечи файла, дубли схлопываются по open_time.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{BooleanArray, Float64Array, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tokio_tungstenite::tungstenite::Message;

const ROOT: &str = "data/klines";
const API: &str = "https://api.bybit.com";
/// Глобальный темп запросов к REST: 115 мс/запрос ≈ 8.7 req/s (лимит Bybit 600/мин).
const PACE_MS: u64 = 115;
/// Защита от бесконечного цикла (2000 страниц ≈ 5.5 лет 1m-свечей).
const MAX_PAGES: usize = 2000;
/// Сброс реалтайм-буфера в файлы, сек.
const FLUSH_SEC: u64 = 30;

#[derive(Debug, Clone)]
struct Candle {
    start: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    turnover: f64,
}

impl Candle {
    fn is_green(&self) -> bool {
        self.close >= self.open
    }
}

#[derive(Debug, Clone)]
struct FileState {
    path: PathBuf,
    category: String,
    symbol: String,
}

fn discover() -> Result<Vec<FileState>> {
    let mut out = vec![];
    for cat in ["linear", "spot"] {
        let dir = Path::new(ROOT).join(cat);
        for entry in fs::read_dir(&dir).with_context(|| format!("read_dir {dir:?}"))? {
            let p = entry?.path();
            if p.extension().map_or(false, |e| e == "parquet") {
                let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                let suffix = format!("_{cat}_1m");
                if let Some(sym) = name.strip_suffix(&suffix) {
                    out.push(FileState { path: p, category: cat.to_string(), symbol: sym.to_string() });
                }
            }
        }
    }
    Ok(out)
}

/// Читает свечи из паркета. Столбцы: open_time, open, high, low, close, volume, turnover, is_green.
fn read_parquet(path: &Path) -> Result<Vec<Candle>> {
    let file = File::open(path).with_context(|| format!("open {path:?}"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet header {path:?}"))?;
    let reader = builder.build()?;
    let mut out = vec![];
    for batch in reader {
        let b = batch?;
        let ts = b.column(0).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        let o = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let h = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let l = b.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let c = b.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let v = b.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let t = b.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push(Candle {
                start: ts.value(i),
                open: o.value(i),
                high: h.value(i),
                low: l.value(i),
                close: c.value(i),
                volume: v.value(i),
                turnover: t.value(i),
            });
        }
    }
    Ok(out)
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("open_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("is_green", DataType::Boolean, false),
    ]))
}

fn write_parquet(path: &Path, rows: &[Candle]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let ts: Vec<i64> = rows.iter().map(|c| c.start).collect();
    let o: Vec<f64> = rows.iter().map(|c| c.open).collect();
    let h: Vec<f64> = rows.iter().map(|c| c.high).collect();
    let l: Vec<f64> = rows.iter().map(|c| c.low).collect();
    let cl: Vec<f64> = rows.iter().map(|c| c.close).collect();
    let v: Vec<f64> = rows.iter().map(|c| c.volume).collect();
    let t: Vec<f64> = rows.iter().map(|c| c.turnover).collect();
    let g: Vec<bool> = rows.iter().map(|c| c.is_green()).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMillisecondArray::from(ts)),
            Arc::new(Float64Array::from(o)),
            Arc::new(Float64Array::from(h)),
            Arc::new(Float64Array::from(l)),
            Arc::new(Float64Array::from(cl)),
            Arc::new(Float64Array::from(v)),
            Arc::new(Float64Array::from(t)),
            Arc::new(BooleanArray::from(g)),
        ],
    )?;
    let tmp = path.with_extension("parquet.tmp");
    let file = File::create(&tmp).with_context(|| format!("create {tmp:?}"))?;
    let mut writer = ArrowWriter::try_new(file, schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    fs::rename(&tmp, path).with_context(|| format!("rename {tmp:?} -> {path:?}"))?;
    Ok(())
}

/// Слияние старых + новых свечей, схлопывание дублей по open_time (побеждает новое), запись.
fn append_rows(path: &Path, new: &[Candle]) -> Result<()> {
    if new.is_empty() {
        return Ok(());
    }
    let old = if path.exists() { read_parquet(path)? } else { vec![] };
    let mut map: HashMap<i64, Candle> = HashMap::with_capacity(old.len() + new.len());
    for c in old {
        map.insert(c.start, c);
    }
    for c in new {
        map.insert(c.start, c.clone());
    }
    let mut rows: Vec<Candle> = map.into_values().collect();
    rows.sort_unstable_by_key(|c| c.start);
    write_parquet(path, &rows)
}

fn f(a: &[Value], i: usize) -> Result<f64> {
    Ok(a.get(i).and_then(|v| v.as_str()).unwrap_or("0").parse()?)
}

/// Дофилл одного символа от последней свечи файла до текущего момента.
async fn backfill_one(st: &FileState, client: &Client, tokens: Arc<Mutex<mpsc::Receiver<()>>>) -> Result<Vec<Candle>> {
    let last_ts = if st.path.exists() {
        read_parquet(&st.path)?.last().map(|c| c.start)
    } else {
        None
    };
    let mut cursor = last_ts.map(|t| t + 60_000).unwrap_or_else(|| Utc::now().timestamp_millis() - 2 * 86_400_000);
    let end = Utc::now().timestamp_millis();
    let mut out: Vec<Candle> = Vec::new();
    for _ in 0..MAX_PAGES {
        if cursor >= end - 60_000 {
            break;
        }
        {
            let mut rx = tokens.lock().await;
            if rx.recv().await.is_none() {
                break;
            }
        }
        let url = format!(
            "{API}/v5/market/kline?category={}&symbol={}&interval=1&start={cursor}&end={end}&limit=1000",
            st.category, st.symbol
        );
        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()?
            .json::<Value>()
            .await?;
        if resp["retCode"].as_i64() != Some(0) {
            eprintln!("[{}] retCode={}: {}", st.symbol, resp["retCode"], resp["retMsg"]);
            break; // делистинг/невалидный символ — файл оставляем как есть
        }
        let list = resp["result"]["list"].as_array().cloned().unwrap_or_default(); // новые сначала
        if list.is_empty() {
            break;
        }
        let mut page_max = 0i64;
        for row in list.iter().rev() {
            let a = row.as_array().with_context(|| "row not array")?;
            let start: i64 = a.first().and_then(|v| v.as_str()).unwrap_or("0").parse()?;
            page_max = page_max.max(start);
            if start < cursor {
                continue;
            }
            out.push(Candle {
                start,
                open: f(a, 1)?,
                high: f(a, 2)?,
                low: f(a, 3)?,
                close: f(a, 4)?,
                volume: f(a, 5)?,
                turnover: f(a, 6)?,
            });
        }
        let next_cursor = page_max + 60_000;
        if next_cursor <= cursor || list.len() < 1000 {
            break;
        }
        cursor = next_cursor;
    }
    Ok(out)
}

// ------------------------- реалтайм -------------------------

async fn ws_loop(category: &str, symbols: Vec<String>, tx: mpsc::Sender<(String, Vec<Candle>)>) {
    loop {
        let url = format!("wss://stream.bybit.com/v5/public/{category}");
        match ws_run(&url, &symbols, &tx).await {
            Ok(_) => {}
            Err(e) => eprintln!("[ws {category}] {e:#}; переподключение через 3с"),
        }
        sleep(Duration::from_secs(3)).await;
    }
}

async fn ws_run(url: &str, symbols: &[String], tx: &mpsc::Sender<(String, Vec<Candle>)>) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut sink, mut stream) = ws.split();
    for chunk in symbols.chunks(10) {
        let args: Vec<String> = chunk.iter().map(|s| format!("kline.1.{s}")).collect();
        sink.send(Message::Text(json!({"op": "subscribe", "args": args}).to_string().into())).await?;
    }
    eprintln!("[ws] {url} подписан: {} символов", symbols.len());
    while let Some(msg) = stream.next().await {
        let msg = msg?;
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(t.as_str())?;
            if v["op"].as_str() == Some("ping") {
                sink.send(Message::Text(json!({"op": "pong"}).to_string().into())).await?;
                continue;
            }
            let Some(sym) = v["topic"].as_str().and_then(|t| t.strip_prefix("kline.1.")) else {
                continue;
            };
            let Some(arr) = v["data"].as_array() else { continue };
            let mut candles = Vec::with_capacity(arr.len());
            for d in arr {
                let Some(start) = d["start"].as_i64() else { continue };
                let parse = |k: &str| -> Option<f64> { d[k].as_str().and_then(|s| s.parse().ok()) };
                let (Some(open), Some(high), Some(low), Some(close), Some(volume), Some(turnover)) =
                    (parse("open"), parse("high"), parse("low"), parse("close"), parse("volume"), parse("turnover"))
                else {
                    continue;
                };
                candles.push(Candle { start, open, high, low, close, volume, turnover });
            }
            if !candles.is_empty() {
                send_candles(tx, url, sym, candles).await?;
            }
        }
    }
    Ok(())
}

async fn send_candles(tx: &mpsc::Sender<(String, Vec<Candle>)>, url: &str, sym: &str, candles: Vec<Candle>) -> Result<()> {
    let cat = if url.contains("/public/spot") { "spot" } else { "linear" };
    let key = format!("{cat}/{sym}");
    tx.send((key, candles)).await.context("ws tx закрыт")
}

/// Сброс буфера реалтайма в файлы каждые FLUSH_SEC или при 1000 накопленных свечей.
async fn flush_loop(mut rx: mpsc::Receiver<(String, Vec<Candle>)>) {
    let mut pending: HashMap<String, Vec<Candle>> = HashMap::new();
    let mut last_flush = Instant::now();
    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some((key, mut cs)) => {
                    let big = cs.len() >= 1000;
                    pending.entry(key).or_default().append(&mut cs);
                    if big { last_flush = Instant::now() - Duration::from_secs(FLUSH_SEC); }
                }
                None => break,
            },
            _ = sleep(Duration::from_secs(FLUSH_SEC)) => {}
        }
        if last_flush.elapsed() < Duration::from_secs(FLUSH_SEC) || pending.is_empty() {
            continue;
        }
        let total: usize = pending.values().map(|v| v.len()).sum();
        let keys: Vec<String> = pending.keys().cloned().collect();
        for key in keys {
            let rows = pending.remove(&key).unwrap_or_default();
            let (cat, sym) = key.split_once('/').expect("key cat/sym");
            let path = Path::new(ROOT).join(cat).join(format!("{sym}_{cat}_1m.parquet"));
            if let Err(e) = append_rows(&path, &rows) {
                eprintln!("[flush {sym}] {e:#}");
            }
        }
        eprintln!("[flush] записано свечей: {total}");
        last_flush = Instant::now();
    }
}

// ------------------------- main -------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let states = discover()?;
    let linear = states.iter().filter(|s| s.category == "linear").count();
    let spot = states.iter().filter(|s| s.category == "spot").count();
    println!("найдено паркетов: {linear} linear + {spot} spot");

    // Глобальный дроссель REST: канал, питаемый интервалом PACE_MS.
    let (ptx, prx) = mpsc::channel(4096);
    let tokens = Arc::new(Mutex::new(prx));
    tokio::spawn(async move {
        let mut it = tokio::time::interval(Duration::from_millis(PACE_MS));
        it.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            it.tick().await;
            if ptx.send(()).await.is_err() {
                break;
            }
        }
    });

    let client = Client::builder().timeout(Duration::from_secs(20)).build()?;
    let client = Arc::new(client);

    // Фаза 1: дофилл.
    let total = states.len();
    println!("дофилл: {total} символов, темп ~8.7 req/s, ориентир: несколько часов");
    let done = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    for st in states.clone() {
        let tokens = tokens.clone();
        let client = client.clone();
        let done = done.clone();
        tasks.spawn(async move {
            let res = backfill_one(&st, &client, tokens).await;
            (st, res, done)
        });
    }
    let mut failed = 0usize;
    while let Some(joined) = tasks.join_next().await {
        let (st, res, done) = joined?;
        match res {
            Ok(rows) => {
                if let Err(e) = append_rows(&st.path, &rows) {
                    eprintln!("[{}] запись: {e:#}", st.symbol);
                    failed += 1;
                } else if !rows.is_empty() {
                    let d = done.fetch_add(1, Ordering::SeqCst) + 1;
                    if d % 50 == 0 || d == 1 {
                        println!("дофилл: {d}/{total}...");
                    }
                }
            }
            Err(e) => {
                eprintln!("[{}] дофилл не удался: {e:#}", st.symbol);
                failed += 1;
            }
        }
    }
    println!("дофилл завершён. неудач: {failed}. Переключаюсь в реалтайм.");

    // Фаза 2: реалтайм.
    let (wtx, wrx) = mpsc::channel(8192);
    for cat in ["linear", "spot"] {
        let syms: Vec<String> = states.iter().filter(|s| s.category == cat).map(|s| s.symbol.clone()).collect();
        let wtx = wtx.clone();
        tokio::spawn(async move { ws_loop(cat, syms, wtx).await });
    }
    drop(wtx);
    flush_loop(wrx).await;
    Ok(())
}