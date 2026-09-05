//! marketdata: реалтайм-сбор расширенных рыночных потоков Bybit (linear, USDT-перпетуалы).
//!
//! Собирает parquet-файлы для групп признаков ТЗ §12-§15:
//!   - trades     : publicTrade.{sym}  -> data/market/trades/linear/{sym}.parquet  (§13 order flow)
//!   - orderbook  : event-driven capture -> data/market/orderbook/captures/{event_id}.parquet (§3 candle trigger)
//!   - futures    : tickers.{sym}      -> data/market/futures/linear/{sym}.parquet  (§14 funding/OI, §15 basis)
//!   - liquidation: allLiquidation.{sym} -> data/market/liquidation/linear/{sym}.parquet
//!   - ratio      : REST account-ratio (поллинг) -> data/market/ratio/linear/{sym}.parquet
//!
//! Архитектура orderbook (§3-§7):
//!   Python candle_trigger.py создаёт JSON-файлы в data/triggers/
//!   -> trigger_watcher обнаруживает новый триггер
//!   -> ob_capture_task подключается к WS, подписывается на orderbook.50.{symbol}
//!   -> собирает данные capture_duration_sec
//!   -> сохраняет parquet с event_id в имени файла
//!   -> удаляет JSON-файл триггера

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{BooleanArray, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use futures_util::{Sink, SinkExt, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;

const API: &str = "https://api.bybit.com";
const PACE_MS: u64 = 200;
const OB_LEVELS: usize = 10;
const FLUSH_SEC: u64 = 30;
const RATIO_POLL_SEC: u64 = 300;
const FUTURES_POLL_SEC: u64 = 60;
const SUB_CHUNK: usize = 100;
const DATA_ROOT: &str = "data/market";
const TRIGGERS_DIR: &str = "data/triggers";
const CAPTURES_DIR: &str = "data/market/orderbook/captures";


/// Один элемент столбца.
#[derive(Debug, Clone)]
enum Cell {
    I(i64),
    F(f64),
    B(bool),
}

type Col = Vec<Cell>;
type Table = Vec<(&'static str, Col)>;

fn len(t: &Table) -> usize {
    t.first().map(|(_, c)| c.len()).unwrap_or(0)
}

fn write_parquet(path: &Path, t: &Table) -> Result<()> {
    let n = len(t);
    if n == 0 { return Ok(()); }
    let mut fields = Vec::with_capacity(t.len());
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(t.len());
    for (name, col) in t {
        match &col[0] {
            Cell::I(_) => {
                fields.push(Field::new(*name, DataType::Int64, false));
                let v: Vec<i64> = col.iter().map(|c| if let Cell::I(i) = c { *i } else { 0 }).collect();
                arrays.push(Arc::new(Int64Array::from(v)));
            }
            Cell::F(_) => {
                fields.push(Field::new(*name, DataType::Float64, false));
                let v: Vec<f64> = col.iter().map(|c| if let Cell::F(f) = c { *f } else { 0.0 }).collect();
                arrays.push(Arc::new(Float64Array::from(v)));
            }
            Cell::B(_) => {
                fields.push(Field::new(*name, DataType::Boolean, false));
                let v: Vec<bool> = col.iter().map(|c| if let Cell::B(b) = c { *b } else { false }).collect();
                arrays.push(Arc::new(BooleanArray::from(v)));
            }
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let tmp = path.with_extension("parquet.tmp");
    let file = File::create(&tmp).with_context(|| format!("create {tmp:?}"))?;
    let mut w = ArrowWriter::try_new(file, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    fs::rename(&tmp, path).with_context(|| format!("rename {tmp:?} -> {path:?}"))?;
    Ok(())
}

fn read_parquet(path: &Path) -> Result<Table> {
    let file = File::open(path)?;
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
            if let Some(arr) = a.as_any().downcast_ref::<Int64Array>() {
                for j in 0..arr.len() { col.push(Cell::I(arr.value(j))); }
            } else if let Some(arr) = a.as_any().downcast_ref::<Float64Array>() {
                for j in 0..arr.len() { col.push(Cell::F(arr.value(j))); }
            } else if let Some(arr) = a.as_any().downcast_ref::<BooleanArray>() {
                for j in 0..arr.len() { col.push(Cell::B(arr.value(j))); }
            }
        }
    }
    Ok(out)
}

/// Слияние новых строк с файлом, сортировка по ts, запись.
fn append_table(path: &Path, new: &Table) -> Result<()> {
    if len(new) == 0 { return Ok(()); }
    let mut old = if path.exists() { read_parquet(path)? } else { Table::new() };
    if old.is_empty() {
        old = new.clone();
    } else if old.len() == new.len() {
        for (i, (_, col)) in new.iter().enumerate() {
            old[i].1.extend(col.clone());
        }
    }
    if let Some(ti) = old.iter().position(|(n, _)| *n == "ts") {
        let n = old[ti].1.len();
        let ts: Vec<i64> = old[ti].1.iter().map(|c| if let Cell::I(v) = c { *v } else { 0 }).collect();
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_unstable_by_key(|&i| ts[i]);
        for (_, col) in &mut old {
            let orig = std::mem::replace(col, Col::with_capacity(n));
            for &i in &order { col.push(orig[i].clone()); }
        }
    }
    write_parquet(path, &old)
}

// ---------- WS ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stream { Trades, Orderbook, Liquidation }

impl Stream {
    fn dir(&self) -> &'static str {
        match self {
            Stream::Trades => "trades",
            Stream::Orderbook => "orderbook",
            Stream::Liquidation => "liquidation",
        }
    }
    fn topic(&self, sym: &str) -> String {
        match self {
            Stream::Trades => format!("publicTrade.{sym}"),
            Stream::Orderbook => format!("orderbook.50.{sym}"),
            Stream::Liquidation => "allLiquidation".to_string(),
        }
    }
    fn strip<'a>(&self, topic: &'a str) -> Option<&'a str> {
        match self {
            Stream::Trades => topic.strip_prefix("publicTrade."),
            Stream::Orderbook => topic.strip_prefix("orderbook.50."),
            Stream::Liquidation => {
                if topic == "allLiquidation" { Some("broadcast") }
                else { topic.strip_prefix("allLiquidation.") }
            }
        }
    }
}

fn pf(s: Option<&str>) -> f64 { s.and_then(|x| x.parse().ok()).unwrap_or(f64::NAN) }

/// Анализирует WS-сообщение в parquet-таблицу.
fn parse_message(stream: Stream, v: &Value) -> Option<(String, Table)> {
    let topic = v["topic"].as_str()?;
    let sym = stream.strip(topic)?.to_string();
    let ts = v["ts"].as_i64()?;
    let data = &v["data"];
    match stream {
        Stream::Trades => {
            let arr = data.as_array()?;
            let mut t = Table::from([
                ("ts", Col::new()), ("seq", Col::new()), ("is_buy", Col::new()),
                ("price", Col::new()), ("size", Col::new()),
            ]);
            for d in arr {
                t[0].1.push(Cell::I(ts));
                t[1].1.push(Cell::I(d["seq"].as_i64().unwrap_or(0)));
                t[2].1.push(Cell::B(d["S"].as_str() == Some("Buy")));
                t[3].1.push(Cell::F(pf(d["p"].as_str())));
                t[4].1.push(Cell::F(pf(d["v"].as_str())));
            }
            if t[0].1.is_empty() { return None; }
            Some((sym, t))
        }
        Stream::Orderbook => {
            let d = data.as_object()?;
            let bids = d.get("b").and_then(|x| x.as_array()).cloned().unwrap_or_default();
            let asks = d.get("a").and_then(|x| x.as_array()).cloned().unwrap_or_default();
            let seq = d.get("seq").and_then(|x| x.as_i64()).unwrap_or(0);
            let is_delta = v["type"].as_str() == Some("delta");
            let mut t = Table::from([
                ("ts", Col::new()), ("seq", Col::new()), ("is_delta", Col::new()), ("level", Col::new()),
                ("bid_px", Col::new()), ("bid_sz", Col::new()), ("ask_px", Col::new()), ("ask_sz", Col::new()),
            ]);
            for i in 0..OB_LEVELS {
                let bp = bids.get(i).and_then(|x| x.as_array());
                let ap = asks.get(i).and_then(|x| x.as_array());
                if bp.is_none() && ap.is_none() { continue; }
                t[0].1.push(Cell::I(ts));
                t[1].1.push(Cell::I(seq));
                t[2].1.push(Cell::B(is_delta));
                t[3].1.push(Cell::I(i as i64 + 1));
                t[4].1.push(Cell::F(pf(bp.and_then(|a| a.get(0)).and_then(|x| x.as_str()))));
                t[5].1.push(Cell::F(pf(bp.and_then(|a| a.get(1)).and_then(|x| x.as_str()))));
                t[6].1.push(Cell::F(pf(ap.and_then(|a| a.get(0)).and_then(|x| x.as_str()))));
                t[7].1.push(Cell::F(pf(ap.and_then(|a| a.get(1)).and_then(|x| x.as_str()))));
            }
            if t[0].1.is_empty() { return None; }
            Some((sym, t))
        }
        Stream::Liquidation => {
            let arr = data.as_array()?;
            let sym = if stream.strip(topic) == Some("broadcast") {
                arr.first()?.get("symbol")?.as_str()?.to_string()
            } else {
                stream.strip(topic)?.to_string()
            };
            let mut t = Table::from([
                ("ts", Col::new()), ("is_sell", Col::new()), ("price", Col::new()), ("size", Col::new()),
            ]);
            for d in arr {
                t[0].1.push(Cell::I(ts));
                t[1].1.push(Cell::B(d["S"].as_str() == Some("Sell")));
                t[2].1.push(Cell::F(pf(d["p"].as_str())));
                t[3].1.push(Cell::F(pf(d["v"].as_str())));
            }
            if t[0].1.is_empty() { return None; }
            Some((sym, t))
        }
    }
}

async fn subscribe_all<S>(sink: &mut S, stream: Stream, syms: &[String]) -> Result<()>
where S: Sink<Message> + Unpin, S::Error: std::error::Error + Send + Sync + 'static,
{
    if stream == Stream::Liquidation {
        let args = vec!["allLiquidation".to_string()];
        sink.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await?;
        return Ok(());
    }
    for c in syms.chunks(SUB_CHUNK) {
        let args: Vec<String> = c.iter().map(|s| stream.topic(s)).collect();
        sink.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await?;
    }
    Ok(())
}

async fn ws_loop(stream: Stream, syms: Arc<Vec<String>>, tx: mpsc::Sender<(String, Table)>) {
    let mut backoff = 1u64;
    loop {
        let url = "wss://stream.bybit.com/v5/public/linear";
        match ws_run(stream, url, &syms, &tx).await {
            Ok(()) => { backoff = 1; }
            Err(e) => {
                eprintln!("[ws {}] {e:#}; переподключение через {backoff}с", stream.dir());
                sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(60);
            }
        }
    }
}

async fn ws_run(stream: Stream, url: &str, syms: &Arc<Vec<String>>, tx: &mpsc::Sender<(String, Table)>) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut sink, mut stream_ws) = ws.split();
    subscribe_all(&mut sink, stream, syms).await?;
    eprintln!("[ws {}] подписан: {} символов", stream.dir(), syms.len());
    loop {
        tokio::select! {
            msg = stream_ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(t.as_str())?;
                    if v["op"].as_str() == Some("ping") {
                        sink.send(Message::Text(json!({"op":"pong"}).to_string().into())).await?;
                        continue;
                    }
                    if v.get("topic").is_none() { continue; }
                    if let Some((sym, rows)) = parse_message(stream, &v) {
                        tx.send((format!("{}/{}", stream.dir(), sym), rows)).await?;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => break Ok(()),
            },
            _ = sleep(Duration::from_secs(60)) => subscribe_all(&mut sink, stream, syms).await?,
        }
    }
}

async fn flush_loop(mut rx: mpsc::Receiver<(String, Table)>) {
    let mut pending: HashMap<String, Table> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(FLUSH_SEC));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some((key, table)) => { pending.entry(key).or_default().extend(table); }
                None => break,
            },
            _ = tick.tick() => {}
        }
        flush_pending(&mut pending);
    }
}

fn flush_pending(pending: &mut HashMap<String, Table>) {
    if pending.is_empty() { return; }
    let keys: Vec<String> = pending.keys().cloned().collect();
    for key in keys {
        let rows = pending.remove(&key).unwrap_or_default();
        let (dir_name, sym) = key.split_once('/').expect("key");
        let path = Path::new(DATA_ROOT).join(dir_name).join("linear").join(format!("{sym}.parquet"));
        if let Err(e) = append_table(&path, &rows) { eprintln!("[flush {sym}] {e:#}"); }
    }
}

// ---------- REST ratio ----------

async fn futures_loop(syms: Arc<Vec<String>>, client: Arc<Client>, tokens: Arc<Mutex<mpsc::Receiver<()>>>) {
    loop {
        for sym in syms.iter() {
            let url = format!("{API}/v5/market/tickers?category=linear&symbol={sym}");
            {
                let mut rx = tokens.lock().await;
                if rx.recv().await.is_none() { return; }
            }
            if let Ok(r) = client.get(&url).send().await {
                if let Ok(r) = r.error_for_status() {
                    if let Ok(v) = r.json::<Value>().await {
                        if let Some(t) = v["result"]["list"].as_array().and_then(|l| l.first()) {
                            let d = t.as_object();
                            let ts = Utc::now().timestamp_millis();
                            let gi = |k: &str| -> i64 { d.and_then(|o| o.get(k)).and_then(|x| x.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0) };
                            let pf2 = |k: &str| -> f64 { d.and_then(|o| o.get(k)).and_then(|x| x.as_str()).and_then(|s| s.parse().ok()).unwrap_or(f64::NAN) };
                            let path = Path::new(DATA_ROOT).join("futures").join("linear").join(format!("{sym}.parquet"));
                            let table: Table = vec![
                                ("ts", vec![Cell::I(ts)]),
                                ("funding_rate", vec![Cell::F(pf2("fundingRate"))]),
                                ("next_funding_time", vec![Cell::I(gi("nextFundingTime"))]),
                                ("oi", vec![Cell::F(pf2("openInterest"))]),
                                ("oi_value", vec![Cell::F(pf2("openInterestValue"))]),
                                ("last_px", vec![Cell::F(pf2("lastPrice"))]),
                                ("mark_px", vec![Cell::F(pf2("markPrice"))]),
                                ("index_px", vec![Cell::F(pf2("indexPrice"))]),
                                ("bid1_px", vec![Cell::F(pf2("bid1Price"))]),
                                ("bid1_sz", vec![Cell::F(pf2("bid1Size"))]),
                                ("ask1_px", vec![Cell::F(pf2("ask1Price"))]),
                                ("ask1_sz", vec![Cell::F(pf2("ask1Size"))]),
                                ("basis_rate", vec![Cell::F(pf2("basisRate"))]),
                            ];
                            if let Err(e) = append_table(&path, &table) { eprintln!("[futures {sym}] {e:#}"); }
                        }
                    }
                }
            }
        }
        sleep(Duration::from_secs(FUTURES_POLL_SEC)).await;
    }
}

async fn ratio_loop(syms: Arc<Vec<String>>, client: Arc<Client>, tokens: Arc<Mutex<mpsc::Receiver<()>>>) {
    loop {
        for sym in syms.iter() {
            let url = format!("{API}/v5/market/account-ratio?category=linear&symbol={sym}&period=1h&limit=1");
            {
                let mut rx = tokens.lock().await;
                if rx.recv().await.is_none() { return; }
            }
            if let Ok(r) = client.get(&url).send().await {
                if let Ok(r) = r.error_for_status() {
                    if let Ok(v) = r.json::<Value>().await {
                        if let Some(row) = v["result"]["list"].as_array().and_then(|l| l.first()) {
                            let ts = Utc::now().timestamp_millis();
                            let buy = pf(row["buyRatio"].as_str());
                            let sell = pf(row["sellRatio"].as_str());
                            let path = Path::new(DATA_ROOT).join("ratio").join("linear").join(format!("{sym}.parquet"));
                            let table: Table = vec![
                                ("ts", vec![Cell::I(ts)]),
                                ("buy_ratio", vec![Cell::F(buy)]),
                                ("sell_ratio", vec![Cell::F(sell)]),
                            ];
                            if let Err(e) = append_table(&path, &table) { eprintln!("[ratio {sym}] {e:#}"); }
                        }
                    }
                }
            }
        }
        sleep(Duration::from_secs(RATIO_POLL_SEC)).await;
    }
}

// ---------- Trigger + Capture ----------

/// JSON-файл триггера от Python candle_trigger.py.
#[derive(Debug, Deserialize)]
struct TriggerFile {
    event_id: String,
    symbol: String,
    #[allow(dead_code)]
    category: String,
    #[allow(dead_code)]
    trigger_type: String,
    #[allow(dead_code)]
    trigger_version: String,
    #[allow(dead_code)]
    trigger_config_hash: String,
    #[allow(dead_code)]
    trigger_params: serde_json::Value,
    #[allow(dead_code)]
    horizons: Vec<u64>,
    capture_duration_sec: u64,
    #[allow(dead_code)]
    created_at: String,
}

/// Наблюдатель за директорией триггеров. Обнаруживает новые JSON-файлы.
async fn trigger_watcher(tx: mpsc::Sender<PathBuf>) {
    let dir = Path::new(TRIGGERS_DIR);
    let mut known: HashMap<String, bool> = HashMap::new();

    loop {
        if dir.exists() {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().map_or(false, |e| e == "json") {
                        let key = path.to_string_lossy().to_string();
                        if !known.contains_key(&key) {
                            known.insert(key, true);
                            // Читаем и парсим чтобы убедиться что это валидный триггер
                            if let Ok(contents) = fs::read_to_string(&path) {
                                if serde_json::from_str::<TriggerFile>(&contents).is_ok() {
                                    if tx.send(path).await.is_err() { return; }
                                }
                            }
                        }
                    }
                }
            }
        }
        known.retain(|k, _| Path::new(k).exists());
        sleep(Duration::from_secs(2)).await;
    }
}

/// Запуск WS для orderbook capture на один символ.
async fn ob_capture_once(
    symbol: &str,
    tx: &mpsc::Sender<(String, Table)>,
    deadline: &std::time::Instant,
) -> Result<()> {
    let url = "wss://stream.bybit.com/v5/public/linear";
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut sink, mut stream_ws) = ws.split();

    // Подписываемся на orderbook
    let topic = format!("orderbook.50.{symbol}");
    sink.send(Message::Text(json!({"op":"subscribe","args":[topic]}).to_string().into())).await?;
    eprintln!("[ob_capture] подписан на orderbook.50.{symbol}");

    loop {
        if std::time::Instant::now() >= *deadline {
            let _ = sink.send(Message::Text(json!({"op":"unsubscribe","args":[topic]}).to_string().into())).await;
            break;
        }
        tokio::select! {
            msg = stream_ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(t.as_str())?;
                    if v["op"].as_str() == Some("ping") {
                        sink.send(Message::Text(json!({"op":"pong"}).to_string().into())).await?;
                        continue;
                    }
                    if v.get("topic").is_none() { continue; }
                    if let Some((sym, rows)) = parse_message(Stream::Orderbook, &v) {
                        tx.send((format!("orderbook_captures/{sym}"), rows)).await?;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => break,
            },
            _ = sleep(Duration::from_millis(500)) => {}
        }
    }
    Ok(())
}

/// Обработка одного триггера: WS, сбор данных в буфер, сохранение parquet.
async fn ob_capture_task(trigger: TriggerFile) {
    let event_id = trigger.event_id.clone();
    let symbol = trigger.symbol.clone();
    let duration = Duration::from_secs(trigger.capture_duration_sec);

    eprintln!("[capture] старт: {event_id} для {symbol} ({}с)", duration.as_secs());

    let deadline = std::time::Instant::now() + duration;
    let mut backoff = 1u64;

    // Собираем данные в буфер через отдельный канал
    let (buf_tx, mut buf_rx) = mpsc::channel::<(String, Table)>(1024);

    // Запускаем WS в фоне
    let sym = symbol.clone();
    let ws_handle = tokio::spawn(async move {
        loop {
            match ob_capture_once(&sym, &buf_tx, &deadline).await {
                Ok(()) => break,
                Err(e) => {
                    if std::time::Instant::now() >= deadline { break; }
                    eprintln!("[ob_capture {sym}] {e:#}; retry через {backoff}с");
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(10);
                }
            }
        }
    });

    // Собираем данные в буфер пока не истечёт время
    let mut all_rows: Table = Table::from([
        ("ts", Col::new()), ("seq", Col::new()), ("is_delta", Col::new()), ("level", Col::new()),
        ("bid_px", Col::new()), ("bid_sz", Col::new()), ("ask_px", Col::new()), ("ask_sz", Col::new()),
    ]);

    loop {
        tokio::select! {
            maybe = buf_rx.recv() => match maybe {
                Some((_key, rows)) => {
                    for (i, (_, col)) in rows.into_iter().enumerate() {
                        if i < all_rows.len() {
                            all_rows[i].1.extend(col);
                        }
                    }
                }
                None => break,
            },
            _ = sleep(Duration::from_millis(500)) => {
                if std::time::Instant::now() >= deadline { break; }
            }
        }
    }

    // Ждём завершения WS
    let _ = ws_handle.await;

    // Сохраняем parquet
    let dir = Path::new(CAPTURES_DIR);
    if let Err(e) = fs::create_dir_all(dir) {
        eprintln!("[capture] ошибка создания директории: {e:#}");
        return;
    }

    let n = len(&all_rows);
    if n > 0 {
        let path = dir.join(format!("{event_id}.parquet"));
        if let Err(e) = write_parquet(&path, &all_rows) {
            eprintln!("[capture] ошибка записи parquet: {e:#}");
        } else {
            eprintln!("[capture] сохранено: {event_id} ({n} записей)");
        }

        // Метаданные
        let meta = json!({
            "event_id": event_id,
            "symbol": symbol,
            "capture_duration_sec": duration.as_secs(),
            "records": n,
            "status": "completed",
        });
        let meta_path = dir.join(format!("{event_id}.meta.json"));
        let _ = fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap_or_default());
    } else {
        eprintln!("[capture] нет данных для {event_id}");
        // Пишем метаданные с ошибкой
        let meta = json!({
            "event_id": event_id,
            "symbol": symbol,
            "capture_duration_sec": duration.as_secs(),
            "records": 0,
            "status": "no_data",
        });
        let meta_path = dir.join(format!("{event_id}.meta.json"));
        let _ = fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap_or_default());
    }

    // Удаляем JSON-файл триггера
    let trigger_path = Path::new(TRIGGERS_DIR).join(format!("{event_id}.json"));
    let _ = fs::remove_file(trigger_path);
    eprintln!("[capture] завершено: {event_id}");
}

/// Основной цикл обработки триггеров. Максимум max_concurrent одновременных захватов.
async fn capture_manager(
    mut trigger_rx: mpsc::Receiver<PathBuf>,
    _data_tx: mpsc::Sender<(String, Table)>,
    max_concurrent: usize,
) {
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    while let Some(path) = trigger_rx.recv().await {
        // Чистим завершённые задачи
        handles.retain(|h| !h.is_finished());

        // Если достигли лимита — ждём
        while handles.len() >= max_concurrent {
            sleep(Duration::from_secs(1)).await;
            handles.retain(|h| !h.is_finished());
        }

        // Читаем триггер
        let contents = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[capture_manager] ошибка чтения {path:?}: {e:#}");
                continue;
            }
        };
        let trigger: TriggerFile = match serde_json::from_str(&contents) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("[capture_manager] невалидный триггер {path:?}: {e:#}");
                let _ = fs::remove_file(&path);
                continue;
            }
        };

        let handle = tokio::spawn(async move {
            ob_capture_task(trigger).await;
        });
        handles.push(handle);
    }
}

// ---------- main ----------

/// Получение торгуемых linear USDT-перпетуалов из REST (для авто-универсума).
async fn fetch_linear_symbols(client: &Client, tokens: &Arc<Mutex<mpsc::Receiver<()>>>) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut cursor = String::new();
    loop {
        {
            let mut rx = tokens.lock().await;
            if rx.recv().await.is_none() { break; }
        }
        let mut url = format!("{API}/v5/market/instruments-info?category=linear&status=Trading&limit=1000");
        if !cursor.is_empty() { url.push_str(&format!("&cursor={cursor}")); }
        let resp = client.get(&url).send().await?.error_for_status()?.json::<Value>().await?;
        let list = resp["result"]["list"].as_array().cloned().unwrap_or_default();
        for it in &list {
            if let Some(sym) = it["symbol"].as_str() {
                if sym.ends_with("USDT") { out.push(sym.to_string()); }
            }
        }
        match resp["result"]["nextPageCursor"].as_str() {
            Some(c) if !c.is_empty() && list.len() >= 1000 => cursor = c.to_string(),
            _ => break,
        }
    }
    Ok(out)
}

fn discover_symbols() -> Result<Vec<String>> {
    let f = Path::new(DATA_ROOT).join("symbols/linear.txt");
    if f.exists() {
        let s = fs::read_to_string(&f)?;
        let syms: Vec<String> = s.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect();
        if !syms.is_empty() { return Ok(syms); }
    }
    let kdir = Path::new("data/klines/linear");
    let mut out = vec![];
    if let Ok(rd) = fs::read_dir(kdir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map_or(false, |x| x == "parquet") {
                if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                    if let Some(sym) = stem.strip_suffix("_linear_1m") { out.push(sym.to_string()); }
                }
            }
        }
    }
    Ok(out)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let (ptx, prx) = mpsc::channel(8);
    let tokens = Arc::new(Mutex::new(prx));
    tokio::spawn(async move {
        let mut it = tokio::time::interval(Duration::from_millis(PACE_MS));
        it.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop { it.tick().await; if ptx.send(()).await.is_err() { break; } }
    });
    let client = Arc::new(Client::builder().timeout(Duration::from_secs(20)).build()?);

    // Универсум символов: локальный файл > klines > REST-список.
    let mut syms = discover_symbols()?;
    if syms.is_empty() {
        println!("локальный универсум пуст, получаю список USDT-перпетуалов из REST...");
        let fetched = fetch_linear_symbols(&client, &tokens).await?;
        syms = fetched;
        if !syms.is_empty() {
            fs::create_dir_all(Path::new(DATA_ROOT).join("symbols"))?;
            fs::write(Path::new(DATA_ROOT).join("symbols/linear.txt"), syms.join("\n"))?;
        }
    }
    if syms.is_empty() {
        anyhow::bail!("нет символов: ни локального файла, ни REST-списка");
    }

    // Создаём директории
    for s in ["trades", "futures", "liquidation", "ratio"] {
        fs::create_dir_all(Path::new(DATA_ROOT).join(s).join("linear"))?;
    }
    fs::create_dir_all(Path::new(TRIGGERS_DIR))?;
    fs::create_dir_all(Path::new(CAPTURES_DIR))?;

    println!("marketdata: {} символов (linear), orderbook — event-driven capture (triggers/)", syms.len());
    let syms = Arc::new(syms);

    let (tx, rx) = mpsc::channel(16384);

    // Trades + Liquidation — постоянные подписки
    {
        let tx = tx.clone();
        let syms = syms.clone();
        tokio::spawn(async move { ws_loop(Stream::Trades, syms, tx).await });
    }
    {
        let tx = tx.clone();
        let syms = syms.clone();
        tokio::spawn(async move { ws_loop(Stream::Liquidation, syms, tx).await });
    }

    // Futures + Ratio — REST polling
    {
        let syms = syms.clone();
        let client = client.clone();
        let tokens = tokens.clone();
        tokio::spawn(async move { futures_loop(syms, client, tokens).await });
    }
    {
        let syms = syms.clone();
        let client = client.clone();
        let tokens = tokens.clone();
        tokio::spawn(async move { ratio_loop(syms, client, tokens).await });
    }

    // Orderbook — event-driven capture через trigger_watcher + capture_manager
    let max_concurrent: usize = std::env::var("OB_MAX_CONCURRENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    {
        let tx = tx.clone();
        let (trigger_tx, trigger_rx) = mpsc::channel(64);
        tokio::spawn(async move { trigger_watcher(trigger_tx).await });
        tokio::spawn(async move { capture_manager(trigger_rx, tx, max_concurrent).await });
    }

    println!("marketdata: запущен. Orderbook capture: data/triggers/ -> data/market/orderbook/captures/");
    println!("marketdata: max_concurrent captures = {max_concurrent}");

    drop(tx);
    flush_loop(rx).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn liquidation_strip_broadcast() {
        assert_eq!(Stream::Liquidation.strip("allLiquidation"), Some("broadcast"));
        assert_eq!(Stream::Liquidation.strip("allLiquidation.BTCUSDT"), Some("BTCUSDT"));
    }

    #[test]
    fn liquidation_parse_broadcast() {
        let msg = json!({
            "topic": "allLiquidation",
            "ts": 1234567890000u64,
            "data": [{"symbol": "BTCUSDT", "S": "Sell", "p": "50000", "v": "0.1"}]
        });
        let (sym, table) = parse_message(Stream::Liquidation, &msg).unwrap();
        assert_eq!(sym, "BTCUSDT");
        assert_eq!(table[0].1.len(), 1);
    }

    #[test]
    fn liquidation_parse_direct() {
        let msg = json!({
            "topic": "allLiquidation.ETHUSDT",
            "ts": 1234567890000u64,
            "data": [{"S": "Buy", "p": "3000", "v": "1.0"}]
        });
        let (sym, table) = parse_message(Stream::Liquidation, &msg).unwrap();
        assert_eq!(sym, "ETHUSDT");
    }

    #[test]
    fn trades_strip() {
        assert_eq!(Stream::Trades.strip("publicTrade.BTCUSDT"), Some("BTCUSDT"));
        assert_eq!(Stream::Trades.strip("something.else"), None);
    }

    #[test]
    fn orderbook_strip() {
        assert_eq!(Stream::Orderbook.strip("orderbook.50.ETHUSDT"), Some("ETHUSDT"));
    }

    #[test]
    fn trigger_file_parse() {
        let json_str = r#"{
            "event_id": "20260905T120000Z_BTCUSDT_abc123",
            "symbol": "BTCUSDT",
            "category": "linear",
            "trigger_type": "candle_features",
            "trigger_version": "1.0",
            "trigger_config_hash": "abc123def456",
            "trigger_params": {"relative_volume": 5.0},
            "horizons": [5, 10],
            "capture_duration_sec": 1200,
            "created_at": "2026-09-05T12:00:00Z"
        }"#;
        let trigger: TriggerFile = serde_json::from_str(json_str).unwrap();
        assert_eq!(trigger.event_id, "20260905T120000Z_BTCUSDT_abc123");
        assert_eq!(trigger.symbol, "BTCUSDT");
        assert_eq!(trigger.capture_duration_sec, 1200);
    }
}
