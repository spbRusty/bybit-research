//! marketdata: реалтайм-сбор расширенных рыночных потоков Bybit (linear, USDT-перпетуалы).
//!
//! Собирает parquet-файлы для групп признаков ТЗ §12-§15:
//!   - trades     : publicTrade.{sym}  -> data/market/trades/linear/{sym}.parquet  (§13 order flow)
//!   - orderbook  : orderbook.50.{sym} -> data/market/orderbook/linear/{sym}.parquet (§12 стакан, top-N)
//!   - futures    : tickers.{sym}      -> data/market/futures/linear/{sym}.parquet  (§14 funding/OI, §15 basis)
//!   - liquidation: allLiquidation.{sym} -> data/market/liquidation/linear/{sym}.parquet
//!   - ratio      : REST account-ratio (поллинг) -> data/market/ratio/linear/{sym}.parquet
//!
//! WS-соединения шардируются по лимитам Bybit (orderbook-глубина ~10/соединение,
//! обычные topic ~100/соединение); общий mpsc-буфер, сброс раз в FLUSH_SEC.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;
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
// Bybit WS caps: orderbook-глубина ~10 потоков/соединение; обычные topic ~200/соединение.
const OB_PER_CONN: usize = 10;
const TOPIC_PER_CONN: usize = 100;
const OB_UNIVERSE: usize = 50;

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
                backoff = (backoff * 2).min(30);
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
                None => break,
            },
            _ = sleep(Duration::from_secs(60)) => subscribe_all(&mut sink, stream, syms).await?,
        }
    }
    Ok(())
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

/// Топ-N символов по 24h-обороту (для orderbook-подмножества).
async fn fetch_top_symbols(client: &Client, tokens: &Arc<Mutex<mpsc::Receiver<()>>>, n: usize) -> Result<Vec<String>> {
    {
        let mut rx = tokens.lock().await;
        if rx.recv().await.is_none() { return Ok(vec![]); }
    }
    let url = format!("{API}/v5/market/tickers?category=linear");
    let resp = client.get(&url).send().await?.error_for_status()?.json::<Value>().await?;
    let mut ranked: Vec<(String, f64)> = vec![];
    for it in resp["result"]["list"].as_array().cloned().unwrap_or_default() {
        let sym = it["symbol"].as_str().unwrap_or("");
        if !sym.ends_with("USDT") { continue; }
        let to = it["turnover24h"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        ranked.push((sym.to_string(), to));
    }
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    Ok(ranked.into_iter().take(n).map(|(s, _)| s).collect())
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
    // Подмножество для orderbook.50: топ-N по 24h-обороту (лимит глубины Bybit ~10/соединение).
    let ob_syms = fetch_top_symbols(&client, &tokens, OB_UNIVERSE).await?;
    for s in ["trades", "orderbook", "futures", "liquidation", "ratio"] {
        fs::create_dir_all(Path::new(DATA_ROOT).join(s).join("linear"))?;
    }
    println!("marketdata: {} символов (linear), orderbook.50 на {} топ-символов", syms.len(), ob_syms.len());
    let syms = Arc::new(syms);
    let ob_syms = Arc::new(ob_syms);

    let (tx, rx) = mpsc::channel(16384);
    let spawn = |stream: Stream, list: Arc<Vec<String>>, tx: &mpsc::Sender<(String, Table)>| {
        if stream == Stream::Liquidation {
            let tx = tx.clone();
            tokio::spawn(async move { ws_loop(stream, list, tx).await });
            return;
        }
        let per = if stream == Stream::Orderbook { OB_PER_CONN } else { TOPIC_PER_CONN };
        for chunk in list.chunks(per) {
            let tx = tx.clone();
            let chunk: Vec<String> = chunk.to_vec();
            tokio::spawn(async move { ws_loop(stream, Arc::new(chunk), tx).await });
        }
    };
    spawn(Stream::Trades, syms.clone(), &tx);
    spawn(Stream::Orderbook, ob_syms.clone(), &tx);
    spawn(Stream::Liquidation, syms.clone(), &tx);
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
}
