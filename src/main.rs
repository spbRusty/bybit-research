//! bybit_rs: дофилл 1m-свечей + реалтайм-дозапись.
//!
//! Фаза 1 (дофилл): для каждого символа читает последнюю свечу и пагинационно
//! докачивает REST-ом до текущего момента (лимит Bybit 600 req/min, глобальный
//! темп ~16 req/s). Фаза 2 (реалтайм): подписка WebSocket kline.1.{symbol},
//! буфер в памяти, сброс в партиции раз в 10 минут.
//!
//! Хранение: `data/klines/{linear,spot}/{SYMBOL}/{YYYY-MM}.parquet`.
//! Закрытые месяцы неизменяемы; при flush переписывается только текущий месяц.
//! ZSTD-сжатие, поэтому 11 ГБ исходных данных не переписываются целиком каждые
//! 10 минут. Перезапуск безопасен: курсор берётся из последней свечи, дубли
//! схлопываются по open_time.
//!
//! L2 orderbook здесь сознательно не собирается: его с большим качеством
//! (seq/gap, 5-сек снапшоты) собирает построение/ob_reconstructor. Дублирующая
//! подписка orderbook.50 на 1442 символа давала ~60% входящего трафика.

use std::collections::{BTreeMap, HashMap, HashSet};
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
use futures_util::{Sink, SinkExt, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tokio_tungstenite::tungstenite::Message;

const ROOT: &str = "data/klines";
const API: &str = "https://api.bybit.com";
/// Глобальный темп REST: 60 мс/запрос ≈ 16.7 req/s (лимит Bybit 600/5с на IP).
const PACE_MS: u64 = 60;
/// Защита от бесконечного цикла (4000 страниц ≈ 7.6 лет 1m-свечей).
const MAX_PAGES: usize = 4000;
/// Сброс реалтайм-буфера, сек. Переписывается только текущий месяц каждого
/// символа (не весь файл истории), поэтому 10 минут — безопасный компромисс.
const FLUSH_SEC: u64 = 600;
/// Одновременных repair_one: ограничивает пик RAM при полном чтении истории.
const REPAIR_CONCURRENCY: usize = 4;
/// Ping каждые 20 с (рекомендация Bybit); ресабскрайб раз в 60 с убран.
const PING_SEC: u64 = 20;

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
    category: String,
    symbol: String,
}

fn symbol_dir(category: &str, symbol: &str) -> PathBuf {
    Path::new(ROOT).join(category).join(symbol)
}

fn month_key(ts_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ts_ms)
        .unwrap_or_default()
        .format("%Y-%m")
        .to_string()
}

fn part_path(category: &str, symbol: &str, ts_ms: i64) -> PathBuf {
    symbol_dir(category, symbol).join(format!("{}.parquet", month_key(ts_ms)))
}

/// Все партиции символа, отсортированные по имени месяца.
fn symbol_parts(category: &str, symbol: &str) -> Result<Vec<PathBuf>> {
    let dir = symbol_dir(category, symbol);
    if !dir.exists() {
        return Ok(vec![]);
    }
    let mut out = vec![];
    for entry in fs::read_dir(&dir).with_context(|| format!("read_dir {dir:?}"))? {
        let p = entry?.path();
        if p.extension().map_or(false, |e| e == "parquet")
            && !p.to_string_lossy().ends_with(".tmp")
        {
            out.push(p);
        }
    }
    out.sort();
    Ok(out)
}

fn discover() -> Result<Vec<FileState>> {
    let mut out = vec![];
    for cat in ["linear", "spot"] {
        let dir = Path::new(ROOT).join(cat);
        let entries = match fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format!("read_dir {dir:?}")),
        };
        for entry in entries {
            let p = entry?.path();
            if p.is_dir() {
                let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
                if !name.is_empty() {
                    out.push(FileState { category: cat.to_string(), symbol: name });
                }
            }
        }
    }
    Ok(out)
}

/// Читает свечи из паркета. Столбцы: open_time, open, high, low, close, volume, turnover, is_green.
fn read_parquet(path: &Path) -> Result<Vec<Candle>> {
    if !path.exists() {
        return Ok(vec![]);
    }
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

/// Вся история символа из всех месячных партиций.
#[allow(dead_code)]
fn read_symbol(category: &str, symbol: &str) -> Result<Vec<Candle>> {
    let mut out = vec![];
    for p in symbol_parts(category, symbol)? {
        out.extend(read_parquet(&p)?);
    }
    out.sort_unstable_by_key(|c| c.start);
    out.dedup_by_key(|c| c.start);
    Ok(out)
}

/// Последняя свеча без чтения всей истории (только последняя месячная партиция).
fn read_last(category: &str, symbol: &str) -> Result<Option<Candle>> {
    let parts = symbol_parts(category, symbol)?;
    for p in parts.iter().rev() {
        let rows = read_parquet(p)?;
        if let Some(c) = rows.last() {
            return Ok(Some(c.clone()));
        }
    }
    Ok(None)
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

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build()
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
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let file = File::create(&tmp).with_context(|| format!("create {tmp:?}"))?;
    let mut writer = ArrowWriter::try_new(file, schema(), Some(writer_props()))?;
    writer.write(&batch)?;
    writer.close()?;
    fs::rename(&tmp, path).with_context(|| format!("rename {tmp:?} -> {path:?}"))?;
    Ok(())
}

/// Слияние старых + новых свечей внутри одной месячной партиции.
fn append_rows(path: &Path, new: &[Candle]) -> Result<()> {
    if new.is_empty() {
        return Ok(());
    }
    let old = read_parquet(path)?;
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

/// Раскладывает новые свечи по месячным партициям и дописывает каждую.
fn append_candles(category: &str, symbol: &str, new: &[Candle]) -> Result<()> {
    if new.is_empty() {
        return Ok(());
    }
    let mut by_month: BTreeMap<String, Vec<Candle>> = BTreeMap::new();
    for c in new {
        by_month.entry(month_key(c.start)).or_default().push(c.clone());
    }
    for (month, rows) in by_month {
        let path = part_path(category, symbol, rows[0].start);
        append_rows(&path, &rows)?;
    }
    Ok(())
}

fn f(a: &[Value], i: usize) -> Result<f64> {
    Ok(a.get(i).and_then(|v| v.as_str()).unwrap_or("0").parse()?)
}

/// Дофилл одного символа в диапазоне [start, end].
///
/// Bybit kline при диапазоне шире limit=1000 возвращает **новейшие** 1000 свечей,
/// поэтому курсор двигается НАЗАД: каждая страница = 1000 последних минут до
/// текущей верхней границы, граница опускается до самой старой свечи страницы.
async fn backfill_range(
    st: &FileState,
    start: i64,
    end: i64,
    client: &Client,
    tokens: Arc<Mutex<mpsc::Receiver<()>>>,
) -> Result<Vec<Candle>> {
    let mut cursor = end;
    let mut out: Vec<Candle> = Vec::new();
    for _ in 0..MAX_PAGES {
        if cursor <= start {
            break;
        }
        {
            let mut rx = tokens.lock().await;
            if rx.recv().await.is_none() {
                break;
            }
        }
        let url = format!(
            "{API}/v5/market/kline?category={}&symbol={}&interval=1&start={start}&end={cursor}&limit=1000",
            st.category, st.symbol
        );
        let resp = {
            let mut attempts = 0;
            loop {
                let resp = client
                    .get(&url)
                    .send()
                    .await
                    .with_context(|| format!("GET {url}"))?
                    .error_for_status()?
                    .json::<Value>()
                    .await?;
                let rc = resp["retCode"].as_i64().unwrap_or(-1);
                if matches!(rc, 10006 | 10016) && attempts < 10 {
                    attempts += 1;
                    sleep(Duration::from_millis(2000 * attempts)).await;
                    continue;
                }
                break resp;
            }
        };
        if resp["retCode"].as_i64() != Some(0) {
            let rc = resp["retCode"].as_i64();
            eprintln!("[{}] retCode={}: {}", st.symbol, resp["retCode"], resp["retMsg"]);
            if rc == Some(10001) {
                break; // Not supported symbols — не ретраим
            }
            anyhow::bail!("retCode {:?}: {}", rc, resp["retMsg"]);
        }
        let list = resp["result"]["list"].as_array().cloned().unwrap_or_default();
        if list.is_empty() {
            break;
        }
        let mut page_min = i64::MAX;
        for row in list.iter().rev() {
            let a = row.as_array().with_context(|| "row not array")?;
            let s: i64 = a.first().and_then(|v| v.as_str()).unwrap_or("0").parse()?;
            page_min = page_min.min(s);
            if s < start {
                continue;
            }
            out.push(Candle {
                start: s,
                open: f(a, 1)?,
                high: f(a, 2)?,
                low: f(a, 3)?,
                close: f(a, 4)?,
                volume: f(a, 5)?,
                turnover: f(a, 6)?,
            });
        }
        if out.len() % 50_000 < 1000 {
            eprintln!("[backfill] {} rows={} cursor={}", st.symbol, out.len(), cursor);
        }
        if list.len() < 1000 {
            break;
        }
        cursor = page_min - 60_000;
    }
    Ok(out)
}

/// Ремонт одного символа: чинит голову, внутреннюю дыру >= 1 часа, отставший хвост.
async fn repair_one(
    st: &FileState,
    listed: Option<i64>,
    client: &Client,
    tokens: Arc<Mutex<mpsc::Receiver<()>>>,
) -> Result<Vec<Candle>> {
    let now = Utc::now().timestamp_millis();
    // Startup repair is intentionally tail-only: reading full history for 1442
    // symbols cost GBs of RAM and minutes of I/O. Head/gap audit is a separate
    // offline operation; here we only close the downtime gap.
    let Some(last) = read_last(&st.category, &st.symbol)? else {
        let start = listed.unwrap_or_else(|| now - 2 * 86_400_000);
        return backfill_range(st, start, now, client, tokens).await;
    };
    if last.start < now - 300_000 {
        return backfill_range(st, last.start + 60_000, now, client, tokens).await;
    }
    Ok(vec![])
}

// ------------------------- реалтайм -------------------------

async fn fetch_usdt_symbols(
    client: &Client,
    category: &str,
    tokens: &Arc<Mutex<mpsc::Receiver<()>>>,
) -> Result<Vec<(String, Option<i64>)>> {
    let mut out = Vec::new();
    let mut cursor = String::new();
    loop {
        {
            let mut rx = tokens.lock().await;
            if rx.recv().await.is_none() {
                break;
            }
        }
        let mut url = format!(
            "{API}/v5/market/instruments-info?category={category}&status=Trading&limit=1000"
        );
        if !cursor.is_empty() {
            url.push_str(&format!("&cursor={cursor}"));
        }
        let resp = client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let list = resp["result"]["list"].as_array().cloned().unwrap_or_default();
        for it in &list {
            let Some(sym) = it["symbol"].as_str() else { continue };
            if sym.ends_with("USDT") {
                let listed = it["launchTime"]
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| it["listedTime"].as_str().and_then(|s| s.parse::<i64>().ok()));
                out.push((sym.to_string(), listed));
            }
        }
        match resp["result"]["nextPageCursor"].as_str() {
            Some(c) if !c.is_empty() && list.len() >= 1000 => cursor = c.to_string(),
            _ => break,
        }
    }
    Ok(out)
}

async fn refresh_loop(
    client: Arc<Client>,
    tokens: Arc<Mutex<mpsc::Receiver<()>>>,
    registry: Arc<RwLock<HashMap<String, Vec<String>>>>,
) {
    loop {
        for category in ["linear", "spot"] {
            let syms = match fetch_usdt_symbols(&client, category, &tokens).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[refresh {category}] список не получен: {e:#}");
                    continue;
                }
            };
            let known: HashSet<String> = registry
                .read()
                .await
                .get(category)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
            let total = syms.len();
            let new: Vec<(String, Option<i64>)> = syms
                .into_iter()
                .filter(|(s, _)| !known.contains(s))
                .collect();
            if new.is_empty() {
                println!("[refresh {category}] новых пар нет (из {total} USDT на бирже)");
                continue;
            }
            let mut added = Vec::new();
            let mut tasks = JoinSet::new();
            for (sym, listed) in &new {
                let st = FileState { category: category.to_string(), symbol: sym.clone() };
                let tokens = tokens.clone();
                let client = client.clone();
                let listed = *listed;
                tasks.spawn(async move {
                    let start = listed.unwrap_or_else(|| Utc::now().timestamp_millis() - 2 * 86_400_000);
                    let res = backfill_range(&st, start, Utc::now().timestamp_millis(), &client, tokens).await;
                    (st, res)
                });
            }
            while let Some(joined) = tasks.join_next().await {
                let Ok((st, res)) = joined else {
                    eprintln!("[refresh {category}] задача join упала");
                    continue;
                };
                match res {
                    Ok(rows) => {
                        if let Err(e) = append_candles(&st.category, &st.symbol, &rows) {
                            eprintln!("[refresh {}] запись: {e:#}", st.symbol);
                        } else if !rows.is_empty() {
                            println!("[refresh {}] новая пара, +{} свечек ({})", st.symbol, rows.len(), st.category);
                            added.push(st.symbol.clone());
                        }
                    }
                    Err(e) => eprintln!("[refresh {}] дофилл не удался: {e:#}", st.symbol),
                }
            }
            if !added.is_empty() {
                let n = added.len();
                registry.write().await.get_mut(category).expect("cat").extend(added);
                println!("[refresh {category}] новых пар добавлено: {n} (из {total} USDT на бирже)");
            } else {
                println!("[refresh {category}] новых пар добавлено: 0 (из {total} USDT на бирже)");
            }
        }
        sleep(Duration::from_secs(86_400)).await;
    }
}

async fn ws_loop(
    category: &str,
    registry: Arc<RwLock<HashMap<String, Vec<String>>>>,
    tx: mpsc::Sender<(String, Vec<Candle>)>,
) {
    let mut backoff = 1u64;
    loop {
        let url = format!("wss://stream.bybit.com/v5/public/{category}");
        match ws_run(&url, category, &registry, &tx).await {
            Ok(_) => backoff = 1,
            Err(e) => {
                eprintln!("[ws {category}] {e:#}; переподключение через {backoff}с");
                sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(60);
            }
        }
    }
}

async fn ws_run(
    url: &str,
    category: &str,
    registry: &Arc<RwLock<HashMap<String, Vec<String>>>>,
    tx: &mpsc::Sender<(String, Vec<Candle>)>,
) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut sink, mut stream) = ws.split();
    subscribe_all(&mut sink, registry, category).await?;
    eprintln!(
        "[ws] {url} подписан: {} символов",
        registry.read().await.get(category).map(|v| v.len()).unwrap_or(0)
    );
    let mut ping = tokio::time::interval(Duration::from_secs(PING_SEC));
    ping.tick().await;
    loop {
        let msg = tokio::select! {
            msg = stream.next() => match msg {
                Some(m) => m?,
                None => break,
            },
            _ = ping.tick() => {
                sink.send(Message::Text(json!({"op": "ping"}).to_string().into())).await?;
                continue;
            }
        };
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(t.as_str())?;
            match v["op"].as_str() {
                Some("ping") => {
                    sink.send(Message::Text(json!({"op": "pong"}).to_string().into())).await?;
                    continue;
                }
                Some("pong") => continue,
                _ => {}
            }
            let topic = v["topic"].as_str().unwrap_or("");
            if let Some(sym) = topic.strip_prefix("kline.1.") {
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
    }
    Ok(())
}

/// Подписка на kline.1.* по всем символам категории из registry.
/// Повторный subscribe не нужен: подписка живёт на соединении; Bybit
/// рекомендует только heartbeat (ping) каждые 20 с.
async fn subscribe_all<S>(
    sink: &mut S,
    registry: &Arc<RwLock<HashMap<String, Vec<String>>>>,
    category: &str,
) -> Result<()>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let syms = registry.read().await.get(category).cloned().unwrap_or_default();
    for chunk in syms.chunks(10) {
        let args: Vec<String> = chunk.iter().map(|s| format!("kline.1.{s}")).collect();
        sink.send(Message::Text(json!({"op": "subscribe", "args": args}).to_string().into())).await?;
    }
    Ok(())
}

async fn send_candles(
    tx: &mpsc::Sender<(String, Vec<Candle>)>,
    url: &str,
    sym: &str,
    candles: Vec<Candle>,
) -> Result<()> {
    let cat = if url.contains("/public/spot") { "spot" } else { "linear" };
    let key = format!("{cat}/{sym}");
    tx.send((key, candles)).await.context("ws tx закрыт")
}

/// Сброс буфера реалтайма: только добавленные строки в месячные партиции.
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
            if let Err(e) = append_candles(cat, sym, &rows) {
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
    println!("найдено символов: {linear} linear + {spot} spot");

    // Глобальный дроссель REST: канал, питаемый интервалом PACE_MS.
    let (ptx, prx) = mpsc::channel(8);
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

    let client = Arc::new(Client::builder().timeout(Duration::from_secs(20)).build()?);

    let registry = Arc::new(RwLock::new(HashMap::from([
        (
            "linear".to_string(),
            states.iter().filter(|s| s.category == "linear").map(|s| s.symbol.clone()).collect(),
        ),
        (
            "spot".to_string(),
            states.iter().filter(|s| s.category == "spot").map(|s| s.symbol.clone()).collect(),
        ),
    ])));

    {
        let client = client.clone();
        let tokens = tokens.clone();
        let registry = registry.clone();
        tokio::spawn(async move { refresh_loop(client, tokens, registry).await });
    }

    // Фаза 1: ремонт (дофилл дыр/голов/хвостов).
    let total = states.len();
    println!("ремонт: {total} символов, темп ~{} req/s, ориентир: ~5 ч", 1000 / PACE_MS);
    let mut listed_map: HashMap<String, i64> = HashMap::new();
    for cat in ["linear", "spot"] {
        if let Ok(syms) = fetch_usdt_symbols(&client, cat, &tokens).await {
            for (s, l) in syms {
                if let Some(l) = l {
                    listed_map.insert(s, l);
                }
            }
        }
    }
    println!("времена листинга получены: {}", listed_map.len());

    let mut worst = 0usize;
    for label in ["ремонт", "хвост-синк"] {
        let done = Arc::new(AtomicUsize::new(0));
        let mut tasks = JoinSet::new();
        let repair_sem = Arc::new(Semaphore::new(REPAIR_CONCURRENCY));
        for st in states.clone() {
            let tokens = tokens.clone();
            let client = client.clone();
            let done = done.clone();
            let sem = repair_sem.clone();
            let listed = listed_map.get(&st.symbol).copied();
            tasks.spawn(async move {
                if sem.acquire_owned().await.is_err() {
                    return (st, Err(anyhow::anyhow!("repair semaphore closed")), done);
                }
                let res = repair_one(&st, listed, &client, tokens).await;
                (st, res, done)
            });
        }
        let mut failed = 0usize;
        while let Some(joined) = tasks.join_next().await {
            let (st, res, done) = joined?;
            match res {
                Ok(rows) => {
                    if let Err(e) = append_candles(&st.category, &st.symbol, &rows) {
                        eprintln!("[{}] запись: {e:#}", st.symbol);
                        failed += 1;
                    } else if !rows.is_empty() {
                        let d = done.fetch_add(1, Ordering::SeqCst) + 1;
                        if d % 50 == 0 || d == 1 {
                            println!("{label}: {d}/{total}...");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[{}] {label} не удался: {e:#}", st.symbol);
                    failed += 1;
                }
            }
        }
        worst = worst.max(failed);
        println!("{label} завершён. неудач: {failed}.");
    }
    println!("ремонт завершён (худший проход: {worst} неудач). Переключаюсь в реалтайм.");

    // Фаза 2: реалтайм. Канал покрывает пик обновлений за цикл flush.
    let (wtx, wrx) = mpsc::channel(1_000_000);
    for cat in ["linear", "spot"] {
        let wtx = wtx.clone();
        let registry = registry.clone();
        tokio::spawn(async move { ws_loop(cat, registry, wtx).await });
    }
    drop(wtx);
    flush_loop(wrx).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_key_boundaries() {
        assert_eq!(month_key(0), "1970-01");
        assert_eq!(month_key(1_790_000_000_000), "2026-09");
        assert_eq!(month_key(1_792_500_000_000), "2026-10");
    }

    #[test]
    fn month_path_is_partitioned_by_symbol() {
        let p = part_path("linear", "BTCUSDT", 1_792_500_000_000);
        assert!(p.ends_with("data/klines/linear/BTCUSDT/2026-10.parquet"));
    }
}
