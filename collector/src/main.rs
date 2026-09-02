//! bybit_rs: дофилл 1m-свечек в существующие parquet-файлы + реалтайм-дозапись.
//!
//! Фаза 1 (дофилл): для каждого `data/klines/{linear,spot}/*_1m.parquet` читает последнюю
//! свечу и пагинационно докачивает REST-ом до текущего момента (лимит Bybit 600 req/min,
//! глобальный темп ~9 req/s). Фаза 2 (реалтайм): подписка WebSocket kline.1.{symbol},
//! буфер в памяти, сброс в файлы раз в 30 секунд. Перезапуск безопасен: курсор всегда
//! берётся из последней свечи файла, дубли схлопываются по open_time.

use std::collections::{HashMap, HashSet};
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
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tokio_tungstenite::tungstenite::Message;

const ROOT: &str = "data/klines";
const API: &str = "https://api.bybit.com";
/// Глобальный темп запросов к REST: 115 мс/запрос ≈ 8.7 req/s (лимит Bybit 600/мин).
const PACE_MS: u64 = 60;
/// Защита от бесконечного цикла (4000 страниц ≈ 7.6 лет 1m-свечей, покрывает любой листинг Bybit).
const MAX_PAGES: usize = 4000;
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

/// Дофилл одного символа в диапазоне [start, end].
///
/// Bybit kline при диапазоне шире limit=1000 возвращает **новейшие** 1000 свечей
/// (проверено живьём), поэтому курсор двигается НАЗАД: каждая страница = 1000 последних
/// минут до текущей верхней границы, граница опускается до самой старой свечи страницы.
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
        // retCode=10006 (rate limit) — повторяем страницу с паузой, а не теряем её
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
                if resp["retCode"].as_i64() == Some(10006) && attempts < 10 {
                    attempts += 1;
                    sleep(Duration::from_millis(2000 * attempts)).await;
                    continue;
                }
                break resp;
            }
        };
        if resp["retCode"].as_i64() != Some(0) {
            eprintln!("[{}] retCode={}: {}", st.symbol, resp["retCode"], resp["retMsg"]);
            break; // делистинг/невалидный символ — файл оставляем как есть
        }
        let list = resp["result"]["list"].as_array().cloned().unwrap_or_default(); // новые сначала
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
        if list.len() < 1000 {
            break; // дошли до начала истории
        }
        cursor = page_min - 60_000;
    }
    Ok(out)
}

/// Дыра > 3 минут считается пропуском данных.
const GAP_MIN: i64 = 3 * 60_000;
/// Ремонтируем файл, если в нём есть дыра >= 1 часа (меньше — естественные паузы неликвидных пар).
const GAP_TRIGGER: i64 = 60 * 60_000;

/// Ремонт одного файла: чинит голову (сломанный refresh — файл начинается позже листинга),
/// внутреннюю дыру >= 1 часа, отставший хвост. Чистые файлы пропускаются без запросов.
/// Дубли (уже имеющиеся свечи в перекачанном диапазоне) схлопнутся в append_rows.
async fn repair_one(
    st: &FileState,
    listed: Option<i64>,
    client: &Client,
    tokens: Arc<Mutex<mpsc::Receiver<()>>>,
) -> Result<Vec<Candle>> {
    let now = Utc::now().timestamp_millis();
    let candles = if st.path.exists() { read_parquet(&st.path)? } else { vec![] };
    if candles.is_empty() {
        // файла нет — качаем от листинга (или последних 2 дней, если листинг неизвестен)
        let start = listed.unwrap_or_else(|| now - 2 * 86_400_000);
        return backfill_range(st, start, now, client, tokens).await;
    }
    let first = candles[0].start;
    let last = candles[candles.len() - 1].start;
    // 1) голова: файл создан недавно (< 7 дней) — вероятно, сломанный refresh
    //    (тогда в файле только хвост 1000 свечей, а не история от листинга).
    //    Для linear листинг известен (launchTime); для spot — проба на 180 дней.
    if now - first < 7 * 86_400_000 {
        let start = match listed {
            Some(l) if first > l + 120_000 => l,
            Some(_) => return Ok(vec![]), // файл начинается с листинга — голова цела
            None => now - 180 * 86_400_000,
        };
        return backfill_range(st, start, now, client, tokens).await;
    }
    // 2) внутренняя дыра >= 1 часа: качаем от начала первой дыры до now,
    //    уже имеющиеся свечи схлопнутся в append_rows
    let max_gap = candles.windows(2).map(|w| w[1].start - w[0].start).max().unwrap_or(0);
    if max_gap >= GAP_TRIGGER {
        let gap_start = candles.windows(2)
            .find(|w| w[1].start - w[0].start > GAP_MIN)
            .map(|w| w[0].start + 60_000)
            .unwrap_or(first);
        return backfill_range(st, gap_start, now, client, tokens).await;
    }
    // 3) отстал хвост (сервис долго не работал) — докачиваем с последней свечи
    if last < now - 300_000 {
        return backfill_range(st, last + 60_000, now, client, tokens).await;
    }
    Ok(vec![]) // файл в порядке
}

// ------------------------- реалтайм -------------------------

/// Возвращает торгуемые USDT-пары категории (linear/spot): (символ, время листинга, мс).
async fn fetch_usdt_symbols(client: &Client, category: &str, tokens: &Arc<Mutex<mpsc::Receiver<()>>>) -> Result<Vec<(String, Option<i64>)>> {
    let mut out = Vec::new();
    let mut cursor = String::new();
    loop {
        {
            let mut rx = tokens.lock().await;
            if rx.recv().await.is_none() {
                break;
            }
        }
        let mut url = format!("{API}/v5/market/instruments-info?category={category}&status=Trading&limit=1000");
        if !cursor.is_empty() {
            url.push_str(&format!("&cursor={cursor}"));
        }
        let resp = client
            .get(&url)
            .send().await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let list = resp["result"]["list"].as_array().cloned().unwrap_or_default();
        for it in &list {
            let Some(sym) = it["symbol"].as_str() else { continue };
            if sym.ends_with("USDT") {
                // linear: launchTime; spot: поля листинга нет вовсе — вернётся None
                let listed = it["launchTime"].as_str()
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

/// Раз в сутки: обновляет список символов для дозаписи — добавляет новые USDT-пары
/// с биржи, дофиливает их от времени листинга и регистрирует в realtime-подписке.
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
                .read().await
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
                let st = FileState {
                    path: Path::new(ROOT).join(category).join(format!("{sym}_{category}_1m.parquet")),
                    category: category.to_string(),
                    symbol: sym.clone(),
                };
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
                        if let Err(e) = append_rows(&st.path, &rows) {
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

async fn ws_loop(category: &str, registry: Arc<RwLock<HashMap<String, Vec<String>>>>, tx: mpsc::Sender<(String, Vec<Candle>)>) {
    loop {
        let url = format!("wss://stream.bybit.com/v5/public/{category}");
        match ws_run(&url, category, &registry, &tx).await {
            Ok(_) => {}
            Err(e) => eprintln!("[ws {category}] {e:#}; переподключение через 3с"),
        }
        sleep(Duration::from_secs(3)).await;
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
    eprintln!("[ws] {url} подписан: {} символов", registry.read().await.get(category).map(|v| v.len()).unwrap_or(0));
    loop {
        let msg = tokio::select! {
            msg = stream.next() => match msg {
                Some(m) => m?,
                None => break,
            },
            _ = sleep(Duration::from_secs(60)) => {
                subscribe_all(&mut sink, registry, category).await?;
                continue;
            }
        };
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

/// Подписка на kline.1.* по всем символам категории из registry. Bybit идемпотентен:
/// повторная отправка subscribe — не ошибка, поэтому ресабскриб каждые 60с безопасен.
async fn subscribe_all<S>(sink: &mut S, registry: &Arc<RwLock<HashMap<String, Vec<String>>>>, category: &str) -> Result<()>
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
    // Ёмкость малая: продюсер не успевает накопить бэклог, иначе стартовый burst
    // 1377 задач разом выстреливает запросами и ловит 10006 (rate limit по UID).
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

    let client = Client::builder().timeout(Duration::from_secs(20)).build()?;
    let client = Arc::new(client);

    // Реестр символов для реалтайм-подписки: стартует с существующих файлов,
    // пополняется refresh_loop раз в сутки.
    let registry = Arc::new(RwLock::new(HashMap::from([
        ("linear".to_string(), states.iter().filter(|s| s.category == "linear").map(|s| s.symbol.clone()).collect()),
        ("spot".to_string(), states.iter().filter(|s| s.category == "spot").map(|s| s.symbol.clone()).collect()),
    ])));

    // Суточный refresh списка монет: новые USDT-пары дофиливаются от времени листинга.
    {
        let client = client.clone();
        let tokens = tokens.clone();
        let registry = registry.clone();
        tokio::spawn(async move { refresh_loop(client, tokens, registry).await });
    }

    // Фаза 1: ремонт (дофилл дыр/голов/хвостов).
    let total = states.len();
    println!("ремонт: {total} символов, темп ~{} req/s, ориентир: ~5 ч", 1000 / PACE_MS);
    // Времена листинга для починки головы у свежесозданных файлов (сломанный refresh).
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

    // Проход 1 — тяжёлый: дыры/головы/хвосты.
    // Проход 2 — лёгкий tail-sync: файлы, отремонтированные в начале прохода 1,
    // отстали от now на часы; повторный repair_one дочинит только хвост (1 запрос на файл).
    let mut worst = 0usize;
    for label in ["ремонт", "хвост-синк"] {
        let done = Arc::new(AtomicUsize::new(0));
        let mut tasks = JoinSet::new();
        // Одновременно активен лишь лимит repair_one: иначе 1377 задач крутятся в одной
        // токен-очереди по кругу (~83 с/токен на задачу) и ремонт растягивается на дни.
        let repair_sem = Arc::new(Semaphore::new(24));
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
                    if let Err(e) = append_rows(&st.path, &rows) {
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

    // Фаза 2: реалтайм.
    let (wtx, wrx) = mpsc::channel(8192);
    for cat in ["linear", "spot"] {
        let wtx = wtx.clone();
        let registry = registry.clone();
        tokio::spawn(async move { ws_loop(cat, registry, wtx).await });
    }
    drop(wtx);
    flush_loop(wrx).await;
    Ok(())
}