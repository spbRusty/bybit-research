# marketdata — реалтайм-сбор расширенных рыночных потоков Bybit

Rust-коллектор (см. `src/bin/marketdata.rs`) для групп признаков ТЗ §12–§15.
Пишет parquet в `data/market/{trades,orderbook,futures,liquidation,ratio}/linear/{sym}.parquet`.

## Сборка

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cd collector
cargo build --release --bin marketdata
```

## systemd-сервис (user, без root)

Устанавливается как **user**-юнит (linger уже включён для автостарта при загрузке):

```bash
# установка (один раз):
cp collector/bybit_marketdata.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable bybit_marketdata.service
systemctl --user start bybit_marketdata.service
```

Управление:

```bash
systemctl --user status bybit_marketdata   # статус
systemctl --user restart bybit_marketdata # перезапуск
systemctl --user stop bybit_marketdata    # остановка
journalctl --user -u bybit_marketdata -f  # live-лог
tail -f collector/logs/marketdata.log     # лог коллектора
```

Юнит: `WorkingDirectory=collector/`, `Restart=on-failure`, лог в `collector/logs/marketdata.log`.

## Универсум символов

- `data/market/symbols/linear.txt` — если есть, используется как есть (полный список USDT-перпетуалов).
- Иначе fallback: klines → REST (`instruments-info`), пишет список в файл.
- **orderbook.50** — только top-50 по 24h-обороту (`fetch_top_symbols`), из-за лимита Bybit ~10 потоков-глубины/соединение.
- WS-соединения шардируются: orderbook ~10/соединение, обычные topic ~100/соединение.

## Потоки

| Поток | Источник | Схема | Группа ТЗ |
|---|---|---|---|
| trades | WS `publicTrade.{sym}` | ts, seq, is_buy, price, size | §13 order flow |
| orderbook | WS `orderbook.50.{sym}` | ts, seq, is_delta, level, bid_px, bid_sz, ask_px, ask_sz | §12 стакан (top-50) |
| futures | REST tickers (60с) | ts, funding_rate, next_funding_time, oi, oi_value, last_px, mark_px, index_px, bid1_px, bid1_sz, ask1_px, ask1_sz, basis_rate | §14 funding/OI, §15 basis |
| liquidation | WS `allLiquidation.{sym}` | ts, is_sell, price, size | ликвидации |
| ratio | REST account-ratio (300с) | ts, buy_ratio, sell_ratio | long/short ratio |

Замечание: `basis_rate` из tickers для перпетуалов обычно пуст — спот/фьючерсный спред (basis) считается отдельно на Python-стороне по ценам спота+фьючерса.
