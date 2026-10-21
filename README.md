# bybit-research

Монорепозиторий: Rust-коллекторы Bybit + Python-исследование + dashboard.

## Структура

```
collector/            Rust (один пакет bybit_collector, несколько bin)
  src/bin/bybit_klines.rs     1m-свечи: REST backfill + WS, ZSTD-партиции
  src/bin/marketdata.rs       trades / futures / liquidation / ratio
  src/bin/ob_reconstructor.rs orderbook.50 -> reconstructed snapshots
  data/klines/                {linear,spot}/{SYM}/{YYYY-MM}.parquet
  data/market/                {kind}/linear/{SYM}/{YYYY-MM-DD}.parquet + orderbook
src/                  Python: research pipeline, screening, paper, dashboard
scripts/              build_l2_events, l2_paper, миграции
ops/systemd/          system/ и user/ юниты
data/                 исследовательские артефакты (events, results, paper)
```

## Сборка и тесты

```bash
cd collector && ~/.cargo/bin/cargo build --release
~/.cargo/bin/cargo test --release
.venv/bin/python -m pytest -q
```

## Сервисы

| Юнит | Тип | Роль |
|---|---|---|
| `bybit_klines.service` | system | 1m-свечи (`bybit_klines`) |
| `bybit_marketdata.service` | user | trades/futures/liquidation/ratio |
| `bybit_ob_reconstructor.service` | user | реконструкция стакана (`--no-raw`) |
| `dashboard.service` | user | FastAPI dashboard |
| `l2_search.timer` | user | сборка L2-событий, скрин, paper |

Данные klines и рыночных потоков лежат в `collector/data/`; research читает их через `config/settings.py`.
