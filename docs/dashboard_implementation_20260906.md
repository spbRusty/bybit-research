# Dashboard Implementation — 2026-09-06

Extended existing research dashboard with collector monitoring, data quality, research conclusion, and alerts.

## New API Endpoints

| Endpoint | Source | Returns |
|---|---|---|
| `GET /api/ob-collector` | `_metrics.jsonl` | Status, per-symbol update/row/error counts, disk free |
| `GET /api/candle-collector` | `data/raw/{linear,spot}/*.parquet` | File count, size, newest timestamp, lag, status |
| `GET /api/data-quality` | OB metrics + klines | ok/stale/gap/missing counts per source |
| `GET /api/research-conclusion` | `results/acceptance_*.json` | Verdict, finalist, reject reasons, hypothesis counts |
| `GET /api/alerts` | All sources | Severity-tagged alerts from collector/quality/pipeline/disk |

## Files Changed

- `src/dashboard/collectors.py` — 5 new collector functions (OB, candle, quality, conclusion, alerts)
- `src/dashboard/server.py` — 5 new FastAPI endpoints
- `src/dashboard/static/index.html` — alerts bar, OB panel, candle panel, quality panel, research conclusion panel

## Startup

```bash
python -m src.dashboard.server
# → http://127.0.0.1:8420
```

## Behavior

- Auto-refresh every 3s (all 17 data endpoints polled in parallel)
- SSE live log stream at `/api/stream`
- Status dot: green (OK) or red (stale)
- Alerts bar appears only when active alerts exist; color = severity (red=critical/error, yellow=warning)
