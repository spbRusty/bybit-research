"""FastAPI dashboard server. Read-only API over existing pipeline artifacts.

Usage: python -m src.dashboard.server
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from src.dashboard import collectors

app = FastAPI(title="Bybit Research Dashboard", version="0.2.0")

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/api/paper")
def api_paper():
    return JSONResponse(collectors.get_paper())


@app.get("/api/risk")
def api_risk():
    return JSONResponse(collectors.get_risk_params())


@app.get("/api/costs")
def api_costs():
    return JSONResponse(collectors.get_trading_costs())


@app.get("/api/instruments")
def api_instruments():
    return JSONResponse(collectors.get_instrument_info())


@app.get("/api/stakes")
def api_stakes():
    return JSONResponse(collectors.get_stake_levels())


@app.get("/api/winrate")
def api_winrate():
    return JSONResponse(collectors.get_winrate_by_stake())


@app.get("/api/pipeline")
def api_pipeline():
    return JSONResponse(collectors.get_pipeline_status())


@app.get("/api/hypotheses")
def api_hypotheses():
    return JSONResponse(collectors.get_hypotheses())


@app.get("/api/data")
def api_data():
    return JSONResponse(collectors.get_data_status())


@app.get("/api/market-data")
def api_market_data():
    return JSONResponse(collectors.get_market_data())


@app.get("/api/market")
def api_market():
    return JSONResponse(collectors.get_market_metrics())


@app.get("/api/signals")
def api_signals():
    return JSONResponse(collectors.get_signals())


@app.get("/api/captures")
def api_captures():
    return JSONResponse(collectors.get_captures())


@app.get("/api/status")
def api_status():
    return JSONResponse(collectors.get_system_status())


@app.get("/api/logs")
def api_logs(n: int = Query(default=30, ge=1, le=200)):
    return JSONResponse(collectors.get_logs(n=n))


@app.get("/api/ob-collector")
def api_ob_collector():
    return JSONResponse(collectors.get_ob_collector())


@app.get("/api/candle-collector")
def api_candle_collector():
    return JSONResponse(collectors.get_candle_collector())


@app.get("/api/data-quality")
def api_data_quality():
    return JSONResponse(collectors.get_data_quality())


@app.get("/api/research-conclusion")
def api_research_conclusion():
    return JSONResponse(collectors.get_research_conclusion())


@app.get("/api/research-gate")
def api_research_gate():
    return JSONResponse(collectors.get_research_gate())


@app.get("/api/alerts")
def api_alerts():
    return JSONResponse(collectors.get_alerts())


@app.get("/api/shadow-paper")
def api_shadow_paper():
    return JSONResponse(collectors.get_shadow_paper())


@app.get("/api/stream")
async def api_stream():
    from config.settings import LOGS_DIR, ROOT
    async def event_gen():
        log_path = LOGS_DIR / "orchestrator.log"
        collector_log = ROOT / "collector" / "logs" / "marketdata.log"
        last_pos = 0
        last_collector_pos = 0
        if log_path.exists():
            last_pos = log_path.stat().st_size
        if collector_log.exists():
            last_collector_pos = collector_log.stat().st_size
        while True:
            try:
                if log_path.exists():
                    size = log_path.stat().st_size
                    if size > last_pos:
                        with open(log_path, "r") as f:
                            f.seek(last_pos)
                            new = f.read()
                        last_pos = size
                        for line in new.strip().splitlines():
                            yield f"data: {json.dumps({'source': 'orchestrator', 'line': line})}\n\n"
                if collector_log.exists():
                    size = collector_log.stat().st_size
                    if size > last_collector_pos:
                        with open(collector_log, "r") as f:
                            f.seek(last_collector_pos)
                            new = f.read()
                        last_collector_pos = size
                        for line in new.strip().splitlines():
                            yield f"data: {json.dumps({'source': 'collector', 'line': line})}\n\n"
            except Exception:
                pass
            await asyncio.sleep(2)
            yield ": keepalive\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream")


def main():
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8420, log_level="info")


if __name__ == "__main__":
    main()
