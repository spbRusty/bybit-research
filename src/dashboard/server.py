"""FastAPI dashboard server. Read-only API over existing pipeline artifacts.

Usage: python -m src.dashboard.server
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from src.dashboard import collectors

app = FastAPI(title="Bybit Research Dashboard", version="0.1.0")

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/api/status")
async def api_status():
    return JSONResponse(collectors.get_system_status())


@app.get("/api/data")
async def api_data():
    return JSONResponse(collectors.get_data_status())


@app.get("/api/market")
async def api_market():
    return JSONResponse(collectors.get_market_metrics())


@app.get("/api/signals")
async def api_signals():
    return JSONResponse(collectors.get_signals())


@app.get("/api/hypotheses")
async def api_hypotheses():
    return JSONResponse(collectors.get_hypotheses())


@app.get("/api/paper")
async def api_paper():
    return JSONResponse(collectors.get_paper_trading())


@app.get("/api/logs")
async def api_logs(n: int = Query(default=50, ge=1, le=500)):
    return JSONResponse(collectors.get_logs(n=n))


@app.get("/api/acceptance")
async def api_acceptance():
    """Latest acceptance report (convenience endpoint)."""
    hyp = collectors.get_hypotheses()
    return JSONResponse(hyp.get("acceptance") or {})


@app.get("/api/stream")
async def api_stream():
    """SSE: tail orchestrator.log every 2s."""
    async def event_gen():
        log_path = collectors.LOGS_DIR / "orchestrator.log"
        last_pos = 0
        if log_path.exists():
            last_pos = log_path.stat().st_size
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
                            yield f"data: {json.dumps({'line': line})}\n\n"
            except Exception:
                pass
            await asyncio.sleep(2)
            # keepalive
            yield ": keepalive\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream")


def main():
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8420, log_level="info")


if __name__ == "__main__":
    main()
