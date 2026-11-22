"""
app.py — FastAPI backend for MACRO-TERM v4.88 (Retro CRT Global Macro Terminal).

Serves:
  - GET  /                  -> HTML5 Canvas + JS Retro CRT frontend
  - GET  /styles.css        -> Retro CRT stylesheet
  - GET  /api/snapshot      -> Real-time watchlist quotes, 10Y-2Y spread & ±1.5% alerts
  - GET  /api/chart         -> Real OHLCV series for selected ticker & timeframe
  - GET  /api/macro         -> Real FRED & BCRP macroeconomic benchmarks
  - POST /api/sync          -> Clears TTL caches to force an immediate refresh
  - GET  /api/health        -> Healthcheck endpoint for Render / cloud monitoring
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from data_fetcher import (
    ALL_ASSETS,
    ASSET_CATALOG,
    TIMEFRAME_MAP,
    clear_all_caches,
    compute_live_yield_spread,
    fetch_chart_series_json,
    fetch_macro_indicators,
    fetch_watchlist_snapshot,
)

BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML = BASE_DIR / "static" / "index.html"
STYLES_CSS = BASE_DIR / "styles.css"

app = FastAPI(
    title="MACRO-TERM v4.88 API",
    description="Global Macroeconomic Retro CRT Terminal powered by Yahoo Finance, FRED & BCRP.",
    version="4.88.0",
)

# Enable CORS so the terminal can also be embedded in Google Sites or external dashboards
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
async def serve_index() -> FileResponse:
    return FileResponse(INDEX_HTML, media_type="text/html")


@app.get("/styles.css", include_in_schema=False)
async def serve_css() -> FileResponse:
    return FileResponse(STYLES_CSS, media_type="text/css")


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "MACRO-TERM v4.88"}


@app.get("/api/snapshot")
async def api_snapshot() -> dict[str, Any]:
    snap, macro = await asyncio.gather(
        asyncio.to_thread(fetch_watchlist_snapshot),
        asyncio.to_thread(fetch_macro_indicators),
    )
    spread = compute_live_yield_spread(snap["quotes"], macro["curve"])
    return {
        "catalog": ASSET_CATALOG,
        "quotes": snap["quotes"],
        "alerts": snap["alerts"],
        "logs": snap["logs"],
        "all_closed": snap["all_closed"],
        "fetched_at": snap["fetched_at"],
        "spread": spread,
    }


@app.get("/api/chart")
async def api_chart(
    ticker: str = Query(..., description="Yahoo Finance ticker symbol (e.g. ^GSPC)"),
    tf: str = Query("1D", description="Timeframe key: 1D, 5D, 1M, 1Y"),
) -> dict[str, Any]:
    if ticker not in ALL_ASSETS:
        raise HTTPException(status_code=400, detail=f"Unsupported ticker: {ticker}")
    if tf not in TIMEFRAME_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {tf}")
    return await asyncio.to_thread(fetch_chart_series_json, ticker, tf)


@app.get("/api/macro")
async def api_macro() -> dict[str, Any]:
    return await asyncio.to_thread(fetch_macro_indicators)


@app.post("/api/sync")
async def api_sync() -> dict[str, str]:
    clear_all_caches()
    return {"status": "caches_cleared"}
