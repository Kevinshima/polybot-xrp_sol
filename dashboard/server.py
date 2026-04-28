"""FastAPI dashboard server with WebSocket live updates."""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from config import settings
from dashboard.api.routes import router
from database import db
from utils.logger import logger

app = FastAPI(title="Polymarket Bot Dashboard", docs_url=None, redoc_url=None)

# Mount static files
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Register REST routes
app.include_router(router)

# WebSocket connection manager
_ws_clients: list[WebSocket] = []


@app.get("/", response_class=HTMLResponse)
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.websocket("/ws/live")
async def live_ws(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.append(websocket)
    logger.debug("Dashboard WS client connected")
    try:
        while True:
            # Keep-alive: expect ping from client
            await asyncio.wait_for(websocket.receive_text(), timeout=30)
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        _ws_clients.remove(websocket) if websocket in _ws_clients else None
        logger.debug("Dashboard WS client disconnected")


async def _broadcast_loop():
    """Push live updates to all WebSocket clients every 2 seconds."""
    while True:
        if _ws_clients:
            try:
                from core.risk_manager import get_risk_manager
                from core.portfolio import get_portfolio
                risk = get_risk_manager()
                portfolio = get_portfolio()

                payload = {
                    "ts": int(time.time()),
                    "daily_pnl": round(db.get_daily_pnl(), 4),
                    "cumulative_pnl": round(db.get_cumulative_pnl(), 4),
                    "open_value": round(portfolio.total_value(), 4),
                    "risk": risk.stats(),
                    "positions": db.get_open_positions(),
                }
                data = json.dumps(payload)
                dead = []
                for ws in list(_ws_clients):
                    try:
                        await ws.send_text(data)
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    if ws in _ws_clients:
                        _ws_clients.remove(ws)
            except Exception as exc:
                logger.warning(f"Dashboard broadcast error: {exc}")
        await asyncio.sleep(2)


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(_broadcast_loop())
    logger.info(f"Dashboard started on http://{settings.DASHBOARD_HOST}:{settings.DASHBOARD_PORT}")
