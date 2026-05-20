"""Binance futures liquidation stream — cascade regime detector.

Connects to the Binance USDT-M futures all-market forceOrder stream:
  wss://fstream.binance.com/ws/!forceOrder@arr

Each message is a forced-liquidation event with symbol, side, filled qty,
and average fill price.  We track rolling 5-minute notional volume per symbol
and expose is_cascade_active(asset) for the strategy to gate entries.

Thresholds (configurable via .env):
  BTC  > $20M / 5 min  → macro cascade  → pause ALL assets
  XRP  >  $3M / 5 min  → XRP cascade    → pause XRP entries
  SOL  >  $2M / 5 min  → SOL cascade    → pause SOL entries
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Optional

import aiohttp

from config import settings
from utils.logger import logger

_STREAM_URL      = "wss://fstream.binance.com/ws/!forceOrder@arr"
_RECONNECT_DELAY = 10.0

# Map Binance perp symbol → internal asset name
_SYMBOL_TO_ASSET: dict[str, str] = {
    "BTCUSDT": "BTC",
    "XRPUSDT": "XRP",
    "SOLUSDT": "SOL",
}

# Dollar thresholds and pause duration come from settings
_THRESHOLDS: dict[str, float] = {}   # filled in __init__


class LiquidationFeed:
    """
    Tracks rolling 5-minute liquidation notional per asset.
    Exposes is_cascade_active(asset) for the regime gate.
    """

    def __init__(self):
        # deque of (timestamp, usd_notional) per asset
        self._liq_buckets: dict[str, deque] = {
            "BTC": deque(),
            "XRP": deque(),
            "SOL": deque(),
        }
        # When a cascade was last detected per asset
        self._cascade_until: dict[str, float] = {}
        self._running = False
        self._total_events = 0

    # ── Public interface ──────────────────────────────────────────────────────

    def is_cascade_active(self, asset: str) -> bool:
        """
        Returns True when:
        - a cascade was detected for this asset within the last LIQ_CASCADE_PAUSE_SECS, OR
        - BTC cascade is active (macro event affects all assets).
        """
        now = time.time()
        asset_paused = now < self._cascade_until.get(asset, 0.0)
        btc_paused   = now < self._cascade_until.get("BTC",  0.0)
        return asset_paused or btc_paused

    def get_5m_volume(self, asset: str) -> float:
        """Rolling 5-minute liquidation notional in USD for the given asset."""
        cutoff = time.time() - settings.LIQ_CASCADE_WINDOW_SECS
        return sum(v for ts, v in self._liq_buckets.get(asset, []) if ts >= cutoff)

    def get_status(self) -> dict:
        """Summary dict for dashboard / heartbeat logging."""
        return {
            asset: {
                "5m_usd":       round(self.get_5m_volume(asset)),
                "cascade_active": self.is_cascade_active(asset),
                "pause_remaining": max(0.0, self._cascade_until.get(asset, 0.0) - time.time()),
            }
            for asset in ("BTC", "XRP", "SOL")
        }

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("LiquidationFeed starting")
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"LiquidationFeed error: {exc}")
            if self._running:
                logger.warning(
                    f"LiquidationFeed disconnected — reconnecting in {_RECONNECT_DELAY:.0f}s"
                )
                await asyncio.sleep(_RECONNECT_DELAY)

    async def stop(self) -> None:
        self._running = False

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _connect_and_listen(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                _STREAM_URL,
                heartbeat=20,
                timeout=aiohttp.ClientWSTimeout(ws_close=30),
            ) as ws:
                logger.info("LiquidationFeed: Binance futures liquidation stream connected")
                async for msg in ws:
                    if not self._running:
                        await ws.close()
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._handle(msg.data)
                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        break

    def _handle(self, raw: str) -> None:
        try:
            data = json.loads(raw)
            order = data.get("o", {})
            symbol   = order.get("s", "")
            avg_px   = float(order.get("ap", 0) or 0)
            filled_q = float(order.get("l",  0) or 0)   # last filled qty
        except (KeyError, ValueError, json.JSONDecodeError):
            return

        asset = _SYMBOL_TO_ASSET.get(symbol)
        if asset is None or avg_px == 0 or filled_q == 0:
            return

        usd = avg_px * filled_q
        now = time.time()

        bucket = self._liq_buckets[asset]
        bucket.append((now, usd))
        # Prune old entries outside the rolling window
        cutoff = now - settings.LIQ_CASCADE_WINDOW_SECS
        while bucket and bucket[0][0] < cutoff:
            bucket.popleft()

        self._total_events += 1
        self._check_cascade(asset, now)

    def _check_cascade(self, asset: str, now: float) -> None:
        """Evaluate rolling window; set cascade_until if threshold breached."""
        thresholds = {
            "BTC": settings.LIQ_CASCADE_BTC_USD,
            "XRP": settings.LIQ_CASCADE_XRP_USD,
            "SOL": settings.LIQ_CASCADE_SOL_USD,
        }
        threshold = thresholds.get(asset, float("inf"))
        vol_5m = self.get_5m_volume(asset)

        if vol_5m >= threshold:
            # Only log + reset pause timer when crossing fresh (not on every event)
            already_active = now < self._cascade_until.get(asset, 0.0)
            self._cascade_until[asset] = now + settings.LIQ_CASCADE_PAUSE_SECS
            if not already_active:
                logger.warning(
                    f"LiquidationFeed: CASCADE DETECTED [{asset}] "
                    f"5m_vol=${vol_5m:,.0f} threshold=${threshold:,.0f} "
                    f"→ entries paused {settings.LIQ_CASCADE_PAUSE_SECS}s"
                )


# Singleton
_feed: Optional[LiquidationFeed] = None


def get_liquidation_feed() -> LiquidationFeed:
    global _feed
    if _feed is None:
        _feed = LiquidationFeed()
    return _feed
