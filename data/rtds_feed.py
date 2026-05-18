"""Polymarket Real-Time Data Socket — streams Chainlink crypto prices.

Connects to wss://ws-live-data.polymarket.com and subscribes to Chainlink
price feeds for SOL and XRP. Used as an independent oracle confirmation:
when Chainlink (slow, aggregated) agrees with Binance (fast, single-exchange),
the price move is broad-based and more likely to persist through the window.

Protocol:
  Subscribe:  {"action": "subscribe", "subscriptions": [...]}
  Events:     {"topic": "crypto_prices_chainlink", "type": "update",
               "payload": {"symbol": "sol/usd", "value": 150.25, "timestamp": ...}}
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import aiohttp

from utils.logger import logger

RTDS_URL = "wss://ws-live-data.polymarket.com"
_CHAINLINK_STALE_SECS = 300.0   # Chainlink updates less frequently than Binance
_RECONNECT_DELAY = 10.0


class RTDSFeed:
    """
    Subscribes to Polymarket's RTDS for Chainlink crypto prices.
    Exposes get_chainlink_price(asset) for SOL and XRP.
    Used to confirm that Binance momentum is reflected in an independent oracle.
    """

    def __init__(self):
        # asset (lowercase, e.g. "sol") → {price, prev_price, ts}
        self._chainlink: dict[str, dict] = {}
        self._running = False

    # ── Public API ────────────────────────────────────────────────────────────

    def get_chainlink_price(self, asset: str) -> Optional[float]:
        """Latest Chainlink price for 'sol' or 'xrp'. None if stale or unavailable."""
        d = self._chainlink.get(asset.lower())
        if d is None:
            return None
        if time.time() - d["ts"] > _CHAINLINK_STALE_SECS:
            return None
        return d.get("price")

    def get_chainlink_trend(self, asset: str) -> Optional[str]:
        """
        Direction of the last Chainlink price change: 'UP', 'DOWN', or None.
        Chainlink aggregates multiple sources and updates every ~60-120s.
        When Chainlink agrees with our Binance signal, the move is broad-based.
        """
        d = self._chainlink.get(asset.lower())
        if d is None:
            return None
        if time.time() - d["ts"] > _CHAINLINK_STALE_SECS:
            return None
        prev = d.get("prev_price")
        current = d.get("price")
        if not prev or not current or prev <= 0:
            return None
        change = (current - prev) / prev
        if change > 0.0001:    # > 0.01% move
            return "UP"
        if change < -0.0001:
            return "DOWN"
        return None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run indefinitely with auto-reconnect. Called from engine.py as a task."""
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"RTDSFeed error: {exc}")
            if self._running:
                logger.info(f"RTDSFeed reconnecting in {_RECONNECT_DELAY:.0f}s")
                await asyncio.sleep(_RECONNECT_DELAY)

    async def stop(self) -> None:
        self._running = False

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _connect_and_listen(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                RTDS_URL,
                heartbeat=20,
                timeout=aiohttp.ClientWSTimeout(ws_close=30),
            ) as ws:
                logger.info("RTDSFeed: WebSocket connected")

                # Subscribe to Chainlink prices for traded assets
                sub = {
                    "action": "subscribe",
                    "subscriptions": [
                        {
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": '{"symbol":"sol/usd"}',
                        },
                        {
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": '{"symbol":"xrp/usd"}',
                        },
                    ],
                }
                await ws.send_str(json.dumps(sub))
                logger.info("RTDSFeed: subscribed to Chainlink sol/usd, xrp/usd")

                async for msg in ws:
                    if not self._running:
                        await ws.close()
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._handle_raw(msg.data)
                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        break

    def _handle_raw(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        if isinstance(data, list):
            for item in data:
                self._process_message(item)
        elif isinstance(data, dict):
            self._process_message(data)

    def _process_message(self, msg: dict) -> None:
        topic = msg.get("topic", "")
        if "chainlink" not in topic:
            return

        payload = msg.get("payload")
        if not payload or not isinstance(payload, dict):
            return

        symbol_raw = (payload.get("symbol") or "").lower()
        value = payload.get("value")
        if not symbol_raw or value is None:
            return

        try:
            price = float(value)
        except (ValueError, TypeError):
            return

        # Normalise "sol/usd" → "sol", "xrp/usd" → "xrp"
        asset = symbol_raw.split("/")[0]

        prev = self._chainlink.get(asset, {}).get("price")
        self._chainlink[asset] = {
            "price": price,
            "prev_price": prev,
            "ts": time.time(),
        }
        logger.debug(
            f"RTDSFeed Chainlink {asset}: {price:.5f}"
            + (f" (prev={prev:.5f})" if prev else "")
        )


# ── Singleton ─────────────────────────────────────────────────────────────────

_rtds_feed: Optional[RTDSFeed] = None


def get_rtds_feed() -> RTDSFeed:
    global _rtds_feed
    if _rtds_feed is None:
        _rtds_feed = RTDSFeed()
    return _rtds_feed
