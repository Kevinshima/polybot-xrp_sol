"""Polymarket WebSocket feed — subscribes to order-book updates."""
from __future__ import annotations

import asyncio
import json
from typing import Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from config import settings
from utils.logger import logger


class PolymarketFeed:
    """
    Subscribes to wss://ws-subscriptions-clob.polymarket.com/ws/market
    and emits order-book / price updates.
    """

    def __init__(self, on_update: Callable[[dict], None]):
        self._on_update = on_update
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._subscribed_tokens: set[str] = set()
        self._running = False
        self._prices: dict[str, float] = {}

    def subscribe(self, token_id: str) -> None:
        self._subscribed_tokens.add(token_id)

    def get_price(self, token_id: str) -> Optional[float]:
        return self._prices.get(token_id)

    async def run(self) -> None:
        self._running = True
        backoff = 1.0
        while self._running:
            try:
                async with websockets.connect(
                    settings.WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    backoff = 1.0
                    logger.info("Polymarket WS connected")

                    # Subscribe to all token IDs
                    if self._subscribed_tokens:
                        sub_msg = {
                            "assets_ids": list(self._subscribed_tokens),
                            "type": "market",
                        }
                        await ws.send(json.dumps(sub_msg))

                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            self._handle_message(msg)
                        except (json.JSONDecodeError, KeyError) as exc:
                            logger.warning(f"WS parse error: {exc}")

            except ConnectionClosed as exc:
                logger.warning(f"Polymarket WS disconnected: {exc}. Reconnecting in {backoff}s")
            except Exception as exc:
                logger.error(f"Polymarket WS error: {exc}. Reconnecting in {backoff}s")

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def _handle_message(self, msg: dict | list) -> None:
        if isinstance(msg, list):
            for item in msg:
                self._handle_message(item)
            return

        event_type = msg.get("event_type", "")
        asset_id = msg.get("asset_id", "")

        if event_type in ("book", "price_change", "tick_size_change"):
            if "mid" in msg:
                self._prices[asset_id] = float(msg["mid"])
            self._on_update(msg)

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
