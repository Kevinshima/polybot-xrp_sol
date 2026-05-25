"""Real-time Polymarket market feed via WebSocket.

Subscribes to token IDs and streams live best_bid / best_ask from the CLOB.
Replaces HTTP get_midpoint() polling in latency-sensitive entry decisions,
eliminating 50–200ms round-trip latency on every tick.

Protocol:
  Subscribe:  {"assets_ids": ["token_id_1", ...], "type": "subscribe"}
  Events:     {"event_type": "book", "asset_id": "...", "bids": [...], "asks": [...]}
              {"event_type": "price_change", "asset_id": "...", "changes": [...]}
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import aiohttp

from utils.logger import logger

WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_STALE_SECS = 15.0     # treat cached price as stale after this many seconds
_RECONNECT_DELAY = 5.0


class PolymarketFeed:
    """
    Persistent WebSocket connection to Polymarket's market stream.
    Call subscribe(token_ids) any time to add new tokens.
    Call get_best_ask(token_id) for sub-millisecond price lookups.
    Returns None gracefully when data is stale or the feed has not yet received
    a book snapshot for the requested token — callers should fall back to HTTP.
    """

    def __init__(self):
        self._token_data: dict[str, dict] = {}  # token_id → {best_bid, best_ask, ts}
        self._last_trade_ts: dict[str, float] = {}  # token_id → timestamp of last executed trade
        self._all_ids: set[str] = set()          # all tokens we ever want subscribed
        self._running = False
        # Broadcast queue — one entry per price event; consumed by Heartbeat for
        # event-driven trail-stop checks instead of polling every 10 seconds.
        self._price_events: asyncio.Queue = asyncio.Queue(maxsize=500)

    # ── Public API ────────────────────────────────────────────────────────────

    def subscribe(self, token_ids: list[str]) -> None:
        """Add token IDs to the subscription set. Applied on next message cycle."""
        self._all_ids.update(token_ids)

    def get_best_ask(self, token_id: str) -> Optional[float]:
        """
        Current best ask price, or None if unavailable / stale.
        Best ask = the price we pay when buying (slightly above mid).
        Using ask instead of mid gives a more accurate effective entry cost.
        """
        d = self._token_data.get(token_id)
        if d is None:
            return None
        if time.time() - d["ts"] > _STALE_SECS:
            return None
        return d.get("best_ask")

    def get_best_bid(self, token_id: str) -> Optional[float]:
        d = self._token_data.get(token_id)
        if d is None:
            return None
        if time.time() - d["ts"] > _STALE_SECS:
            return None
        return d.get("best_bid")

    def get_last_trade_age(self, token_id: str) -> Optional[float]:
        """Seconds since the last trade executed on this token. None = never seen."""
        ts = self._last_trade_ts.get(token_id)
        if ts is None:
            return None
        return time.time() - ts

    def get_mid(self, token_id: str) -> Optional[float]:
        """Return (best_bid + best_ask) / 2 or None if unavailable."""
        d = self._token_data.get(token_id)
        if d is None:
            return None
        if time.time() - d["ts"] > _STALE_SECS:
            return None
        bid = d.get("best_bid")
        ask = d.get("best_ask")
        if bid and ask:
            return (bid + ask) / 2.0
        return ask or bid

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
                logger.warning(f"PolymarketFeed error: {exc}")
            if self._running:
                logger.info(f"PolymarketFeed reconnecting in {_RECONNECT_DELAY:.0f}s")
                await asyncio.sleep(_RECONNECT_DELAY)

    def get_price_queue(self) -> asyncio.Queue:
        """Return the broadcast queue for event-driven exit management."""
        return self._price_events

    async def stop(self) -> None:
        self._running = False

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _connect_and_listen(self) -> None:
        session_subscribed: set[str] = set()

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                WS_MARKET_URL,
                heartbeat=20,
                timeout=aiohttp.ClientWSTimeout(ws_close=30),
            ) as ws:
                logger.info("PolymarketFeed: WebSocket connected")

                # Subscribe to all known token IDs immediately on connect
                if self._all_ids:
                    await self._send_subscribe(ws, list(self._all_ids))
                    session_subscribed.update(self._all_ids)

                async for msg in ws:
                    if not self._running:
                        await ws.close()
                        break

                    # Subscribe any tokens added since last cycle (new markets)
                    new_ids = self._all_ids - session_subscribed
                    if new_ids:
                        await self._send_subscribe(ws, list(new_ids))
                        session_subscribed.update(new_ids)

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._handle_raw(msg.data)
                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        break

    async def _send_subscribe(self, ws, token_ids: list[str]) -> None:
        payload = json.dumps({"assets_ids": token_ids, "type": "subscribe"})
        await ws.send_str(payload)
        logger.info(f"PolymarketFeed: subscribed to {len(token_ids)} token(s)")

    def _handle_raw(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        if isinstance(data, list):
            for item in data:
                self._process_event(item)
        elif isinstance(data, dict):
            self._process_event(data)

    def _process_event(self, event: dict) -> None:
        asset_id = event.get("asset_id")
        if not asset_id:
            return

        event_type = event.get("event_type", "")

        # Track last executed trade timestamp — used for oracle freshness scoring.
        # A quiet Polymarket token (no trades in 30+s) while Binance is moving
        # signals an unexploited oracle lag window.
        if event_type == "last_trade_price":
            price_str = event.get("price") or event.get("last_trade_price")
            if price_str:
                try:
                    self._last_trade_ts[asset_id] = time.time()
                    logger.debug(
                        f"PolymarketFeed last_trade [{asset_id[:16]}…]: "
                        f"price={float(price_str):.4f}"
                    )
                except (ValueError, TypeError):
                    pass
            return  # not an orderbook update — exit early
        best_ask: Optional[float] = None
        best_bid: Optional[float] = None

        # Some API versions include direct best_bid / best_ask fields
        for key, target in (("best_ask", "best_ask"), ("best_bid", "best_bid")):
            if key in event:
                try:
                    val = float(event[key])
                    if key == "best_ask":
                        best_ask = val
                    else:
                        best_bid = val
                except (ValueError, TypeError):
                    pass

        # book event: full snapshot — derive best levels from sorted arrays
        if event_type == "book" or (best_ask is None and best_bid is None):
            bids = event.get("bids") or []
            asks = event.get("asks") or []
            try:
                valid_asks = [float(a["price"]) for a in asks
                              if float(a.get("size", "0")) > 0]
                if valid_asks:
                    best_ask = min(valid_asks)
            except (ValueError, KeyError, TypeError):
                pass
            try:
                valid_bids = [float(b["price"]) for b in bids
                              if float(b.get("size", "0")) > 0]
                if valid_bids:
                    best_bid = max(valid_bids)
            except (ValueError, KeyError, TypeError):
                pass

        if best_ask is not None or best_bid is not None:
            self._token_data[asset_id] = {
                "best_ask": best_ask,
                "best_bid": best_bid,
                "ts": time.time(),
            }
            # Wake any waiting heartbeat trail-stop checker
            try:
                self._price_events.put_nowait(asset_id)
            except asyncio.QueueFull:
                pass  # consumer is behind — next event will wake it
            logger.debug(
                f"PolymarketFeed {asset_id[:16]}… "
                f"bid={f'{best_bid:.4f}' if best_bid else 'n/a'} "
                f"ask={f'{best_ask:.4f}' if best_ask else 'n/a'}"
            )


# ── Singleton ─────────────────────────────────────────────────────────────────

_pm_feed: Optional[PolymarketFeed] = None


def get_polymarket_feed() -> PolymarketFeed:
    global _pm_feed
    if _pm_feed is None:
        _pm_feed = PolymarketFeed()
    return _pm_feed
