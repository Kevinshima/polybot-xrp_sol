"""Binance WebSocket real-time price feed with Kraken HTTP fallback."""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Optional

import aiohttp

from utils.logger import logger


# Maps internal symbol → Binance aggTrade stream URL
_BINANCE_STREAMS = {
    "BTC/USDT": "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
    "ETH/USDT": "wss://stream.binance.com:9443/ws/ethusdt@aggTrade",
}

# Binance order book depth streams — top-20 levels updated every 1 second
_BINANCE_OB_STREAMS = {
    "BTC/USDT": "wss://stream.binance.com:9443/ws/btcusdt@depth20@1000ms",
    "ETH/USDT": "wss://stream.binance.com:9443/ws/ethusdt@depth20@1000ms",
}

# Maps internal symbol → (Kraken query pair, Kraken result key)
_KRAKEN_PAIRS = {
    "BTC/USDT": ("XBTUSD", "XXBTZUSD"),
    "ETH/USDT": ("ETHUSD", "XETHZUSD"),
}

_FALLBACK_TIMEOUT = 10.0   # seconds before activating Kraken fallback
_FALLBACK_INTERVAL = 10.0  # Kraken poll interval when fallback is active
_RECONNECT_DELAY = 5.0     # seconds to wait before WebSocket reconnect
_MOMENTUM_WINDOW = 30.0    # seconds of trade history for momentum calculation
_LOG_EVERY_N_TRADES = 100  # log a DEBUG price line once per N trades


class ExchangeFeed:
    """
    Real-time BTC price feed via Binance public aggTrade WebSocket stream.
    No API key required. Falls back to Kraken HTTP polling if WebSocket is
    unavailable after 10 seconds.

    Public interface (unchanged from previous implementation):
      get_price(symbol)    → Optional[float]
      get_momentum(symbol) → float
      run()                → coroutine (main loop, called by engine)
      stop()               → coroutine
    """

    def __init__(self):
        self._prices: dict[str, float] = {}
        self._trade_history: dict[str, deque] = {
            sym: deque() for sym in _BINANCE_STREAMS
        }
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._trade_tasks: list[asyncio.Task] = []   # one per symbol, concurrent
        self._ob_tasks: list[asyncio.Task] = []
        self._trade_counts: dict[str, int] = {sym: 0 for sym in _BINANCE_STREAMS}
        self._window_seconds = _MOMENTUM_WINDOW
        self._ob_imbalance: dict[str, float | None] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_price(self, symbol: str) -> Optional[float]:
        """Latest price. symbol like 'BTC/USDT' or 'BTC'."""
        if "/" not in symbol:
            symbol = f"{symbol}/USDT"
        return self._prices.get(symbol)

    def get_momentum(self, symbol: str) -> float:
        """
        30-second momentum as a fraction (e.g. 0.003 = 0.3%).
        Computed as (newest_price - oldest_price_in_window) / oldest_price_in_window.
        Returns 0.0 if fewer than 2 data points are in the window.
        """
        if "/" not in symbol:
            symbol = f"{symbol}/USDT"
        history = self._trade_history.get(symbol)
        if history is None or len(history) < 2:
            return 0.0
        oldest_price = history[0][1]
        newest_price = history[-1][1]
        if oldest_price == 0:
            return 0.0
        return (newest_price - oldest_price) / oldest_price

    def get_order_book_imbalance(self, symbol: str) -> float | None:
        """
        Order book imbalance as a fraction in [-1.0, +1.0].
        Positive = bid-heavy (buying pressure), negative = ask-heavy (selling pressure).
        Returns None until the first order book message arrives after startup.
        """
        if "/" not in symbol:
            symbol = f"{symbol}/USDT"
        return self._ob_imbalance.get(symbol, None)

    async def run(self) -> None:
        """Start the feed. Blocks until stop() is called."""
        self._running = True
        self._task = asyncio.current_task()
        # One independent reconnect loop per trade symbol (concurrent, not sequential)
        for symbol, url in _BINANCE_STREAMS.items():
            task = asyncio.ensure_future(self._run_trade_loop(symbol, url))
            self._trade_tasks.append(task)
        # One order book depth stream per symbol
        for symbol, ob_url in _BINANCE_OB_STREAMS.items():
            task = asyncio.ensure_future(self._run_orderbook_loop(symbol, ob_url))
            self._ob_tasks.append(task)
        # Block until cancelled (shutdown)
        try:
            await asyncio.gather(*self._trade_tasks, *self._ob_tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        """Signal the feed to stop and cancel all background tasks."""
        self._running = False
        for task in self._trade_tasks + self._ob_tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._task = None
        self._trade_tasks = []
        self._ob_tasks = []

    # ------------------------------------------------------------------
    # Internal: per-symbol trade stream loop (concurrent, one per symbol)
    # ------------------------------------------------------------------

    async def _run_trade_loop(self, symbol: str, url: str) -> None:
        """
        Independent reconnect loop for one symbol's aggTrade stream.
        Falls back to Kraken HTTP polling if Binance is unavailable for >10s.
        Runs until stop() is called.
        """
        start_time = time.time()
        fallback_active = False

        while self._running:
            # Activate Kraken fallback if this symbol has no price after timeout
            if (
                not fallback_active
                and self._prices.get(symbol) is None
                and time.time() - start_time >= _FALLBACK_TIMEOUT
            ):
                logger.warning(f"Binance WebSocket unavailable for {symbol}, using Kraken fallback")
                fallback_active = True

            if fallback_active:
                await self._kraken_fallback_tick_symbol(symbol)
                await asyncio.sleep(_FALLBACK_INTERVAL)
                if self._prices.get(symbol) is not None:
                    fallback_active = False
                continue

            connected = await self._listen_symbol(symbol, url)
            if not connected and self._prices.get(symbol) is None:
                pass  # still counting toward fallback

            if self._running:
                logger.warning(f"Binance WebSocket {symbol} disconnected, reconnecting in {_RECONNECT_DELAY:.0f}s")
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _listen_symbol(self, symbol: str, url: str) -> bool:
        """
        Listen to one symbol's aggTrade stream until disconnect or error.
        Returns True if connection was established.
        """
        connected = False
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    url,
                    heartbeat=20,
                    timeout=aiohttp.ClientWSTimeout(ws_close=30),
                ) as ws:
                    connected = True
                    logger.info(f"Binance WebSocket connected — {symbol} real-time feed active")

                    async for msg in ws:
                        if not self._running:
                            await ws.close()
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._handle_trade_message(symbol, msg.data)
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if connected:
                logger.warning(f"Binance WebSocket {symbol} error: {exc}")
            else:
                logger.warning(f"Binance WebSocket {symbol} failed to connect: {exc}")
        return connected

    # ------------------------------------------------------------------
    # Internal: order book depth stream
    # ------------------------------------------------------------------

    async def _run_orderbook_loop(self, symbol: str, url: str) -> None:
        """Reconnect loop for one symbol's order book depth stream. Runs until stop()."""
        while self._running:
            try:
                await self._listen_orderbook(symbol, url)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"Binance order book stream error ({symbol}): {exc}")
            if self._running:
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _listen_orderbook(self, symbol: str, url: str) -> None:
        """
        Listen to the depth20 stream for one symbol until disconnect or error.
        Updates self._ob_imbalance on every message.
        """
        connected = False
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    url,
                    heartbeat=20,
                    timeout=aiohttp.ClientWSTimeout(ws_close=30),
                ) as ws:
                    connected = True
                    logger.info(
                        f"Binance order book WebSocket connected — {symbol} depth feed active"
                    )
                    async for msg in ws:
                        if not self._running:
                            await ws.close()
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._handle_orderbook_message(symbol, msg.data)
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if connected:
                logger.warning(f"Binance order book {symbol} error: {exc}")
            else:
                logger.warning(f"Binance order book {symbol} failed to connect: {exc}")

    def _handle_orderbook_message(self, symbol: str, raw: str) -> None:
        """Parse a depth20 snapshot and update order book imbalance."""
        try:
            data = json.loads(raw)
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            bid_vol = sum(float(qty) for _, qty in bids)
            ask_vol = sum(float(qty) for _, qty in asks)
            total = bid_vol + ask_vol
            if total == 0:
                return
            imbalance = (bid_vol - ask_vol) / total
            self._ob_imbalance[symbol] = imbalance
            logger.debug(
                f"{symbol} order book: imbalance={imbalance:+.3f} "
                f"(bid_vol={bid_vol:.2f} ask_vol={ask_vol:.2f})"
            )
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            logger.debug(f"Order book message parse error for {symbol}: {exc}")

    def _handle_trade_message(self, symbol: str, raw: str) -> None:
        """Parse an aggTrade message and update price + history."""
        try:
            data = json.loads(raw)
            price = float(data["p"])
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            logger.debug(f"Binance message parse error for {symbol}: {exc}")
            return

        now = time.time()
        self._prices[symbol] = price

        history = self._trade_history[symbol]
        history.append((now, price))

        # Purge entries older than the momentum window
        cutoff = now - self._window_seconds
        while history and history[0][0] < cutoff:
            history.popleft()

        # Periodic debug log — every N trades
        self._trade_counts[symbol] = self._trade_counts.get(symbol, 0) + 1
        if self._trade_counts[symbol] % _LOG_EVERY_N_TRADES == 0:
            momentum = self.get_momentum(symbol)
            logger.debug(
                f"{symbol} trade: price={price:.2f} momentum={momentum:+.4%} "
                f"(window={len(history)} pts)"
            )

    # ------------------------------------------------------------------
    # Kraken HTTP fallback
    # ------------------------------------------------------------------

    async def _kraken_fallback_tick_symbol(self, symbol: str) -> None:
        """Fetch price for one symbol from Kraken REST API (no auth required)."""
        if symbol not in _KRAKEN_PAIRS:
            return
        kraken_pair, result_key = _KRAKEN_PAIRS[symbol]
        url = f"https://api.kraken.com/0/public/Ticker?pair={kraken_pair}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Kraken fallback {symbol} returned {resp.status}")
                        return
                    data = await resp.json()
                    if data.get("error"):
                        logger.warning(f"Kraken fallback {symbol} error: {data['error']}")
                        return
                    price = float(data["result"][result_key]["c"][0])
                    now = time.time()
                    self._prices[symbol] = price
                    history = self._trade_history[symbol]
                    history.append((now, price))
                    cutoff = now - self._window_seconds
                    while history and history[0][0] < cutoff:
                        history.popleft()
                    logger.debug(f"Kraken fallback {symbol}: price={price:.2f}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"Kraken fallback fetch failed ({symbol}): {exc}")


# Singleton
_feed: Optional[ExchangeFeed] = None


def get_exchange_feed() -> ExchangeFeed:
    global _feed
    if _feed is None:
        _feed = ExchangeFeed()
    return _feed
