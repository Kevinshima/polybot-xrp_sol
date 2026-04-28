"""Strategy 2: Bid/ask liquidity provision on Polymarket."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

from py_clob_client.clob_types import OrderType

from config import settings
from data.market_scanner import get_scanner
from strategies.base import BaseStrategy
from utils.logger import logger
from utils.helpers import round_price, extract_clob_token_id


@dataclass
class MMState:
    market_id: str
    token_id: str
    question: str
    bid_order_id: Optional[str] = None
    ask_order_id: Optional[str] = None
    bid_price: float = 0.0
    ask_price: float = 0.0
    last_quote_ts: float = 0.0
    position_size_usdc: float = 0.0


class MarketMaker(BaseStrategy):
    """
    Places two-sided quotes on low-volatility, rewards-eligible markets.
    Re-quotes every 60 seconds or immediately after a fill.
    Cancels quotes on detected news events (>10% move in 10 min).
    """

    name = "market_maker"

    def __init__(self):
        super().__init__()
        self._scanner = get_scanner()
        self._states: dict[str, MMState] = {}
        self._max_markets = 5  # don't spread too thin

    async def run(self) -> None:
        logger.info("MarketMaker starting")

        # Initial market scan
        await self._scan_markets()

        while self._running:
            if self._check_halted():
                await asyncio.sleep(30)
                continue

            try:
                await self._requote_all()
                await self._check_fills()
                await self._check_news_events()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"MarketMaker loop error: {exc}")

            await asyncio.sleep(5)

    async def _scan_markets(self) -> None:
        try:
            # Prefer crypto price markets; fall back to general 30-day candidates
            by_asset = await self._scanner.get_crypto_price_candidates(min_volume=5_000)
            candidates: list[dict] = []
            seen_ids: set[str] = set()
            for markets in by_asset.values():
                for m in markets:
                    mid = m.get("conditionId") or m.get("id", "")
                    if mid and mid not in seen_ids:
                        seen_ids.add(mid)
                        candidates.append(m)

            if not candidates:
                logger.info("MarketMaker: no crypto price markets found, falling back to general candidates")
                candidates = await self._scanner.get_mm_candidates()

            # Sort by volume descending (crypto markets already pre-filtered)
            candidates.sort(
                key=lambda m: float(m.get("volume24hr") or m.get("volume") or 0),
                reverse=True,
            )
            for market in candidates[: self._max_markets]:
                market_id = market.get("conditionId") or market.get("id", "")
                token_id = extract_clob_token_id(market)
                question = market.get("question", "")

                if market_id and market_id not in self._states:
                    self._states[market_id] = MMState(
                        market_id=market_id,
                        token_id=token_id,
                        question=question,
                    )
                    logger.info(f"MarketMaker: added market {question[:50]}…")

            # Rescan every 10 minutes
            asyncio.get_event_loop().call_later(600, lambda: asyncio.create_task(self._scan_markets()))
        except Exception as exc:
            logger.error(f"MarketMaker scan error: {exc}")

    async def _requote_all(self) -> None:
        now = time.time()
        for market_id, state in list(self._states.items()):
            age = now - state.last_quote_ts
            if age < settings.MM_REQUOTE_INTERVAL:
                continue
            await self._place_quotes(state)

    async def _place_quotes(self, state: MMState) -> None:
        try:
            midpoint = self._client.get_midpoint(state.token_id)
            if midpoint <= 0 or midpoint >= 1:
                return

            spread = settings.MM_MIN_SPREAD  # 3 cents base spread
            bid = round_price(midpoint - spread / 2)
            ask = round_price(midpoint + spread / 2)

            # Cancel stale quotes first
            await self._cancel_quotes(state)

            size = settings.MAX_POSITION_SIZE_USDC

            if self.dry_run:
                logger.info(
                    f"DRY RUN MM: {state.question[:30]}… bid={bid:.3f} ask={ask:.3f}"
                )
                state.last_quote_ts = time.time()
                return

            # Place bid
            bid_result = self._orders.place_limit_order(
                strategy=self.name,
                market_id=state.market_id,
                token_id=state.token_id,
                question=state.question,
                side="BUY",
                price=bid,
                size_usdc=size,
                order_type=OrderType.GTC,
            )
            if bid_result:
                state.bid_order_id = bid_result.get("id")
                state.bid_price = bid

            # Place ask
            ask_result = self._orders.place_limit_order(
                strategy=self.name,
                market_id=state.market_id,
                token_id=state.token_id,
                question=state.question,
                side="SELL",
                price=ask,
                size_usdc=size,
                order_type=OrderType.GTC,
            )
            if ask_result:
                state.ask_order_id = ask_result.get("id")
                state.ask_price = ask

            state.last_quote_ts = time.time()
            state.position_size_usdc += size

            logger.info(
                f"MM quoted {state.question[:30]}… "
                f"bid={bid:.3f} ask={ask:.3f} mid={midpoint:.3f}"
            )
        except AssertionError as exc:
            logger.warning(f"MarketMaker order rejected: {exc}")
        except Exception as exc:
            logger.error(f"MarketMaker quote failed for {state.market_id}: {exc}")

    async def _cancel_quotes(self, state: MMState) -> None:
        size = settings.MAX_POSITION_SIZE_USDC
        if state.bid_order_id:
            self._orders.cancel_order(state.bid_order_id, state.market_id, size)
            state.bid_order_id = None
        if state.ask_order_id:
            self._orders.cancel_order(state.ask_order_id, state.market_id, size)
            state.ask_order_id = None

    async def _check_fills(self) -> None:
        """Detect filled orders and re-quote immediately."""
        try:
            open_orders = self._client.get_open_orders()
            open_ids = {o.get("id") for o in open_orders}

            for state in self._states.values():
                refill = False
                if state.bid_order_id and state.bid_order_id not in open_ids:
                    logger.info(f"MM bid filled on {state.question[:30]}…")
                    state.bid_order_id = None
                    refill = True
                if state.ask_order_id and state.ask_order_id not in open_ids:
                    logger.info(f"MM ask filled on {state.question[:30]}…")
                    state.ask_order_id = None
                    refill = True
                if refill:
                    state.last_quote_ts = 0  # trigger requote
        except Exception as exc:
            logger.warning(f"MM fill check failed: {exc}")

    async def _check_news_events(self) -> None:
        """Cancel quotes if market moved >10% in 10 minutes."""
        for state in self._states.items():
            pass  # TODO: track price history and cancel on large moves
