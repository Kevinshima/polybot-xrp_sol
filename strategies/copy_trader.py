"""Strategy 4: Mirror top-wallet trades on Polymarket."""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from config import settings
from strategies.base import BaseStrategy
from utils.logger import logger


class CopyTrader(BaseStrategy):
    """
    Polls the Polymarket Data API for activity from target wallets.
    Mirrors their trades at COPY_RATIO of their size.
    """

    name = "copy_trader"

    def __init__(self):
        super().__init__()
        self._seen_trade_ids: set[str] = set()
        self._wallet_positions: dict[str, dict] = {}  # wallet → {market_id: side}

    async def run(self) -> None:
        if not settings.TARGET_WALLETS:
            logger.warning("CopyTrader: no TARGET_WALLETS configured — strategy idle")
            while self._running:
                await asyncio.sleep(60)
            return

        logger.info(f"CopyTrader: tracking {len(settings.TARGET_WALLETS)} wallets")

        while self._running:
            if self._check_halted():
                await asyncio.sleep(30)
                continue
            try:
                await self._poll_wallets()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"CopyTrader poll error: {exc}")
            await asyncio.sleep(settings.CT_POLL_INTERVAL)

    async def _poll_wallets(self) -> None:
        async with aiohttp.ClientSession() as session:
            for wallet in settings.TARGET_WALLETS:
                await self._check_wallet(session, wallet)

    async def _check_wallet(self, session: aiohttp.ClientSession, wallet: str) -> None:
        url = f"{settings.DATA_API_URL}/activity"
        params = {"user": wallet, "limit": 20}

        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
                activities = data if isinstance(data, list) else data.get("data", [])
        except Exception as exc:
            logger.warning(f"CopyTrader: API call for {wallet[:8]}… failed: {exc}")
            return

        for activity in activities:
            trade_id = activity.get("id") or activity.get("transactionHash", "")
            if not trade_id or trade_id in self._seen_trade_ids:
                continue
            self._seen_trade_ids.add(trade_id)
            await self._maybe_mirror(wallet, activity)

    async def _maybe_mirror(self, wallet: str, activity: dict) -> None:
        market_id = activity.get("conditionId") or activity.get("market_id", "")
        side = (activity.get("side") or activity.get("type", "")).upper()
        size_usdc = float(activity.get("usdcSize") or activity.get("amount", 0))
        price = float(activity.get("price") or activity.get("outcomePrice", 0))
        outcome_index = int(activity.get("outcomeIndex", 0))
        question = activity.get("question") or activity.get("title", "")

        if not market_id or side not in ("BUY", "SELL") or size_usdc <= 0:
            return

        # Skip if market is closing soon
        end_date = activity.get("endDate") or activity.get("expiresAt")
        if end_date:
            try:
                from datetime import datetime
                expiry = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
                secs_remaining = (expiry - datetime.now(expiry.tzinfo)).total_seconds()
                if secs_remaining < settings.CT_MIN_SECONDS_TO_CLOSE:
                    logger.debug(f"CopyTrader: skipping {market_id[:16]}… — closes in {secs_remaining:.0f}s")
                    return
            except Exception:
                pass

        # Skip if we already have a position
        if self._portfolio.has_position(market_id):
            # Check if wallet is exiting — then we exit too
            prev_side = self._wallet_positions.get(wallet, {}).get(market_id)
            if prev_side and side != prev_side:
                logger.info(f"CopyTrader: wallet {wallet[:8]}… exited {market_id[:16]}… — closing our position")
                pnl = self._portfolio.close_position(market_id, price)
                if pnl is not None:
                    self._risk.record_fill(pnl)
            return

        mirror_size = size_usdc * settings.COPY_RATIO

        # Need token_id from market — try to get it
        token_id = activity.get("tokenId") or market_id

        if self.dry_run:
            logger.info(
                f"DRY RUN CopyTrader: {side} {mirror_size:.2f} USDC on {market_id[:20]}… "
                f"(mirroring {wallet[:8]}…)"
            )
            return

        try:
            from py_clob_client.clob_types import OrderType
            result = self._orders.place_limit_order(
                strategy=self.name,
                market_id=market_id,
                token_id=token_id,
                question=question,
                side=side,
                price=price,
                size_usdc=mirror_size,
                order_type=OrderType.GTC,
            )
            if result:
                self._portfolio.add_position(
                    market_id=market_id,
                    token_id=token_id,
                    question=question,
                    strategy=self.name,
                    side=side,
                    size=mirror_size / price if price else 0,
                    entry_price=price,
                )
                self._wallet_positions.setdefault(wallet, {})[market_id] = side
                logger.info(
                    f"CopyTrader: mirrored {wallet[:8]}… {side} {mirror_size:.2f} USDC"
                    f" on {question[:40]}…"
                )
        except AssertionError as exc:
            logger.warning(f"CopyTrader: order rejected: {exc}")
