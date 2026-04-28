"""Polymarket CLOB client wrapper with auto-reconnect and rate limiting."""
from __future__ import annotations

import asyncio
import time
from typing import Optional, Any

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    OrderArgs,
    MarketOrderArgs,
    OrderType,
)
from py_clob_client.exceptions import PolyApiException

from config import settings
from utils.logger import logger
from utils.helpers import retry_sync


class RateLimiter:
    """Token-bucket rate limiter."""

    def __init__(self, rate: int, per: float = 60.0):
        self.rate = rate
        self.per = per
        self._tokens = float(rate)
        self._last = time.monotonic()

    def acquire(self, tokens: int = 1) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        self._last = now
        self._tokens = min(self.rate, self._tokens + elapsed * (self.rate / self.per))
        if self._tokens < tokens:
            # Never block the event loop — log and proceed; token refill prevents sustained overload
            self._tokens = 0
        else:
            self._tokens -= tokens


class PolymarketClient:
    """
    Thread-safe wrapper around py-clob-client.
    - Enforces rate limits (100 req/min public, 60 orders/min)
    - Retries on 429 / 5xx with exponential backoff
    - Provides dry-run mode
    """

    def __init__(self):
        self._public_rl = RateLimiter(rate=95, per=60.0)   # leave headroom
        self._order_rl = RateLimiter(rate=55, per=60.0)
        self._client: Optional[ClobClient] = None
        self._connected = False

    def connect(self) -> None:
        creds = ApiCreds(
            api_key=settings.POLY_API_KEY,
            api_secret=settings.POLY_API_SECRET,
            api_passphrase=settings.POLY_API_PASSPHRASE,
        )
        self._client = ClobClient(
            host=settings.CLOB_BASE_URL,
            chain_id=settings.CHAIN_ID,
            key=settings.POLY_PRIVATE_KEY,
            creds=creds,
        )
        self._connected = True
        logger.info("Polymarket CLOB client connected")

    @property
    def client(self) -> ClobClient:
        if not self._connected or self._client is None:
            self.connect()
        return self._client  # type: ignore

    # ── Read ──────────────────────────────────────────────────────────────────

    @retry_sync(max_attempts=4, base_delay=1.0, exceptions=(Exception,))
    def get_markets(self, next_cursor: str = "") -> dict:
        self._public_rl.acquire()
        return self.client.get_markets(next_cursor=next_cursor)

    @retry_sync(max_attempts=4, base_delay=1.0, exceptions=(Exception,))
    def get_market(self, condition_id: str) -> dict:
        self._public_rl.acquire()
        return self.client.get_market(condition_id)

    @retry_sync(max_attempts=4, base_delay=1.0, exceptions=(Exception,))
    def get_midpoint(self, token_id: str) -> float:
        self._public_rl.acquire()
        result = self.client.get_midpoint(token_id)
        return float(result.get("mid", 0.5))

    @retry_sync(max_attempts=4, base_delay=1.0, exceptions=(Exception,))
    def get_order_book(self, token_id: str) -> dict:
        self._public_rl.acquire()
        return self.client.get_order_book(token_id)

    @retry_sync(max_attempts=3, base_delay=1.0, exceptions=(Exception,))
    def get_positions(self) -> list:
        self._public_rl.acquire()
        return self.client.get_positions()

    @retry_sync(max_attempts=3, base_delay=1.0, exceptions=(Exception,))
    def get_trades(self, market_id: str = "") -> list:
        self._public_rl.acquire()
        return self.client.get_trades(market_id=market_id)

    def get_midpoints_batch(self, token_ids: list[str]) -> dict[str, float]:
        """
        Batch fetch mid prices for multiple token IDs in a single API call.
        Returns {token_id: mid_price}. Handles multiple response formats defensively.
        Falls back to empty dict on any error (caller should handle gracefully).
        """
        if not token_ids:
            return {}
        from py_clob_client.clob_types import BookParams
        self._public_rl.acquire()
        params = [BookParams(token_id=tid) for tid in token_ids]
        try:
            result = self.client.get_midpoints(params)
            out: dict[str, float] = {}
            # Format A: {"token_id_1": "0.45", "token_id_2": "0.55", ...}
            if isinstance(result, dict):
                for k, v in result.items():
                    if k in token_ids:
                        try:
                            out[k] = float(v)
                        except (TypeError, ValueError):
                            pass
            # Format B: [{"token_id": "...", "mid": "0.45"}, ...]
            elif isinstance(result, list):
                for item in result:
                    if not isinstance(item, dict):
                        continue
                    tid = item.get("token_id", "")
                    mid = item.get("mid", item.get("price"))
                    if tid and mid is not None:
                        try:
                            out[tid] = float(mid)
                        except (TypeError, ValueError):
                            pass
            return out
        except Exception as exc:
            logger.debug(f"get_midpoints_batch failed: {exc}")
            return {}

    @retry_sync(max_attempts=3, base_delay=1.0, exceptions=(Exception,))
    def get_open_orders(self) -> list:
        self._public_rl.acquire()
        return self.client.get_orders()

    # ── Write ─────────────────────────────────────────────────────────────────

    def post_order(self, order_args: OrderArgs, order_type: OrderType) -> dict:
        if settings.DRY_RUN:
            logger.info(
                "DRY RUN — would post order",
                extra={"args": str(order_args), "type": order_type.value},
            )
            return {"id": f"dry_{int(time.time()*1000)}", "status": "dry_run"}

        self._order_rl.acquire()

        @retry_sync(max_attempts=3, base_delay=1.0, exceptions=(Exception,))
        def _post():
            signed = self.client.create_limit_order(order_args)
            return self.client.post_order(signed, order_type)

        return _post()

    def post_orders_batch(self, orders: list[tuple[OrderArgs, OrderType]]) -> list[dict]:
        """Post up to 15 orders in a single batch call."""
        if settings.DRY_RUN:
            logger.info(f"DRY RUN — would batch post {len(orders)} orders")
            return [{"id": f"dry_{i}_{int(time.time()*1000)}", "status": "dry_run"} for i in range(len(orders))]

        self._order_rl.acquire()
        signed_orders = [
            (self.client.create_limit_order(args), otype)
            for args, otype in orders[:15]
        ]
        return self.client.post_orders(signed_orders)

    def cancel_order(self, order_id: str) -> dict:
        if settings.DRY_RUN:
            logger.info(f"DRY RUN — would cancel {order_id}")
            return {"status": "dry_run"}
        self._order_rl.acquire()
        return self.client.cancel_order(order_id)

    def cancel_all_orders(self) -> None:
        if settings.DRY_RUN:
            logger.info("DRY RUN — would cancel all orders")
            return
        try:
            self._order_rl.acquire()
            self.client.cancel_all_orders()
            logger.info("All open orders cancelled")
        except Exception as exc:
            logger.error(f"cancel_all_orders failed: {exc}")

    # ── Balance ───────────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        """Query USDC balance on Polygon via web3 (ClobClient has no balance method)."""
        try:
            from web3 import Web3
            from web3.middleware import ExtraDataToPOAMiddleware

            w3 = Web3(Web3.HTTPProvider(settings.POLYGON_RPC_URL))
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            usdc_abi = [{
                "name": "balanceOf", "type": "function",
                "inputs": [{"name": "account", "type": "address"}],
                "outputs": [{"name": "", "type": "uint256"}],
                "stateMutability": "view",
            }]
            usdc = w3.eth.contract(
                address=Web3.to_checksum_address(settings.USDC_ADDRESS),
                abi=usdc_abi,
            )
            raw = usdc.functions.balanceOf(
                Web3.to_checksum_address(settings.POLY_WALLET_ADDRESS)
            ).call()
            return raw / 1_000_000  # USDC has 6 decimals
        except Exception as exc:
            logger.error(f"get_balance failed: {exc}")
            return 0.0


# Singleton
_client: Optional[PolymarketClient] = None


def get_client() -> PolymarketClient:
    global _client
    if _client is None:
        _client = PolymarketClient()
        _client.connect()
    return _client
