"""Order placement and lifecycle management. Every order goes through RiskManager."""
from __future__ import annotations

import time
import uuid
from typing import Optional

from py_clob_client.clob_types import OrderArgs, OrderType

from core.client import get_client
from core.risk_manager import get_risk_manager
from database.db import insert_trade, update_trade
from utils.logger import logger
from utils.helpers import round_price


class OrderManager:
    """
    All order placements MUST flow through this class.
    Assertion: risk manager approval is enforced before any network call.
    """

    def __init__(self):
        self._client = get_client()
        self._risk = get_risk_manager()

    def place_limit_order(
        self,
        strategy: str,
        market_id: str,
        token_id: str,
        question: str,
        side: str,        # "BUY" or "SELL"
        price: float,
        size_usdc: float,
        order_type: OrderType = OrderType.GTC,
    ) -> Optional[dict]:
        price = round_price(price)
        shares = size_usdc / price if price > 0 else 0

        approved, reason = self._risk.approve_order(side, size_usdc, market_id, strategy)
        assert approved, f"RiskManager rejected order: {reason}"

        from config import settings
        if settings.DRY_RUN:
            order_id = f"dry_lmt_{int(time.time() * 1000)}"
            insert_trade(
                trade_id=order_id,
                strategy=strategy,
                market_id=market_id,
                question=question,
                side=side,
                size=shares,
                price=price,
                status="open",
                dry_run=True,
            )
            logger.info(
                f"DRY RUN — limit order {side} {shares:.4f} shares @ {price:.3f} on {market_id}"
            )
            return {"id": order_id, "status": "dry_run"}

        order_args = OrderArgs(
            price=price,
            size=shares,
            side=side,
            token_id=token_id,
        )

        try:
            result = self._client.post_order(order_args, order_type)
            order_id = result.get("id", str(uuid.uuid4()))
            self._risk.record_order_placed(market_id, size_usdc)
            insert_trade(
                trade_id=order_id,
                strategy=strategy,
                market_id=market_id,
                question=question,
                side=side,
                size=shares,
                price=price,
                status="open",
            )
            logger.info(
                "Order placed",
                extra={
                    "id": order_id,
                    "strategy": strategy,
                    "market": market_id,
                    "side": side,
                    "price": price,
                    "size_usdc": size_usdc,
                },
            )
            return result
        except AssertionError:
            raise
        except Exception as exc:
            error_str = str(exc).lower()
            if "not accepting orders" in error_str:
                logger.warning(f"Market {market_id} not accepting orders — skipping")
                return None
            if "insufficient balance" in error_str:
                logger.critical("Insufficient balance! Halting bot.")
                self._risk.kill_all("insufficient balance")
                return None
            logger.error(f"Order placement failed: {exc}")
            return None

    def place_market_order(
        self,
        strategy: str,
        market_id: str,
        token_id: str,
        question: str,
        side: str,
        size_usdc: float,
        price: float = 0.0,
        asset: str = "BTC",
        momentum_at_entry: float | None = None,
        ob_imbalance_at_entry: float | None = None,
        trend_slope_at_entry: float | None = None,
        trend_direction_at_entry: str | None = None,
        consec_losses_at_entry: int | None = None,
        timeframe: str | None = None,
        ml_win_prob: float | None = None,
    ) -> Optional[dict]:
        """FOK market order for latency-sensitive strategies."""
        from py_clob_client.clob_types import MarketOrderArgs

        approved, reason = self._risk.approve_order(side, size_usdc, market_id, strategy)
        assert approved, f"RiskManager rejected order: {reason}"

        from config import settings
        try:
            if settings.DRY_RUN:
                order_id = f"dry_mkt_{int(time.time() * 1000)}"
                insert_trade(
                    trade_id=order_id,
                    strategy=strategy,
                    market_id=market_id,
                    question=question,
                    side=side,
                    size=size_usdc / price if price > 0 else size_usdc,
                    price=price,
                    fill_price=price,
                    status="filled",
                    dry_run=True,
                    asset=asset,
                    momentum_at_entry=momentum_at_entry,
                    ob_imbalance_at_entry=ob_imbalance_at_entry,
                    trend_slope_at_entry=trend_slope_at_entry,
                    trend_direction_at_entry=trend_direction_at_entry,
                    consec_losses_at_entry=consec_losses_at_entry,
                    timeframe=timeframe,
                    ml_win_prob=ml_win_prob,
                )
                logger.debug(f"DB insert OK — dry run trade {order_id} saved")
                logger.info(
                    f"DRY RUN — FOK market order {side} {size_usdc} USDC on {market_id}"
                )
                return {"id": order_id, "status": "dry_run"}

            order_args = MarketOrderArgs(token_id=token_id, amount=size_usdc)
            signed = self._client.client.create_market_order(order_args)
            result = self._client.client.post_order(signed, OrderType.FOK)
            order_id = result.get("id", str(uuid.uuid4()))
            self._risk.record_order_placed(market_id, size_usdc)
            insert_trade(
                trade_id=order_id,
                strategy=strategy,
                market_id=market_id,
                question=question,
                side=side,
                size=size_usdc,
                price=0.0,
                status="open",
                asset=asset,
                momentum_at_entry=momentum_at_entry,
                ob_imbalance_at_entry=ob_imbalance_at_entry,
                trend_slope_at_entry=trend_slope_at_entry,
                trend_direction_at_entry=trend_direction_at_entry,
                consec_losses_at_entry=consec_losses_at_entry,
                timeframe=timeframe,
                ml_win_prob=ml_win_prob,
            )
            return result
        except AssertionError:
            raise
        except Exception as exc:
            logger.error(f"Market order failed: {exc}")
            return None

    def cancel_order(self, order_id: str, market_id: str, size_usdc: float) -> bool:
        try:
            self._client.cancel_order(order_id)
            self._risk.record_order_cancelled(market_id, size_usdc)
            update_trade(order_id, fill_price=0.0, pnl=0.0, status="cancelled")
            logger.info(f"Order {order_id} cancelled")
            return True
        except Exception as exc:
            logger.error(f"cancel_order {order_id} failed: {exc}")
            return False

    def cancel_all(self) -> None:
        self._client.cancel_all_orders()

    def record_fill(self, order_id: str, fill_price: float, pnl: float) -> None:
        update_trade(order_id, fill_price=fill_price, pnl=pnl, status="filled")
        self._risk.record_fill(pnl)


# Singleton
_order_manager: Optional[OrderManager] = None


def get_order_manager() -> OrderManager:
    global _order_manager
    if _order_manager is None:
        _order_manager = OrderManager()
    return _order_manager
