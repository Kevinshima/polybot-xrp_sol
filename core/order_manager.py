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
        if not approved:
            raise AssertionError(f"RiskManager rejected order: {reason}")

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
        asset: str = "SOL",
        momentum_at_entry: float | None = None,
        ob_imbalance_at_entry: float | None = None,
        cvd_at_entry: float | None = None,
        trend_slope_at_entry: float | None = None,
        trend_direction_at_entry: str | None = None,
        consec_losses_at_entry: int | None = None,
        timeframe: str | None = None,
        ml_win_prob: float | None = None,
        momentum_delta: float | None = None,
        secs_since_trend_change: float | None = None,
        prev_trend_direction: str | None = None,
        entry_path: str | None = None,
        consec_wins: int | None = None,
        ob_at_queue_time: float | None = None,
        cross_asset_agree: int | None = None,
        asset_range_15m: float | None = None,
    ) -> Optional[dict]:
        """FOK market order for latency-sensitive strategies."""
        from py_clob_client.clob_types import MarketOrderArgs

        approved, reason = self._risk.approve_order(side, size_usdc, market_id, strategy)
        if not approved:
            raise AssertionError(f"RiskManager rejected order: {reason}")

        from config import settings
        try:
            if settings.DRY_RUN:
                order_id = f"dry_mkt_{int(time.time() * 1000)}"
                # Apply taker fee: DRY_RUN mirrors live trading economics.
                # Fetch real dynamic fee from Polymarket API (cached per token).
                # Falls back to TAKER_FEE_RATE if the API call fails.
                try:
                    _fee_bps = self._client.client.get_fee_rate_bps(token_id)
                    _fee_rate = _fee_bps / 10000 if _fee_bps else settings.TAKER_FEE_RATE
                except Exception:
                    _fee_rate = settings.TAKER_FEE_RATE
                # Polymarket fee formula: fee = collateral × feeRate × p × (1-p)
                # At p=0.455 with feeRate=0.10: effective ~2.5% of collateral (not flat 10%)
                _fee_usdc = size_usdc * _fee_rate * price * (1 - price) if price > 0 else 0.0
                _shares = ((size_usdc - _fee_usdc) / price) if price > 0 else size_usdc
                _effective_entry = (size_usdc / _shares) if _shares > 0 else price
                insert_trade(
                    trade_id=order_id,
                    strategy=strategy,
                    market_id=market_id,
                    question=question,
                    side=side,
                    size=_shares,
                    price=_effective_entry,
                    fill_price=_effective_entry,
                    status="filled",
                    dry_run=True,
                    asset=asset,
                    momentum_at_entry=momentum_at_entry,
                    ob_imbalance_at_entry=ob_imbalance_at_entry,
                    cvd_at_entry=cvd_at_entry,
                    trend_slope_at_entry=trend_slope_at_entry,
                    trend_direction_at_entry=trend_direction_at_entry,
                    consec_losses_at_entry=consec_losses_at_entry,
                    timeframe=timeframe,
                    ml_win_prob=ml_win_prob,
                    momentum_delta=momentum_delta,
                    secs_since_trend_change=secs_since_trend_change,
                    prev_trend_direction=prev_trend_direction,
                    entry_path=entry_path,
                    consec_wins=consec_wins,
                    ob_at_queue_time=ob_at_queue_time,
                    cross_asset_agree=cross_asset_agree,
                    asset_range_15m=asset_range_15m,
                )
                logger.debug(f"DB insert OK — dry run trade {order_id} saved")
                logger.info(
                    f"DRY RUN — FOK market order {side} {size_usdc} USDC on {market_id} "
                    f"(fee_rate={_fee_rate:.1%} fee_usdc={_fee_usdc:.3f} effective_entry={_effective_entry:.4f} shares={_shares:.4f})"
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
                momentum_delta=momentum_delta,
                secs_since_trend_change=secs_since_trend_change,
                prev_trend_direction=prev_trend_direction,
                entry_path=entry_path,
                consec_wins=consec_wins,
                ob_at_queue_time=ob_at_queue_time,
                cross_asset_agree=cross_asset_agree,
                asset_range_15m=asset_range_15m,
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
