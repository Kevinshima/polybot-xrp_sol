"""Risk engine — every order MUST be approved here before execution."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime
from threading import Lock
from typing import Optional

from config import settings
from utils.logger import logger


class RiskManager:
    """
    Enforces all trading guardrails.
    Thread-safe via a Lock; also safe for asyncio via run_in_executor.
    """

    def __init__(self):
        self._lock = Lock()

        self.daily_loss_cap = settings.DAILY_LOSS_CAP_USDC
        self.max_position_usdc = settings.MAX_POSITION_SIZE_USDC
        self.max_open_orders = settings.MAX_OPEN_ORDERS

        self._open_orders: int = 0
        self._positions: dict[str, float] = {}   # market_id → exposure USDC
        self._halted: bool = False
        self._last_reset_day: int = self._today()

        # Restore today's closed PnL from DB so restarts don't reset the counter.
        # This means the daily loss cap check stays accurate across crashes.
        from database import db as _db
        try:
            self._daily_pnl: float = _db.get_daily_pnl()
        except Exception:
            self._daily_pnl = 0.0

    # ── Approval ──────────────────────────────────────────────────────────────

    def approve_order(
        self,
        side: str,
        size_usdc: float,
        market_id: str,
        strategy: str = "unknown",
    ) -> tuple[bool, str]:
        """
        Returns (approved: bool, reason: str).
        Every call to order_manager MUST check this first.
        """
        with self._lock:
            self._maybe_reset()

            if self._halted:
                return False, "Bot is halted (kill switch active)"

            if self._daily_pnl <= -self.daily_loss_cap:
                self._halted = True
                return False, f"Daily loss cap breached (${self._daily_pnl:.2f})"

            if self._open_orders >= self.max_open_orders:
                return False, f"Max open orders reached ({self.max_open_orders})"

            current_exposure = self._positions.get(market_id, 0.0)
            if current_exposure + size_usdc > self.max_position_usdc:
                return False, (
                    f"Position size limit: current={current_exposure:.2f} "
                    f"+ new={size_usdc:.2f} > max={self.max_position_usdc:.2f}"
                )

            if self._correlated_exposure(market_id) + size_usdc > self.max_position_usdc * 3:
                return False, "Correlated market concentration too high"

            logger.debug(
                "Order approved",
                extra={
                    "strategy": strategy,
                    "market": market_id,
                    "side": side,
                    "size_usdc": size_usdc,
                },
            )
            return True, "ok"

    def record_order_placed(self, market_id: str, size_usdc: float) -> None:
        with self._lock:
            self._open_orders += 1
            self._positions[market_id] = self._positions.get(market_id, 0.0) + size_usdc

    def record_order_cancelled(self, market_id: str, size_usdc: float) -> None:
        with self._lock:
            self._open_orders = max(0, self._open_orders - 1)
            self._positions[market_id] = max(
                0.0, self._positions.get(market_id, 0.0) - size_usdc
            )

    def record_fill(self, pnl_delta: float) -> None:
        with self._lock:
            self._daily_pnl += pnl_delta
            self._open_orders = max(0, self._open_orders - 1)
            logger.info(
                "Fill recorded",
                extra={"pnl_delta": pnl_delta, "daily_pnl": self._daily_pnl},
            )
            if self._daily_pnl <= -self.daily_loss_cap:
                logger.critical(
                    f"DAILY LOSS CAP BREACHED: {self._daily_pnl:.2f} USDC. "
                    "Bot halted."
                )
                self._halted = True

    def record_position_closed(self, market_id: str) -> None:
        with self._lock:
            self._positions.pop(market_id, None)

    # ── Kill Switch ───────────────────────────────────────────────────────────

    def kill_all(self, reason: str = "manual") -> None:
        with self._lock:
            self._halted = True
        logger.critical(f"KILL SWITCH ACTIVATED: {reason}")

    def resume(self) -> None:
        with self._lock:
            self._halted = False
        logger.warning("Risk manager resumed (halted flag cleared)")

    @property
    def is_halted(self) -> bool:
        return self._halted

    # ── PnL Getters ───────────────────────────────────────────────────────────

    def get_daily_pnl(self) -> float:
        """In-memory daily PnL; resets at midnight. Always current."""
        with self._lock:
            return self._daily_pnl

    def get_cumulative_pnl(self) -> float:
        """Cumulative PnL from the database (persists across restarts)."""
        from database import db as _db
        return _db.get_cumulative_pnl()

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        with self._lock:
            return {
                "daily_pnl": self._daily_pnl,
                "daily_loss_cap": self.daily_loss_cap,
                "open_orders": self._open_orders,
                "max_open_orders": self.max_open_orders,
                "halted": self._halted,
                "positions": dict(self._positions),
            }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _today(self) -> int:
        """Return local calendar date as an integer (YYYYMMDD) so the daily
        reset fires at local midnight rather than UTC midnight."""
        now = datetime.now()
        return now.year * 10000 + now.month * 100 + now.day

    def _maybe_reset(self) -> None:
        today = self._today()
        if today != self._last_reset_day:
            self.daily_reset()
            self._last_reset_day = today

    def daily_reset(self) -> None:
        logger.info(f"Daily reset — yesterday PnL: {self._daily_pnl:.2f} USDC")
        self._daily_pnl = 0.0
        # Don't reset halted here — operator must manually resume

    def _correlated_exposure(self, market_id: str) -> float:
        """
        Rough correlation check: sum exposure in markets with similar token.
        In practice, treat all open positions as correlated for simplicity.
        """
        return sum(self._positions.values())


# Singleton
_risk_manager: Optional[RiskManager] = None


def get_risk_manager() -> RiskManager:
    global _risk_manager
    if _risk_manager is None:
        _risk_manager = RiskManager()
    return _risk_manager
