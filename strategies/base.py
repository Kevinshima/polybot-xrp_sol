"""Abstract base class for all strategies."""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Optional

from core.client import get_client
from core.order_manager import get_order_manager
from core.risk_manager import get_risk_manager
from core.portfolio import get_portfolio
from utils.logger import logger


class BaseStrategy(ABC):
    """
    All strategies must inherit from this class.
    Provides shared client/order/risk/portfolio access.
    """

    name: str = "base"

    def __init__(self, dry_run: bool = False):
        from config import settings
        self.dry_run = dry_run or settings.DRY_RUN
        self._client = get_client()
        self._orders = get_order_manager()
        self._risk = get_risk_manager()
        self._portfolio = get_portfolio()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @abstractmethod
    async def run(self) -> None:
        """Main strategy loop. Must run forever until stopped."""
        ...

    async def start(self) -> None:
        self._running = True
        logger.info(f"Strategy {self.name} starting")
        try:
            await self.run()
        except asyncio.CancelledError:
            logger.info(f"Strategy {self.name} cancelled")
        except Exception as exc:
            logger.exception(f"Strategy {self.name} crashed: {exc}")
            raise

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"Strategy {self.name} stopped")

    def _check_halted(self) -> bool:
        if self._risk.is_halted:
            logger.warning(f"{self.name}: risk manager halted — skipping")
            return True
        return False
