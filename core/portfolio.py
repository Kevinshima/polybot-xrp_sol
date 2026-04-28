"""Position tracking and PnL calculation."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from py_clob_client.exceptions import PolyApiException

from config import settings
from core.client import get_client
from database import db
from utils.logger import logger


@dataclass
class Position:
    market_id: str
    token_id: str
    question: str
    strategy: str
    side: str
    size: float
    entry_price: float
    current_price: float = 0.0
    opened_at: int = field(default_factory=lambda: int(time.time()))
    metadata_json: str = ""

    @property
    def unrealized_pnl(self) -> float:
        if self.side == "BUY":
            return (self.current_price - self.entry_price) * self.size
        else:
            return (self.entry_price - self.current_price) * self.size

    @property
    def value_usdc(self) -> float:
        return self.current_price * self.size


class Portfolio:
    """In-memory position tracker, synced to SQLite."""

    def __init__(self):
        self._positions: dict[str, Position] = {}
        self._client = get_client()
        self._closed_markets: set[str] = set()  # token_ids that returned 404 — skip forever

    def add_position(
        self,
        market_id: str,
        token_id: str,
        question: str,
        strategy: str,
        side: str,
        size: float,
        entry_price: float,
        metadata_json: str = "",
        opened_at: Optional[int] = None,
    ) -> None:
        pos = Position(
            market_id=market_id,
            token_id=token_id,
            question=question,
            strategy=strategy,
            side=side,
            size=size,
            entry_price=entry_price,
            current_price=entry_price,
            metadata_json=metadata_json or "",
            opened_at=opened_at if opened_at is not None else int(time.time()),
        )
        self._positions[market_id] = pos
        db.upsert_position(
            market_id=market_id,
            token_id=token_id,
            question=question,
            strategy=strategy,
            side=side,
            size=size,
            entry_price=entry_price,
            current_price=entry_price,
            metadata_json=metadata_json or "",
        )
        logger.info(f"Position opened: {market_id[:20]}… {side} {size:.2f}@{entry_price:.3f}")

    def close_position(self, market_id: str, exit_price: float) -> Optional[float]:
        pos = self._positions.pop(market_id, None)
        if pos is None:
            return None
        pos.current_price = exit_price
        pnl = pos.unrealized_pnl
        db.remove_position(market_id)
        db.close_trade_by_market_id(
            market_id=market_id,
            strategy=pos.strategy,
            question=pos.question,
            side=pos.side,
            size=pos.size,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            pnl=pnl,
            dry_run=settings.DRY_RUN,
        )
        logger.info(
            f"Position closed: {market_id[:20]}… PnL={pnl:.4f} USDC"
        )
        return pnl

    def update_prices(self) -> None:
        """Refresh current_price for all open positions from CLOB."""
        for market_id, pos in list(self._positions.items()):
            token_or_market = pos.token_id or market_id
            if token_or_market in self._closed_markets:
                continue
            try:
                price = self._client.get_midpoint(token_or_market)
                pos.current_price = price
                db.upsert_position(
                    market_id=market_id,
                    token_id=pos.token_id,
                    question=pos.question,
                    strategy=pos.strategy,
                    side=pos.side,
                    size=pos.size,
                    entry_price=pos.entry_price,
                    current_price=price,
                    metadata_json=pos.metadata_json,
                )
            except PolyApiException as exc:
                if exc.status_code == 404:
                    self._closed_markets.add(token_or_market)
                    logger.debug(f"Price update skipped — market closed for {token_or_market[:20]}")
                else:
                    raise
            except Exception as exc:
                logger.warning(f"Price update failed for {market_id}: {exc}")

    def get_position(self, market_id: str) -> Optional[Position]:
        return self._positions.get(market_id)

    def has_position(self, market_id: str) -> bool:
        return market_id in self._positions

    def all_positions(self) -> list[Position]:
        return list(self._positions.values())

    def total_unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self._positions.values())

    def total_value(self) -> float:
        return sum(p.value_usdc for p in self._positions.values())


# Singleton
_portfolio: Optional[Portfolio] = None


def get_portfolio() -> Portfolio:
    global _portfolio
    if _portfolio is None:
        _portfolio = Portfolio()
    return _portfolio
