#!/usr/bin/env python3
"""
Simple backtester: replays historical Polymarket data against strategy logic.
Uses the Data API to fetch resolved market history.
"""
import sys
import json
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import aiohttp
from config import settings


async def fetch_resolved_markets(days: int = 30) -> list[dict]:
    url = f"{settings.GAMMA_API_URL}/markets"
    params = {
        "active": "false",
        "closed": "true",
        "limit": 200,
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                print(f"Gamma API error: {resp.status}")
                return []
            data = await resp.json()
            return data if isinstance(data, list) else data.get("data", [])


async def fetch_price_history(token_id: str) -> list[dict]:
    url = f"{settings.CLOB_BASE_URL}/prices-history"
    params = {
        "market": token_id,
        "interval": "1m",
        "fidelity": 1,
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return data.get("history", [])


def simulate_mm_strategy(price_history: list[dict], spread: float = 0.04) -> float:
    """
    Simulate market making on a price series.
    Returns estimated PnL from spread capture.
    """
    pnl = 0.0
    position = 0.0
    avg_price = 0.0

    for i in range(1, len(price_history)):
        price = float(price_history[i].get("p", 0.5))
        prev = float(price_history[i - 1].get("p", 0.5))
        bid = prev - spread / 2
        ask = prev + spread / 2

        # Simulate fill at bid/ask
        if price <= bid and position >= 0:
            pnl -= bid  # bought at bid
            position += 1
            avg_price = bid
        elif price >= ask and position > 0:
            pnl += ask  # sold at ask
            position -= 1

    # Close remaining position at last price
    if position > 0 and price_history:
        last_price = float(price_history[-1].get("p", 0.5))
        pnl += last_price * position

    return pnl


async def main():
    print("Fetching resolved markets…")
    markets = await fetch_resolved_markets(days=30)
    print(f"Found {len(markets)} resolved markets")

    total_pnl = 0.0
    tested = 0

    for market in markets[:20]:  # test first 20
        tokens = market.get("tokens", [])
        if not tokens:
            continue
        token_id = tokens[0].get("token_id", "")
        question = market.get("question", "")[:50]

        history = await fetch_price_history(token_id)
        if len(history) < 10:
            continue

        pnl = simulate_mm_strategy(history)
        total_pnl += pnl
        tested += 1
        print(f"  {question}… MM PnL={pnl:+.4f}")

    print(f"\nBacktest ({tested} markets): Total simulated PnL = {total_pnl:+.4f} USDC")
    print("Note: does not account for gas, spread slippage, or adverse selection.")


if __name__ == "__main__":
    asyncio.run(main())
