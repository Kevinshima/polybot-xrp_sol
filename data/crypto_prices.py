"""
Live crypto price feed — Binance public REST API (no API key required).

Provides a simple cached price getter used to inject real-time context into
the Groq prompt so it can actually evaluate crypto price-target markets.

Fallback chain:
  1. Binance (wss-free REST, global)  — primary
  2. CoinGecko simple price API       — fallback (rate-limited to ~30 req/min free)
  3. Coinbase public price ticker     — last resort

Cache TTL: 30 seconds — fresh enough for entry decisions, won't spam APIs.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from utils.logger import logger

# ── Cache ─────────────────────────────────────────────────────────────────────
_CACHE_TTL = 30.0  # seconds

_price_cache: dict[str, float] = {}
_cache_ts: float = 0.0
_cache_lock = asyncio.Lock()

# Symbols to always fetch (covers every Polymarket crypto market we know about)
_SYMBOLS = {
    "BTC":  "BTCUSDT",
    "ETH":  "ETHUSDT",
    "SOL":  "SOLUSDT",
    "BNB":  "BNBUSDT",
    "XRP":  "XRPUSDT",
    "DOGE": "DOGEUSDT",
    "ADA":  "ADAUSDT",
    "AVAX": "AVAXUSDT",
    "LINK": "LINKUSDT",
    "DOT":  "DOTUSDT",
}

# Reverse map: Binance symbol → short ticker
_SYMBOL_TO_TICKER = {v: k for k, v in _SYMBOLS.items()}


async def get_crypto_prices(force_refresh: bool = False) -> dict[str, float]:
    """
    Returns {ticker: usd_price} for major crypto assets.
    Uses cached data if < 30s old. Thread-safe via asyncio.Lock.

    Example return value:
        {"BTC": 83420.5, "ETH": 1612.3, "SOL": 132.1, ...}
    """
    global _price_cache, _cache_ts

    async with _cache_lock:
        now = time.time()
        if not force_refresh and _price_cache and (now - _cache_ts) < _CACHE_TTL:
            return dict(_price_cache)

        prices = await _fetch_from_binance()
        if not prices:
            prices = await _fetch_from_coingecko()
        if not prices:
            prices = await _fetch_from_coinbase()

        if prices:
            _price_cache = prices
            _cache_ts = now
            logger.debug(
                f"CryptoPrices: refreshed — BTC=${prices.get('BTC', 0):,.0f} "
                f"ETH=${prices.get('ETH', 0):,.0f} SOL=${prices.get('SOL', 0):,.0f}"
            )
        else:
            logger.warning("CryptoPrices: all sources failed — returning stale cache")

        return dict(_price_cache)


def format_prices_for_prompt(prices: dict[str, float]) -> str:
    """
    Formats prices into a compact string for injecting into LLM prompts.

    Example: "BTC=$83,420 ETH=$1,612 SOL=$132 BNB=$608 XRP=$2.14"
    """
    if not prices:
        return ""
    parts = []
    for ticker in ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA"]:
        price = prices.get(ticker)
        if price is None:
            continue
        if price >= 1000:
            parts.append(f"{ticker}=${price:,.0f}")
        elif price >= 1:
            parts.append(f"{ticker}=${price:.2f}")
        else:
            parts.append(f"{ticker}=${price:.4f}")
    return " ".join(parts)


# ── Sources ───────────────────────────────────────────────────────────────────

async def _fetch_from_binance() -> dict[str, float]:
    """
    Binance ticker/price endpoint — returns all symbols in one call.
    Public, no API key, no rate limit concern at this frequency.
    """
    url = "https://api.binance.com/api/v3/ticker/price"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
                prices: dict[str, float] = {}
                for item in data:
                    symbol = item.get("symbol", "")
                    ticker = _SYMBOL_TO_TICKER.get(symbol)
                    if ticker:
                        try:
                            prices[ticker] = float(item["price"])
                        except (KeyError, ValueError):
                            pass
                return prices
    except Exception as exc:
        logger.debug(f"CryptoPrices Binance fetch failed: {exc}")
        return {}


async def _fetch_from_coingecko() -> dict[str, float]:
    """CoinGecko simple price — free tier, ~30 req/min."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    ids = "bitcoin,ethereum,solana,binancecoin,ripple,dogecoin,cardano,avalanche-2,chainlink,polkadot"
    id_to_ticker = {
        "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
        "binancecoin": "BNB", "ripple": "XRP", "dogecoin": "DOGE",
        "cardano": "ADA", "avalanche-2": "AVAX", "chainlink": "LINK",
        "polkadot": "DOT",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params={"ids": ids, "vs_currencies": "usd"},
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
                prices: dict[str, float] = {}
                for cg_id, ticker in id_to_ticker.items():
                    val = (data.get(cg_id) or {}).get("usd")
                    if val is not None:
                        prices[ticker] = float(val)
                return prices
    except Exception as exc:
        logger.debug(f"CryptoPrices CoinGecko fetch failed: {exc}")
        return {}


async def _fetch_from_coinbase() -> dict[str, float]:
    """Coinbase public spot price — last resort, one ticker at a time."""
    pairs = [("BTC", "BTC-USD"), ("ETH", "ETH-USD"), ("SOL", "SOL-USD")]
    prices: dict[str, float] = {}
    try:
        async with aiohttp.ClientSession() as session:
            for ticker, pair in pairs:
                try:
                    url = f"https://api.coinbase.com/v2/prices/{pair}/spot"
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            amount = (data.get("data") or {}).get("amount")
                            if amount:
                                prices[ticker] = float(amount)
                except Exception:
                    pass
    except Exception as exc:
        logger.debug(f"CryptoPrices Coinbase fetch failed: {exc}")
    return prices
