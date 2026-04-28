"""
Kalshi public market data — free, no API key, no auth required.
Kalshi is a regulated real-money US prediction exchange.
Higher signal quality than Manifold (real money at stake).

Covers: US politics, tariffs, congress, Fed, elections, macro events.
API base: https://api.elections.kalshi.com/trade-api/v2

Provides:
  get_kalshi_probability(question) → float | None

Returns the Yes mid-price [0.0, 1.0] or None when:
  - No matching open binary market found (< 35% word overlap)
  - Market has no valid price

Cache:
  - Full market list: 10 minutes (large response, ~300 markets)
  - Per-question result: 5 minutes
"""
from __future__ import annotations

import re
import time
from typing import Optional

import aiohttp

from utils.logger import logger

_MARKETS_CACHE: dict = {}
_RESULT_CACHE: dict = {}
_MARKETS_TTL = 600.0   # 10 min — market list
_RESULT_TTL  = 300.0   # 5 min — per-question

_KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Curated list of political/economic series tickers.
# Generic /markets endpoint returns sports parlays — we need specific series.
_POLITICS_SERIES = [
    # Tariffs / Trade
    "KXDISAPPROVETARIFF",   # Will Congress disapprove tariffs?
    "KXTARIFFBILL",         # Will new tariffs become law?
    "KXTRUMPSCOTUSVOTE",    # Supreme Court tariff decision
    # Congress / Legislation
    "KXCALLIMPEACHRCONGRESS",  # Republican Congressman impeachment
    "KXGREENLANDMILITARYBILL", # Military action bill
    "RSENATESEATS",         # Republican senate seats
    "SENATECO", "SENATEOK", "SENATEPA", "SENATEIN", "SENATEPARTYMN",
    # Fed / Rates
    "TERMINALRATE",         # Fed funds terminal rate
    "BARR",                 # Fed Vice Chairman confirmation
    "KXMORTGAGERATE",       # Mortgage rate
    # Trump administration
    "KXTRUMPADMINOFFICIAL", # People in Trump administration
    "KXBLOCKH1B",           # Courts block Trump H1B
    "KXTRUMPSAYMONTH",      # Trump Monthly
    # Elections
    "KXNJPRIMARY",          # NJ primary
    "KXTORONTOMAYOR",       # Toronto mayor
    "KXMAYORDETROIT",       # Detroit mayor
    "KXMAYORFW",            # Fort Worth mayor
    "KXCANNDPSEATS",        # Canada NDP seats
    "KXBULGARIAPRES",       # Bulgarian presidential election
    "PRESPARTYSTATE-PA",    # Pennsylvania presidential winner
    "KXITALYSENATE",        # Italy Senate
    # Sports — NBA Playoffs
    "KXNBAPLAYOFFS",        # NBA playoff advancement (series winner markets)
    "KXNBAFINALS",          # NBA Finals champion
    "KXNBACONFERENCE",      # Conference finals winner
    "NBAEASTFINALS",        # Eastern Conference Finals
    "NBAWESTFINALS",        # Western Conference Finals
    # Sports — Soccer / Champions League
    "KXUCLWINNER",          # UEFA Champions League winner
    "KXPREMIERLEAGUE",      # Premier League champion
    # Sports — Other
    "KXMLBWS",              # MLB World Series
    "KXNHLSTANLEY",         # NHL Stanley Cup
]

_STOP = frozenset({
    "will", "the", "a", "an", "in", "on", "at", "by", "to", "of", "for",
    "be", "is", "are", "was", "were", "has", "have", "had", "that", "this",
    "it", "its", "or", "and", "not", "from", "with", "as", "2026", "2025",
    "2027", "do", "does", "did", "would", "could", "should", "may",
    "before", "after", "during", "between", "through", "who", "what",
})


def _meaningful_words(text: str) -> set[str]:
    return set(re.findall(r'\b\w{3,}\b', text.lower())) - _STOP


def _word_overlap(query: str, title: str) -> float:
    wq = _meaningful_words(query)
    wt = _meaningful_words(title)
    if not wq:
        return 0.0
    return len(wq & wt) / len(wq)


async def _fetch_markets() -> list[dict]:
    """
    Fetch open Kalshi markets from curated political/economic series.
    The generic /markets endpoint returns sports parlays — we target specific
    series tickers for politics/trade/Fed/elections content.
    """
    cache_key = "kalshi:politics"
    cached = _MARKETS_CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _MARKETS_TTL:
        return cached["data"]

    all_markets: list[dict] = []

    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            timeout = aiohttp.ClientTimeout(total=15)
            for ticker in _POLITICS_SERIES:
                try:
                    params = {"status": "active", "series_ticker": ticker, "limit": 10}
                    async with session.get(
                        f"{_KALSHI_BASE}/markets",
                        params=params,
                        timeout=timeout,
                    ) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json(content_type=None)
                        all_markets.extend(data.get("markets") or [])
                except Exception:
                    continue

        _MARKETS_CACHE[cache_key] = {"ts": time.time(), "data": all_markets}
        logger.debug(f"Kalshi: fetched {len(all_markets)} politics/econ markets from {len(_POLITICS_SERIES)} series")

    except Exception as exc:
        logger.debug(f"Kalshi: fetch failed: {exc}")
        return _MARKETS_CACHE.get(cache_key, {}).get("data", [])

    return all_markets


def _yes_probability(market: dict) -> Optional[float]:
    """
    Extract the Yes probability from a Kalshi market.
    Uses yes_ask_dollars (ask price = probability in cents-per-dollar).
    Falls back to last_price_dollars if ask is missing.
    """
    yes_ask = market.get("yes_ask_dollars")
    if yes_ask is not None:
        try:
            return float(yes_ask)
        except (TypeError, ValueError):
            pass

    last = market.get("last_price_dollars")
    if last is not None:
        try:
            return float(last)
        except (TypeError, ValueError):
            pass

    return None


async def get_kalshi_probability(question: str) -> Optional[float]:
    """
    Find the best-matching open Kalshi market and return its
    real-money Yes probability [0.0, 1.0], or None.

    Requires ≥ 35% word overlap between Polymarket question and Kalshi title.
    """
    cache_key = question[:80]
    cached = _RESULT_CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _RESULT_TTL:
        return cached["data"]

    markets = await _fetch_markets()
    if not markets:
        _RESULT_CACHE[cache_key] = {"ts": time.time(), "data": None}
        return None

    best_market = None
    best_score = 0.0

    for market in markets:
        if market.get("status") not in ("open", "active"):
            continue
        title = market.get("title") or ""
        score = _word_overlap(question, title)
        if score > best_score:
            best_score = score
            best_market = market

    if best_market is None or best_score < 0.35:
        logger.debug(f"Kalshi: no match for '{question[:60]}' (best={best_score:.2f})")
        _RESULT_CACHE[cache_key] = {"ts": time.time(), "data": None}
        return None

    prob = _yes_probability(best_market)
    if prob is None:
        _RESULT_CACHE[cache_key] = {"ts": time.time(), "data": None}
        return None

    logger.info(
        f"Kalshi: '{best_market.get('title', '')[:60]}' "
        f"→ yes={prob:.2f} overlap={best_score:.2f}"
    )
    _RESULT_CACHE[cache_key] = {"ts": time.time(), "data": prob}
    return prob
