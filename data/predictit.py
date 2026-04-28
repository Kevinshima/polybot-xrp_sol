"""
PredictIt public market data — free, no API key, no auth.
Returns real-money crowd probabilities for US politics markets.

PredictIt is a real-money regulated prediction exchange focused on US politics:
government shutdowns, Trump approval, legislation, elections, Fed nominees, etc.
Real money = higher signal quality than Manifold's play-money.

Provides:
  get_predictit_probability(question) → float | None

Cache: 5 minutes for full market list (~300 markets, ~200KB response).
Returns a value only when:
  - Binary market (Yes/No single-outcome) — skips horse-race multi-candidate markets
  - ≥ 35% word overlap between Polymarket question and PredictIt market name
  - Has a valid last trade price > 0
"""
from __future__ import annotations

import re
import time
from typing import Optional

import aiohttp

from utils.logger import logger

_MARKETS_CACHE: dict = {}   # {"ts": float, "data": list[dict]}
_RESULT_CACHE: dict = {}    # per-question cache
_MARKETS_TTL = 300.0        # 5 min — full market list
_RESULT_TTL  = 300.0        # 5 min — per-question results

_PREDICTIT_API = "https://www.predictit.org/api/marketdata/all/"

_STOP = frozenset({
    "will", "the", "a", "an", "in", "on", "at", "by", "to", "of", "for",
    "be", "is", "are", "was", "were", "has", "have", "had", "that", "this",
    "it", "its", "or", "and", "not", "from", "with", "as", "2026", "2025",
    "2027", "do", "does", "did", "would", "could", "should", "may",
    "before", "after", "during", "between", "through",
})


def _meaningful_words(text: str) -> set[str]:
    return set(re.findall(r'\b\w{3,}\b', text.lower())) - _STOP


def _word_overlap(query: str, title: str) -> float:
    """Fraction of meaningful words in `query` that appear in `title`."""
    wq = _meaningful_words(query)
    wt = _meaningful_words(title)
    if not wq:
        return 0.0
    return len(wq & wt) / len(wq)


def _is_binary_market(contracts: list[dict]) -> bool:
    """
    A binary market has exactly 1 or 2 contracts where one is named Yes/No.
    Multi-candidate horse-race markets (Biden vs Trump vs X) are excluded.
    """
    if not contracts:
        return False
    names = {str(c.get("name") or "").strip().lower() for c in contracts}
    if len(contracts) == 1:
        return True  # only one outcome
    if names <= {"yes", "no"}:
        return True
    return False


def _yes_price(contracts: list[dict]) -> Optional[float]:
    """
    Return the Yes contract's last trade price for a binary market.
    For a single-contract market, returns that contract's price.
    """
    for c in contracts:
        name = str(c.get("name") or "").strip().lower()
        if name in ("yes", ""):
            price = c.get("lastTradePrice")
            if price is not None and float(price) > 0:
                return float(price)

    # Single contract — return whatever price is there
    if len(contracts) == 1:
        price = contracts[0].get("lastTradePrice")
        if price is not None and float(price) > 0:
            return float(price)

    return None


async def _fetch_all_markets() -> list[dict]:
    """Fetch and cache the full PredictIt market list."""
    now = time.time()
    cached = _MARKETS_CACHE.get("all")
    if cached and now - cached["ts"] < _MARKETS_TTL:
        return cached["data"]

    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(_PREDICTIT_API, timeout=timeout) as resp:
                if resp.status != 200:
                    logger.debug(f"PredictIt: HTTP {resp.status}")
                    return _MARKETS_CACHE.get("all", {}).get("data", [])
                data = await resp.json(content_type=None)

        markets = data.get("markets") or []
        _MARKETS_CACHE["all"] = {"ts": now, "data": markets}
        logger.debug(f"PredictIt: fetched {len(markets)} markets")
        return markets

    except Exception as exc:
        logger.debug(f"PredictIt: fetch failed: {exc}")
        return _MARKETS_CACHE.get("all", {}).get("data", [])


async def get_predictit_probability(question: str) -> Optional[float]:
    """
    Find the best-matching open binary PredictIt market and return its
    real-money crowd probability [0.0, 1.0], or None.

    Only works for binary (Yes/No) markets — skips candidate horse-races.
    Requires ≥ 35% word overlap between the Polymarket question and PredictIt name.
    """
    cache_key = question[:80]
    cached = _RESULT_CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _RESULT_TTL:
        return cached["data"]

    markets = await _fetch_all_markets()
    if not markets:
        return None

    best_market = None
    best_score = 0.0

    for market in markets:
        if market.get("status") != "Open":
            continue
        contracts = market.get("contracts") or []
        if not _is_binary_market(contracts):
            continue

        name = market.get("name") or ""
        score = _word_overlap(question, name)
        if score > best_score:
            best_score = score
            best_market = market

    if best_market is None or best_score < 0.35:
        logger.debug(f"PredictIt: no match for '{question[:60]}' (best={best_score:.2f})")
        _RESULT_CACHE[cache_key] = {"ts": time.time(), "data": None}
        return None

    price = _yes_price(best_market.get("contracts") or [])
    if price is None:
        _RESULT_CACHE[cache_key] = {"ts": time.time(), "data": None}
        return None

    logger.info(
        f"PredictIt: '{best_market.get('name','')}' "
        f"→ yes={price:.2f} overlap={best_score:.2f}"
    )
    _RESULT_CACHE[cache_key] = {"ts": time.time(), "data": price}
    return price
