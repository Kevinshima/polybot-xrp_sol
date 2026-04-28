"""
Election polling / prediction market probability fetcher.
Uses Manifold Markets public API — free, no API key, no auth.
Caches results for 30 minutes.

Provides:
  get_election_probability(question) → float | None  (crowd forecast probability)

Only returns a value when:
  - ≥ 15 unique bettors on Manifold (sufficient crowd wisdom)
  - ≥ 40% word overlap between our question and the matched Manifold market
  - Market is still open
"""
from __future__ import annotations

import re
import time
from typing import Optional

import aiohttp

from utils.logger import logger

_CACHE: dict = {}
_CACHE_TTL = 1800.0  # 30 minutes

_MANIFOLD_API = "https://api.manifold.markets/v0/search-markets"

_STOP = frozenset({
    "will", "the", "a", "an", "in", "on", "at", "by", "to", "of", "for",
    "be", "is", "are", "was", "were", "has", "have", "had", "that", "this",
    "it", "its", "or", "and", "not", "from", "with", "as", "2026", "2025",
    "2027", "do", "does", "did", "would", "could", "should", "may",
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


def _build_search_term(question: str) -> str:
    """Top-8 meaningful words for the Manifold search."""
    words = [w for w in re.findall(r'\b\w+\b', question.lower()) if w not in _STOP]
    return " ".join(words[:8])


async def get_manifold_probability(
    question: str,
    min_bettors: int = 15,
    min_overlap: float = 0.40,
) -> Optional[float]:
    """
    Search Manifold Markets for the best-matching open binary market.
    Returns the crowd probability [0.0, 1.0] or None.

    Args:
        question:     The Polymarket question to match against Manifold titles.
        min_bettors:  Minimum unique bettors required (higher = more credible).
        min_overlap:  Minimum word-overlap fraction required (0–1).
    """
    search_term = _build_search_term(question)
    cache_key = f"manifold:{min_bettors}:{search_term[:55]}"
    cached = _CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _CACHE_TTL:
        return cached["data"]

    try:
        params = {
            "term": search_term,
            "limit": 5,
            "sort": "score",
        }
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(_MANIFOLD_API, params=params, timeout=timeout) as resp:
                if resp.status != 200:
                    logger.debug(f"polling: Manifold HTTP {resp.status} for '{search_term}'")
                    _CACHE[cache_key] = {"ts": time.time(), "data": None}
                    return None
                results = await resp.json()

        best_q = None
        best_score = 0.0

        for market in (results or []):
            # Only consider open binary markets with a probability
            if market.get("isResolved"):
                continue
            if market.get("outcomeType") not in ("BINARY", "PSEUDO_NUMERIC"):
                continue
            if market.get("probability") is None:
                continue

            title = market.get("question") or ""
            score = _word_overlap(question, title)
            if score > best_score:
                best_score = score
                best_q = market

        if best_q is None or best_score < min_overlap:
            logger.debug(f"polling: no Manifold match for '{search_term}' (best={best_score:.2f})")
            _CACHE[cache_key] = {"ts": time.time(), "data": None}
            return None

        bettors = int(best_q.get("uniqueBettorCount") or 0)
        if bettors < min_bettors:
            logger.debug(
                f"polling: Manifold '{best_q.get('question','')}' "
                f"only {bettors} bettors (need {min_bettors}) — skip"
            )
            _CACHE[cache_key] = {"ts": time.time(), "data": None}
            return None

        prob = float(best_q["probability"])
        logger.info(
            f"polling: Manifold '{best_q.get('question','')}' "
            f"({bettors} bettors) → prob={prob:.2f} overlap={best_score:.2f}"
        )
        _CACHE[cache_key] = {"ts": time.time(), "data": prob}
        return prob

    except Exception as e:
        logger.debug(f"polling: Manifold error for '{search_term}': {e}")
        _CACHE[cache_key] = {"ts": time.time(), "data": None}
        return None


# Backwards-compatible alias used by _try_direct_polling_eval
async def get_election_probability(question: str) -> Optional[float]:
    return await get_manifold_probability(question, min_bettors=15, min_overlap=0.40)
