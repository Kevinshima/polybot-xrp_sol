"""Gamma API market scanner — finds opportunities matching strategy criteria."""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from config import settings
from utils.logger import logger
from utils.circuit_breaker import CircuitBreaker

# One circuit breaker shared across all Gamma API calls in this scanner
_gamma_cb = CircuitBreaker(
    "gamma-api",
    failure_threshold=5,     # open after 5 consecutive failures
    recovery_timeout=60.0,   # probe again after 60 seconds
    success_threshold=2,
)


class MarketScanner:
    """
    Queries Gamma API to discover and filter Polymarket markets.
    Results are cached for 5 minutes to avoid hammering the API.
    """

    CACHE_TTL = 300  # seconds

    def __init__(self):
        self._cache: list[dict] = []
        self._cache_ts: float = 0.0
        self._last_logged_slug: dict[str, str] = {}

    async def get_active_markets(self) -> list[dict]:
        """Return all markets currently accepting orders."""
        if time.time() - self._cache_ts < self.CACHE_TTL:
            return self._cache

        markets = await self._fetch_markets()
        self._cache = markets
        self._cache_ts = time.time()
        return markets

    async def get_mm_candidates(self) -> list[dict]:
        """
        Markets suitable for market making:
        - acceptingOrders = true
        - not closed or archived
        - resolves within 7 days
        - 24h volume >= $10,000
        """
        from datetime import datetime, timezone, timedelta

        all_markets = await self.get_active_markets()
        now = datetime.now(timezone.utc)
        deadline = now + timedelta(days=30)

        candidates = []
        rewarded = 0

        for m in all_markets:
            if not m.get("acceptingOrders", False):
                continue
            if m.get("closed", False) or m.get("archived", False):
                continue

            # Must resolve within 30 days
            end_str = m.get("endDate") or m.get("endDateIso") or ""
            if not end_str:
                continue
            try:
                # Normalize: strip Z, drop timezone offset, parse as UTC
                normalized = end_str.rstrip("Z").split("+")[0].strip()
                end_dt = datetime.fromisoformat(normalized).replace(tzinfo=timezone.utc)
                if end_dt < now or end_dt > deadline:
                    continue
            except Exception:
                continue

            # Minimum 24h volume $10,000
            volume = float(m.get("volume24hr") or m.get("volume") or 0)
            if volume < 10_000:
                continue

            candidates.append(m)
            clob_rewards = m.get("clobRewards") or []
            if any(float(r.get("rewardsDailyRate", 0) or 0) > 0 for r in clob_rewards):
                rewarded += 1

        logger.info(f"MM candidates: {len(candidates)} markets ({rewarded} with active rewards)")
        return candidates

    # Asset keyword patterns for get_crypto_price_candidates().
    # Uses multi-word phrases to avoid 'eth'→'Netherlands', 'sol'→'resolution' false positives.
    # Add 'updown-15m' / 'up-or-down-15' here when Polymarket lists those slugs.
    _ASSET_PATTERNS: list[tuple[str, list[str]]] = [
        ("BTC", ["btc", "bitcoin"]),
        ("ETH", ["ethereum", "eth price", "eth above", "eth below", "eth hits", "will eth"]),
        ("SOL", ["solana", "sol price", "sol above", "sol below", "sol hits", "will sol"]),
    ]

    async def get_crypto_price_candidates(
        self, min_volume: float = 5_000
    ) -> dict[str, list[dict]]:
        """
        Returns {asset: [markets]} for BTC/ETH/SOL price outcome markets
        with 24h volume >= min_volume.

        NOTE: The target slugs 'updown-15m' / 'up-or-down-15' do not yet exist
        on Polymarket. This method currently matches the available BTC/ETH/SOL
        price milestone markets (e.g. 'Will Bitcoin hit $150k by ...'). When
        Polymarket adds 15-min updown markets, add their slug patterns to
        _ASSET_PATTERNS above and they will be picked up automatically.
        """
        all_markets = await self.get_active_markets()
        result: dict[str, list[dict]] = {"BTC": [], "ETH": [], "SOL": []}

        for m in all_markets:
            if not m.get("acceptingOrders", False):
                continue
            if m.get("closed", False) or m.get("archived", False):
                continue

            slug = (m.get("slug", "") or "").lower()
            q = (m.get("question", "") or "").lower()
            text = f"{slug} {q}"

            vol = float(m.get("volume24hr") or m.get("volume") or 0)
            if vol < min_volume:
                continue

            for asset, patterns in self._ASSET_PATTERNS:
                if any(p in text for p in patterns):
                    result[asset].append(m)
                    break  # assign to first matching asset only

        total = sum(len(v) for v in result.values())
        logger.info(
            f"Crypto price candidates: {total} markets "
            f"(BTC={len(result['BTC'])}, ETH={len(result['ETH'])}, SOL={len(result['SOL'])})"
        )
        return result

    async def get_updown_market_for(
        self, asset: str, window_minutes: int
    ) -> Optional[dict]:
        """
        Fetch the current up-down market for any asset and window size.

        Slug pattern: {asset}-updown-{window_minutes}m-{unix_timestamp}

        Tries the current interval first, then the next one (in case the
        current window is expiring). Returns a dict with keys:
          slug, market, up_token_id, down_token_id
        or None if no active market is found.
        """
        import json

        interval_secs = window_minutes * 60
        now_ts = int(time.time())
        ts = (now_ts // interval_secs) * interval_secs
        intervals = [ts, ts + interval_secs]

        for t in intervals:
            slug = f"{asset.lower()}-updown-{window_minutes}m-{t}"
            url = f"{settings.GAMMA_API_URL}/markets"

            # Skip immediately if Gamma API circuit is open
            if _gamma_cb.is_open:
                logger.debug(f"MarketScanner: circuit OPEN — skipping fetch for {slug}")
                continue

            async def _fetch_slug(slug=slug, url=url):
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        params={"slug": slug},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status != 200:
                            raise ValueError(f"HTTP {resp.status}")
                        return await resp.json()

            data = await _gamma_cb.call(_fetch_slug(), fallback=None)
            if data is None:
                continue

            try:
                markets = (
                    data
                    if isinstance(data, list)
                    else data.get("data", data.get("markets", []))
                )
                if not markets:
                    continue
                m = markets[0]
                if not m.get("acceptingOrders", False):
                    continue
                # Parse both token IDs: index 0 = UP, index 1 = DOWN
                raw = m.get("clobTokenIds", "")
                try:
                    ids = json.loads(raw) if isinstance(raw, str) else (raw or [])
                except Exception:
                    ids = []
                if len(ids) < 2:
                    logger.warning(
                        f"Updown market {slug} has fewer than 2 token IDs: {ids}"
                    )
                    continue
                if self._last_logged_slug.get(slug) != slug:
                    self._last_logged_slug[slug] = slug
                    logger.debug(f"Updown market found: {slug}")
                return {
                    "slug": slug,
                    "market": m,
                    "up_token_id": ids[0],
                    "down_token_id": ids[1],
                }
            except Exception as exc:
                logger.warning(f"Updown market parse failed for {slug}: {exc}")

        logger.debug(f"No active {asset} updown-{window_minutes}m market found")
        return None

    async def get_updown_market(self) -> Optional[dict]:
        """Fetch the current BTC 5-minute up-down market. Delegates to get_updown_market_for()."""
        return await self.get_updown_market_for("btc", 5)

    async def find_markets_for_keyword(self, keyword: str, max_results: int = 5) -> list[dict]:
        """Fuzzy-match markets by keyword in the question."""
        from rapidfuzz import fuzz

        all_markets = await self.get_active_markets()
        scored = []
        kw_lower = keyword.lower()
        for m in all_markets:
            question = m.get("question", "")
            score = fuzz.partial_ratio(kw_lower, question.lower())
            if score >= 50:
                scored.append((score, m))

        scored.sort(reverse=True, key=lambda x: x[0])
        return [m for _, m in scored[:max_results]]

    async def get_crypto_markets(self, asset: str) -> list[dict]:
        """Find markets related to a crypto asset (BTC/ETH/SOL price movements)."""
        return await self.find_markets_for_keyword(f"{asset} price", max_results=10)

    async def _fetch_markets(self) -> list[dict]:
        if _gamma_cb.is_open:
            logger.debug("MarketScanner: circuit OPEN — returning cached markets")
            return self._cache

        url = f"{settings.GAMMA_API_URL}/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": 500,
        }

        async def _do_fetch():
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        raise ValueError(f"Gamma API returned {resp.status}")
                    data = await resp.json()
                    if isinstance(data, list):
                        return data
                    return data.get("data", data.get("markets", []))

        result = await _gamma_cb.call(_do_fetch(), fallback=None)
        if result is None:
            return self._cache
        return result


_scanner: Optional[MarketScanner] = None


def get_scanner() -> MarketScanner:
    global _scanner
    if _scanner is None:
        _scanner = MarketScanner()
    return _scanner
