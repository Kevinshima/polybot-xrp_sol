"""
FRED (Federal Reserve Economic Data) — free API key required.
Get a free key (instant) at: https://fred.stlouisfed.org/docs/api/api_key.html
Add to .env: FRED_API_KEY=your_32_char_key

Official US government economic data used to anchor Polymarket economic questions.

Covers:
  - CPI (inflation): CPIAUCSL — monthly, year-over-year % change
  - PCE (Fed's preferred inflation): PCEPI
  - Unemployment rate: UNRATE
  - Nonfarm payrolls (monthly change): PAYEMS
  - GDP growth (quarterly): A191RL1Q225SBEA
  - 10-year Treasury yield: DGS10

Provides:
  get_fred_series(series_id)  → latest value + previous value
  get_cpi_yoy()               → CPI year-over-year % change
  get_unemployment_rate()     → latest unemployment %
  evaluate_econ_question(q)   → (fair_probability, reasoning) or (None, None)

Cache: 1 hour — FRED data is released monthly, intraday freshness not needed.
"""
from __future__ import annotations

import os
import re
import time
from typing import Optional

import aiohttp

from utils.logger import logger

_CACHE: dict = {}
_CACHE_TTL = 3600.0  # 1 hour — economic data changes at most monthly

_FRED_API_BASE = "https://api.stlouisfed.org/fred/series/observations"

# Series we care about
_SERIES = {
    "CPI":          "CPIAUCSL",    # Consumer Price Index, All Urban Consumers
    "PCE":          "PCEPI",       # Personal Consumption Expenditures Price Index
    "UNEMPLOYMENT": "UNRATE",      # Unemployment Rate (%)
    "PAYROLLS":     "PAYEMS",      # Nonfarm Payrolls (thousands)
    "GDP":          "A191RL1Q225SBEA",  # Real GDP growth rate (quarterly %)
    "YIELD_10Y":    "DGS10",       # 10-Year Treasury Yield (%)
    "FED_FUNDS":    "FEDFUNDS",    # Effective Fed Funds Rate (%)
}


async def _fetch_series(series_id: str) -> list[dict]:
    """
    Fetch a FRED series as [{date, value}, ...] sorted oldest→newest.
    Returns empty list on failure or missing API key.
    """
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        logger.debug("FRED: FRED_API_KEY not set — skipping")
        return []

    cache_key = f"fred:{series_id}"
    cached = _CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _CACHE_TTL:
        return cached["data"]

    try:
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "limit": 24,        # last 24 observations (2 years for monthly data)
            "sort_order": "asc",
        }
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(_FRED_API_BASE, params=params, timeout=timeout) as resp:
                if resp.status != 200:
                    logger.debug(f"FRED: HTTP {resp.status} for {series_id}")
                    return []
                data = await resp.json(content_type=None)

        # FRED API returns {"observations": [{"date": "...", "value": "..."}, ...]}
        raw_obs = data.get("observations") or []
        observations = []
        for obs in raw_obs:
            try:
                v = float(obs["value"])  # raises if value is "." (not yet released)
                observations.append({"date": obs["date"], "value": v})
            except (TypeError, ValueError, KeyError):
                pass

        _CACHE[cache_key] = {"ts": time.time(), "data": observations}
        logger.debug(f"FRED: fetched {len(observations)} obs for {series_id}")
        return observations

    except Exception as exc:
        logger.debug(f"FRED: fetch failed for {series_id}: {exc}")
        return []


async def get_fred_latest(series_key: str) -> Optional[tuple[float, float]]:
    """
    Returns (latest_value, previous_value) for the given series key.
    Series keys: CPI, PCE, UNEMPLOYMENT, PAYROLLS, GDP, YIELD_10Y, FED_FUNDS
    Returns None if data unavailable.
    """
    series_id = _SERIES.get(series_key.upper())
    if not series_id:
        return None

    obs = await _fetch_series(series_id)
    if len(obs) < 2:
        return None

    return obs[-1]["value"], obs[-2]["value"]


async def get_cpi_yoy() -> Optional[float]:
    """
    Returns CPI year-over-year % change (latest 12-month inflation rate).
    """
    obs = await _fetch_series(_SERIES["CPI"])
    if len(obs) < 13:
        return None
    latest = obs[-1]["value"]
    year_ago = obs[-13]["value"]
    if year_ago == 0:
        return None
    return ((latest - year_ago) / year_ago) * 100.0


async def get_unemployment_rate() -> Optional[float]:
    """Returns latest unemployment rate (%)."""
    result = await get_fred_latest("UNEMPLOYMENT")
    return result[0] if result else None


# ── Sigmoid helper ─────────────────────────────────────────────────────────────

def _sigmoid_prob(value: float, target: float, direction: str, scale: float = 1.0) -> float:
    """
    Probability that `value` is above/below `target` given continuous data.
    Uses a soft sigmoid so we grade out gradually instead of cliff-edge.

    direction: "above" → P(actual > target), "below" → P(actual < target)
    scale: controls how steep the transition is (higher = sharper)
    """
    import math
    dist = (value - target) / (abs(target) * 0.05 + 0.001)  # normalized distance
    raw = 1.0 / (1.0 + math.exp(-dist * scale))
    return raw if direction == "above" else (1.0 - raw)


# ── Main evaluator ─────────────────────────────────────────────────────────────

async def evaluate_econ_question(question: str, current_price: float) -> tuple[Optional[float], Optional[str]]:
    """
    Try to compute a fair probability for a Polymarket economic question
    using real FRED data.

    Returns (fair_probability, reasoning_string) or (None, None) if no match.

    Matches patterns like:
      - "Will CPI exceed 3.5% in March?"
      - "Will inflation be above 4%?"
      - "Will unemployment rise above 4.5%?"
      - "Will unemployment fall below 4%?"
      - "Will nonfarm payrolls exceed 200K?"
      - "Will the 10-year yield exceed 5%?"
    """
    q = question.lower()

    # ── CPI / Inflation ───────────────────────────────────────────────────────
    is_cpi = any(w in q for w in ("cpi", "inflation", "consumer price"))
    if is_cpi:
        target_match = re.search(r'(\d+(?:\.\d+)?)\s*%', question)
        if target_match:
            target = float(target_match.group(1))
            actual = await get_cpi_yoy()
            if actual is not None:
                is_above = any(w in q for w in ("above", "exceed", "over", "higher", "more than"))
                is_below = any(w in q for w in ("below", "under", "less than", "lower", "fall"))
                if is_above or is_below:
                    direction = "above" if is_above else "below"
                    fair_prob = _sigmoid_prob(actual, target, direction, scale=3.0)
                    reasoning = f"CPI YoY={actual:.2f}% vs target {target}% ({direction})"
                    logger.info(f"FRED eval: {reasoning} → P={fair_prob:.2f}")
                    return fair_prob, reasoning

    # ── Unemployment ──────────────────────────────────────────────────────────
    is_unemp = any(w in q for w in ("unemployment", "jobless", "jobs report"))
    if is_unemp:
        target_match = re.search(r'(\d+(?:\.\d+)?)\s*%', question)
        if target_match:
            target = float(target_match.group(1))
            actual = await get_unemployment_rate()
            if actual is not None:
                is_above = any(w in q for w in ("above", "exceed", "over", "rise above", "higher"))
                is_below = any(w in q for w in ("below", "under", "fall below", "drop below", "lower"))
                if is_above or is_below:
                    direction = "above" if is_above else "below"
                    fair_prob = _sigmoid_prob(actual, target, direction, scale=3.0)
                    reasoning = f"Unemployment={actual:.1f}% vs target {target}% ({direction})"
                    logger.info(f"FRED eval: {reasoning} → P={fair_prob:.2f}")
                    return fair_prob, reasoning

    # ── 10-Year Treasury Yield ────────────────────────────────────────────────
    is_yield = any(w in q for w in ("10-year", "10 year", "treasury yield", "10yr", "ten year"))
    if is_yield:
        target_match = re.search(r'(\d+(?:\.\d+)?)\s*%', question)
        if target_match:
            target = float(target_match.group(1))
            result = await get_fred_latest("YIELD_10Y")
            if result:
                actual = result[0]
                is_above = any(w in q for w in ("above", "exceed", "over", "higher"))
                is_below = any(w in q for w in ("below", "under", "lower", "fall"))
                if is_above or is_below:
                    direction = "above" if is_above else "below"
                    fair_prob = _sigmoid_prob(actual, target, direction, scale=4.0)
                    reasoning = f"10Y yield={actual:.2f}% vs target {target}% ({direction})"
                    logger.info(f"FRED eval: {reasoning} → P={fair_prob:.2f}")
                    return fair_prob, reasoning

    # ── Nonfarm Payrolls ──────────────────────────────────────────────────────
    is_payrolls = any(w in q for w in ("nonfarm", "payroll", "jobs added", "job creation"))
    if is_payrolls:
        # Match "200K", "200,000", "200k"
        target_match = re.search(r'(\d[\d,]*)\s*[kK]?', question)
        if target_match:
            raw = target_match.group(1).replace(",", "")
            multiplier = 1000 if re.search(r'\d\s*[kK]', target_match.group(0)) else 1
            try:
                target_thousands = float(raw) * multiplier / 1000
                result = await get_fred_latest("PAYROLLS")
                if result:
                    # PAYEMS is in thousands — monthly change = latest - previous
                    monthly_change = result[0] - result[1]
                    is_above = any(w in q for w in ("above", "exceed", "over", "more than"))
                    is_below = any(w in q for w in ("below", "under", "less than", "fewer"))
                    if is_above or is_below:
                        direction = "above" if is_above else "below"
                        fair_prob = _sigmoid_prob(monthly_change, target_thousands, direction, scale=2.0)
                        reasoning = f"Payrolls change={monthly_change:.0f}K vs target {target_thousands:.0f}K ({direction})"
                        logger.info(f"FRED eval: {reasoning} → P={fair_prob:.2f}")
                        return fair_prob, reasoning
            except (ValueError, TypeError):
                pass

    return None, None
