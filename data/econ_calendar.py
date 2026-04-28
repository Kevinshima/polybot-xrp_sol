"""
Economic calendar and Fed rate-cut probability fetcher.
Uses Yahoo Finance 30-Day Fed Funds futures (ZQ contracts) — free, no API key.
Caches results for 10 minutes.

Provides:
  find_fomc_meeting(question_lower) → "YYYY-MM-DD" | None
  get_fed_cut_probability(meeting_date_str) → float | None   (0.0–1.0)
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Optional

import aiohttp

from utils.logger import logger

_CACHE: dict = {}
_CACHE_TTL = 600.0  # 10 minutes

# FOMC decision dates for 2026 (second day of each 2-day meeting)
# Source: Federal Reserve website — set in January each year
_FOMC_2026 = [
    "2026-01-29",
    "2026-03-19",
    "2026-05-07",
    "2026-06-18",
    "2026-07-29",
    "2026-09-17",
    "2026-10-29",
    "2026-12-10",
]

# Cached inferred current rate (fetched dynamically from current-month futures)
_inferred_rate: dict = {}  # {"rate": float, "ts": float}

# CME 30-Day Fed Funds futures month codes → Yahoo Finance ticker prefix
# Contract naming: ZQ + month_code + year_suffix + ".CBT"
_MONTH_CODE = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}


def _futures_ticker(year: int, month: int) -> str:
    code = _MONTH_CODE.get(month, "K")
    yr = str(year)[-2:]
    return f"ZQ{code}{yr}.CBT"


def _post_meeting_contract(meeting_date_str: str) -> tuple[int, int]:
    """
    For a given FOMC decision date, return (year, month) of the futures contract
    that best reflects the post-meeting rate.

    Convention (matching CME FedWatch):
    - If meeting falls in the first half of the month (day ≤ 15): use the month
      AFTER next (skip blend effect)
    - If meeting falls in second half (day > 15): use the following month
    """
    dt = datetime.strptime(meeting_date_str, "%Y-%m-%d")
    if dt.day > 15:
        m, y = dt.month + 1, dt.year
    else:
        m, y = dt.month + 2, dt.year
    if m > 12:
        m -= 12
        y += 1
    return y, m


async def _yahoo_price(ticker: str) -> Optional[float]:
    """Fetch last price using the Yahoo Finance chart API (v8, no auth required)."""
    url = (
        f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
        "?interval=1d&range=5d"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            timeout = aiohttp.ClientTimeout(total=8)
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status != 200:
                    logger.debug(f"econ: Yahoo chart HTTP {resp.status} for {ticker}")
                    return None
                data = await resp.json()
        meta = (
            (data.get("chart") or {})
            .get("result") or [{}]
        )[0].get("meta") or {}
        price = meta.get("regularMarketPrice") or meta.get("chartPreviousClose")
        return float(price) if price is not None else None
    except Exception as e:
        logger.debug(f"econ: Yahoo Finance chart error for {ticker}: {e}")
        return None


async def _get_current_rate() -> Optional[float]:
    """
    Infer the current Fed Funds rate from the present month's 30-Day futures contract.
    The current month's contract is essentially the known prevailing rate (it's nearly over).
    Caches for 1 hour.
    """
    now_ts = time.time()
    cached = _inferred_rate.get("ts", 0)
    if now_ts - cached < 3600 and "rate" in _inferred_rate:
        return _inferred_rate["rate"]

    now = datetime.utcnow()
    ticker = _futures_ticker(now.year, now.month)
    price = await _yahoo_price(ticker)
    if price is None:
        return None
    rate = 100.0 - price
    _inferred_rate["rate"] = rate
    _inferred_rate["ts"] = now_ts
    logger.debug(f"econ: inferred current Fed Funds rate = {rate:.3f}% from {ticker}")
    return rate


async def get_fed_cut_probability(meeting_date_str: str) -> Optional[float]:
    """
    Estimate P(at least one 25bp rate cut at this FOMC meeting) from futures.
    Uses the standard CME FedWatch formula with a dynamically inferred current rate.
    Returns [0.0, 1.0] or None if data is unavailable.
    """
    cache_key = f"fed:{meeting_date_str}"
    cached = _CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _CACHE_TTL:
        return cached["data"]

    current_rate = await _get_current_rate()
    if current_rate is None:
        return None

    year, month = _post_meeting_contract(meeting_date_str)
    ticker = _futures_ticker(year, month)
    price = await _yahoo_price(ticker)
    if price is None:
        _CACHE[cache_key] = {"ts": time.time(), "data": None}
        return None

    implied_rate = 100.0 - price
    # P(cut) = (current_rate - implied_rate) / 0.25  — standard FedWatch formula
    # Positive → market expects cuts; negative → market expects hikes
    prob = (current_rate - implied_rate) / 0.25
    prob = max(0.0, min(1.0, prob))

    logger.debug(
        f"econ: {ticker} price={price:.4f} implied={implied_rate:.3f}% "
        f"current={current_rate:.3f}% → P(cut @ {meeting_date_str})={prob:.2f}"
    )
    _CACHE[cache_key] = {"ts": time.time(), "data": prob}
    return prob


def find_fomc_meeting(question_lower: str) -> Optional[str]:
    """
    Detect which FOMC meeting a Polymarket question refers to.
    Returns YYYY-MM-DD or None.
    """
    now = datetime.utcnow()

    month_names = {
        "january": 1, "jan": 1, "february": 2, "feb": 2,
        "march": 3, "mar": 3, "april": 4, "apr": 4,
        "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
        "august": 8, "aug": 8, "september": 9, "sep": 9,
        "october": 10, "oct": 10, "november": 11, "nov": 11,
        "december": 12, "dec": 12,
    }

    # "next meeting" → first upcoming
    if "next meeting" in question_lower or "next fomc" in question_lower:
        for md in _FOMC_2026:
            if datetime.strptime(md, "%Y-%m-%d").date() >= now.date():
                return md

    for md in _FOMC_2026:
        dt = datetime.strptime(md, "%Y-%m-%d")
        if dt.date() < now.date():
            continue
        mfull = dt.strftime("%B").lower()   # "may"
        mshort = dt.strftime("%b").lower()  # "may"
        yr = dt.strftime("%Y")             # "2026"

        # Match "May 2026" or "May meeting" or just "May" (current year implied)
        if (mfull in question_lower or mshort in question_lower):
            if yr in question_lower or dt.year == now.year:
                return md

    return None
