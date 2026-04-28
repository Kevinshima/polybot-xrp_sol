"""
Weather forecast fetcher using Open-Meteo free API (no key required).
Caches per-city results for 15 minutes.

Provides:
  find_city(question_lower)            → city_key | None
  parse_date_from_text(text)           → "YYYY-MM-DD" | None
  get_city_forecast(city_key, date)    → {temp_max_c, temp_min_c, precip_prob_pct, snowfall_mm} | None
"""
from __future__ import annotations

import math
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import aiohttp

from utils.logger import logger

_CACHE: dict = {}
_CACHE_TTL = 900.0  # 15 minutes

_CITIES: dict[str, dict] = {
    "new york city": {"lat": 40.71, "lon": -74.01, "tz": "America/New_York"},
    "new york":      {"lat": 40.71, "lon": -74.01, "tz": "America/New_York"},
    "nyc":           {"lat": 40.71, "lon": -74.01, "tz": "America/New_York"},
    "chicago":       {"lat": 41.88, "lon": -87.63, "tz": "America/Chicago"},
    "los angeles":   {"lat": 34.05, "lon": -118.24, "tz": "America/Los_Angeles"},
    "san francisco": {"lat": 37.77, "lon": -122.42, "tz": "America/Los_Angeles"},
    "miami":         {"lat": 25.77, "lon": -80.19,  "tz": "America/New_York"},
    "phoenix":       {"lat": 33.45, "lon": -112.07, "tz": "America/Phoenix"},
    "houston":       {"lat": 29.76, "lon": -95.37,  "tz": "America/Chicago"},
    "dallas":        {"lat": 32.78, "lon": -96.80,  "tz": "America/Chicago"},
    "seattle":       {"lat": 47.61, "lon": -122.33, "tz": "America/Los_Angeles"},
    "denver":        {"lat": 39.74, "lon": -104.98, "tz": "America/Denver"},
    "boston":        {"lat": 42.36, "lon": -71.06,  "tz": "America/New_York"},
    "atlanta":       {"lat": 33.75, "lon": -84.39,  "tz": "America/New_York"},
    "minneapolis":   {"lat": 44.98, "lon": -93.27,  "tz": "America/Chicago"},
    "las vegas":     {"lat": 36.17, "lon": -115.14, "tz": "America/Los_Angeles"},
    "toronto":       {"lat": 43.65, "lon": -79.38,  "tz": "America/Toronto"},
    "london":        {"lat": 51.51, "lon":  -0.13,  "tz": "Europe/London"},
    "paris":         {"lat": 48.85, "lon":   2.35,  "tz": "Europe/Paris"},
    "berlin":        {"lat": 52.52, "lon":  13.40,  "tz": "Europe/Berlin"},
    "madrid":        {"lat": 40.42, "lon":  -3.70,  "tz": "Europe/Madrid"},
    "tokyo":         {"lat": 35.68, "lon": 139.69,  "tz": "Asia/Tokyo"},
    "sydney":        {"lat": -33.87, "lon": 151.21, "tz": "Australia/Sydney"},
    "dubai":         {"lat": 25.20, "lon":  55.27,  "tz": "Asia/Dubai"},
}

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def find_city(question_lower: str) -> Optional[str]:
    """Return the city key that matches the question — longest match wins."""
    best: Optional[str] = None
    best_len = 0
    for city in _CITIES:
        if city in question_lower and len(city) > best_len:
            best = city
            best_len = len(city)
    return best


def parse_date_from_text(text: str) -> Optional[str]:
    """
    Extract a calendar date from free-form text.
    Returns YYYY-MM-DD only if it falls within the next 10 days (forecast window).
    """
    text_lower = text.lower()
    now = datetime.utcnow()

    if "today" in text_lower:
        return now.strftime("%Y-%m-%d")
    if "tomorrow" in text_lower:
        return (now + timedelta(days=1)).strftime("%Y-%m-%d")

    for month_name, month_num in _MONTH_MAP.items():
        pattern = rf'\b{month_name}\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s*(\d{{4}}))?'
        m = re.search(pattern, text_lower)
        if m:
            day = int(m.group(1))
            year = int(m.group(2)) if m.group(2) else now.year
            try:
                dt = datetime(year, month_num, day)
                days_away = (dt.date() - now.date()).days
                if -1 <= days_away <= 10:
                    return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    return None


async def get_city_forecast(city_key: str, target_date: str) -> Optional[dict]:
    """
    Fetch daily forecast for a city on a specific date from Open-Meteo.
    Returns {temp_max_c, temp_min_c, precip_prob_pct, snowfall_mm} or None.
    """
    cache_key = f"{city_key}|{target_date}"
    cached = _CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _CACHE_TTL:
        return cached["data"]

    info = _CITIES.get(city_key)
    if not info:
        return None

    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={info['lat']}&longitude={info['lon']}"
        "&daily=temperature_2m_max,temperature_2m_min"
        ",precipitation_probability_max,snowfall_sum"
        "&forecast_days=10"
        f"&timezone={info['tz']}"
    )

    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            timeout = aiohttp.ClientTimeout(total=8)
            async with session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    logger.debug(f"weather: Open-Meteo HTTP {resp.status} for {city_key}")
                    _CACHE[cache_key] = {"ts": time.time(), "data": None}
                    return None
                data = await resp.json()

        daily = data.get("daily", {})
        dates = daily.get("time", [])
        if target_date not in dates:
            logger.debug(f"weather: {target_date} not in forecast window for {city_key}")
            _CACHE[cache_key] = {"ts": time.time(), "data": None}
            return None

        idx = dates.index(target_date)
        result = {
            "temp_max_c": float((daily.get("temperature_2m_max") or [20] * 10)[idx] or 20),
            "temp_min_c": float((daily.get("temperature_2m_min") or [10] * 10)[idx] or 10),
            "precip_prob_pct": float((daily.get("precipitation_probability_max") or [50] * 10)[idx] or 0),
            "snowfall_mm": float((daily.get("snowfall_sum") or [0] * 10)[idx] or 0),
        }
        logger.debug(
            f"weather: {city_key} {target_date} → "
            f"max={result['temp_max_c']:.1f}°C min={result['temp_min_c']:.1f}°C "
            f"rain={result['precip_prob_pct']:.0f}% snow={result['snowfall_mm']:.1f}mm"
        )
        _CACHE[cache_key] = {"ts": time.time(), "data": result}
        return result

    except Exception as e:
        logger.debug(f"weather: fetch error for {city_key}: {e}")
        _CACHE[cache_key] = {"ts": time.time(), "data": None}
        return None
