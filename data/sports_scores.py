"""
Free sports score fetcher using ESPN public APIs.
No API key required. Caches results for 60 seconds to avoid hammering the API.

Provides:
  get_finished_game_winners() → dict[team_name, "won"|"lost"]
  get_live_game_leaders()     → dict[team_name, {"status", "confidence", "detail"}]
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from utils.logger import logger

_CACHE: dict = {}
_CACHE_TTL = 60.0  # seconds

_ESPN_ENDPOINTS = [
    # NBA
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    # NFL
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    # MLB
    "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    # NHL
    "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    # MLS Soccer
    "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard",
    # Premier League
    "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard",
    # Champions League
    "https://site.api.espn.com/apis/site/v2/sports/soccer/UEFA.CHAMPIONS/scoreboard",
    # La Liga
    "https://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/scoreboard",
]

# How recently a game must have ended to be considered actionable (minutes)
_GAME_FRESH_MINUTES = 120


async def _fetch_scoreboard(url: str, session: aiohttp.ClientSession) -> Optional[dict]:
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        logger.debug(f"sports_scores: fetch failed for {url}: {e}")
    return None


def _parse_game_winners(payload: dict) -> dict[str, str]:
    """
    Parse an ESPN scoreboard payload and return {team_display_name: "won"|"lost"}
    for games that have recently finished.
    """
    results: dict[str, str] = {}
    now = time.time()
    cutoff = now - _GAME_FRESH_MINUTES * 60

    events = payload.get("events") or []
    for event in events:
        status = event.get("status", {})
        status_type = status.get("type", {})
        completed = status_type.get("completed", False)
        if not completed:
            continue

        # Check if game ended recently enough to be actionable
        date_str = event.get("date", "")
        if date_str:
            try:
                from datetime import datetime, timezone
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if dt.timestamp() < cutoff:
                    continue  # game ended too long ago — market already priced it
            except Exception:
                pass

        competitors = []
        for competition in event.get("competitions", []):
            competitors = competition.get("competitors", [])
            break

        if len(competitors) < 2:
            continue

        for comp in competitors:
            team = comp.get("team", {})
            name = team.get("displayName") or team.get("shortDisplayName") or team.get("abbreviation")
            if not name:
                continue
            winner = comp.get("winner")
            if winner is True:
                results[name] = "won"
            elif winner is False:
                results[name] = "lost"

    return results


def _parse_live_leaders(payload: dict) -> dict[str, dict]:
    """
    Parse an ESPN scoreboard payload and return live-game leaders who have a
    decisive advantage with little time left.

    Return format: {team_display_name: {"status": "likely_winning", "confidence": float, "detail": str}}

    Sport-specific thresholds (all require game to be in progress, not finished):
      NBA  — Q4 ≤ 5 min remaining, lead ≥ 15 pts  → conf=0.90
           — Q4 ≤ 2 min remaining, lead ≥ 10 pts  → conf=0.93
      NFL  — Q4 ≤ 2 min remaining, lead ≥ 14 pts  → conf=0.92
      NHL  — P3 ≤ 2 min remaining, lead ≥ 2 goals → conf=0.88
      Soccer (any) — 2nd half ≥ 80 min elapsed, lead ≥ 2 goals → conf=0.87
                   — "added time" (90+), lead ≥ 1 goal          → conf=0.85
    """
    results: dict[str, dict] = {}

    sport_slug = ""
    # Try to detect sport from league/season info in the payload
    sport_raw = (payload.get("leagues") or [{}])[0].get("slug", "") if payload.get("leagues") else ""
    if not sport_raw:
        sport_raw = (payload.get("leagues") or [{}])[0].get("name", "")

    events = payload.get("events") or []
    for event in events:
        status = event.get("status", {})
        status_type = status.get("type", {})

        # Only evaluate in-progress games
        if status_type.get("completed", False):
            continue
        if status_type.get("state", "") != "in":
            continue

        period = status.get("period", 0)
        clock_secs = float(status.get("clock", 0) or 0)  # seconds remaining (basketball/football/hockey)
        display_clock = status.get("displayClock", "")    # "4:05" remaining OR "87:00" elapsed (soccer)
        short_detail = status_type.get("shortDetail", "").lower()  # e.g. "4th - 4:05" or "2nd half - 87:00"

        # Detect sport by short_detail or sport_raw
        is_soccer = (
            "half" in short_detail
            or "soccer" in sport_raw.lower()
            or "football" in sport_raw.lower()
            or "eng.1" in sport_raw or "esp.1" in sport_raw
            or "champions" in sport_raw.lower()
            or "usa.1" in sport_raw
        )

        competitors = []
        for competition in event.get("competitions", []):
            competitors = competition.get("competitors", [])
            # Also try to detect sport from competition notes
            competition_type = competition.get("type", {}).get("slug", "")
            if "soccer" in competition_type or "football" in competition_type:
                is_soccer = True
            break

        if len(competitors) < 2:
            continue

        # Parse scores
        try:
            scores = []
            team_names = []
            for comp in competitors:
                raw_score = comp.get("score", "0") or "0"
                scores.append(float(raw_score))
                team = comp.get("team", {})
                name = team.get("displayName") or team.get("shortDisplayName") or team.get("abbreviation") or ""
                team_names.append(name)
            if len(scores) < 2 or not all(team_names):
                continue
        except (ValueError, TypeError):
            continue

        lead = abs(scores[0] - scores[1])
        leading_idx = 0 if scores[0] > scores[1] else 1
        trailing_idx = 1 - leading_idx
        leading_team = team_names[leading_idx]
        trailing_team = team_names[trailing_idx]

        if lead == 0:
            continue  # tied — no decisive leader

        conf = 0.0
        detail_str = ""

        if is_soccer:
            # For soccer, displayClock = elapsed time "87:00" or "90+4:00"
            elapsed_min = 0
            is_added_time = "90+" in display_clock or "+" in display_clock[:4]
            try:
                # "87:00" → 87, "90+3:00" → treat as 93
                if is_added_time:
                    elapsed_min = 93
                else:
                    elapsed_min = int(display_clock.split(":")[0])
            except Exception:
                elapsed_min = 0

            if period >= 2:  # second half
                if is_added_time and lead >= 1:
                    conf = 0.85
                    detail_str = f"added time, +{lead:.0f}"
                elif elapsed_min >= 80 and lead >= 2:
                    conf = 0.87
                    detail_str = f"{elapsed_min}', +{lead:.0f}"
        else:
            # Basketball, football, hockey — clock = seconds REMAINING in period
            # Detect sport by period count and score magnitude
            is_nba = (
                "nba" in sport_raw.lower()
                or (period <= 4 and max(scores) > 60)  # NBA scores are high
            )
            is_nfl = (
                "nfl" in sport_raw.lower()
                or (period <= 4 and max(scores) < 60 and max(scores) > 0 and lead % 7 == 0)
            )
            is_nhl = (
                "nhl" in sport_raw.lower()
                or (period <= 3 and max(scores) <= 10)
            )

            if is_nba and period == 4:
                if clock_secs <= 300 and lead >= 15:
                    conf = 0.90
                    detail_str = f"Q4 {display_clock}, +{lead:.0f}"
                elif clock_secs <= 120 and lead >= 10:
                    conf = 0.93
                    detail_str = f"Q4 {display_clock}, +{lead:.0f}"
            elif is_nfl and period == 4:
                if clock_secs <= 120 and lead >= 14:
                    conf = 0.92
                    detail_str = f"Q4 {display_clock}, +{lead:.0f}"
            elif is_nhl and period == 3:
                if clock_secs <= 120 and lead >= 2:
                    conf = 0.88
                    detail_str = f"P3 {display_clock}, +{lead:.0f}"

        if conf > 0 and detail_str and leading_team:
            results[leading_team] = {
                "status": "likely_winning",
                "confidence": conf,
                "detail": detail_str,
            }
            results[trailing_team] = {
                "status": "likely_losing",
                "confidence": conf,
                "detail": detail_str,
            }

    return results


async def get_live_game_leaders() -> dict[str, dict]:
    """
    Fetch in-progress games from ESPN with decisive scores in final minutes.
    Returns dict mapping team name → {"status", "confidence", "detail"}.
    Caches for 30 seconds (shorter than finished games — live scores change fast).
    """
    cache_key = "live_leaders"
    cached = _CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < 30.0:
        return cached["data"]

    all_leaders: dict[str, dict] = {}
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [_fetch_scoreboard(url, session) for url in _ESPN_ENDPOINTS]
            payloads = await asyncio.gather(*tasks, return_exceptions=True)
            for payload in payloads:
                if isinstance(payload, dict):
                    leaders = _parse_live_leaders(payload)
                    all_leaders.update(leaders)
    except Exception as e:
        logger.debug(f"sports_scores: error fetching live scoreboards: {e}")

    if all_leaders:
        logger.info(
            f"sports_scores: {len(all_leaders)} live decisive game leaders: "
            + str([(k, v['detail']) for k, v in list(all_leaders.items())[:4]])
        )

    _CACHE[cache_key] = {"ts": time.time(), "data": all_leaders}
    return all_leaders


async def get_finished_game_winners() -> dict[str, str]:
    """
    Fetch all recently finished game results from ESPN (free, no auth).
    Returns dict mapping team name → "won" or "lost".
    Caches for 60 seconds.
    """
    cache_key = "all_winners"
    cached = _CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _CACHE_TTL:
        return cached["data"]

    all_winners: dict[str, str] = {}
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [_fetch_scoreboard(url, session) for url in _ESPN_ENDPOINTS]
            payloads = await asyncio.gather(*tasks, return_exceptions=True)
            for payload in payloads:
                if isinstance(payload, dict):
                    winners = _parse_game_winners(payload)
                    all_winners.update(winners)
    except Exception as e:
        logger.debug(f"sports_scores: error fetching scoreboards: {e}")

    if all_winners:
        logger.debug(f"sports_scores: {len(all_winners)} recent game results: {list(all_winners.items())[:5]}")

    _CACHE[cache_key] = {"ts": time.time(), "data": all_winners}
    return all_winners
