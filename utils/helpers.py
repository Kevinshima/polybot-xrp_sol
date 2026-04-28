"""Utility helpers: retry, time, price math."""
import asyncio
import time
import functools
from typing import Callable, TypeVar, Any
from utils.logger import logger

T = TypeVar("T")


def retry_async(
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple = (Exception,),
):
    """Async exponential-backoff retry decorator."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts - 1:
                        break
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.debug(
                        f"{func.__name__} attempt {attempt+1}/{max_attempts} failed: {exc}. "
                        f"Retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


def retry_sync(
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple = (Exception,),
):
    """Sync exponential-backoff retry decorator."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts - 1:
                        break
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.debug(
                        f"{func.__name__} attempt {attempt+1}/{max_attempts} failed: {exc}. "
                        f"Retrying in {delay:.1f}s"
                    )
                    time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


def now_ms() -> int:
    """Current time as Unix milliseconds."""
    return int(time.time() * 1000)


def now_ts() -> int:
    """Current time as Unix seconds."""
    return int(time.time())


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def round_price(price: float, decimals: int = 4) -> float:
    return round(clamp(price, 0.01, 0.99), decimals)


def usdc_to_shares(usdc: float, price: float) -> float:
    """Convert USDC amount to shares at a given price."""
    if price <= 0:
        return 0.0
    return usdc / price


def shares_to_usdc(shares: float, price: float) -> float:
    """Convert shares to USDC value at a given price."""
    return shares * price


def pct_change(old: float, new: float) -> float:
    """Percentage change from old to new."""
    if old == 0:
        return 0.0
    return (new - old) / old


def extract_clob_token_id(market: dict) -> str:
    """
    Extract the YES-token CLOB token ID from a Gamma API market object.
    Gamma API returns clobTokenIds as a JSON-encoded string, e.g.:
      '["1234...", "5678..."]'
    Returns the first (YES) token ID, falling back to conditionId.
    """
    import json
    raw = market.get("clobTokenIds", "")
    fallback = market.get("conditionId") or market.get("id", "")
    if not raw:
        return fallback
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
        return ids[0] if ids else fallback
    except Exception:
        return fallback


def extract_clob_token_ids(market: dict) -> list[str]:
    """
    Extract all CLOB token IDs from a Gamma API market object.
    Returns an empty list when parsing fails.
    """
    import json

    raw = market.get("clobTokenIds", "")
    if not raw:
        return []
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
        return [str(token_id) for token_id in (ids or []) if str(token_id)]
    except Exception:
        return []
