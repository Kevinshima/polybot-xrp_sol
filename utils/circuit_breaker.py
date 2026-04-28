"""
Circuit breaker for external HTTP calls.

States:
  CLOSED   — normal operation, calls pass through
  OPEN     — too many failures, calls are blocked and return None immediately
  HALF_OPEN — cooldown elapsed, one probe call allowed; success → CLOSED, failure → OPEN

Usage:
    cb = CircuitBreaker("gamma-api", failure_threshold=5, recovery_timeout=60)

    result = await cb.call(some_async_coro())
    # result is None if circuit is OPEN (caller should use cached data / skip)
"""
from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Any, Awaitable, Optional, TypeVar

from utils.logger import logger

T = TypeVar("T")


class _State(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """
    Async-safe circuit breaker.

    Args:
        name: Human-readable name for log messages.
        failure_threshold: Consecutive failures before opening.
        recovery_timeout: Seconds to wait before attempting a probe call.
        success_threshold: Consecutive successes in HALF_OPEN to close again.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
    ):
        self.name = name
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._success_threshold = success_threshold

        self._state = _State.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: float = 0.0
        self._lock = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        return self._state == _State.OPEN

    @property
    def state(self) -> str:
        return self._state.value

    async def call(self, coro: Awaitable[T], fallback: T = None) -> T:  # type: ignore[assignment]
        """
        Await `coro` through the breaker. Returns `fallback` if the circuit
        is OPEN or if the call raises an exception.

        The caller is responsible for passing an *unawaited* coroutine so the
        breaker can decide whether to run it at all.
        """
        async with self._lock:
            should_run = await self._check_state()

        if not should_run:
            # Circuit open — skip the call entirely
            try:
                coro.close()  # avoid "coroutine never awaited" warning
            except Exception:
                pass
            return fallback  # type: ignore[return-value]

        try:
            result = await coro
            await self._on_success()
            return result
        except Exception as exc:
            await self._on_failure(exc)
            return fallback  # type: ignore[return-value]

    # ── Internal state machine ────────────────────────────────────────────────

    async def _check_state(self) -> bool:
        """Returns True if the call should proceed."""
        if self._state == _State.CLOSED:
            return True

        if self._state == _State.OPEN:
            if time.monotonic() - self._opened_at >= self._recovery_timeout:
                self._state = _State.HALF_OPEN
                self._success_count = 0
                logger.info(f"CircuitBreaker [{self.name}]: HALF_OPEN — probing")
                return True
            return False

        # HALF_OPEN — allow one call through
        return True

    async def _on_success(self) -> None:
        async with self._lock:
            if self._state == _State.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._success_threshold:
                    self._state = _State.CLOSED
                    self._failure_count = 0
                    logger.info(f"CircuitBreaker [{self.name}]: CLOSED — recovered")
            elif self._state == _State.CLOSED:
                self._failure_count = 0

    async def _on_failure(self, exc: Exception) -> None:
        async with self._lock:
            self._failure_count += 1
            if self._state == _State.HALF_OPEN:
                # Probe failed — re-open immediately
                self._state = _State.OPEN
                self._opened_at = time.monotonic()
                logger.warning(
                    f"CircuitBreaker [{self.name}]: re-OPEN after probe failure: {exc}"
                )
            elif self._failure_count >= self._failure_threshold:
                self._state = _State.OPEN
                self._opened_at = time.monotonic()
                logger.error(
                    f"CircuitBreaker [{self.name}]: OPEN after {self._failure_count} failures "
                    f"(last: {exc}) — pausing for {self._recovery_timeout:.0f}s"
                )
