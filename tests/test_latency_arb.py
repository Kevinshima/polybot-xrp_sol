"""Focused tests for latency arb branch logic and trend warmup."""
from __future__ import annotations

import asyncio
import time
from collections import deque
from types import SimpleNamespace

import pytest

from strategies.latency_arb import LatencyArb
import strategies.latency_arb as latency_module


def _build_strategy_shell() -> LatencyArb:
    strategy = object.__new__(LatencyArb)
    strategy._consecutive_losses = {"5m": 0, "15m": 0}
    strategy._loss_cooldown_until = {"5m": 0.0, "15m": 0.0}
    strategy._last_cooldown_log = {"5m": 0.0, "15m": 0.0}
    strategy._branch_limits = {"5m": 3, "15m": 3}
    strategy.MAX_CONCURRENT = 3
    strategy._trend_slope_threshold_pct = 0.3
    strategy._price_history = deque(maxlen=50)
    strategy._btc_signal_history = deque()
    strategy._trend_direction = None
    strategy._last_slope = 0.0
    strategy._last_warmup_log = 0.0
    strategy._last_warmup_sample_count = -1
    strategy._last_signal_sample_ts = 0.0
    strategy._last_trend_sample_ts = 0.0
    strategy._last_logged_trend = "WARMUP"
    strategy._rejection_log_state = {
        "5m": {"signature": None, "suppressed": 0, "last_log": 0.0},
        "15m": {"signature": None, "suppressed": 0, "last_log": 0.0},
    }
    strategy._weak_15m_log_state = {
        "signature": None,
        "suppressed": 0,
        "last_log": 0.0,
    }
    return strategy


@pytest.mark.parametrize(
    ("momentum", "threshold", "expected"),
    [
        (0.012, 0.010, "UP"),
        (-0.012, 0.010, "DOWN"),
        (0.009, 0.010, None),
        (-0.009, 0.010, None),
    ],
)
def test_direction_from_momentum_handles_no_signal(momentum, threshold, expected):
    strategy = _build_strategy_shell()
    assert strategy._direction_from_momentum(momentum, threshold) == expected


def test_cooldown_isolated_by_branch(monkeypatch):
    strategy = _build_strategy_shell()
    monkeypatch.setattr(latency_module.time, "time", lambda: 1000.0)

    strategy.on_loss("15m")
    assert strategy._consecutive_losses["15m"] == 1
    assert strategy._consecutive_losses["5m"] == 0
    assert strategy._loss_cooldown_until["15m"] == 0.0
    assert strategy._loss_cooldown_until["5m"] == 0.0

    strategy.on_loss("15m")
    assert strategy._consecutive_losses["15m"] == 0
    assert strategy._loss_cooldown_until["15m"] == pytest.approx(1600.0)
    assert strategy._loss_cooldown_until["5m"] == 0.0

    availability_5m = strategy._branch_trade_availability("5m", {"5m": 0, "15m": 0}, 0)
    availability_15m = strategy._branch_trade_availability("15m", {"5m": 0, "15m": 0}, 0)
    assert availability_5m[0] is True
    assert availability_5m[1] == "ready"
    assert availability_15m[0] is False
    assert availability_15m[1] == "cooldown_active"


def test_trend_warmup_ignores_invalid_prices(monkeypatch):
    strategy = _build_strategy_shell()
    monkeypatch.setattr(latency_module.settings, "TREND_FILTER_TICKS", 3)

    strategy._update_trend(0.0)
    assert list(strategy._price_history) == []
    assert strategy._trend_state_label() == "WARMUP"
    assert strategy._last_slope == 0.0

    strategy._update_trend(100000.0)
    strategy._update_trend(100010.0)
    assert len(strategy._price_history) == 2
    assert strategy._trend_state_label() == "WARMUP"
    assert strategy._last_slope == 0.0

    strategy._update_trend(100020.0)
    assert len(strategy._price_history) == 3
    assert strategy._trend_state_label() in {"UP", "FLAT"}


def test_tick_skips_15m_queue_and_confirmation_during_warmup(monkeypatch):
    strategy = _build_strategy_shell()
    strategy._traded_this_cycle = set()
    strategy._last_momentum_log = 0.0
    strategy._current_updowns = {
        "BTC_5m": {"slug": "btc-5m"},
        "BTC_15m": {"slug": "btc-15m"},
    }
    strategy._last_market_fetch = {"BTC_5m": 0.0, "BTC_15m": 0.0}
    strategy._cooldown = {"5m": {}, "15m": {}}
    strategy._pending_15m = {}
    strategy._entered_15m_slugs = set()
    strategy._last_15m_slug = ""
    strategy._holding_logged = set()
    strategy._last_15m_direction = None
    strategy._last_15m_direction_ts = None
    strategy._last_mid_discard_ts = {}
    strategy._exchange_feed = SimpleNamespace(
        get_momentum=lambda symbol: 0.02,
        get_price=lambda symbol: 0.0,
        get_order_book_imbalance=lambda symbol: None,
    )
    strategy._portfolio = SimpleNamespace(all_positions=lambda: [])

    async def _noop_refresh():
        return None

    strategy._refresh_all_markets = _noop_refresh
    strategy._cleanup_15m_entered_slugs = lambda: None

    called = {"check": 0, "queue": 0}

    async def _check_pending(*args, **kwargs):
        called["check"] += 1

    async def _queue_pending(*args, **kwargs):
        called["queue"] += 1

    strategy._check_pending_15m = _check_pending
    strategy._queue_15m_pending = _queue_pending
    strategy._branch_trade_availability = lambda *args, **kwargs: pytest.fail(
        "branch availability should not run during warmup"
    )
    strategy._trade_updown = lambda *args, **kwargs: pytest.fail(
        "trade execution should not run during warmup"
    )
    strategy._log_rejection = lambda *args, **kwargs: pytest.fail(
        "rejection logging should not run during warmup short-circuit"
    )

    monkeypatch.setattr(latency_module.time, "time", lambda: 100.0)
    monkeypatch.setattr(latency_module.settings, "TREND_FILTER_TICKS", 3)

    asyncio.run(strategy._tick())

    assert called["check"] == 0
    assert called["queue"] == 0
    assert strategy._trend_state_label() == "WARMUP"


def test_15m_uses_longer_signal_window_when_available():
    strategy = _build_strategy_shell()
    strategy._btc_signal_history.extend(
        [
            (0.0, 100.0),
            (60.0, 102.0),
            (120.0, 104.0),
            (180.0, 106.0),
        ]
    )

    fast_momentum = 0.0025
    slow_momentum = strategy._momentum_for_timeframe("15m", fast_momentum)

    assert slow_momentum == pytest.approx(0.06)
    assert strategy._momentum_for_timeframe("5m", fast_momentum) == pytest.approx(fast_momentum)
