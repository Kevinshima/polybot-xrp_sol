"""Unit tests for RiskManager."""
import sys
import os
import importlib
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Minimal env for tests
os.environ["POLY_PRIVATE_KEY"] = "0x" + "a" * 64
os.environ["DAILY_LOSS_CAP_USDC"] = "500"
os.environ["MAX_POSITION_SIZE_USDC"] = "2000"
os.environ["MAX_OPEN_ORDERS"] = "20"

import pytest
import config.settings as settings_module
settings_module = importlib.reload(settings_module)
import core.risk_manager as risk_manager_module
risk_manager_module = importlib.reload(risk_manager_module)
RiskManager = risk_manager_module.RiskManager


@pytest.fixture
def rm():
    return RiskManager()


def test_approve_normal_order(rm):
    ok, reason = rm.approve_order("BUY", 100.0, "market_1", "test")
    assert ok, reason


def test_reject_when_halted(rm):
    rm.kill_all("test")
    ok, reason = rm.approve_order("BUY", 100.0, "market_1", "test")
    assert not ok
    assert "halted" in reason.lower()


def test_reject_exceeds_position_limit(rm):
    ok, reason = rm.approve_order("BUY", 2500.0, "market_1", "test")
    assert not ok
    assert "limit" in reason.lower()


def test_daily_loss_cap_halts_bot(rm):
    # Fill with big loss
    rm.record_order_placed("market_1", 1000.0)
    rm.record_fill(-600.0)  # exceeds 500 cap
    assert rm.is_halted


def test_resume_clears_halt(rm):
    rm.kill_all("test")
    assert rm.is_halted
    rm.resume()
    assert not rm.is_halted


def test_max_open_orders(rm):
    for i in range(20):
        rm.record_order_placed(f"market_{i}", 10.0)

    ok, reason = rm.approve_order("BUY", 10.0, "new_market", "test")
    assert not ok
    assert "max open orders" in reason.lower()


def test_position_size_cumulates(rm):
    rm.approve_order("BUY", 1500.0, "market_1", "test")
    rm.record_order_placed("market_1", 1500.0)

    # Second order on same market would exceed 2000 limit
    ok, reason = rm.approve_order("BUY", 600.0, "market_1", "test")
    assert not ok
    assert "limit" in reason.lower()


def test_daily_reset(rm):
    rm.record_order_placed("market_1", 100.0)
    rm.record_fill(-200.0)
    assert rm._daily_pnl == -200.0

    rm.daily_reset()
    assert rm._daily_pnl == 0.0


def test_stats_returns_dict(rm):
    stats = rm.stats()
    assert "daily_pnl" in stats
    assert "halted" in stats
    assert "open_orders" in stats
