"""Tests for order sizing logic."""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ.setdefault("POLY_PRIVATE_KEY", "0x" + "a" * 64)
os.environ.setdefault("DAILY_LOSS_CAP_USDC", "500")
os.environ.setdefault("MAX_POSITION_SIZE_USDC", "2000")

import pytest
from utils.helpers import usdc_to_shares, round_price


@pytest.mark.parametrize("price,usdc,expected_shares", [
    (0.50, 100.0, 200.0),
    (0.25, 100.0, 400.0),
    (0.75, 150.0, 200.0),
    (0.10, 50.0,  500.0),
])
def test_usdc_to_shares_parametrized(price, usdc, expected_shares):
    assert usdc_to_shares(usdc, price) == pytest.approx(expected_shares)


@pytest.mark.parametrize("raw_price,expected", [
    (0.001, 0.01),    # below min → clamp to 0.01
    (0.999, 0.99),
    (0.5555, 0.5555),
    (1.1,   0.99),    # above max → clamp to 0.99
])
def test_price_rounding(raw_price, expected):
    assert round_price(raw_price) == pytest.approx(expected, abs=1e-4)


def test_zero_price_returns_zero_shares():
    assert usdc_to_shares(100.0, 0.0) == 0.0
