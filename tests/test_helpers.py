"""Unit tests for utility helpers."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.helpers import (
    clamp,
    round_price,
    usdc_to_shares,
    shares_to_usdc,
    pct_change,
    now_ms,
    now_ts,
    extract_clob_token_ids,
)


def test_clamp():
    assert clamp(0.5, 0.0, 1.0) == 0.5
    assert clamp(-1.0, 0.0, 1.0) == 0.0
    assert clamp(2.0, 0.0, 1.0) == 1.0


def test_round_price():
    assert round_price(0.001) == 0.01   # clamped to min 0.01
    assert round_price(0.999) == 0.99
    assert round_price(1.5) == 0.99    # clamped to max 0.99
    assert round_price(0.12345) == 0.1235  # rounded to 4 dp


def test_usdc_to_shares():
    assert usdc_to_shares(100.0, 0.5) == 200.0
    assert usdc_to_shares(100.0, 0.0) == 0.0


def test_shares_to_usdc():
    assert shares_to_usdc(200.0, 0.5) == 100.0


def test_pct_change():
    assert pct_change(100.0, 103.0) == pytest.approx(0.03)
    assert pct_change(0.0, 100.0) == 0.0
    assert pct_change(100.0, 97.0) == pytest.approx(-0.03)


def test_now_ms_increases():
    import time
    t1 = now_ms()
    time.sleep(0.01)
    t2 = now_ms()
    assert t2 > t1


def test_now_ts():
    import time
    assert abs(now_ts() - int(time.time())) <= 1


def test_extract_clob_token_ids():
    market = {"clobTokenIds": '["yes-token","no-token"]'}
    assert extract_clob_token_ids(market) == ["yes-token", "no-token"]


import pytest
