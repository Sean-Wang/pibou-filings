"""Tests for the rate limiter — token bucket correctness + singleton semantics."""

from __future__ import annotations

import logging
import time

import pytest

from piboufilings.core.rate_limiter import GlobalRateLimiter, TokenBucketRateLimiter


@pytest.fixture(autouse=True)
def _reset_global_rate_limiter():
    GlobalRateLimiter.reset()
    yield
    GlobalRateLimiter.reset()


def test_token_bucket_allows_burst_up_to_capacity():
    bucket = TokenBucketRateLimiter(rate=100.0, capacity=5)
    # Should allow 5 immediate acquires, then block.
    for _ in range(5):
        assert bucket.acquire(block=False) is True
    assert bucket.acquire(block=False) is False


def test_token_bucket_refills_at_rate():
    bucket = TokenBucketRateLimiter(rate=10.0, capacity=1)
    assert bucket.acquire(block=False) is True  # drain
    assert bucket.acquire(block=False) is False
    time.sleep(0.2)  # ≈ 2 new tokens accrued
    assert bucket.acquire(block=False) is True


def test_global_rate_limiter_is_singleton():
    a = GlobalRateLimiter(rate=10.0, safety_factor=0.7)
    b = GlobalRateLimiter(rate=999.0, safety_factor=0.1)  # ignored
    assert a is b
    assert a.limiter.rate == pytest.approx(7.0)


def test_global_rate_limiter_warns_on_conflicting_second_init(caplog):
    GlobalRateLimiter(rate=10.0, safety_factor=0.7)
    with caplog.at_level(logging.WARNING, logger="piboufilings.core.rate_limiter"):
        GlobalRateLimiter(rate=2.0, safety_factor=0.5)
    assert any("ignoring new values" in rec.message for rec in caplog.records)


def test_global_rate_limiter_reset_allows_reinit():
    a = GlobalRateLimiter(rate=10.0, safety_factor=0.7)
    assert a.limiter.rate == pytest.approx(7.0)
    GlobalRateLimiter.reset()
    b = GlobalRateLimiter(rate=2.0, safety_factor=0.5)
    assert b.limiter.rate == pytest.approx(1.0)
    assert a is not b
