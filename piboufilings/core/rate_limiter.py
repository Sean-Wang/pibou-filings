"""
Rate limiter implementation for SEC EDGAR API access.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """
    Implementation of the token bucket algorithm for rate limiting.
    This ensures requests to the SEC API don't exceed the allowed rate.
    """

    def __init__(self, rate: float, capacity: int = None):
        """
        Initialize the rate limiter.

        Args:
            rate: Rate at which tokens are added to the bucket (tokens per second)
            capacity: Maximum number of tokens the bucket can hold (defaults to rate)
        """
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        self.tokens = self.capacity
        self.last_refill_time = time.time()
        self.lock = threading.RLock()  # Use RLock for thread safety

    def _refill(self):
        """Refill the token bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill_time

        # Calculate how many new tokens to add based on elapsed time
        new_tokens = elapsed * self.rate

        # Update token count, capped at capacity
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill_time = now

    def acquire(self, tokens: int = 1, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire tokens from the bucket. If not enough tokens are available,
        either wait until they become available or return False.

        Args:
            tokens: Number of tokens to acquire
            block: Whether to block until tokens become available
            timeout: Maximum time to wait for tokens (in seconds)

        Returns:
            bool: True if tokens were acquired, False otherwise
        """
        start_time = time.time()

        while True:
            with self.lock:
                self._refill()

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

                if not block:
                    return False  # Not enough tokens and not blocking

                # Blocking mode: calculate wait time, then sleep *outside* the
                # lock. Holding the lock while sleeping serializes every other
                # worker behind the sleeping thread and can make concurrent
                # downloads look stalled.
                deficit = tokens - self.tokens  # Deficit should be > 0 here

                if self.rate <= 0:  # Cannot acquire if rate is zero or negative
                    return False

                required_wait_time = deficit / self.rate

                if timeout is not None:
                    elapsed_time = time.time() - start_time
                    remaining_timeout = timeout - elapsed_time
                    if remaining_timeout <= 0:  # Timeout expired
                        return False
                    if required_wait_time > remaining_timeout:
                        # Not enough time left in timeout to wait for the needed tokens,
                        # so sleep for the remaining_timeout and then re-check (will likely fail if still deficit).
                        # Or, we could return False directly here, but sleeping for remaining_timeout
                        # gives a chance if tokens are added by a very small amount in that window.
                        # For simplicity and to ensure timeout is respected strictly for *this* attempt to acquire:
                        return False  # Cannot wait long enough

                # Determine actual sleep time
                sleep_duration = required_wait_time
                if timeout is not None:
                    sleep_duration = min(
                        required_wait_time, remaining_timeout
                    )  # Ensure we don't sleep past timeout

            if sleep_duration > 0:  # Only sleep if there's a positive duration
                time.sleep(sleep_duration)

            # After sleep (or if no sleep was needed but still in loop due to
            # timeout logic), the loop will continue, _refill, and check tokens
            # again. If timeout occurred and we returned False, loop is exited.
            # If timeout is None, loop continues until tokens are acquired.


class GlobalRateLimiter:
    """Process-wide rate limiter that ensures SEC API requests stay under 10/s.

    Implemented as a singleton so that multiple ``SECDownloader`` instances in
    the same process share a single token bucket. If you need an isolated
    limiter (for tests, or for a concurrent second workload), construct a
    ``TokenBucketRateLimiter`` directly and pass it to ``SECDownloader``.

    **Note**: calling ``GlobalRateLimiter(rate=..., safety_factor=...)`` a
    second time with different values will NOT change the active limit — the
    first init wins. A warning is logged when that happens. Use
    :meth:`reset` (intended for tests only) to re-initialize.
    """

    _instance: Optional[GlobalRateLimiter] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, rate: float = 10.0, safety_factor: float = 0.7):
        """
        Initialize the global rate limiter.

        Args:
            rate: Maximum allowed requests per second (defaults to 10.0)
            safety_factor: Factor to apply to rate for safety margin (defaults to 0.7)
        """
        if self._initialized:
            if (rate, safety_factor) != (self._rate, self._safety_factor):
                logger.warning(
                    "GlobalRateLimiter already initialized at rate=%s safety_factor=%s; "
                    "ignoring new values (rate=%s safety_factor=%s). Use "
                    "GlobalRateLimiter.reset() first if you really need to change.",
                    self._rate,
                    self._safety_factor,
                    rate,
                    safety_factor,
                )
            return

        self._rate = rate
        self._safety_factor = safety_factor
        self.limiter = TokenBucketRateLimiter(rate * safety_factor)
        self._initialized = True

    @classmethod
    def reset(cls) -> None:
        """Drop the singleton instance. Test-only.

        Do not call this in production code — concurrent callers may still hold
        a reference to the old limiter, so the process-wide rate cap can be
        briefly violated.
        """
        with cls._lock:
            cls._instance = None

    def acquire(self, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire permission to make a request.

        Args:
            block: Whether to block until a token becomes available
            timeout: Maximum time to wait for a token (in seconds)

        Returns:
            bool: True if permission was granted, False otherwise
        """
        return self.limiter.acquire(tokens=1, block=block, timeout=timeout)
