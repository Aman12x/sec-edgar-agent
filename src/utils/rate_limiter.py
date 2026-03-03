"""
rate_limiter.py — Thread-safe token-bucket rate limiter for SEC EDGAR compliance.

SEC's fair-use policy: max 10 requests per second, max 10 concurrent connections.
This module provides two enforcement mechanisms:
  1. RateLimiter class: token-bucket algorithm, safe for multi-threaded Prefect workers
  2. sec_rate_limited decorator: drop-in decorator for any function making SEC requests

Why token-bucket over the `ratelimit` library decorator?
  - Token-bucket handles bursts gracefully (10 tokens accumulated over 1s can be
    spent in a burst, then must wait) which matches EDGAR's actual behavior
  - The ratelimit library uses a fixed window which can double-spend at window edges
  - Thread-safe via threading.Lock, compatible with Prefect's concurrent task runner
"""

import time
import threading
from functools import wraps
from typing import Callable

from src.utils.logger import get_logger

logger = get_logger("rate_limiter")


class RateLimiter:
    """
    Token-bucket rate limiter.

    Args:
        rate:     Maximum calls per second (default 10 for SEC)
        capacity: Burst capacity — max tokens the bucket can hold
    """

    def __init__(self, rate: float = 10.0, capacity: float = 10.0):
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Add tokens based on elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self.rate
        self._tokens = min(self.capacity, self._tokens + new_tokens)
        self._last_refill = now

    def acquire(self, tokens: float = 1.0) -> None:
        """
        Block until the requested number of tokens are available.
        Called before each SEC request.
        """
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                # Calculate wait time needed for enough tokens
                wait = (tokens - self._tokens) / self.rate

            time.sleep(wait)
            logger.debug(f"Rate limiter: waited {wait:.3f}s for {tokens} token(s)")

    def __call__(self, func: Callable) -> Callable:
        """Allow use as a decorator: @rate_limiter_instance"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.acquire()
            return func(*args, **kwargs)
        return wrapper


# Module-level singleton for SEC requests — shared across all threads/tasks
sec_rate_limiter = RateLimiter(rate=10.0, capacity=10.0)


def sec_rate_limited(func: Callable) -> Callable:
    """Decorator: apply SEC rate limiting to any function."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        sec_rate_limiter.acquire()
        return func(*args, **kwargs)
    return wrapper
