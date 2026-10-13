"""
ingestion/rate_limiter.py
─────────────────────────
Thread-safe dual-window token-bucket rate limiter:
  • 30 requests / 60 s  (per-minute budget)
  • 3  requests /  1 s  (burst guard)

Usage:
    limiter = RateLimiter(rpm=30, rps=3)
    limiter.acquire()          # blocks until a token is available
    response = session.get(url)
"""

from __future__ import annotations

import threading
import time


class _TokenBucket:
    """A single token-bucket window."""

    def __init__(self, capacity: int, refill_period_s: float) -> None:
        self._capacity = capacity
        self._tokens = float(capacity)
        self._refill_period = refill_period_s
        self._rate = capacity / refill_period_s   # tokens per second
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        added = elapsed * self._rate
        self._tokens = min(self._capacity, self._tokens + added)
        self._last_refill = now

    def acquire(self, timeout: float = 120.0) -> None:
        """Block until a token is available or timeout is exceeded."""
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Calculate how long until the next token arrives
                wait = (1.0 - self._tokens) / self._rate

            if time.monotonic() + wait > deadline:
                raise TimeoutError(
                    f"Rate-limiter timed out after {timeout}s waiting for a token."
                )
            time.sleep(min(wait, 0.05))   # sleep in small increments


class RateLimiter:
    """
    Dual-bucket rate limiter that enforces both RPM and RPS limits.

    Args:
        rpm: Max requests per minute (default: 30 – OpenF1 limit).
        rps: Max requests per second (default: 3  – OpenF1 burst limit).
    """

    def __init__(self, rpm: int = 30, rps: int = 3) -> None:
        self._per_minute = _TokenBucket(capacity=rpm, refill_period_s=60.0)
        self._per_second = _TokenBucket(capacity=rps, refill_period_s=1.0)

    def acquire(self) -> None:
        """
        Block until both the per-minute and per-second budgets have a token.
        The per-second check comes first (cheaper to satisfy).
        """
        self._per_second.acquire()
        self._per_minute.acquire()
