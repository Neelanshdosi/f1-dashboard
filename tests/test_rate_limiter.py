"""
tests/test_rate_limiter.py
──────────────────────────
Unit tests for the dual-window token-bucket rate limiter.
These run purely in memory – no network or GCS calls.
"""

from __future__ import annotations

import threading
import time

import pytest

from ingestion.rate_limiter import RateLimiter, _TokenBucket

# ── _TokenBucket ──────────────────────────────────────────────────────────────

class TestTokenBucket:
    def test_initial_capacity_all_tokens_available(self):
        """Bucket starts full; first N acquires should be instant."""
        bucket = _TokenBucket(capacity=5, refill_period_s=60.0)
        start = time.monotonic()
        for _ in range(5):
            bucket.acquire()
        elapsed = time.monotonic() - start
        # Should complete in well under 1 second
        assert elapsed < 0.5

    def test_exhausted_bucket_blocks_briefly(self):
        """After exhausting tokens, the next acquire should wait."""
        # 2 tokens, refill = 2 tokens/s → 1 token every 0.5s
        bucket = _TokenBucket(capacity=2, refill_period_s=1.0)
        bucket.acquire()
        bucket.acquire()
        start = time.monotonic()
        bucket.acquire()   # must wait ~0.5s for 1 new token
        elapsed = time.monotonic() - start
        assert 0.3 <= elapsed <= 1.5, f"Expected ~0.5s wait, got {elapsed:.2f}s"

    def test_timeout_raises(self):
        """A bucket with capacity=0 should raise TimeoutError quickly."""
        bucket = _TokenBucket(capacity=1, refill_period_s=9999.0)
        bucket.acquire()    # drain
        with pytest.raises(TimeoutError):
            bucket.acquire(timeout=0.2)

    def test_thread_safety_no_over_draw(self):
        """Multiple threads must not overdraw the bucket beyond capacity."""
        capacity = 10
        bucket = _TokenBucket(capacity=capacity, refill_period_s=60.0)
        acquired: list[int] = []
        lock = threading.Lock()

        def worker():
            try:
                bucket.acquire(timeout=0.5)
                with lock:
                    acquired.append(1)
            except TimeoutError:
                pass

        threads = [threading.Thread(target=worker) for _ in range(capacity * 3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(acquired) <= capacity, (
            f"Over-drawn: {len(acquired)} acquires for capacity {capacity}"
        )


# ── RateLimiter ───────────────────────────────────────────────────────────────

class TestRateLimiter:
    def test_acquire_returns_within_rps_budget(self):
        """3 acquires at 3 rps should complete in ~1s."""
        rl = RateLimiter(rpm=30, rps=3)
        start = time.monotonic()
        for _ in range(3):
            rl.acquire()
        elapsed = time.monotonic() - start
        # First 3 tokens are pre-filled; should be instant
        assert elapsed < 1.0

    def test_rps_throttling_after_burst(self):
        """After exhausting the 3-rps bucket, 4th call is rate-limited."""
        rl = RateLimiter(rpm=30, rps=3)
        for _ in range(3):
            rl.acquire()  # drain per-second bucket
        start = time.monotonic()
        rl.acquire()      # must wait for per-second refill
        elapsed = time.monotonic() - start
        assert elapsed >= 0.2, f"Expected throttling, got {elapsed:.3f}s"
