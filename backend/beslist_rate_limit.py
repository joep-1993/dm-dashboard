"""Process-global rate limit for productsearch-v2.api.beslist.nl.

All in-process callers should `productsearch_bucket.acquire()` immediately
before issuing a productsearch HTTP call. Caps total throughput at
PRODUCTSEARCH_QPS regardless of how many ThreadPoolExecutor workers are
running concurrently.

Subprocess workers (e.g. rurl_optimizer_v2's main_parallel_v2 run) cannot
share this in-memory bucket; they enforce the same cap via their own
SEARCH_QPS constant in src/search_derived.py.
"""

from __future__ import annotations

import threading
import time

PRODUCTSEARCH_QPS = 20.0


class _TokenBucket:
    """Reserve-slot rate limiter. Each acquire() atomically claims the next
    interval; concurrent threads sleep outside the lock so realised
    throughput converges to exactly `qps` even under high worker counts.
    """

    def __init__(self, qps: float):
        self._lock = threading.Lock()
        self._interval = 1.0 / qps if qps > 0 else 0.0
        self._next_slot = 0.0

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._interval
        wait = slot - time.monotonic()
        if wait > 0:
            time.sleep(wait)


productsearch_bucket = _TokenBucket(PRODUCTSEARCH_QPS)
