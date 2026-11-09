"""Process-local token bucket (single instance, or fallback when Redis is down)."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable

from app.ratelimit.base import BucketConfig, RateLimitDecision, compute_decision


class InMemoryTokenBucket:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic, max_keys: int = 100_000) -> None:
        self._clock = clock
        self._max_keys = max_keys
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()  # key -> (tokens, updated_at)

    async def acquire(self, key: str, bucket: BucketConfig, cost: int = 1) -> RateLimitDecision:
        now = self._clock()
        tokens, updated_at = self._buckets.get(key, (float(bucket.capacity), now))
        tokens = min(float(bucket.capacity), tokens + max(0.0, now - updated_at) * bucket.refill_per_s)
        allowed = tokens >= cost
        if allowed:
            tokens -= cost
        self._buckets[key] = (tokens, now)
        self._buckets.move_to_end(key)
        while len(self._buckets) > self._max_keys:
            self._buckets.popitem(last=False)
        return compute_decision(tokens, bucket, cost, allowed)
