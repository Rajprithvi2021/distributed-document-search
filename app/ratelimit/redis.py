"""Distributed token bucket: one atomic Lua script per request.

The read-refill-spend-write cycle runs inside Redis, so concurrent requests
from any number of API replicas can't over-admit (no read-then-write race)
and each decision is a single round trip.

If Redis is unavailable, the limiter falls back to a per-instance bucket. Each
replica then limits on its own, which is less precise but still protects the
backend. Failing open (no limit) or failing closed (rejecting everything)
would both be worse.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.cache.redis import redis_unavailable
from app.core.errors import DependencyUnavailableError
from app.core.metrics import RATE_LIMIT_FALLBACK
from app.core.resilience import CircuitBreaker
from app.ratelimit.base import BucketConfig, RateLimitDecision, compute_decision
from app.ratelimit.memory import InMemoryTokenBucket

logger = logging.getLogger(__name__)

TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now_ms
end

local elapsed = math.max(0, now_ms - ts)
tokens = math.min(capacity, tokens + elapsed * refill_per_ms)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now_ms))
-- Expire idle buckets once they would have refilled completely.
redis.call('PEXPIRE', key, math.ceil(capacity / refill_per_ms) + 1000)
return {allowed, tostring(tokens)}
"""


class RedisTokenBucket:
    def __init__(
        self,
        client: Redis,
        *,
        breaker: CircuitBreaker,
        fallback: InMemoryTokenBucket | None = None,
        clock_ms: Callable[[], int] = lambda: int(time.time() * 1000),
        key_prefix: str = "rl:",
    ) -> None:
        self._script = client.register_script(TOKEN_BUCKET_LUA)
        self._breaker = breaker
        self._fallback = fallback or InMemoryTokenBucket()
        self._clock_ms = clock_ms
        self._prefix = key_prefix

    async def _eval(self, key: str, bucket: BucketConfig, cost: int) -> RateLimitDecision:
        try:
            allowed, tokens = await self._script(
                keys=[f"{self._prefix}{key}"],
                args=[bucket.capacity, bucket.refill_per_s / 1000.0, self._clock_ms(), cost],
            )
        except RedisError as exc:
            raise redis_unavailable(exc) from exc
        return compute_decision(float(tokens), bucket, cost, bool(int(allowed)))

    async def acquire(self, key: str, bucket: BucketConfig, cost: int = 1) -> RateLimitDecision:
        try:
            return await self._breaker.call(self._eval, key, bucket, cost)
        except DependencyUnavailableError as exc:
            RATE_LIMIT_FALLBACK.inc()
            logger.warning("rate_limit_local_fallback", extra={"error": exc.message})
            return await self._fallback.acquire(key, bucket, cost)
