"""Cache composition: fault isolation (``GuardedCache``) and L1/L2 tiering (``TieredCache``)."""

from __future__ import annotations

import logging

from app.cache.base import Cache
from app.cache.memory import MemoryCache
from app.core.errors import DependencyUnavailableError
from app.core.metrics import CACHE_ERRORS
from app.core.resilience import CircuitBreaker

logger = logging.getLogger(__name__)


class GuardedCache:
    """Makes a remote cache *optional*: failures are logged, counted and swallowed.

    A circuit breaker stops every request from paying a socket timeout while
    Redis is down. Reads become misses, writes become no-ops, and ``add`` fails
    open (returns True), so idempotency is best-effort during an outage
    rather than blocking writes.
    """

    def __init__(self, inner: Cache, breaker: CircuitBreaker) -> None:
        self.inner = inner
        self.breaker = breaker
        self.name = inner.name

    def _failed(self, operation: str, exc: DependencyUnavailableError) -> None:
        CACHE_ERRORS.labels(operation=operation).inc()
        logger.warning("cache_bypassed", extra={"operation": operation, "error": exc.message})

    async def get(self, key: str) -> str | None:
        try:
            return await self.breaker.call(self.inner.get, key)
        except DependencyUnavailableError as exc:
            self._failed("get", exc)
            return None

    async def set(self, key: str, value: str, ttl_s: float) -> None:
        try:
            await self.breaker.call(self.inner.set, key, value, ttl_s)
        except DependencyUnavailableError as exc:
            self._failed("set", exc)

    async def add(self, key: str, value: str, ttl_s: float) -> bool:
        try:
            return await self.breaker.call(self.inner.add, key, value, ttl_s)
        except DependencyUnavailableError as exc:
            self._failed("add", exc)
            return True

    async def delete(self, *keys: str) -> None:
        try:
            await self.breaker.call(self.inner.delete, *keys)
        except DependencyUnavailableError as exc:
            self._failed("delete", exc)

    async def incr(self, key: str) -> int:
        try:
            return await self.breaker.call(self.inner.incr, key)
        except DependencyUnavailableError as exc:
            self._failed("incr", exc)
            return 0

    async def ping(self) -> None:
        await self.inner.ping()


class TieredCache:
    """L1 (in-process, short TTL) in front of L2 (shared Redis).

    L1 absorbs hot keys with zero network hops. Its short TTL bounds how long
    one replica can serve data that another replica has invalidated (default
    2 s, in line with Elasticsearch's ~1 s refresh visibility). Counters and
    SET-NX always go straight to L2 because they must be globally consistent.
    """

    def __init__(self, l1: MemoryCache, l2: Cache, *, l1_ttl_s: float) -> None:
        self.l1 = l1
        self.l2 = l2
        self.l1_ttl_s = l1_ttl_s
        self.name = f"tiered({l2.name})"

    async def get(self, key: str) -> str | None:
        if self.l1_ttl_s > 0 and (value := await self.l1.get(key)) is not None:
            return value
        value = await self.l2.get(key)
        if value is not None and self.l1_ttl_s > 0:
            await self.l1.set(key, value, self.l1_ttl_s)
        return value

    async def set(self, key: str, value: str, ttl_s: float) -> None:
        if self.l1_ttl_s > 0:
            await self.l1.set(key, value, min(ttl_s, self.l1_ttl_s))
        await self.l2.set(key, value, ttl_s)

    async def add(self, key: str, value: str, ttl_s: float) -> bool:
        return await self.l2.add(key, value, ttl_s)

    async def delete(self, *keys: str) -> None:
        await self.l1.delete(*keys)
        await self.l2.delete(*keys)

    async def incr(self, key: str) -> int:
        await self.l1.delete(key)  # drop the locally cached copy of the counter
        return await self.l2.incr(key)

    async def ping(self) -> None:
        await self.l2.ping()
