"""Redis cache adapter (shared L2 across all API replicas)."""

from __future__ import annotations

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.errors import DependencyUnavailableError


def redis_unavailable(exc: Exception) -> DependencyUnavailableError:
    return DependencyUnavailableError(f"Redis error: {type(exc).__name__}", dependency="redis")


class RedisCache:
    name = "redis"

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def get(self, key: str) -> str | None:
        try:
            return await self._client.get(key)
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def set(self, key: str, value: str, ttl_s: float) -> None:
        try:
            await self._client.set(key, value, px=max(1, int(ttl_s * 1000)))
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def add(self, key: str, value: str, ttl_s: float) -> bool:
        try:
            return bool(await self._client.set(key, value, px=max(1, int(ttl_s * 1000)), nx=True))
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def delete(self, *keys: str) -> None:
        if not keys:
            return
        try:
            await self._client.delete(*keys)
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def incr(self, key: str) -> int:
        # No TTL: with maxmemory-policy=volatile-lru, keys without TTL are never evicted.
        try:
            return int(await self._client.incr(key))
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def ping(self) -> None:
        try:
            await self._client.ping()
        except RedisError as exc:
            raise redis_unavailable(exc) from exc
