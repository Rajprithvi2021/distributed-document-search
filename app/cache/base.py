"""The key-value cache port. Values are strings; serialisation is the caller's job."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Cache(Protocol):
    name: str

    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str, ttl_s: float) -> None: ...

    async def add(self, key: str, value: str, ttl_s: float) -> bool:
        """Set only if absent (SET NX). Returns True if this call stored the value."""
        ...

    async def delete(self, *keys: str) -> None: ...

    async def incr(self, key: str) -> int:
        """Atomically increment a counter. Counters never expire and are never evicted."""
        ...

    async def ping(self) -> None: ...
