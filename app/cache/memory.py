"""Bounded in-process cache with per-entry TTL and LRU eviction."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable


class MemoryCache:
    def __init__(
        self, max_items: int = 10_000, *, clock: Callable[[], float] = time.monotonic, name: str = "memory"
    ) -> None:
        self.name = name
        self._max_items = max_items
        self._clock = clock
        self._entries: OrderedDict[str, tuple[str, float]] = OrderedDict()
        # Counters live apart from the LRU: evicting a generation counter would
        # reset it and could resurrect stale cache entries.
        self._counters: dict[str, int] = {}

    def __len__(self) -> int:
        return len(self._entries)

    async def get(self, key: str) -> str | None:
        if key in self._counters:
            return str(self._counters[key])
        entry = self._entries.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at <= self._clock():
            del self._entries[key]
            return None
        self._entries.move_to_end(key)
        return value

    async def set(self, key: str, value: str, ttl_s: float) -> None:
        if ttl_s <= 0:
            return
        self._entries[key] = (value, self._clock() + ttl_s)
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_items:
            self._entries.popitem(last=False)

    async def add(self, key: str, value: str, ttl_s: float) -> bool:
        if await self.get(key) is not None:
            return False
        await self.set(key, value, ttl_s)
        return True

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self._entries.pop(key, None)
            self._counters.pop(key, None)

    async def incr(self, key: str) -> int:
        self._counters[key] = self._counters.get(key, 0) + 1
        return self._counters[key]

    async def ping(self) -> None:
        return None
