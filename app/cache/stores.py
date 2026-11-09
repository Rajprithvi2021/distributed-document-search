"""Typed caches built on the generic ``Cache`` port.

Key design (all keys are tenant-namespaced):

    gen:search:{tenant}                  search generation counter (no TTL)
    search:{tenant}:{gen}:{fingerprint}  cached SearchPage + stored_at
    doc:{tenant}:{id}                    cached Document + state (indexed | pending)
    tomb:{tenant}:{id}                   delete tombstone (blocks re-index from a stale queue job)
    idem:{tenant}:{sha256(key)}          Idempotency-Key -> document id

Invalidation is O(1). A write bumps the tenant's generation, so every older
search key becomes unreachable and expires on its own TTL. There's no
``KEYS``/``SCAN`` and no fan-out delete, and other tenants are untouched.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from app.cache.base import Cache
from app.core.metrics import CACHE_LOOKUPS
from app.domain.models import Document, SearchPage, SearchQuery

DocumentState = Literal["indexed", "pending"]


@dataclass(frozen=True)
class SearchLookup:
    key: str
    page: SearchPage | None
    fresh: bool


class SearchResultCache:
    def __init__(
        self, cache: Cache, *, fresh_ttl_s: float, stale_ttl_s: float, clock: Callable[[], float] = time.time
    ) -> None:
        self._cache = cache
        self._fresh_ttl_s = fresh_ttl_s
        self._stale_ttl_s = stale_ttl_s
        self._clock = clock

    async def generation(self, tenant_id: str) -> int:
        raw = await self._cache.get(f"gen:search:{tenant_id}")
        return int(raw) if raw else 0

    async def invalidate_tenant(self, tenant_id: str) -> None:
        await self._cache.incr(f"gen:search:{tenant_id}")

    async def lookup(self, tenant_id: str, query: SearchQuery) -> SearchLookup:
        generation = await self.generation(tenant_id)
        key = f"search:{tenant_id}:{generation}:{query.fingerprint()}"
        raw = await self._cache.get(key)
        if raw is None:
            CACHE_LOOKUPS.labels(cache="search", result="miss").inc()
            return SearchLookup(key=key, page=None, fresh=False)
        envelope = json.loads(raw)
        fresh = self._clock() - envelope["stored_at"] < self._fresh_ttl_s
        CACHE_LOOKUPS.labels(cache="search", result="hit" if fresh else "miss").inc()
        return SearchLookup(key=key, page=SearchPage.model_validate(envelope["page"]), fresh=fresh)

    async def store(self, key: str, page: SearchPage) -> None:
        envelope = {"stored_at": self._clock(), "page": page.model_dump(mode="json")}
        # Kept for the *stale* TTL so it can be served if the backend fails (stale-if-error).
        await self._cache.set(key, json.dumps(envelope, separators=(",", ":")), self._stale_ttl_s)


@dataclass(frozen=True)
class CachedDocument:
    document: Document
    state: DocumentState


class DocumentCache:
    TOMBSTONE_TTL_S = 3600.0  # must exceed the worst-case queue delay
    IDEMPOTENCY_TTL_S = 24 * 3600.0

    def __init__(self, cache: Cache, *, ttl_s: float) -> None:
        self._cache = cache
        self._ttl_s = ttl_s

    @staticmethod
    def _key(tenant_id: str, document_id: str) -> str:
        return f"doc:{tenant_id}:{document_id}"

    async def get(self, tenant_id: str, document_id: str) -> CachedDocument | None:
        raw = await self._cache.get(self._key(tenant_id, document_id))
        if raw is None:
            CACHE_LOOKUPS.labels(cache="document", result="miss").inc()
            return None
        CACHE_LOOKUPS.labels(cache="document", result="hit").inc()
        payload = json.loads(raw)
        document = Document.model_validate(payload["document"])
        if document.tenant_id != tenant_id:  # defence in depth; keys are tenant-scoped
            return None
        return CachedDocument(document=document, state=payload["state"])

    async def put(self, document: Document, state: DocumentState = "indexed") -> None:
        payload = {"document": document.model_dump(mode="json"), "state": state}
        # Pending entries must outlive the queue delay so GET-after-POST works in async mode.
        ttl = self.TOMBSTONE_TTL_S if state == "pending" else self._ttl_s
        await self._cache.set(self._key(document.tenant_id, document.id), json.dumps(payload), ttl)

    async def invalidate(self, tenant_id: str, document_id: str) -> None:
        await self._cache.delete(self._key(tenant_id, document_id))

    async def tombstone(self, tenant_id: str, document_id: str) -> None:
        await self._cache.set(f"tomb:{tenant_id}:{document_id}", "1", self.TOMBSTONE_TTL_S)

    async def is_tombstoned(self, tenant_id: str, document_id: str) -> bool:
        return await self._cache.get(f"tomb:{tenant_id}:{document_id}") is not None

    @staticmethod
    def _idempotency_key(tenant_id: str, key: str) -> str:
        return f"idem:{tenant_id}:{hashlib.sha256(key.encode()).hexdigest()}"

    async def claim_idempotency_key(self, tenant_id: str, key: str, document_id: str) -> str | None:
        """Claim ``key`` for ``document_id``. Returns ``None`` when claimed, otherwise the earlier document id."""
        cache_key = self._idempotency_key(tenant_id, key)
        if await self._cache.add(cache_key, document_id, self.IDEMPOTENCY_TTL_S):
            return None
        return await self._cache.get(cache_key)

    async def release_idempotency_key(self, tenant_id: str, key: str) -> None:
        await self._cache.delete(self._idempotency_key(tenant_id, key))
