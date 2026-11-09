"""Typed caches built on the generic ``Cache`` port.

Key design (all keys are tenant-namespaced):

    gen:search:{tenant}                  search generation counter (no TTL)
    genat:search:{tenant}                time of the last generation bump (short TTL)
    search:{tenant}:{gen}:{fingerprint}  cached SearchPage + stored_at + fresh_until
    doc:{tenant}:{id}                    cached Document + state (indexed | pending)
    tomb:{tenant}:{id}                   delete tombstone (blocks re-index from a stale queue job)
    idem:{tenant}:{sha256(key)}          Idempotency-Key -> request fingerprint + original response

Invalidation is O(1). A write bumps the tenant's generation, so every older
search key becomes unreachable and expires on its own TTL. There's no
``KEYS``/``SCAN`` and no fan-out delete, and other tenants are untouched.

Visibility race: Elasticsearch makes an acknowledged write searchable only after
its next refresh (about 1 s). A search that runs inside that window, after the
bump, would cache the *pre-write* page under the *new* generation. Pages computed
within ``visibility_s`` of a bump are therefore only fresh until the window closes,
so staleness stays bounded by the refresh interval instead of the 60 s cache TTL.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel

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
        self,
        cache: Cache,
        *,
        fresh_ttl_s: float,
        stale_ttl_s: float,
        visibility_s: float = 0.0,
        shared: Cache | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._cache = cache
        # The bump timestamp must be read from the shared tier (never an L1 copy) to be correct across replicas.
        self._shared = shared or cache
        self._fresh_ttl_s = fresh_ttl_s
        self._stale_ttl_s = stale_ttl_s
        self._visibility_s = visibility_s
        self._clock = clock

    async def generation(self, tenant_id: str) -> int:
        raw = await self._cache.get(f"gen:search:{tenant_id}")
        return int(raw) if raw else 0

    async def invalidate_tenant(self, tenant_id: str) -> None:
        await self._cache.incr(f"gen:search:{tenant_id}")
        if self._visibility_s > 0:
            await self._shared.set(f"genat:search:{tenant_id}", repr(self._clock()), max(2 * self._visibility_s, 1.0))

    async def lookup(self, tenant_id: str, query: SearchQuery) -> SearchLookup:
        generation = await self.generation(tenant_id)
        key = f"search:{tenant_id}:{generation}:{query.fingerprint()}"
        raw = await self._cache.get(key)
        if raw is None:
            CACHE_LOOKUPS.labels(cache="search", result="miss").inc()
            return SearchLookup(key=key, page=None, fresh=False)
        envelope = json.loads(raw)
        fresh = self._clock() < envelope["fresh_until"]
        if fresh:
            CACHE_LOOKUPS.labels(cache="search", result="hit").inc()
        # An expired entry is counted by the caller: "miss" once refreshed, "stale" if served on error.
        return SearchLookup(key=key, page=SearchPage.model_validate(envelope["page"]), fresh=fresh)

    async def store(self, key: str, page: SearchPage, tenant_id: str) -> None:
        now = self._clock()
        fresh_until = now + self._fresh_ttl_s
        if self._visibility_s > 0:
            raw = await self._shared.get(f"genat:search:{tenant_id}")
            if raw is not None and now - float(raw) < self._visibility_s:
                # Computed before the latest write may be searchable: fresh only until the window closes.
                fresh_until = min(fresh_until, float(raw) + self._visibility_s)
        envelope = {"stored_at": now, "fresh_until": fresh_until, "page": page.model_dump(mode="json")}
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

    async def claim_idempotency_key(self, tenant_id: str, key: str, fingerprint: str) -> IdempotencyRecord | None:
        """Claim ``key``. Returns ``None`` if this call claimed it, otherwise the earlier request's record."""
        cache_key = self._idempotency_key(tenant_id, key)
        claim = json.dumps({"state": "in_progress", "fingerprint": fingerprint})
        for _ in range(2):  # second pass only if the record expired between SET NX and GET
            if await self._cache.add(cache_key, claim, self.IDEMPOTENCY_TTL_S):
                return None
            raw = await self._cache.get(cache_key)
            if raw is not None:
                return IdempotencyRecord.model_validate_json(raw)
        return None

    async def complete_idempotency_key(
        self, tenant_id: str, key: str, fingerprint: str, document: Document, status: DocumentState
    ) -> None:
        """Store the original response so retries replay it, even if the document is later deleted."""
        record = IdempotencyRecord(state="done", fingerprint=fingerprint, status=status, document=document)
        await self._cache.set(self._idempotency_key(tenant_id, key), record.model_dump_json(), self.IDEMPOTENCY_TTL_S)

    async def release_idempotency_key(self, tenant_id: str, key: str) -> None:
        await self._cache.delete(self._idempotency_key(tenant_id, key))


class IdempotencyRecord(BaseModel):
    state: Literal["in_progress", "done"]
    fingerprint: str
    status: DocumentState | None = None
    document: Document | None = None
