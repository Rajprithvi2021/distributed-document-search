"""Document use cases: create (sync or queued), bulk create, get, delete."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Literal

from app.cache.stores import DocumentCache, SearchResultCache
from app.core.errors import ConflictError, DocumentNotFoundError, PayloadTooLargeError
from app.core.security import Principal
from app.domain.models import BulkItemResult, Document, MetadataValue, utcnow
from app.queue.base import IndexingJob, IndexingQueue
from app.search.base import SearchBackend

logger = logging.getLogger(__name__)

DocumentStatus = Literal["indexed", "pending"]


@dataclass(frozen=True)
class DocumentInput:
    title: str
    content: str
    tags: list[str]
    metadata: dict[str, MetadataValue]


@dataclass(frozen=True)
class CreateResult:
    document: Document
    status: DocumentStatus
    replayed: bool = False


@dataclass(frozen=True)
class BulkResult:
    items: list[BulkItemResult]

    @property
    def succeeded(self) -> int:
        return sum(1 for item in self.items if item.status != "failed")

    @property
    def failed(self) -> int:
        return len(self.items) - self.succeeded


class DocumentService:
    def __init__(
        self,
        *,
        backend: SearchBackend,
        documents: DocumentCache,
        search_cache: SearchResultCache,
        queue: IndexingQueue | None,
        indexing_mode: Literal["sync", "async"],
        max_document_bytes: int,
    ) -> None:
        self._backend = backend
        self._documents = documents
        self._search_cache = search_cache
        self._queue = queue
        self._async = indexing_mode == "async"
        self._max_document_bytes = max_document_bytes
        if self._async and queue is None:
            raise ValueError("async indexing requires a queue")

    # -- create ------------------------------------------------------------------
    def _build(self, principal: Principal, data: DocumentInput, document_id: str | None = None) -> Document:
        size = len(data.content.encode("utf-8")) + len(data.title.encode("utf-8"))
        if size > self._max_document_bytes:
            raise PayloadTooLargeError(
                f"Document is {size} bytes; the limit is {self._max_document_bytes} bytes",
                details={"size_bytes": size, "limit_bytes": self._max_document_bytes},
            )
        now = utcnow()
        return Document(
            id=document_id or str(uuid.uuid4()),
            tenant_id=principal.tenant_id,
            title=data.title,
            content=data.content,
            tags=data.tags,
            metadata=data.metadata,
            created_at=now,
            updated_at=now,
            created_by=principal.subject,
        )

    async def create(
        self, principal: Principal, data: DocumentInput, *, idempotency_key: str | None = None
    ) -> CreateResult:
        document = self._build(principal, data)

        if idempotency_key:
            previous_id = await self._documents.claim_idempotency_key(principal.tenant_id, idempotency_key, document.id)
            if previous_id is not None:
                return await self._replay(principal.tenant_id, previous_id)

        try:
            status = await self._store(document)
        except Exception:
            if idempotency_key:  # let the client retry with the same key
                await self._documents.release_idempotency_key(principal.tenant_id, idempotency_key)
            raise
        logger.info("document_created", extra={"document_id": document.id, "status": status})
        return CreateResult(document=document, status=status)

    async def _replay(self, tenant_id: str, document_id: str) -> CreateResult:
        try:
            document, status = await self.get(tenant_id, document_id)
        except DocumentNotFoundError:
            raise ConflictError(
                "A request with this Idempotency-Key is still in progress or failed; retry shortly",
                details={"document_id": document_id},
            ) from None
        return CreateResult(document=document, status=status, replayed=True)

    async def _store(self, document: Document) -> DocumentStatus:
        if self._async:
            assert self._queue is not None
            # Cache a "pending" copy first so GET-after-POST works before the worker runs.
            await self._documents.put(document, state="pending")
            await self._queue.publish([IndexingJob(job_id=str(uuid.uuid4()), document=document)])
            return "pending"
        await self._backend.index(document)
        await self._documents.put(document, state="indexed")
        await self._search_cache.invalidate_tenant(document.tenant_id)
        return "indexed"

    async def bulk_create(self, principal: Principal, items: list[DocumentInput]) -> BulkResult:
        documents = [self._build(principal, item) for item in items]
        if self._async:
            assert self._queue is not None
            for document in documents:
                await self._documents.put(document, state="pending")
            await self._queue.publish([IndexingJob(job_id=str(uuid.uuid4()), document=d) for d in documents])
            return BulkResult(items=[BulkItemResult(id=d.id, status="pending") for d in documents])

        results = await self._backend.bulk_index(documents)
        if any(result.status == "indexed" for result in results):
            await self._search_cache.invalidate_tenant(principal.tenant_id)
        logger.info(
            "bulk_indexed", extra={"count": len(documents), "failed": sum(r.status == "failed" for r in results)}
        )
        return BulkResult(items=results)

    # -- read / delete ----------------------------------------------------------
    async def get(self, tenant_id: str, document_id: str) -> tuple[Document, DocumentStatus]:
        cached = await self._documents.get(tenant_id, document_id)
        if cached is not None:
            return cached.document, cached.state
        document = await self._backend.get(tenant_id, document_id)
        if document is None:
            raise DocumentNotFoundError(details={"document_id": document_id})
        await self._documents.put(document, state="indexed")
        return document, "indexed"

    async def delete(self, tenant_id: str, document_id: str) -> None:
        pending = False
        if self._async:
            # Tombstone first: a queued job for this document must not resurrect it.
            await self._documents.tombstone(tenant_id, document_id)
            cached = await self._documents.get(tenant_id, document_id)
            pending = cached is not None and cached.state == "pending"

        deleted = await self._backend.delete(tenant_id, document_id)
        await self._documents.invalidate(tenant_id, document_id)
        if not (deleted or pending):
            raise DocumentNotFoundError(details={"document_id": document_id})
        await self._search_cache.invalidate_tenant(tenant_id)
        logger.info("document_deleted", extra={"document_id": document_id})
