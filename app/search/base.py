"""The search backend port.

Services depend only on this protocol. Adapters:

* ``ElasticsearchBackend``: the production engine (BM25, sharding, replicas).
* ``InMemorySearchBackend``: a faithful BM25 implementation for local runs and tests.
* ``ResilientSearchBackend``: a decorator adding retries and a circuit breaker to any backend.

Contract notes:
* Every method is tenant-scoped. An adapter must never return another tenant's data.
* Write methods raise ``DependencyUnavailableError`` when the write didn't
  happen. They never report success for data that wasn't stored.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.models import BulkItemResult, DependencyHealth, Document, SearchPage, SearchQuery


@runtime_checkable
class SearchBackend(Protocol):
    name: str

    async def startup(self) -> None: ...

    async def shutdown(self) -> None: ...

    async def index(self, document: Document) -> None: ...

    async def bulk_index(self, documents: list[Document]) -> list[BulkItemResult]: ...

    async def get(self, tenant_id: str, document_id: str) -> Document | None: ...

    async def delete(self, tenant_id: str, document_id: str) -> bool: ...

    async def search(self, tenant_id: str, query: SearchQuery, *, fuzzy: bool) -> SearchPage: ...

    async def health(self) -> DependencyHealth: ...
