"""Decorator adding retries and a circuit breaker to any ``SearchBackend``.

Every operation is retried (exponential backoff + full jitter) on transient
``DependencyUnavailableError``s. Each attempt passes through the breaker, so a
dependency that keeps failing trips the circuit, and later requests fail fast
(no thread or socket pile-up) until a half-open trial succeeds.

All operations are idempotent (writes use caller-assigned IDs), so retrying
them is safe.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

from app.core.resilience import CircuitBreaker, RetryPolicy, retry_async
from app.domain.models import BulkItemResult, DependencyHealth, Document, SearchPage, SearchQuery
from app.search.base import SearchBackend

T = TypeVar("T")


class ResilientSearchBackend:
    def __init__(self, inner: SearchBackend, breaker: CircuitBreaker, policy: RetryPolicy) -> None:
        self.inner = inner
        self.breaker = breaker
        self.policy = policy
        self.name = inner.name

    async def _run(self, operation: str, fn: Callable[[], Awaitable[T]]) -> T:
        return await retry_async(lambda: self.breaker.call(fn), policy=self.policy, operation=operation)

    async def startup(self) -> None:
        await self.inner.startup()

    async def shutdown(self) -> None:
        await self.inner.shutdown()

    async def index(self, document: Document) -> None:
        await self._run("index", lambda: self.inner.index(document))

    async def bulk_index(self, documents: list[Document]) -> list[BulkItemResult]:
        return await self._run("bulk_index", lambda: self.inner.bulk_index(documents))

    async def get(self, tenant_id: str, document_id: str) -> Document | None:
        return await self._run("get", lambda: self.inner.get(tenant_id, document_id))

    async def delete(self, tenant_id: str, document_id: str) -> bool:
        return await self._run("delete", lambda: self.inner.delete(tenant_id, document_id))

    async def search(self, tenant_id: str, query: SearchQuery, *, fuzzy: bool) -> SearchPage:
        return await self._run("search", lambda: self.inner.search(tenant_id, query, fuzzy=fuzzy))

    async def health(self) -> DependencyHealth:
        report = await self.inner.health()
        report.details["circuit"] = self.breaker.state.value
        return report
