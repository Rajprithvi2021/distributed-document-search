"""Test doubles and builders shared by unit and integration tests."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta

from app.core.errors import DependencyUnavailableError
from app.domain.models import BulkItemResult, DependencyHealth, Document, SearchPage, SearchQuery
from app.search.memory import InMemorySearchBackend

BASE_TIME = datetime(2024, 1, 1, tzinfo=UTC)


def make_document(
    doc_id: str,
    title: str = "Untitled",
    content: str = "content",
    *,
    tenant: str = "tenant-a",
    tags: list[str] | None = None,
    metadata: dict | None = None,
    age_days: int = 0,
) -> Document:
    created = BASE_TIME + timedelta(days=age_days)
    return Document(
        id=doc_id,
        tenant_id=tenant,
        title=title,
        content=content,
        tags=tags or [],
        metadata=metadata or {},
        created_at=created,
        updated_at=created,
        created_by="test",
    )


class FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FlakySearchBackend(InMemorySearchBackend):
    """In-memory backend that can simulate an unreachable search engine."""

    name = "flaky-memory"

    def __init__(self) -> None:
        super().__init__()
        self.failing = False
        self.crash_with: Exception | None = None
        self.calls: Counter[str] = Counter()

    def _check(self, operation: str) -> None:
        self.calls[operation] += 1
        if self.crash_with is not None:
            raise self.crash_with
        if self.failing:
            raise DependencyUnavailableError("injected outage", dependency="search")

    async def index(self, document: Document) -> None:
        self._check("index")
        await super().index(document)

    async def bulk_index(self, documents: list[Document]) -> list[BulkItemResult]:
        self._check("bulk_index")
        return await super().bulk_index(documents)

    async def get(self, tenant_id: str, document_id: str) -> Document | None:
        self._check("get")
        return await super().get(tenant_id, document_id)

    async def delete(self, tenant_id: str, document_id: str) -> bool:
        self._check("delete")
        return await super().delete(tenant_id, document_id)

    async def search(self, tenant_id: str, query: SearchQuery, *, fuzzy: bool) -> SearchPage:
        self._check("search")
        return await super().search(tenant_id, query, fuzzy=fuzzy)

    async def health(self) -> DependencyHealth:
        if self.failing:
            return DependencyHealth(name="search", status="down", critical=True, details={"error": "injected"})
        return await super().health()


class ScriptedBulkBackend(InMemorySearchBackend):
    """Returns pre-programmed per-item bulk results (to exercise worker retry/DLQ paths)."""

    def __init__(self, outcomes: dict[str, BulkItemResult]) -> None:
        super().__init__()
        self.outcomes = outcomes

    async def bulk_index(self, documents: list[Document]) -> list[BulkItemResult]:
        results = []
        for document in documents:
            outcome = self.outcomes.get(document.id)
            if outcome is None:
                await self.index(document)
                results.append(BulkItemResult(id=document.id, status="indexed"))
            else:
                results.append(outcome)
        return results
