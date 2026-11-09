"""SearchService and IndexingWorker behaviour, tested with in-memory ports."""

import asyncio

import pytest

from app.cache.memory import MemoryCache
from app.cache.stores import DocumentCache, SearchResultCache
from app.core.errors import BadRequestError, DependencyUnavailableError
from app.domain.models import BulkItemResult, FuzzyMode, SearchPage, SearchQuery
from app.queue.base import IndexingJob
from app.queue.memory import InMemoryQueue
from app.search.memory import InMemorySearchBackend
from app.services.search import SearchService
from app.workers.indexer import IndexingWorker
from tests.fakes import FakeClock, FlakySearchBackend, ScriptedBulkBackend, make_document


def search_service(backend, clock=None) -> tuple[SearchService, SearchResultCache]:
    cache = SearchResultCache(MemoryCache(), fresh_ttl_s=60, stale_ttl_s=600, clock=clock or FakeClock())
    return SearchService(backend=backend, cache=cache, max_result_window=1000), cache


# --- SearchService ------------------------------------------------------------------
async def test_concurrent_identical_misses_hit_the_backend_once() -> None:
    class SlowBackend(InMemorySearchBackend):
        calls = 0

        async def search(self, tenant_id, query, *, fuzzy):
            SlowBackend.calls += 1
            await asyncio.sleep(0.05)
            return SearchPage(total=1)

    service, _ = search_service(SlowBackend())
    outcomes = await asyncio.gather(
        *(service.search("t", SearchQuery(text="x", fuzzy=FuzzyMode.OFF)) for _ in range(20))
    )
    assert SlowBackend.calls == 1
    assert all(o.page.total == 1 for o in outcomes)


async def test_fuzzy_auto_retries_only_on_zero_hits() -> None:
    backend = FlakySearchBackend()
    await backend.index(make_document("d1", "Redis cache"))
    service, _ = search_service(backend)

    exact = await service.search("tenant-a", SearchQuery(text="redis"))
    assert exact.page.total == 1 and not exact.page.fuzzy_applied
    assert backend.calls["search"] == 1

    typo = await service.search("tenant-a", SearchQuery(text="rediss cach"))
    assert typo.page.total == 1 and typo.page.fuzzy_applied
    assert backend.calls["search"] == 3


async def test_stale_results_are_served_when_backend_fails() -> None:
    clock = FakeClock()
    backend = FlakySearchBackend()
    await backend.index(make_document("d1", "Redis cache"))
    service, _ = search_service(backend, clock)
    query = SearchQuery(text="redis")

    assert (await service.search("tenant-a", query)).cache == "miss"
    assert (await service.search("tenant-a", query)).cache == "hit"

    clock.advance(120)  # past fresh TTL, within stale TTL
    backend.failing = True
    outcome = await service.search("tenant-a", query)
    assert outcome.cache == "stale" and outcome.degraded and outcome.page.total == 1

    with pytest.raises(DependencyUnavailableError):  # nothing cached for this query
        await service.search("tenant-a", SearchQuery(text="other"))


async def test_deep_pagination_is_rejected() -> None:
    service, _ = search_service(InMemorySearchBackend())
    with pytest.raises(BadRequestError):
        await service.search("t", SearchQuery(text="x", page=101, page_size=10))


# --- IndexingWorker -------------------------------------------------------------------
def worker_for(backend) -> tuple[IndexingWorker, InMemoryQueue, DocumentCache, SearchResultCache]:
    cache = MemoryCache()
    queue = InMemoryQueue()
    documents = DocumentCache(cache, ttl_s=60)
    search_cache = SearchResultCache(cache, fresh_ttl_s=60, stale_ttl_s=600)
    worker = IndexingWorker(
        queue=queue, backend=backend, documents=documents, search_cache=search_cache, max_attempts=2
    )
    return worker, queue, documents, search_cache


async def test_worker_indexes_batches_and_invalidates_search_cache() -> None:
    backend = InMemorySearchBackend()
    worker, queue, documents, search_cache = worker_for(backend)
    for doc_id in ("d1", "d2"):
        document = make_document(doc_id, "Queued doc")
        await documents.put(document, state="pending")
        await queue.publish([IndexingJob(job_id=doc_id, document=document)])

    assert await worker.process_batch(block_ms=0) == 2
    assert await queue.depth() == 0
    assert (await backend.search("tenant-a", SearchQuery(text="queued"), fuzzy=False)).total == 2
    assert (await documents.get("tenant-a", "d1")).state == "indexed"
    assert await search_cache.generation("tenant-a") == 1


async def test_worker_skips_documents_deleted_while_queued() -> None:
    backend = InMemorySearchBackend()
    worker, queue, documents, _ = worker_for(backend)
    await queue.publish([IndexingJob(job_id="j", document=make_document("d1", "Ghost"))])
    await documents.tombstone("tenant-a", "d1")

    await worker.process_batch(block_ms=0)
    assert await backend.get("tenant-a", "d1") is None
    assert await queue.depth() == 0


async def test_outage_requeues_without_spending_attempts() -> None:
    backend = FlakySearchBackend()
    backend.failing = True
    worker, queue, _, _ = worker_for(backend)
    await queue.publish([IndexingJob(job_id="j", document=make_document("d1"))])

    for _ in range(5):  # more outages than max_attempts
        await worker.process_batch(block_ms=0)
    assert queue.dead_letters == []
    (message,) = await queue.consume("c", count=10, block_ms=0)
    assert message.job.attempt == 1


async def test_retryable_item_failures_then_dead_letter() -> None:
    backend = ScriptedBulkBackend(
        {
            "flaky": BulkItemResult(id="flaky", status="failed", error="429", retryable=True),
            "poison": BulkItemResult(id="poison", status="failed", error="mapper_parsing_exception", retryable=False),
        }
    )
    worker, queue, _, _ = worker_for(backend)
    await queue.publish([IndexingJob(job_id=i, document=make_document(i)) for i in ("ok", "flaky", "poison")])

    await worker.process_batch(block_ms=0)  # ok indexed, flaky retried (attempt 2), poison dead-lettered
    assert await backend.get("tenant-a", "ok") is not None
    assert [m.job.document.id for m, _ in queue.dead_letters] == ["poison"]

    await worker.process_batch(block_ms=0)  # flaky fails again at max_attempts -> DLQ
    assert [m.job.document.id for m, _ in queue.dead_letters] == ["poison", "flaky"]
    assert await queue.depth() == 0
