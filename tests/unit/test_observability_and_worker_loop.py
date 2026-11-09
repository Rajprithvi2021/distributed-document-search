import asyncio
import json
import logging

from app.cache.memory import MemoryCache
from app.cache.stores import DocumentCache, SearchResultCache
from app.core.logging import JsonFormatter, request_id_var, tenant_id_var
from app.queue.base import IndexingJob
from app.queue.memory import InMemoryQueue
from app.search.memory import InMemorySearchBackend
from app.workers.indexer import IndexingWorker
from tests.fakes import make_document


def test_json_log_lines_carry_request_and_tenant_context() -> None:
    record = logging.LogRecord("app.test", logging.INFO, __file__, 1, "document_created", (), None)
    record.document_id = "d1"  # structured "extra" field
    rid, tid = request_id_var.set("req-123"), tenant_id_var.set("tenant-a")
    try:
        payload = json.loads(JsonFormatter().format(record))
    finally:
        request_id_var.reset(rid)
        tenant_id_var.reset(tid)
    assert payload["msg"] == "document_created" and payload["level"] == "INFO"
    assert payload["document_id"] == "d1"
    assert "ts" in payload


async def test_worker_run_loop_processes_until_stopped() -> None:
    cache = MemoryCache()
    backend, queue = InMemorySearchBackend(), InMemoryQueue()
    worker = IndexingWorker(
        queue=queue,
        backend=backend,
        documents=DocumentCache(cache, ttl_s=60),
        search_cache=SearchResultCache(cache, fresh_ttl_s=60, stale_ttl_s=600),
        block_ms=20,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    await queue.publish([IndexingJob(job_id="j1", document=make_document("d1", "Looped"))])
    for _ in range(100):
        if await backend.get("tenant-a", "d1"):
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert await backend.get("tenant-a", "d1") is not None
