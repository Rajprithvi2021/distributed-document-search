"""Asynchronous indexing worker.

Run as its own process (``python -m app.workers.indexer``) and scale it
independently of the API. Each loop iteration:

1. pulls up to ``batch_size`` jobs (reclaiming jobs abandoned by crashed workers first);
2. acks and drops jobs whose document was deleted after it was queued (tombstone);
3. bulk-indexes the rest in one request;
4. per item: ack on success, re-queue retryable failures (up to ``max_attempts``),
   dead-letter permanent failures (e.g. a mapping rejection);
5. bumps the search-cache generation of every tenant it touched.

If the search engine itself is unavailable, jobs are re-queued **without**
consuming an attempt and the worker backs off exponentially. An outage
therefore delays indexing but doesn't push healthy jobs into the DLQ.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import socket
import sys

from app.cache.stores import DocumentCache, SearchResultCache
from app.core.errors import DependencyUnavailableError
from app.core.metrics import INDEXING_JOBS, QUEUE_DEPTH
from app.queue.base import IndexingQueue, QueuedMessage
from app.search.base import SearchBackend

logger = logging.getLogger(__name__)


class IndexingWorker:
    def __init__(
        self,
        *,
        queue: IndexingQueue,
        backend: SearchBackend,
        documents: DocumentCache,
        search_cache: SearchResultCache,
        batch_size: int = 100,
        max_attempts: int = 5,
        block_ms: int = 1000,
        consumer_name: str | None = None,
        max_backoff_s: float = 30.0,
    ) -> None:
        self._queue = queue
        self._backend = backend
        self._documents = documents
        self._search_cache = search_cache
        self._batch_size = batch_size
        self._max_attempts = max_attempts
        self._block_ms = block_ms
        self._max_backoff_s = max_backoff_s
        self.consumer_name = consumer_name or f"{socket.gethostname()}-{os.getpid()}"
        self._backoff_s = 0.0

    async def process_batch(self, *, block_ms: int | None = None) -> int:
        """Process one batch. Returns the number of messages handled (0 = queue idle)."""
        messages = await self._queue.consume(
            self.consumer_name, self._batch_size, self._block_ms if block_ms is None else block_ms
        )
        if not messages:
            return 0

        live: list[QueuedMessage] = []
        for message in messages:
            document = message.job.document
            if await self._documents.is_tombstoned(document.tenant_id, document.id):
                await self._queue.ack([message.message_id])
                INDEXING_JOBS.labels(status="skipped_deleted").inc()
            else:
                live.append(message)
        if not live:
            return len(messages)

        try:
            results = await self._backend.bulk_index([m.job.document for m in live])
        except DependencyUnavailableError as exc:
            for message in live:
                await self._queue.requeue(message, exc.message, count_attempt=False)
            INDEXING_JOBS.labels(status="deferred").inc(len(live))
            self._backoff_s = min(self._max_backoff_s, max(0.5, self._backoff_s * 2))
            logger.warning(
                "indexing_deferred", extra={"jobs": len(live), "backoff_s": self._backoff_s, "error": exc.message}
            )
            return len(messages)
        self._backoff_s = 0.0

        touched_tenants: set[str] = set()
        for message, result in zip(live, results, strict=True):
            document = message.job.document
            if result.status == "indexed":
                await self._queue.ack([message.message_id])
                await self._documents.put(document, state="indexed")
                touched_tenants.add(document.tenant_id)
                INDEXING_JOBS.labels(status="indexed").inc()
            elif result.retryable and message.job.attempt < self._max_attempts:
                await self._queue.requeue(message, result.error or "retryable failure", count_attempt=True)
                INDEXING_JOBS.labels(status="retried").inc()
            else:
                await self._queue.dead_letter(message, result.error or "permanent failure")
                await self._documents.invalidate(document.tenant_id, document.id)
                INDEXING_JOBS.labels(status="dead_lettered").inc()
                logger.error("indexing_dead_lettered", extra={"document_id": document.id, "error": result.error})

        for tenant_id in touched_tenants:
            await self._search_cache.invalidate_tenant(tenant_id)
        return len(messages)

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("indexer_started", extra={"consumer": self.consumer_name})
        while not stop.is_set():
            try:
                await self.process_batch()
                QUEUE_DEPTH.set(await self._queue.depth())
            except DependencyUnavailableError as exc:  # queue (Redis) unavailable
                self._backoff_s = min(self._max_backoff_s, max(0.5, self._backoff_s * 2))
                logger.warning("indexer_queue_unavailable", extra={"error": exc.message, "backoff_s": self._backoff_s})
            except Exception:
                self._backoff_s = min(self._max_backoff_s, max(0.5, self._backoff_s * 2))
                logger.exception("indexer_batch_failed")
            if self._backoff_s:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self._backoff_s)
        logger.info("indexer_stopped", extra={"consumer": self.consumer_name})


async def _run_standalone() -> int:
    from app.config import get_settings
    from app.container import Container
    from app.core.logging import configure_logging

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    if settings.state_backend != "redis":
        logger.error(
            "worker_requires_redis", extra={"hint": "set STATE_BACKEND=redis; the memory queue is per-process"}
        )
        return 2

    container = Container(settings)
    await container.startup(start_embedded_worker=False)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # Windows event loops
            signal.signal(sig, lambda *_: stop.set())
    try:
        await container.worker.run(stop)
    finally:
        await container.shutdown()
    return 0


def main() -> None:
    sys.exit(asyncio.run(_run_standalone()))


if __name__ == "__main__":
    main()
