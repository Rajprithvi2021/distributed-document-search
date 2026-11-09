"""Composition root: the only place that knows which concrete adapters are in use.

Everything else receives its collaborators through constructors, which is why
tests can assemble the full application from in-memory or fake adapters.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Literal

from redis.asyncio import Redis

from app.cache.base import Cache
from app.cache.layers import GuardedCache, TieredCache
from app.cache.memory import MemoryCache
from app.cache.redis import RedisCache
from app.cache.stores import DocumentCache, SearchResultCache
from app.config import Settings
from app.core.resilience import CircuitBreaker, RetryPolicy
from app.core.security import Authenticator
from app.domain.models import DependencyHealth
from app.queue.base import IndexingQueue
from app.queue.memory import InMemoryQueue
from app.queue.redis_streams import RedisStreamQueue
from app.ratelimit.base import RateLimiter, TenantRateLimitPolicy
from app.ratelimit.memory import InMemoryTokenBucket
from app.ratelimit.redis import RedisTokenBucket
from app.search.base import SearchBackend
from app.search.memory import InMemorySearchBackend
from app.search.resilient import ResilientSearchBackend
from app.services.documents import DocumentService
from app.services.health import HealthService
from app.services.search import SearchService
from app.workers.indexer import IndexingWorker

logger = logging.getLogger(__name__)


class Container:
    def __init__(
        self,
        settings: Settings,
        *,
        redis_client: Redis | None = None,
        search_backend: SearchBackend | None = None,
    ) -> None:
        self.settings = settings
        self.search_breaker = CircuitBreaker(
            "search",
            failure_threshold=settings.breaker_failure_threshold,
            recovery_timeout_s=settings.breaker_recovery_timeout_s,
        )
        self.redis_breaker = CircuitBreaker(
            "redis",
            failure_threshold=settings.breaker_failure_threshold,
            recovery_timeout_s=settings.breaker_recovery_timeout_s,
        )

        # --- state: cache, rate limiter, queue ------------------------------
        self.redis: Redis | None = None
        self.queue_redis: Redis | None = None
        # Two views of the cache:
        # * ``cache`` (L1 in-process -> L2 shared) for search pages, where <= CACHE_LOCAL_TTL_S of
        #   cross-replica staleness is an accepted, documented trade-off;
        # * ``shared_cache`` (L2 only) for per-document state, tombstones and idempotency keys, which
        #   must be read-your-writes across replicas (a document deleted on replica B must not be
        #   served from replica A's memory).
        self.cache: Cache
        self.shared_cache: Cache
        self.rate_limiter: RateLimiter
        self.queue: IndexingQueue
        if settings.state_backend == "redis":
            self.redis = redis_client or Redis.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_timeout=settings.redis_socket_timeout_s,
                socket_connect_timeout=settings.redis_socket_timeout_s,
                max_connections=settings.redis_max_connections,
                health_check_interval=30,
            )
            self.shared_cache = GuardedCache(RedisCache(self.redis), self.redis_breaker)
            self.cache = TieredCache(
                MemoryCache(settings.cache_local_max_items, name="l1"),
                self.shared_cache,
                l1_ttl_s=settings.cache_local_ttl_s,
            )
            self.rate_limiter = RedisTokenBucket(self.redis, breaker=self.redis_breaker)
            # The queue gets its own connection pool: the worker's blocking XREADGROUP waits up to
            # INDEXING_BLOCK_MS, which must not trip the short socket timeout used for cache calls
            # (otherwise an idle queue looks like a Redis outage and the worker backs off).
            self.queue_redis = redis_client or Redis.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_timeout=settings.redis_socket_timeout_s + settings.indexing_block_ms / 1000 + 1.0,
                socket_connect_timeout=settings.redis_socket_timeout_s,
                max_connections=20,
                health_check_interval=30,
            )
            self.queue = RedisStreamQueue(
                self.queue_redis,
                stream=settings.indexing_stream,
                dead_letter_stream=settings.indexing_dlq_stream,
                group=settings.indexing_consumer_group,
                claim_idle_ms=settings.indexing_claim_idle_ms,
            )
        else:
            self.cache = self.shared_cache = MemoryCache(settings.cache_local_max_items)
            self.rate_limiter = InMemoryTokenBucket()
            self.queue = InMemoryQueue()

        # --- search engine ---------------------------------------------------
        if search_backend is None:
            if settings.search_backend == "elasticsearch":
                from app.search.elasticsearch import ElasticsearchBackend  # optional heavy import

                search_backend = ElasticsearchBackend(settings)
            else:
                search_backend = InMemorySearchBackend()
        self.raw_search_backend = search_backend
        self.search_backend = ResilientSearchBackend(
            search_backend,
            self.search_breaker,
            RetryPolicy(
                settings.retry_attempts,
                settings.retry_base_delay_s,
                settings.retry_max_delay_s,
                deadline_s=settings.retry_deadline_s,
            ),
        )

        # --- services ---------------------------------------------------------
        self.search_cache = SearchResultCache(
            self.cache,
            shared=self.shared_cache,
            fresh_ttl_s=settings.cache_search_ttl_s,
            stale_ttl_s=settings.cache_search_stale_ttl_s,
            visibility_s=0.0 if settings.elasticsearch_refresh == "wait_for" else settings.search_visibility_s,
        )
        self.document_cache = DocumentCache(self.shared_cache, ttl_s=settings.cache_document_ttl_s)
        self.authenticator = Authenticator(settings)
        self.rate_limit_policy = TenantRateLimitPolicy(settings)
        self.documents = DocumentService(
            backend=self.search_backend,
            documents=self.document_cache,
            search_cache=self.search_cache,
            queue=self.queue,
            indexing_mode=settings.indexing_mode,
            max_document_bytes=settings.max_document_bytes,
        )
        self.search = SearchService(
            backend=self.search_backend, cache=self.search_cache, max_result_window=settings.search_max_result_window
        )
        self.worker = IndexingWorker(
            queue=self.queue,
            backend=self.search_backend,
            documents=self.document_cache,
            search_cache=self.search_cache,
            batch_size=settings.indexing_batch_size,
            max_attempts=settings.indexing_max_attempts,
            block_ms=settings.indexing_block_ms,
        )
        self.health = HealthService(self._health_checks())
        self._stop = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        # Readiness is local: true between start-up and the start of shutdown (draining). It deliberately
        # ignores shared dependencies, so an Elasticsearch outage doesn't pull every replica out of the
        # load balancer at once; replicas keep serving cached (degraded) results and clear 503s instead.
        self.accepting_traffic = False

    # -- health ---------------------------------------------------------------
    def _health_checks(self) -> dict:
        checks: dict = {"search": (self.search_backend.health, True)}
        if self.redis is not None:
            checks["redis"] = (self._redis_health, False)
        if self.settings.indexing_mode == "async":
            checks["indexing_queue"] = (self._queue_health, False)
        return checks

    async def _redis_health(self) -> DependencyHealth:
        assert self.redis is not None
        started = time.perf_counter()
        try:
            await self.redis.ping()
            status = "up"
            details: dict[str, object] = {"circuit": self.redis_breaker.state.value}
        except Exception as exc:
            status, details = "down", {"error": type(exc).__name__, "circuit": self.redis_breaker.state.value}
        return DependencyHealth(
            name="redis",
            status=status,
            critical=False,
            latency_ms=round((time.perf_counter() - started) * 1000, 2),
            details=details,
        )

    async def _queue_health(self) -> DependencyHealth:
        started = time.perf_counter()
        depth = await self.queue.depth()
        dead = await self.queue.dead_letter_depth()
        return DependencyHealth(
            name="indexing_queue",
            status="up" if dead == 0 else "degraded",
            critical=False,
            latency_ms=round((time.perf_counter() - started) * 1000, 2),
            details={"backend": self.queue.name, "depth": depth, "dead_letters": dead},
        )

    # -- lifecycle -----------------------------------------------------------
    async def startup(self, *, role: Literal["api", "worker"] = "api") -> None:
        """Start the process. ``role="worker"`` is the standalone indexer (python -m app.workers.indexer)."""
        await self.search_backend.startup()
        if role == "worker" or self.settings.indexing_mode == "async":
            try:
                await self.queue.setup()
            except Exception as exc:  # Redis may still be starting; the worker retries on consume
                logger.warning("queue_setup_deferred", extra={"error": str(exc)})

        if role == "api" and self.settings.indexing_mode == "async" and self.settings.run_embedded_worker:
            self._worker_task = asyncio.create_task(self.worker.run(self._stop), name="embedded-indexer")
        self.accepting_traffic = True
        logger.info(
            "container_started",
            extra={
                "role": role,
                "search_backend": self.raw_search_backend.name,
                "state_backend": self.settings.state_backend,
                "indexing_mode": self.settings.indexing_mode,
                "auth_mode": self.settings.auth_mode,
            },
        )

    async def shutdown(self) -> None:
        self.accepting_traffic = False  # readiness fails first, so the load balancer drains this replica
        self._stop.set()
        if self._worker_task is not None:
            try:
                await asyncio.wait_for(self._worker_task, timeout=5)
            except (TimeoutError, asyncio.CancelledError):
                self._worker_task.cancel()
        await self.search_backend.shutdown()
        if self.queue_redis is not None and self.queue_redis is not self.redis:
            await self.queue_redis.aclose()
        if self.redis is not None:
            await self.redis.aclose()
