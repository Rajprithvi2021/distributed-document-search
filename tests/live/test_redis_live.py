"""Redis adapters against a real server: Lua token bucket, cache primitives, Streams queue."""

import asyncio
import uuid

import pytest
from redis.asyncio import Redis

from app.cache.redis import RedisCache
from app.container import Container
from app.core.resilience import CircuitBreaker
from app.queue.base import IndexingJob
from app.queue.redis_streams import RedisStreamQueue
from app.ratelimit.base import BucketConfig
from app.ratelimit.redis import RedisTokenBucket
from tests.conftest import make_settings
from tests.fakes import make_document
from tests.live.conftest import REDIS_URL, requires_redis

pytestmark = requires_redis


@pytest.fixture
def ns() -> str:
    return f"it:{uuid.uuid4().hex[:10]}"


async def _cleanup(client: Redis, ns: str) -> None:
    keys = [key async for key in client.scan_iter(match=f"{ns}*")]
    if keys:
        await client.delete(*keys)


async def test_idle_worker_blocking_read_is_not_treated_as_an_outage(ns: str) -> None:
    """Regression (found on a real Redis-compatible server): the worker's blocking XREADGROUP must not
    hit the client's short socket timeout, or an idle queue looks like a Redis outage and the worker
    backs off for up to 30 s before indexing new jobs."""
    settings = make_settings(
        state_backend="redis",
        redis_url=REDIS_URL,
        redis_socket_timeout_s=0.5,
        indexing_block_ms=1000,  # longer than the socket timeout on purpose
        indexing_stream=f"{ns}:jobs",
        indexing_dlq_stream=f"{ns}:dlq",
    )
    container = Container(settings)
    try:
        await container.queue.setup()
        assert await container.worker.process_batch() == 0  # waits ~1 s on the empty stream, no exception
    finally:
        assert container.redis is not None
        await _cleanup(container.redis, ns)
        await container.shutdown()


async def test_lua_token_bucket(redis_client: Redis, ns: str) -> None:
    now = [10_000_000]
    limiter = RedisTokenBucket(redis_client, breaker=CircuitBreaker("redis"), clock_ms=lambda: now[0], key_prefix=ns)
    bucket = BucketConfig(capacity=3, refill_per_s=1.0)
    try:
        decisions = [await limiter.acquire("t", bucket) for _ in range(4)]
        assert [d.allowed for d in decisions] == [True, True, True, False]
        assert decisions[3].retry_after_s == pytest.approx(1.0)
        now[0] += 2000
        assert (await limiter.acquire("t", bucket)).remaining == 1
        assert 0 < await redis_client.pttl(f"{ns}t") <= 4000  # idle buckets expire
    finally:
        await _cleanup(redis_client, ns)


async def test_concurrent_requests_never_over_admit(redis_client: Redis, ns: str) -> None:
    limiter = RedisTokenBucket(redis_client, breaker=CircuitBreaker("redis"), key_prefix=ns)
    bucket = BucketConfig(capacity=10, refill_per_s=0.001)
    try:
        decisions = await asyncio.gather(*(limiter.acquire("burst", bucket) for _ in range(50)))
        assert sum(d.allowed for d in decisions) == 10
    finally:
        await _cleanup(redis_client, ns)


async def test_cache_primitives(redis_client: Redis, ns: str) -> None:
    cache = RedisCache(redis_client)
    try:
        await cache.set(f"{ns}:k", "v", ttl_s=5)
        assert await cache.get(f"{ns}:k") == "v"
        assert await cache.add(f"{ns}:nx", "first", ttl_s=5) is True
        assert await cache.add(f"{ns}:nx", "second", ttl_s=5) is False
        assert [await cache.incr(f"{ns}:gen") for _ in range(3)] == [1, 2, 3]
        assert await redis_client.ttl(f"{ns}:gen") == -1  # counters have no TTL (never evicted)
        await cache.delete(f"{ns}:k")
        assert await cache.get(f"{ns}:k") is None
    finally:
        await _cleanup(redis_client, ns)


async def test_streams_queue_lifecycle(redis_client: Redis, ns: str) -> None:
    queue = RedisStreamQueue(
        redis_client, stream=f"{ns}:jobs", dead_letter_stream=f"{ns}:dlq", group="indexers", claim_idle_ms=50
    )
    try:
        await queue.setup()
        await queue.setup()  # idempotent (BUSYGROUP tolerated)
        await queue.publish([IndexingJob(job_id=i, document=make_document(i)) for i in ("d1", "d2", "d3")])
        assert await queue.depth() == 3

        first = await queue.consume("worker-1", count=2, block_ms=100)
        assert [m.job.document.id for m in first] == ["d1", "d2"]
        await queue.ack([first[0].message_id])
        await queue.requeue(first[1], "retryable", count_attempt=True)

        await asyncio.sleep(0.1)  # worker-1 "crashes" holding nothing; d3 + requeued d2 remain
        rest = await queue.consume("worker-2", count=10, block_ms=100)
        assert {m.job.document.id: m.job.attempt for m in rest} == {"d3": 1, "d2": 2}
        crashed = rest  # leave unacked, then let another worker reclaim them
        await asyncio.sleep(0.1)
        reclaimed = await queue.consume("worker-3", count=10, block_ms=0)
        assert {m.message_id for m in reclaimed} == {m.message_id for m in crashed}

        await queue.dead_letter(reclaimed[0], "poison")
        await queue.ack([m.message_id for m in reclaimed[1:]])
        assert await queue.depth() == 0
        assert await queue.dead_letter_depth() == 1
    finally:
        await _cleanup(redis_client, ns)
