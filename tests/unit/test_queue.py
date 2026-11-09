import asyncio

import fakeredis
import pytest

from app.core.errors import DependencyUnavailableError
from app.queue.base import IndexingJob
from app.queue.memory import InMemoryQueue
from app.queue.redis_streams import RedisStreamQueue
from tests.conftest import fake_redis
from tests.fakes import make_document


def redis_queue(server: fakeredis.FakeServer | None = None, claim_idle_ms: int = 60_000) -> RedisStreamQueue:
    return RedisStreamQueue(
        fake_redis(server), stream="jobs", dead_letter_stream="dlq", group="indexers", claim_idle_ms=claim_idle_ms
    )


@pytest.fixture(params=["memory", "redis"])
async def queue(request: pytest.FixtureRequest):
    q = InMemoryQueue() if request.param == "memory" else redis_queue()
    await q.setup()
    return q


def job(doc_id: str) -> IndexingJob:
    return IndexingJob(job_id=f"job-{doc_id}", document=make_document(doc_id))


async def test_publish_consume_ack(queue) -> None:
    await queue.publish([job("d1"), job("d2")])
    assert await queue.depth() == 2
    messages = await queue.consume("c1", count=10, block_ms=0)
    assert [m.job.document.id for m in messages] == ["d1", "d2"]
    await queue.ack([m.message_id for m in messages])
    assert await queue.depth() == 0
    assert await queue.consume("c1", count=10, block_ms=0) == []


async def test_requeue_and_dead_letter(queue) -> None:
    await queue.publish([job("d1"), job("d2")])
    first, second = await queue.consume("c1", count=10, block_ms=0)

    await queue.requeue(first, "transient", count_attempt=True)
    await queue.requeue(second, "es down", count_attempt=False)
    retried = await queue.consume("c1", count=10, block_ms=0)
    assert {m.job.document.id: m.job.attempt for m in retried} == {"d1": 2, "d2": 1}
    assert retried[0].job.last_error in ("transient", "es down")

    await queue.dead_letter(retried[0], "poison")
    assert await queue.dead_letter_depth() == 1


async def test_redis_reclaims_jobs_abandoned_by_a_crashed_consumer() -> None:
    server = fakeredis.FakeServer()
    queue = redis_queue(server, claim_idle_ms=1)
    await queue.setup()
    await queue.publish([job("d1")])
    crashed = await queue.consume("worker-that-crashes", count=10, block_ms=0)
    assert len(crashed) == 1  # delivered, never acked

    await asyncio.sleep(0.05)  # > one tick of the 15.6 ms Windows wall clock fakeredis uses for idle time
    reclaimed = await queue.consume("healthy-worker", count=10, block_ms=0)
    assert [m.job.document.id for m in reclaimed] == ["d1"]


async def test_redis_consume_recreates_missing_group() -> None:
    queue = redis_queue()
    assert await queue.consume("c1", count=10, block_ms=0) == []  # NOGROUP -> setup
    await queue.publish([job("d1")])
    assert len(await queue.consume("c1", count=10, block_ms=0)) == 1


async def test_redis_errors_surface_as_dependency_unavailable() -> None:
    server = fakeredis.FakeServer()
    queue = redis_queue(server)
    server.connected = False
    with pytest.raises(DependencyUnavailableError):
        await queue.publish([job("d1")])
    with pytest.raises(DependencyUnavailableError):
        await queue.depth()


async def test_memory_consume_blocks_until_published() -> None:
    queue = InMemoryQueue()
    consumer = asyncio.create_task(queue.consume("c1", count=1, block_ms=1000))
    await asyncio.sleep(0)
    await queue.publish([job("d1")])
    messages = await asyncio.wait_for(consumer, timeout=1)
    assert messages[0].job.document.id == "d1"
