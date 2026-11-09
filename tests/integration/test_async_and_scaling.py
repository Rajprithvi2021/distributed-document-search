"""Asynchronous indexing via the queue, and behaviour across multiple API instances."""

import asyncio

import fakeredis
from httpx import AsyncClient

from app.search.memory import InMemorySearchBackend
from tests.conftest import TENANT_A, AppFactory, create_doc, fake_redis


async def search_total(client: AsyncClient, q: str) -> int:
    return (await client.get("/search", headers=TENANT_A, params={"q": q})).json()["total"]


async def test_async_indexing_lifecycle(app_factory: AppFactory) -> None:
    client, container = await app_factory(indexing_mode="async", run_embedded_worker=False)
    response = await client.post("/documents", headers=TENANT_A, json={"title": "Queued doc", "content": "via queue"})
    assert response.status_code == 202 and response.json()["status"] == "pending"
    doc_id = response.json()["id"]

    pending = await client.get(f"/documents/{doc_id}", headers=TENANT_A)
    assert pending.status_code == 200 and pending.json()["status"] == "pending"  # read-your-write by id
    assert await search_total(client, "queued") == 0  # not searchable yet (eventual consistency)

    assert await container.worker.process_batch(block_ms=0) == 1
    assert await search_total(client, "queued") == 1
    assert (await client.get(f"/documents/{doc_id}", headers=TENANT_A)).json()["status"] == "indexed"


async def test_async_bulk_is_accepted(app_factory: AppFactory) -> None:
    client, container = await app_factory(indexing_mode="async", run_embedded_worker=False)
    docs = [{"title": f"Batch {i}", "content": "batched"} for i in range(10)]
    response = await client.post("/documents/bulk", headers=TENANT_A, json={"documents": docs})
    assert response.status_code == 202 and response.json()["succeeded"] == 10
    await container.worker.process_batch(block_ms=0)
    assert await search_total(client, "batched") == 10


async def test_delete_before_indexing_prevents_resurrection(app_factory: AppFactory) -> None:
    client, container = await app_factory(indexing_mode="async", run_embedded_worker=False)
    document = await create_doc(client, title="Short lived", content="deleted while queued")
    assert (await client.delete(f"/documents/{document['id']}", headers=TENANT_A)).status_code == 204

    await container.worker.process_batch(block_ms=0)
    assert await search_total(client, "short lived") == 0
    assert (await client.get(f"/documents/{document['id']}", headers=TENANT_A)).status_code == 404


async def test_embedded_worker_indexes_in_background(app_factory: AppFactory) -> None:
    client, _ = await app_factory(indexing_mode="async", run_embedded_worker=True)
    await create_doc(client, title="Background", content="embedded worker")
    for _ in range(100):
        if await search_total(client, "background") == 1:
            break
        await asyncio.sleep(0.02)
    assert await search_total(client, "background") == 1


async def test_async_indexing_over_redis_streams(app_factory: AppFactory) -> None:
    server = fakeredis.FakeServer()
    client, container = await app_factory(
        state_backend="redis", indexing_mode="async", run_embedded_worker=False, redis_client=fake_redis(server)
    )
    await create_doc(client, title="Streamed", content="redis streams job")
    assert await container.queue.depth() == 1
    await container.worker.process_batch(block_ms=0)
    assert await container.queue.depth() == 0
    assert await search_total(client, "streamed") == 1


async def test_two_api_instances_share_cache_rate_limits_and_index(app_factory: AppFactory) -> None:
    """Horizontal scaling: stateless replicas sharing Redis (fakeredis) and one search engine."""
    server = fakeredis.FakeServer()
    shared_engine = InMemorySearchBackend()
    common = {
        "state_backend": "redis",
        "search_backend": shared_engine,
        "cache_local_ttl_s": 0,
        "rate_limit_burst": 6,
        "rate_limit_requests_per_minute": 60,
    }
    replica_1, _ = await app_factory(redis_client=fake_redis(server), **common)
    replica_2, _ = await app_factory(redis_client=fake_redis(server), **common)

    document = await create_doc(replica_1, title="Replicated", content="written on replica one")
    assert await search_total(replica_2, "replicated") == 1  # 2 requests used
    cached = await replica_1.get("/search", headers=TENANT_A, params={"q": "replicated"})
    assert cached.json()["cache"] == "hit"  # cache entry written by replica 2

    await replica_2.delete(f"/documents/{document['id']}", headers=TENANT_A)  # bumps the shared generation
    assert await search_total(replica_1, "replicated") == 0  # no stale hit on replica 1

    # Burst of 6 is shared by both replicas: 5 requests used so far.
    assert (await replica_2.get("/search", headers=TENANT_A, params={"q": "x"})).status_code == 200
    assert (await replica_1.get("/search", headers=TENANT_A, params={"q": "x"})).status_code == 429


async def test_get_after_delete_on_another_replica_is_404(app_factory: AppFactory) -> None:
    """Regression (found on the real 2-replica topology): with the default 2 s L1 cache, a document
    read on replica 1 and deleted on replica 2 must not be served again by replica 1."""
    server = fakeredis.FakeServer()
    shared_engine = InMemorySearchBackend()
    common = {"state_backend": "redis", "search_backend": shared_engine}  # default CACHE_LOCAL_TTL_S (2 s)
    replica_1, _ = await app_factory(redis_client=fake_redis(server), **common)
    replica_2, _ = await app_factory(redis_client=fake_redis(server), **common)

    document = await create_doc(replica_1, title="Short lived", content="read then deleted elsewhere")
    assert (await replica_1.get(f"/documents/{document['id']}", headers=TENANT_A)).status_code == 200
    assert (await replica_2.delete(f"/documents/{document['id']}", headers=TENANT_A)).status_code == 204
    assert (await replica_1.get(f"/documents/{document['id']}", headers=TENANT_A)).status_code == 404
