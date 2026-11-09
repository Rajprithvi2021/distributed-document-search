"""The whole application (HTTP → services → Elasticsearch + Redis), as deployed by docker compose."""

import pytest
from redis.asyncio import Redis

from app.container import Container
from tests.conftest import AppFactory
from tests.live.conftest import ES_URL, REDIS_URL, drop_indices, es_settings, unique_prefix

pytestmark = pytest.mark.skipif(not (ES_URL and REDIS_URL), reason="set ES_TEST_URL and REDIS_TEST_URL")


@pytest.mark.parametrize("indexing_mode", ["sync", "async"])
async def test_required_endpoints_end_to_end(app_factory: AppFactory, indexing_mode: str) -> None:
    prefix = unique_prefix()
    tenant, other = prefix, f"{prefix}-other"  # unique tenants: no state shared with earlier runs
    own, foreign = {"X-Tenant-ID": tenant}, {"X-Tenant-ID": other}
    settings = es_settings(
        prefix,
        state_backend="redis",
        redis_url=REDIS_URL,
        indexing_mode=indexing_mode,
        run_embedded_worker=False,
        indexing_stream=f"{prefix}:jobs",
        indexing_dlq_stream=f"{prefix}:dlq",
    )
    container = Container(settings, redis_client=Redis.from_url(REDIS_URL, decode_responses=True))
    client, _ = await app_factory(container=container)
    try:
        health = (await client.get("/health")).json()
        assert health["status"] == "healthy", health
        assert set(health["dependencies"]) >= {"search", "redis"}

        created = await client.post(
            "/documents",
            headers=own,
            json={
                "title": "Live stack",
                "content": "Elasticsearch and Redis end to end",
                "tags": ["live"],
                "metadata": {"category": "e2e"},
            },
        )
        assert created.status_code == (201 if indexing_mode == "sync" else 202), created.text
        doc_id = created.json()["id"]
        if indexing_mode == "async":
            assert await container.worker.process_batch(block_ms=1000) == 1

        params = {"q": "elasticsearch redis", "tenant": tenant, "facets": "tags,metadata.category"}
        first = await client.get("/search", params=params)
        body = first.json()
        assert first.status_code == 200 and body["total"] == 1 and body["cache"] == "miss", body
        assert body["facets"]["metadata.category"][0]["value"] == "e2e"
        assert int(first.headers["x-ratelimit-remaining"]) >= 0
        assert (await client.get("/search", params=params)).json()["cache"] == "hit"  # served by Redis/L1

        assert (await client.get(f"/documents/{doc_id}", headers=own)).json()["status"] == "indexed"
        assert (await client.get(f"/documents/{doc_id}", headers=foreign)).status_code == 404
        assert (await client.delete(f"/documents/{doc_id}", headers=own)).status_code == 204
        assert (await client.get(f"/documents/{doc_id}", headers=own)).status_code == 404
        after = await client.get("/search", params=params)
        assert after.json()["total"] == 0 and after.json()["cache"] == "miss"  # delete invalidated the cache
    finally:
        await drop_indices(container.raw_search_backend, prefix)
        assert container.redis is not None
        keys = [k async for k in container.redis.scan_iter(match=f"*{prefix}*")]
        if keys:
            await container.redis.delete(*keys)
