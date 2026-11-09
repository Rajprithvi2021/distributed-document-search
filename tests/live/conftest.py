"""Live tests against real Elasticsearch / Redis.

Skipped unless the services are provided, e.g.:

    ES_TEST_URL=http://localhost:9200 REDIS_TEST_URL=redis://localhost:6379/0 pytest tests/live -v

CI starts both as service containers (see .github/workflows/ci.yml). Every test uses unique
index prefixes / key prefixes and cleans up after itself, so it is safe against shared instances.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis

from app.config import Settings
from app.search.elasticsearch import ElasticsearchBackend
from tests.conftest import make_settings

ES_URL = os.environ.get("ES_TEST_URL")
REDIS_URL = os.environ.get("REDIS_TEST_URL")

requires_es = pytest.mark.skipif(not ES_URL, reason="set ES_TEST_URL to run against a real Elasticsearch")
requires_redis = pytest.mark.skipif(not REDIS_URL, reason="set REDIS_TEST_URL to run against a real Redis")


def es_settings(prefix: str, tenancy: str = "index_per_tenant", **overrides) -> Settings:
    return make_settings(
        search_backend="elasticsearch",
        elasticsearch_url=ES_URL,
        elasticsearch_username=os.environ.get("ES_TEST_USERNAME"),
        elasticsearch_password=os.environ.get("ES_TEST_PASSWORD"),
        elasticsearch_index_prefix=prefix,
        elasticsearch_tenancy=tenancy,
        elasticsearch_refresh="wait_for",  # deterministic visibility for assertions
        **overrides,
    )


def unique_prefix() -> str:
    return "it" + uuid.uuid4().hex[:10]


async def drop_indices(backend: ElasticsearchBackend, prefix: str) -> None:
    client = backend._client
    existing = await client.indices.get(index=f"{prefix}-*", allow_no_indices=True, ignore_unavailable=True)
    names = list(existing.body)
    if names:  # explicit names: wildcard deletes are refused by action.destructive_requires_name
        await client.indices.delete(index=",".join(names))


@pytest.fixture(params=["index_per_tenant", "shared_index"])
async def es_backend(request: pytest.FixtureRequest) -> AsyncIterator[ElasticsearchBackend]:
    prefix = unique_prefix()
    backend = ElasticsearchBackend(es_settings(prefix, request.param))
    try:
        yield backend
    finally:
        await drop_indices(backend, prefix)
        await backend.shutdown()


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client = Redis.from_url(REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()
