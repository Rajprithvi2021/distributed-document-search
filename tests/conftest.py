"""Shared fixtures.

The whole application is assembled in-process from in-memory adapters, or from
fakeredis for the Redis paths (including the Lua rate-limit script and Redis
Streams), so the suite needs no Docker and runs in seconds.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import fakeredis
import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.container import Container
from app.main import create_app

TENANT_A = {"X-Tenant-ID": "tenant-a"}
TENANT_B = {"X-Tenant-ID": "tenant-b"}


def make_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "environment": "test",
        "log_json": False,
        "log_level": "WARNING",
        "search_backend": "memory",
        "state_backend": "memory",
        "rate_limit_requests_per_minute": 60_000,
        "rate_limit_burst": 10_000,
        "retry_base_delay_s": 0.0,
        "retry_max_delay_s": 0.0,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def fake_redis(server: fakeredis.FakeServer | None = None) -> fakeredis.FakeAsyncRedis:
    return fakeredis.FakeAsyncRedis(server=server or fakeredis.FakeServer(), decode_responses=True)


AppFactory = Callable[..., Awaitable[tuple[AsyncClient, Container]]]


@pytest.fixture
async def app_factory() -> AsyncIterator[AppFactory]:
    """Build (client, container) pairs; everything is shut down after the test."""
    opened: list[tuple[LifespanManager, AsyncClient]] = []

    async def build(
        *, container: Container | None = None, redis_client: Any = None, search_backend: Any = None, **overrides: Any
    ) -> tuple[AsyncClient, Container]:
        if container is None:
            settings = make_settings(**overrides)
            container = Container(settings, redis_client=redis_client, search_backend=search_backend)
        app = create_app(container.settings, container)
        manager = LifespanManager(app)
        await manager.__aenter__()
        client = AsyncClient(transport=ASGITransport(app=manager.app), base_url="http://testserver")
        opened.append((manager, client))
        return client, container

    yield build
    for manager, client in reversed(opened):
        await client.aclose()
        await manager.__aexit__(None, None, None)


@pytest.fixture
async def client(app_factory: AppFactory) -> AsyncClient:
    client, _ = await app_factory()
    return client


async def create_doc(client: AsyncClient, headers: dict[str, str] = TENANT_A, **body: Any) -> dict[str, Any]:
    payload = {"title": "Distributed search", "content": "Elasticsearch and Redis power fast search."} | body
    response = await client.post("/documents", headers=headers, json=payload)
    assert response.status_code in (201, 202), response.text
    return response.json()
