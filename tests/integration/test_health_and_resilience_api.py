"""Health endpoints, metrics, graceful degradation and failure behaviour."""

import time

import fakeredis
from httpx import AsyncClient

from tests.conftest import TENANT_A, AppFactory, create_doc, fake_redis
from tests.fakes import FlakySearchBackend


async def test_health_endpoints_when_everything_is_up(client: AsyncClient) -> None:
    health = await client.get("/health")
    assert health.status_code == 200
    body = health.json()
    assert body["status"] == "healthy"
    assert body["dependencies"]["search"]["status"] == "up"
    assert body["dependencies"]["search"]["critical"] is True
    assert body["dependencies"]["search"]["details"]["circuit"] == "closed"

    assert (await client.get("/health/live")).json() == {"status": "alive"}
    assert (await client.get("/health/ready")).json() == {"ready": True}

    info = (await client.get("/")).json()
    assert info["search_backend"] == "memory" and info["indexing_mode"] == "sync"


async def test_health_reports_redis_and_queue_when_configured(app_factory: AppFactory) -> None:
    server = fakeredis.FakeServer()
    client, _ = await app_factory(
        state_backend="redis", indexing_mode="async", run_embedded_worker=False, redis_client=fake_redis(server)
    )
    body = (await client.get("/health")).json()
    assert set(body["dependencies"]) == {"search", "redis", "indexing_queue"}
    assert body["status"] == "healthy"

    server.connected = False  # Redis outage = degraded, still serving
    degraded = await client.get("/health")
    assert degraded.status_code == 200 and degraded.json()["status"] == "degraded"
    assert degraded.json()["dependencies"]["redis"]["status"] == "down"


async def test_search_engine_down_is_unhealthy_but_replicas_stay_in_rotation(app_factory: AppFactory) -> None:
    """Readiness must not follow a shared dependency: if every replica went unready during an ES
    outage, the load balancer would have no endpoints and stale-if-error could never serve."""
    backend = FlakySearchBackend()
    client, _ = await app_factory(search_backend=backend)
    backend.failing = True
    health = await client.get("/health")
    assert health.status_code == 503 and health.json()["status"] == "unhealthy"  # monitoring/alerting signal
    assert (await client.get("/health/ready")).status_code == 200  # keep serving degraded results
    assert (await client.get("/health/live")).status_code == 200  # don't restart: it's the dependency


async def test_readiness_fails_while_draining(app_factory: AppFactory) -> None:
    client, container = await app_factory()
    container.accepting_traffic = False  # what shutdown() does first
    response = await client.get("/health/ready")
    assert response.status_code == 503 and response.json() == {"ready": False}


async def test_writes_fail_loudly_when_search_engine_is_down(app_factory: AppFactory) -> None:
    backend = FlakySearchBackend()
    client, _ = await app_factory(search_backend=backend, retry_attempts=2)
    backend.failing = True
    response = await client.post("/documents", headers=TENANT_A, json={"title": "t", "content": "c"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "SERVICE_UNAVAILABLE"
    assert backend.calls["index"] == 2  # retried once, then gave up

    backend.failing = False
    assert (await client.get("/search", headers=TENANT_A, params={"q": "t"})).json()["total"] == 0  # nothing faked


async def test_stale_results_served_when_search_engine_fails(app_factory: AppFactory) -> None:
    backend = FlakySearchBackend()
    client, container = await app_factory(search_backend=backend)
    await create_doc(client, title="Resilient search", content="stale if error")
    assert (await client.get("/search", headers=TENANT_A, params={"q": "resilient"})).json()["cache"] == "miss"

    container.search_cache._clock = lambda: time.time() + 120  # fresh TTL (60 s) elapsed
    backend.failing = True
    body = (await client.get("/search", headers=TENANT_A, params={"q": "resilient"})).json()
    assert body["cache"] == "stale" and body["degraded"] is True and body["total"] == 1

    uncached = await client.get("/search", headers=TENANT_A, params={"q": "never-searched"})
    assert uncached.status_code == 503


async def test_circuit_breaker_stops_calling_a_failing_engine(app_factory: AppFactory) -> None:
    backend = FlakySearchBackend()
    client, container = await app_factory(search_backend=backend, retry_attempts=1, breaker_failure_threshold=3)
    backend.failing = True
    for i in range(3):
        assert (
            await client.get("/search", headers=TENANT_A, params={"q": f"q{i}", "fuzzy": "false"})
        ).status_code == 503
    calls_when_opened = backend.calls["search"]

    fast_fail = await client.get("/search", headers=TENANT_A, params={"q": "another", "fuzzy": "false"})
    assert fast_fail.status_code == 503 and "retry-after" in fast_fail.headers
    assert backend.calls["search"] == calls_when_opened  # backend not touched while circuit is open
    assert container.search_breaker.state.value == "open"


async def test_redis_outage_degrades_gracefully(app_factory: AppFactory) -> None:
    server = fakeredis.FakeServer()
    client, _ = await app_factory(state_backend="redis", redis_client=fake_redis(server), cache_local_ttl_s=0)
    server.connected = False

    document = await create_doc(client, title="Works without redis", content="cache bypassed")
    assert (await client.get(f"/documents/{document['id']}", headers=TENANT_A)).status_code == 200
    search = await client.get("/search", headers=TENANT_A, params={"q": "redis"})
    assert search.status_code == 200 and search.json()["total"] == 1
    assert "x-ratelimit-remaining" in search.headers  # local fallback limiter still applied


async def test_unhandled_errors_return_a_clean_500(app_factory: AppFactory) -> None:
    backend = FlakySearchBackend()
    client, _ = await app_factory(search_backend=backend)
    backend.crash_with = RuntimeError("secret internal detail")
    response = await client.get("/search", headers=TENANT_A, params={"q": "boom"})
    assert response.status_code == 500
    error = response.json()["error"]
    assert error["code"] == "INTERNAL_ERROR" and error["request_id"] == response.headers["x-request-id"]
    assert "secret" not in response.text


async def test_unknown_routes_and_methods_use_envelope(client: AsyncClient) -> None:
    not_found = await client.get("/nope")
    assert not_found.status_code == 404 and not_found.json()["error"]["code"] == "NOT_FOUND"
    method = await client.put("/documents/abc", headers=TENANT_A)
    assert method.status_code == 405 and method.json()["error"]["code"] == "METHOD_NOT_ALLOWED"


async def test_prometheus_metrics_exposed(client: AsyncClient) -> None:
    await client.get("/search", headers=TENANT_A, params={"q": "metrics"})
    response = await client.get("/metrics")
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/plain")
    text = response.text
    assert 'http_requests_total{method="GET",route="/search",status="200"}' in text
    assert "search_backend_duration_seconds" in text or "cache_lookups_total" in text
    assert 'circuit_breaker_state{name="search"}' in text
