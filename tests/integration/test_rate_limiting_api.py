"""Per-tenant rate limiting through the HTTP API."""

from tests.conftest import TENANT_A, TENANT_B, AppFactory


async def test_tenant_is_throttled_after_its_burst(app_factory: AppFactory) -> None:
    client, _ = await app_factory(rate_limit_burst=3, rate_limit_requests_per_minute=60)
    statuses = [(await client.get("/search", headers=TENANT_A, params={"q": "x"})).status_code for _ in range(4)]
    assert statuses == [200, 200, 200, 429]

    throttled = await client.get("/search", headers=TENANT_A, params={"q": "x"})
    assert throttled.status_code == 429
    assert throttled.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"
    assert int(throttled.headers["retry-after"]) >= 1
    assert throttled.headers["x-ratelimit-limit"] == "3"
    assert throttled.headers["x-ratelimit-remaining"] == "0"

    # Noisy neighbour protection: tenant B has its own bucket.
    assert (await client.get("/search", headers=TENANT_B, params={"q": "x"})).status_code == 200


async def test_rate_limit_headers_on_success(app_factory: AppFactory) -> None:
    client, _ = await app_factory(rate_limit_burst=10, rate_limit_requests_per_minute=600)
    response = await client.get("/search", headers=TENANT_A, params={"q": "x"})
    assert response.headers["x-ratelimit-limit"] == "10"
    assert response.headers["x-ratelimit-remaining"] == "9"
    assert int(response.headers["x-ratelimit-reset"]) >= 0


async def test_per_tenant_overrides(app_factory: AppFactory) -> None:
    client, _ = await app_factory(
        rate_limit_burst=2, rate_limit_requests_per_minute=60, rate_limit_overrides={"tenant-b": 300}
    )
    a = [(await client.get("/search", headers=TENANT_A, params={"q": "x"})).status_code for _ in range(3)]
    b = [(await client.get("/search", headers=TENANT_B, params={"q": "x"})).status_code for _ in range(3)]
    assert a == [200, 200, 429]
    assert b == [200, 200, 200]  # premium tenant: 5x rate and burst


async def test_operational_endpoints_are_not_tenant_limited(app_factory: AppFactory) -> None:
    client, _ = await app_factory(rate_limit_burst=1, rate_limit_requests_per_minute=1)
    for _ in range(5):
        assert (await client.get("/health")).status_code == 200


async def test_rate_limiting_can_be_disabled(app_factory: AppFactory) -> None:
    client, _ = await app_factory(rate_limit_enabled=False, rate_limit_burst=1, rate_limit_requests_per_minute=1)
    for _ in range(3):
        assert (await client.get("/search", headers=TENANT_A, params={"q": "x"})).status_code == 200
