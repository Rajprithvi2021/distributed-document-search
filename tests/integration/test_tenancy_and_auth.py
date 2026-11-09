"""Tenant isolation across every endpoint, and the api_key / jwt authentication modes."""

import time

import jwt
from httpx import AsyncClient

from tests.conftest import TENANT_A, TENANT_B, AppFactory, create_doc

SECRET = "k" * 48


async def test_tenants_cannot_see_or_modify_each_others_documents(client: AsyncClient) -> None:
    document = await create_doc(client, title="Tenant A secret plan", content="confidential roadmap")

    search_b = await client.get("/search", headers=TENANT_B, params={"q": "confidential roadmap", "fuzzy": "true"})
    assert search_b.json()["total"] == 0
    assert (await client.get(f"/documents/{document['id']}", headers=TENANT_B)).status_code == 404
    assert (await client.delete(f"/documents/{document['id']}", headers=TENANT_B)).status_code == 404

    # Tenant A is unaffected by B's attempts.
    assert (await client.get(f"/documents/{document['id']}", headers=TENANT_A)).status_code == 200
    assert (await client.get("/search", headers=TENANT_A, params={"q": "confidential"})).json()["total"] == 1


async def test_cache_is_partitioned_by_tenant(client: AsyncClient) -> None:
    await create_doc(client, title="Shared words", content="identical query text")
    assert (await client.get("/search", headers=TENANT_A, params={"q": "identical"})).json()["total"] == 1
    # Same query, other tenant: must not be answered from tenant A's cache entry.
    other = (await client.get("/search", headers=TENANT_B, params={"q": "identical"})).json()
    assert other["total"] == 0 and other["cache"] == "miss"


async def test_tenant_resolution_errors(client: AsyncClient) -> None:
    missing = await client.get("/search", params={"q": "x"})
    assert missing.status_code == 400 and missing.json()["error"]["code"] == "MISSING_TENANT"

    invalid = await client.get("/search", params={"q": "x", "tenant": "docs-*"})
    assert invalid.status_code == 400 and invalid.json()["error"]["code"] == "INVALID_TENANT"

    mismatch = await client.get("/search", headers=TENANT_A, params={"q": "x", "tenant": "tenant-b"})
    assert mismatch.status_code == 400 and mismatch.json()["error"]["code"] == "TENANT_MISMATCH"

    no_tenant_needed = await client.get("/health")
    assert no_tenant_needed.status_code == 200


async def test_api_key_mode(app_factory: AppFactory) -> None:
    client, _ = await app_factory(auth_mode="api_key", api_keys={"key-for-a": "tenant-a"})
    unauthenticated = await client.get("/search", params={"q": "x"})
    assert unauthenticated.status_code == 401
    assert unauthenticated.headers["www-authenticate"] == "ApiKey"
    assert (await client.get("/search", headers={"X-API-Key": "nope"}, params={"q": "x"})).status_code == 401

    spoof = await client.get(
        "/search", headers={"X-API-Key": "key-for-a", "X-Tenant-ID": "tenant-b"}, params={"q": "x"}
    )
    assert spoof.status_code == 403

    created = await client.post("/documents", headers={"X-API-Key": "key-for-a"}, json={"title": "t", "content": "c"})
    assert created.status_code == 201 and created.json()["tenant_id"] == "tenant-a"


async def test_jwt_mode_enforces_scopes(app_factory: AppFactory) -> None:
    client, _ = await app_factory(auth_mode="jwt", jwt_secret=SECRET)

    def bearer(scope: str, **extra) -> dict[str, str]:
        claims = {
            "sub": "svc",
            "tenant_id": "tenant-a",
            "aud": "document-search",
            "scope": scope,
            "exp": int(time.time()) + 60,
        } | extra
        return {"Authorization": f"Bearer {jwt.encode(claims, SECRET, algorithm='HS256')}"}

    writer = bearer("documents:write documents:read search:read")
    reader = bearer("search:read")

    created = await client.post("/documents", headers=writer, json={"title": "JWT doc", "content": "scoped"})
    assert created.status_code == 201 and created.json()["created_by"] == "svc"

    assert (await client.get("/search", headers=reader, params={"q": "scoped"})).json()["total"] == 1
    forbidden = await client.post("/documents", headers=reader, json={"title": "t", "content": "c"})
    assert forbidden.status_code == 403 and forbidden.json()["error"]["details"]["required_scope"] == "documents:write"
    assert (await client.get(f"/documents/{created.json()['id']}", headers=reader)).status_code == 403

    expired = await client.get("/search", headers=bearer("search:read", exp=int(time.time()) - 5), params={"q": "x"})
    assert expired.status_code == 401 and expired.json()["error"]["message"] == "Token has expired"
