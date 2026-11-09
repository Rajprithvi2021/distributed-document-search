"""Required endpoint GET /search?q={query}&tenant={tenantId} plus the bonus search features."""

import asyncio

import pytest
from httpx import AsyncClient

from tests.conftest import TENANT_A, create_doc


@pytest.fixture
async def corpus(client: AsyncClient) -> AsyncClient:
    await create_doc(
        client,
        title="Distributed search architecture",
        content="Sharding and replicas for scale.",
        tags=["architecture", "search"],
        metadata={"category": "engineering", "level": "advanced"},
    )
    await asyncio.sleep(0.02)  # distinct created_at even on a coarse (15.6 ms) wall clock
    await create_doc(
        client,
        title="Caching strategy",
        content="Redis keeps distributed search responses fast.",
        tags=["performance"],
        metadata={"category": "engineering"},
    )
    await asyncio.sleep(0.02)
    await create_doc(
        client, title="Team offsite", content="Agenda, travel and food.", tags=["people"], metadata={"category": "hr"}
    )
    return client


async def test_assignment_contract_tenant_query_parameter(corpus: AsyncClient) -> None:
    response = await corpus.get("/search", params={"q": "distributed search", "tenant": "tenant-a"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["results"][0]["title"] == "Distributed search architecture"  # title boost wins
    assert body["results"][0]["score"] > body["results"][1]["score"]


async def test_response_shape_pagination_and_highlighting(corpus: AsyncClient) -> None:
    body = (await corpus.get("/search", headers=TENANT_A, params={"q": "distributed", "page_size": 1})).json()
    assert (body["total"], body["page"], body["page_size"], body["total_pages"]) == (2, 1, 1, 2)
    assert len(body["results"]) == 1
    hit = body["results"][0]
    assert "<em>Distributed</em>" in hit["highlights"]["title"][0]
    assert body["cache"] == "miss" and body["degraded"] is False and body["took_ms"] >= 0

    page_two = (
        await corpus.get("/search", headers=TENANT_A, params={"q": "distributed", "page_size": 1, "page": 2})
    ).json()
    assert page_two["results"][0]["id"] != hit["id"]


async def test_facets_and_filters(corpus: AsyncClient) -> None:
    body = (
        await corpus.get("/search", headers=TENANT_A, params={"q": "distributed", "facets": "tags,metadata.category"})
    ).json()
    assert {b["value"]: b["count"] for b in body["facets"]["metadata.category"]} == {"engineering": 2}
    assert {b["value"] for b in body["facets"]["tags"]} == {"architecture", "search", "performance"}

    by_tag = (await corpus.get("/search", headers=TENANT_A, params={"q": "distributed", "tags": "performance"})).json()
    assert [r["title"] for r in by_tag["results"]] == ["Caching strategy"]

    by_meta = (
        await corpus.get("/search", headers=TENANT_A, params={"q": "distributed", "filter": "level:advanced"})
    ).json()
    assert [r["title"] for r in by_meta["results"]] == ["Distributed search architecture"]


async def test_fuzzy_auto_and_explicit_modes(corpus: AsyncClient) -> None:
    auto = (await corpus.get("/search", headers=TENANT_A, params={"q": "distibuted"})).json()
    assert auto["total"] == 2 and auto["fuzzy_applied"] is True

    off = (await corpus.get("/search", headers=TENANT_A, params={"q": "distibuted", "fuzzy": "false"})).json()
    assert off["total"] == 0 and off["fuzzy_applied"] is False


async def test_sort_newest(corpus: AsyncClient) -> None:
    body = (await corpus.get("/search", headers=TENANT_A, params={"q": "distributed", "sort": "newest"})).json()
    assert [r["title"] for r in body["results"]] == ["Caching strategy", "Distributed search architecture"]


async def test_cache_hit_then_invalidation_on_write(corpus: AsyncClient) -> None:
    params = {"q": "redis"}
    assert (await corpus.get("/search", headers=TENANT_A, params=params)).json()["cache"] == "miss"
    cached = (await corpus.get("/search", headers=TENANT_A, params=params)).json()
    assert cached["cache"] == "hit" and cached["total"] == 1

    await create_doc(corpus, title="Redis cluster", content="Sharded redis.")
    fresh = (await corpus.get("/search", headers=TENANT_A, params=params)).json()
    assert fresh["cache"] == "miss" and fresh["total"] == 2  # new document visible immediately


async def test_deleted_documents_disappear_from_results(client: AsyncClient) -> None:
    document = await create_doc(client, title="Ephemeral", content="soon gone")
    assert (await client.get("/search", headers=TENANT_A, params={"q": "ephemeral"})).json()["total"] == 1
    await client.delete(f"/documents/{document['id']}", headers=TENANT_A)
    assert (await client.get("/search", headers=TENANT_A, params={"q": "ephemeral"})).json()["total"] == 0


async def test_results_are_html_safe(client: AsyncClient) -> None:
    await create_doc(client, title="<img src=x onerror=alert(1)> payload", content="<script>steal()</script> payload")
    hit = (await client.get("/search", headers=TENANT_A, params={"q": "payload"})).json()["results"][0]
    assert "<script>" not in hit["snippet"] and "&lt;script&gt;" in hit["snippet"]
    assert "<img" not in hit["highlights"]["title"][0]


@pytest.mark.parametrize(
    ("params", "status", "code"),
    [
        ({}, 422, "VALIDATION_ERROR"),
        ({"q": "   "}, 422, "VALIDATION_ERROR"),
        ({"q": "x" * 513}, 422, "VALIDATION_ERROR"),
        ({"q": "x", "page_size": 101}, 422, "VALIDATION_ERROR"),
        ({"q": "x", "sort": "random"}, 422, "VALIDATION_ERROR"),
        ({"q": "x", "facets": "content"}, 422, "VALIDATION_ERROR"),
        ({"q": "x", "filter": "no-colon"}, 422, "VALIDATION_ERROR"),
        ({"q": "x", "page": 1001, "page_size": 10}, 400, "BAD_REQUEST"),
    ],
)
async def test_invalid_search_parameters(client: AsyncClient, params: dict, status: int, code: str) -> None:
    response = await client.get("/search", headers=TENANT_A, params=params)
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
