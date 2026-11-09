"""Required endpoints: POST /documents, GET /documents/{id}, DELETE /documents/{id} (+ bulk)."""

import pytest
from httpx import AsyncClient

from tests.conftest import TENANT_A, AppFactory, create_doc


async def test_create_returns_201_with_location_and_document(client: AsyncClient) -> None:
    response = await client.post(
        "/documents",
        headers=TENANT_A,
        json={
            "title": "  Search 101 ",
            "content": "Inverted indexes.",
            "tags": ["Search", "search", "ops"],
            "metadata": {"category": "eng", "year": 2024, "public": True},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert response.headers["location"] == f"/documents/{body['id']}"
    assert body["title"] == "Search 101" and body["tenant_id"] == "tenant-a" and body["status"] == "indexed"
    assert body["tags"] == ["search", "ops"]  # normalised + de-duplicated
    assert body["metadata"] == {"category": "eng", "year": 2024, "public": True}


async def test_get_and_delete_lifecycle(client: AsyncClient) -> None:
    document = await create_doc(client)
    fetched = await client.get(f"/documents/{document['id']}", headers=TENANT_A)
    assert fetched.status_code == 200 and fetched.json()["content"] == document["content"]

    deleted = await client.delete(f"/documents/{document['id']}", headers=TENANT_A)
    assert deleted.status_code == 204 and deleted.content == b""
    assert deleted.headers["x-ratelimit-remaining"]  # rate-limit headers also on 204

    missing = await client.get(f"/documents/{document['id']}", headers=TENANT_A)
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "DOCUMENT_NOT_FOUND"
    assert (await client.delete(f"/documents/{document['id']}", headers=TENANT_A)).status_code == 404


@pytest.mark.parametrize(
    ("payload", "location"),
    [
        ({"title": "", "content": "x"}, "body.title"),
        ({"title": "   ", "content": "x"}, "body.title"),
        ({"title": "t"}, "body.content"),
        ({"title": "t", "content": "x", "unexpected": 1}, "body.unexpected"),
        ({"title": "t", "content": "x", "tags": ["has space"]}, "body.tags"),
        ({"title": "t", "content": "x", "tags": [f"t{i}" for i in range(51)]}, "body.tags"),
        ({"title": "t", "content": "x", "metadata": {"bad.key": "v"}}, "body.metadata"),
        ({"title": "t", "content": "x", "metadata": {"nested": {"a": 1}}}, "body.metadata"),
    ],
)
async def test_validation_errors_use_the_standard_envelope(client: AsyncClient, payload: dict, location: str) -> None:
    response = await client.post("/documents", headers=TENANT_A, json=payload)
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "VALIDATION_ERROR" and error["request_id"]
    assert any(detail["location"].startswith(location) for detail in error["details"])


async def test_document_and_request_size_limits(app_factory: AppFactory) -> None:
    client, _ = await app_factory(max_document_bytes=100, max_request_bytes=2048)
    too_big_doc = await client.post("/documents", headers=TENANT_A, json={"title": "t", "content": "x" * 200})
    assert too_big_doc.status_code == 413 and too_big_doc.json()["error"]["code"] == "PAYLOAD_TOO_LARGE"

    too_big_request = await client.post("/documents", headers=TENANT_A, json={"title": "t", "content": "x" * 5000})
    assert too_big_request.status_code == 413
    assert too_big_request.json()["error"]["details"] == {"limit_bytes": 2048}


async def test_invalid_document_id_is_rejected(client: AsyncClient) -> None:
    response = await client.get("/documents/bad id!", headers=TENANT_A)
    assert response.status_code == 422


async def test_idempotency_key_replays_the_original_document(client: AsyncClient) -> None:
    headers = TENANT_A | {"Idempotency-Key": "order-42"}
    first = await client.post("/documents", headers=headers, json={"title": "Once", "content": "only once"})
    retry = await client.post("/documents", headers=headers, json={"title": "Once", "content": "only once"})
    other = await client.post(
        "/documents", headers=TENANT_A | {"Idempotency-Key": "order-43"}, json={"title": "Once", "content": "only once"}
    )
    assert first.status_code == 201
    assert retry.status_code == 200 and retry.headers["idempotent-replayed"] == "true"
    assert retry.json()["id"] == first.json()["id"]
    assert other.json()["id"] != first.json()["id"]

    search = await client.get("/search", headers=TENANT_A, params={"q": "once"})
    assert search.json()["total"] == 2


async def test_idempotent_retry_after_delete_replays_the_original_response(client: AsyncClient) -> None:
    """Regression: a retry with the same key must replay the original 201 body, not 409 for 24 h."""
    headers = TENANT_A | {"Idempotency-Key": "replay-after-delete"}
    body = {"title": "Once", "content": "then deleted"}
    first = await client.post("/documents", headers=headers, json=body)
    assert (await client.delete(f"/documents/{first.json()['id']}", headers=TENANT_A)).status_code == 204
    retry = await client.post("/documents", headers=headers, json=body)
    assert retry.status_code == 200 and retry.json()["id"] == first.json()["id"]
    assert retry.headers["idempotent-replayed"] == "true"


async def test_idempotency_key_reused_with_a_different_body_is_rejected(client: AsyncClient) -> None:
    headers = TENANT_A | {"Idempotency-Key": "reused-key"}
    assert (await client.post("/documents", headers=headers, json={"title": "A", "content": "a"})).status_code == 201
    reused = await client.post("/documents", headers=headers, json={"title": "B", "content": "b"})
    assert reused.status_code == 422 and reused.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"


async def test_bulk_create_indexes_everything(client: AsyncClient) -> None:
    documents = [{"title": f"Bulk {i}", "content": f"bulk payload number {i}"} for i in range(25)]
    response = await client.post("/documents/bulk", headers=TENANT_A, json={"documents": documents})
    assert response.status_code == 201
    body = response.json()
    assert (body["total"], body["succeeded"], body["failed"]) == (25, 25, 0)
    search = await client.get("/search", headers=TENANT_A, params={"q": "bulk payload", "page_size": 5})
    assert search.json()["total"] == 25 and search.json()["total_pages"] == 5


async def test_bulk_limits(client: AsyncClient) -> None:
    assert (await client.post("/documents/bulk", headers=TENANT_A, json={"documents": []})).status_code == 422
    too_many = [{"title": "t", "content": "c"}] * 501
    assert (await client.post("/documents/bulk", headers=TENANT_A, json={"documents": too_many})).status_code == 422


async def test_request_id_and_security_headers(client: AsyncClient) -> None:
    response = await client.get("/documents/unknown", headers=TENANT_A | {"X-Request-ID": "trace-abc-123"})
    assert response.headers["x-request-id"] == "trace-abc-123"
    assert response.json()["error"]["request_id"] == "trace-abc-123"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["cache-control"] == "no-store"
    assert float(response.headers["x-response-time-ms"]) >= 0

    generated = await client.get("/", headers={"X-Request-ID": "bad id with spaces"})
    assert generated.headers["x-request-id"] != "bad id with spaces"
