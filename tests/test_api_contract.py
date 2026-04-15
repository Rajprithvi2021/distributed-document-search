from uuid import uuid4

from fastapi.testclient import TestClient

from src.config import settings
from src.main import app
from src.middleware import _LOCAL_RATE_LIMITS


client = TestClient(app)


def tenant() -> str:
    return f"tenant-{uuid4()}"


def test_root_health_and_metrics_endpoints():
    root = client.get("/")
    assert root.status_code == 200
    assert root.json()["service"] == settings.app_name

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] in {"healthy", "degraded"}

    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert "total_requests" in metrics.json()


def test_bulk_create_documents_and_search_pagination():
    tenant_id = tenant()
    response = client.post(
        "/documents/bulk",
        headers={"X-Tenant-ID": tenant_id},
        json={
            "documents": [
                {
                    "title": "Fast Search One",
                    "content": "Distributed indexing with Redis cache.",
                    "metadata": {"batch": "api-contract"},
                },
                {
                    "title": "Fast Search Two",
                    "content": "Distributed indexing with Elasticsearch search.",
                    "metadata": {"batch": "api-contract"},
                },
            ]
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["total"] == 2
    assert body["created"] == 2
    assert body["failed"] == 0

    search = client.get(
        "/search",
        headers={"X-Tenant-ID": tenant_id},
        params={"q": "distributed", "page": 1, "page_size": 1},
    )
    assert search.status_code == 200
    search_body = search.json()
    assert search_body["total_results"] == 2
    assert search_body["page_size"] == 1
    assert search_body["total_pages"] == 2
    assert len(search_body["results"]) == 1


def test_query_validation_errors_are_returned_cleanly():
    tenant_id = tenant()

    missing_query = client.get("/search", headers={"X-Tenant-ID": tenant_id})
    assert missing_query.status_code == 422

    invalid_page_size = client.get(
        "/search",
        headers={"X-Tenant-ID": tenant_id},
        params={"q": "anything", "page_size": 101},
    )
    assert invalid_page_size.status_code == 422

    missing_tenant = client.get("/search", params={"q": "anything"})
    assert missing_tenant.status_code == 400
    assert missing_tenant.json()["error_code"] == "MISSING_TENANT"


def test_assignment_tenant_query_parameter_is_supported():
    tenant_id = tenant()
    created = client.post(
        f"/documents?tenant={tenant_id}",
        json={
            "title": "Tenant Query Parameter",
            "content": "The assignment search contract uses tenant as a query parameter.",
            "metadata": {"category": "contract"},
        },
    )
    assert created.status_code == 201

    searched = client.get(
        "/search",
        params={"tenant": tenant_id, "q": "assignment search"},
    )
    assert searched.status_code == 200
    assert searched.json()["total_results"] == 1


def test_bonus_fuzzy_highlighting_and_facets_work():
    tenant_id = tenant()
    response = client.post(
        "/documents/bulk",
        headers={"X-Tenant-ID": tenant_id},
        json={
            "documents": [
                {
                    "title": "Distributed Search",
                    "content": "Distributed document search supports highlighted snippets.",
                    "metadata": {"category": "architecture", "tags": ["search", "redis"]},
                },
                {
                    "title": "Caching Strategy",
                    "content": "Redis cache keeps search responses fast.",
                    "metadata": {"category": "performance", "tags": ["search"]},
                },
            ]
        },
    )
    assert response.status_code == 201

    search = client.get(
        "/search",
        headers={"X-Tenant-ID": tenant_id},
        params={"q": "distibuted", "facet": "category"},
    )
    assert search.status_code == 200
    body = search.json()
    assert body["total_results"] >= 1
    assert body["results"][0]["highlights"] is not None
    assert "category" in body["facets"]
    assert body["facets"]["category"]["architecture"] == 1


def test_rate_limit_is_applied_per_tenant(monkeypatch):
    tenant_id = tenant()
    _LOCAL_RATE_LIMITS.clear()
    monkeypatch.setattr(settings, "rate_limit_default", 1)
    monkeypatch.setattr(settings, "rate_limit_period", 60)

    first = client.get(
        "/search",
        headers={"X-Tenant-ID": tenant_id},
        params={"q": "anything"},
    )
    second = client.get(
        "/search",
        headers={"X-Tenant-ID": tenant_id},
        params={"q": "anything"},
    )

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json()["error_code"] == "RATE_LIMIT_EXCEEDED"
