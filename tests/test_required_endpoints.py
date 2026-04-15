from fastapi.testclient import TestClient

from src.main import app


client = TestClient(app)


def test_document_lifecycle_and_search_is_tenant_scoped():
    create_response = client.post(
        "/documents",
        headers={"X-Tenant-ID": "tenant-a"},
        json={
            "title": "Distributed Search Architecture",
            "content": "Elasticsearch and Redis support fast distributed document search.",
            "metadata": {"category": "architecture"},
        },
    )

    assert create_response.status_code == 201
    document = create_response.json()
    document_id = document["document_id"]

    search_response = client.get(
        "/search",
        headers={"X-Tenant-ID": "tenant-a"},
        params={"q": "distributed search"},
    )
    assert search_response.status_code == 200
    search_body = search_response.json()
    assert search_body["total_results"] >= 1
    assert search_body["results"][0]["document_id"] == document_id

    isolated_search = client.get(
        "/search",
        headers={"X-Tenant-ID": "tenant-b"},
        params={"q": "distributed search"},
    )
    assert isolated_search.status_code == 200
    assert isolated_search.json()["total_results"] == 0

    get_response = client.get(
        f"/documents/{document_id}",
        headers={"X-Tenant-ID": "tenant-a"},
    )
    assert get_response.status_code == 200
    assert get_response.json()["document_id"] == document_id

    delete_response = client.delete(
        f"/documents/{document_id}",
        headers={"X-Tenant-ID": "tenant-a"},
    )
    assert delete_response.status_code == 204

    missing_response = client.get(
        f"/documents/{document_id}",
        headers={"X-Tenant-ID": "tenant-a"},
    )
    assert missing_response.status_code == 404


def test_missing_tenant_is_rejected_for_document_routes():
    response = client.post(
        "/documents",
        json={"title": "No tenant", "content": "This request should fail."},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "MISSING_TENANT"


def test_health_reports_dependency_status():
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] in {"healthy", "degraded"}
    assert "elasticsearch" in body["dependencies"]
    assert "redis" in body["dependencies"]
