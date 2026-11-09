"""Elasticsearch adapter: pure query building/parsing plus behaviour against a mocked client."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig
from elasticsearch import ApiError, BadRequestError, NotFoundError
from elasticsearch import ConnectionError as ESConnectionError

from app.core.errors import DependencyUnavailableError
from app.domain.models import SearchQuery, SortOrder
from app.search import elasticsearch as es_module
from app.search.elasticsearch import ElasticsearchBackend, build_search_body, parse_search_response
from tests.conftest import make_settings
from tests.fakes import BASE_TIME, make_document


def api_error(cls: type[ApiError], status: int, error_type: str = "error") -> ApiError:
    meta = ApiResponseMeta(
        status=status,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=NodeConfig("http", "localhost", 9200),
    )
    return cls(message=error_type, meta=meta, body={"error": {"type": error_type}, "status": status})


def fake_client() -> MagicMock:
    client = MagicMock()
    client.indices.exists_alias = AsyncMock(return_value=False)
    client.indices.create = AsyncMock(return_value={"acknowledged": True})
    client.index = AsyncMock(return_value={"result": "created"})
    client.get = AsyncMock()
    client.delete = AsyncMock(return_value={"result": "deleted"})
    client.search = AsyncMock()
    client.close = AsyncMock()
    return client


# --- query DSL ------------------------------------------------------------------
def test_query_always_filters_by_tenant_and_boosts_title() -> None:
    body = build_search_body("tenant-a", SearchQuery(text="distributed search"), fuzzy=False)
    bool_query = body["query"]["bool"]
    assert bool_query["filter"][0] == {"term": {"tenant_id": "tenant-a"}}
    assert bool_query["must"][0]["multi_match"]["fields"] == ["title^2", "content"]
    assert "fuzziness" not in bool_query["must"][0]["multi_match"]
    assert body["_source"] and "content" not in body["_source"]
    assert body["sort"] == ["_score", {"created_at": "desc"}]
    assert body["from"] == 0 and body["size"] == 10


def test_query_with_all_options() -> None:
    query = SearchQuery(
        text="redis",
        page=3,
        page_size=20,
        sort=SortOrder.NEWEST,
        highlight=False,
        tags=("ops",),
        metadata_filters=(("category", "eng"),),
        created_after=BASE_TIME,
        created_before=BASE_TIME + timedelta(days=1),
        facets=("tags", "metadata.category"),
    )
    body = build_search_body("tenant-a", query, fuzzy=True)
    multi_match = body["query"]["bool"]["must"][0]["multi_match"]
    assert multi_match["fuzziness"] == "AUTO" and multi_match["prefix_length"] == 1
    filters = body["query"]["bool"]["filter"]
    assert {"term": {"tags": "ops"}} in filters
    assert {"term": {"metadata.category": "eng"}} in filters
    assert filters[-1]["range"]["created_at"]["gte"].startswith("2024-01-01")
    assert body["from"] == 40 and body["size"] == 20
    assert body["sort"][0] == {"created_at": "desc"} and body["track_scores"] is True
    assert body["highlight"]["pre_tags"] == ["<em>"]  # always on: the highlighter produces the snippet
    assert body["aggs"] == {
        "facet_0": {"terms": {"field": "tags", "size": 20}},
        "facet_1": {"terms": {"field": "metadata.category", "size": 20}},
    }


def test_parse_search_response_maps_hits_and_facets() -> None:
    response = {
        "took": 7,
        "hits": {
            "total": {"value": 10000, "relation": "gte"},
            "hits": [
                {
                    "_id": "d1",
                    "_score": 3.2,
                    "_source": {
                        "document_id": "d1",
                        "title": "T",
                        "tags": ["a"],
                        "metadata": {"k": "v"},
                        "created_at": "2024-01-01T00:00:00+00:00",
                    },
                    "highlight": {"content": ["a <em>match</em>"], "title": ["no tags here"]},
                }
            ],
        },
        "aggregations": {"facet_0": {"buckets": [{"key": "a", "doc_count": 4}]}},
    }
    page = parse_search_response(response, SearchQuery(text="match", facets=("tags",)), fuzzy=True)
    assert page.total == 10000 and page.total_relation == "gte" and page.took_ms == 7.0 and page.fuzzy_applied
    hit = page.hits[0]
    assert hit.snippet == "a <em>match</em>"
    assert hit.highlights == {"content": ["a <em>match</em>"]}  # filler fragment without <em> dropped
    assert page.facets["tags"][0].value == "a" and page.facets["tags"][0].count == 4

    plain = parse_search_response(response, SearchQuery(text="match", highlight=False), fuzzy=False)
    assert plain.hits[0].snippet == "a match" and plain.hits[0].highlights == {}
    assert plain.facets == {}


@pytest.mark.parametrize(
    ("tenancy", "alias", "es_id", "routing"),
    [("index_per_tenant", "docs-tenant-a", "d1", None), ("shared_index", "docs-shared", "tenant-a:d1", "tenant-a")],
)
def test_index_naming_per_tenancy_strategy(tenancy: str, alias: str, es_id: str, routing: str | None) -> None:
    backend = ElasticsearchBackend(make_settings(elasticsearch_tenancy=tenancy), client=fake_client())
    assert backend.alias_for("tenant-a") == alias
    assert backend.es_id("tenant-a", "d1") == es_id
    assert backend.routing("tenant-a") == routing


# --- adapter behaviour --------------------------------------------------------------
async def test_index_creates_versioned_index_with_alias_once() -> None:
    client = fake_client()
    backend = ElasticsearchBackend(make_settings(), client=client)
    await backend.index(make_document("d1"))
    await backend.index(make_document("d2"))

    client.indices.create.assert_awaited_once()
    create_kwargs = client.indices.create.call_args.kwargs
    assert create_kwargs["index"] == "docs-tenant-a-v1"
    assert create_kwargs["aliases"] == {"docs-tenant-a": {"is_write_index": True}}
    assert create_kwargs["mappings"]["dynamic"] == "strict"
    index_kwargs = client.index.call_args.kwargs
    assert index_kwargs["index"] == "docs-tenant-a" and index_kwargs["require_alias"] is True
    assert index_kwargs["document"]["document_id"] == "d2" and "id" not in index_kwargs["document"]


async def test_concurrent_index_creation_race_is_tolerated() -> None:
    client = fake_client()
    client.indices.create.side_effect = api_error(BadRequestError, 400, "resource_already_exists_exception")
    backend = ElasticsearchBackend(make_settings(), client=client)
    await backend.index(make_document("d1"))
    client.index.assert_awaited_once()


async def test_get_handles_missing_and_foreign_documents() -> None:
    client = fake_client()
    backend = ElasticsearchBackend(make_settings(), client=client)

    client.get.side_effect = api_error(NotFoundError, 404, "index_not_found_exception")
    assert await backend.get("tenant-a", "d1") is None

    client.get.side_effect = None
    source = backend._source(make_document("d1", tenant="tenant-b"))
    client.get.return_value = {"_source": source}
    assert await backend.get("tenant-a", "d1") is None  # tenant mismatch is never returned

    client.get.return_value = {"_source": backend._source(make_document("d1"))}
    document = await backend.get("tenant-a", "d1")
    assert document is not None and document.id == "d1"


async def test_delete_not_found_returns_false() -> None:
    client = fake_client()
    client.delete.side_effect = api_error(NotFoundError, 404, "not_found")
    backend = ElasticsearchBackend(make_settings(), client=client)
    assert await backend.delete("tenant-a", "d1") is False


async def test_search_on_tenant_without_index_returns_empty_page() -> None:
    client = fake_client()
    client.search.side_effect = api_error(NotFoundError, 404, "index_not_found_exception")
    backend = ElasticsearchBackend(make_settings(), client=client)
    page = await backend.search("tenant-a", SearchQuery(text="x"), fuzzy=False)
    assert page.total == 0


async def test_transient_errors_become_dependency_unavailable() -> None:
    client = fake_client()
    backend = ElasticsearchBackend(make_settings(), client=client)

    client.search.side_effect = ESConnectionError("connection refused")
    with pytest.raises(DependencyUnavailableError):
        await backend.search("tenant-a", SearchQuery(text="x"), fuzzy=False)

    client.search.side_effect = api_error(ApiError, 503, "unavailable")
    with pytest.raises(DependencyUnavailableError):
        await backend.search("tenant-a", SearchQuery(text="x"), fuzzy=False)

    client.search.side_effect = api_error(BadRequestError, 400, "parsing_exception")
    with pytest.raises(BadRequestError):  # caller/programming error: not retried, not masked
        await backend.search("tenant-a", SearchQuery(text="x"), fuzzy=False)


async def test_bulk_reports_per_item_outcomes(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_bulk(client, actions, **kwargs):
        assert kwargs["require_alias"] is True and kwargs["raise_on_error"] is False
        return 1, [
            {"index": {"_id": "d2", "status": 429, "error": {"reason": "es_rejected_execution_exception"}}},
            {"index": {"_id": "d3", "status": 400, "error": {"reason": "mapper_parsing_exception"}}},
        ]

    monkeypatch.setattr(es_module, "async_bulk", fake_bulk)
    backend = ElasticsearchBackend(make_settings(), client=fake_client())
    results = await backend.bulk_index([make_document("d1"), make_document("d2"), make_document("d3")])
    assert [(r.id, r.status, r.retryable) for r in results] == [
        ("d1", "indexed", False),
        ("d2", "failed", True),
        ("d3", "failed", False),
    ]


async def test_health_maps_cluster_status() -> None:
    client = fake_client()
    client.options.return_value.cluster.health = AsyncMock(return_value={"status": "yellow", "number_of_nodes": 1})
    backend = ElasticsearchBackend(make_settings(), client=client)
    assert (await backend.health()).status == "up"

    client.options.return_value.cluster.health = AsyncMock(side_effect=ESConnectionError("down"))
    report = await backend.health()
    assert report.status == "down" and report.details["error"] == "ConnectionError"
