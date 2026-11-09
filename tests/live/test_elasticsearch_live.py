"""The Elasticsearch adapter against a real cluster, in both tenancy layouts."""

from datetime import timedelta

from app.domain.models import SearchQuery, SortOrder
from app.search.elasticsearch import ElasticsearchBackend
from tests.fakes import BASE_TIME, make_document
from tests.live.conftest import requires_es

pytestmark = requires_es


async def test_index_get_search_delete_roundtrip(es_backend: ElasticsearchBackend) -> None:
    document = make_document(
        "d1",
        "Distributed search",
        "Shards and replicas make distributed search fast.",
        tags=["arch"],
        metadata={"category": "eng", "year": 2024, "public": True},
    )
    await es_backend.index(document)

    fetched = await es_backend.get("tenant-a", "d1")
    assert fetched == document

    page = await es_backend.search("tenant-a", SearchQuery(text="distributed search"), fuzzy=False)
    assert page.total == 1
    hit = page.hits[0]
    assert hit.id == "d1" and hit.score and hit.score > 0
    assert "<em>" in hit.highlights["title"][0]
    assert "<em>" in hit.snippet and "distributed" in hit.snippet.lower()

    assert await es_backend.delete("tenant-a", "d1") is True
    assert await es_backend.get("tenant-a", "d1") is None
    assert await es_backend.delete("tenant-a", "d1") is False


async def test_tenants_are_isolated(es_backend: ElasticsearchBackend) -> None:
    await es_backend.index(make_document("a1", "Shared words", "identical text", tenant="tenant-a"))
    await es_backend.index(make_document("b1", "Shared words", "identical text", tenant="tenant-b"))

    page_b = await es_backend.search("tenant-b", SearchQuery(text="identical"), fuzzy=True)
    assert [hit.id for hit in page_b.hits] == ["b1"]
    assert await es_backend.get("tenant-b", "a1") is None
    assert await es_backend.delete("tenant-b", "a1") is False
    assert await es_backend.get("tenant-a", "a1") is not None


async def test_filters_facets_and_sorting(es_backend: ElasticsearchBackend) -> None:
    results = await es_backend.bulk_index(
        [
            make_document(
                "old",
                "Search guide",
                "distributed search basics",
                tags=["arch"],
                metadata={"category": "eng"},
                age_days=0,
            ),
            make_document(
                "mid",
                "Search ops",
                "distributed search operations",
                tags=["arch", "ops"],
                metadata={"category": "ops"},
                age_days=5,
            ),
            make_document(
                "new",
                "Search costs",
                "distributed search budget",
                tags=["finance"],
                metadata={"category": "eng"},
                age_days=10,
            ),
        ]
    )
    assert [r.status for r in results] == ["indexed"] * 3

    faceted = await es_backend.search(
        "tenant-a", SearchQuery(text="distributed", facets=("tags", "metadata.category")), fuzzy=False
    )
    assert faceted.total == 3
    assert {b.value: b.count for b in faceted.facets["tags"]} == {"arch": 2, "ops": 1, "finance": 1}
    assert {b.value: b.count for b in faceted.facets["metadata.category"]} == {"eng": 2, "ops": 1}

    by_tag = await es_backend.search("tenant-a", SearchQuery(text="distributed", tags=("ops",)), fuzzy=False)
    assert [h.id for h in by_tag.hits] == ["mid"]
    by_meta = await es_backend.search(
        "tenant-a", SearchQuery(text="distributed", metadata_filters=(("category", "eng"),)), fuzzy=False
    )
    assert {h.id for h in by_meta.hits} == {"old", "new"}
    recent = await es_backend.search(
        "tenant-a", SearchQuery(text="distributed", created_after=BASE_TIME + timedelta(days=3)), fuzzy=False
    )
    assert {h.id for h in recent.hits} == {"mid", "new"}

    newest = await es_backend.search("tenant-a", SearchQuery(text="distributed", sort=SortOrder.NEWEST), fuzzy=False)
    assert [h.id for h in newest.hits] == ["new", "mid", "old"]
    paged = await es_backend.search("tenant-a", SearchQuery(text="distributed", page=2, page_size=2), fuzzy=False)
    assert paged.total == 3 and len(paged.hits) == 1


async def test_fuzzy_highlighting_and_escaping(es_backend: ElasticsearchBackend) -> None:
    await es_backend.index(make_document("x1", "<b>Distributed</b> caching", "<script>x()</script> redis caching"))

    assert (await es_backend.search("tenant-a", SearchQuery(text="distibuted"), fuzzy=False)).total == 0
    fuzzy = await es_backend.search("tenant-a", SearchQuery(text="distibuted"), fuzzy=True)
    assert fuzzy.total == 1 and fuzzy.fuzzy_applied

    page = await es_backend.search("tenant-a", SearchQuery(text="caching"), fuzzy=False)
    assert "<script>" not in page.hits[0].snippet and "&lt;script&gt;" in page.hits[0].snippet
    plain = await es_backend.search("tenant-a", SearchQuery(text="caching", highlight=False), fuzzy=False)
    assert plain.hits[0].highlights == {} and "<em>" not in plain.hits[0].snippet


async def test_unknown_tenant_and_health(es_backend: ElasticsearchBackend) -> None:
    page = await es_backend.search("never-indexed", SearchQuery(text="anything"), fuzzy=True)
    assert page.total == 0
    assert await es_backend.get("never-indexed", "nope") is None

    health = await es_backend.health()
    assert health.status == "up" and health.details["cluster_status"] in ("green", "yellow")
