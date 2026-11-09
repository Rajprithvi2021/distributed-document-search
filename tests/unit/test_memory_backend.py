from datetime import timedelta

import pytest

from app.domain.models import SearchQuery, SortOrder
from app.search.memory import InMemorySearchBackend
from tests.fakes import BASE_TIME, make_document


@pytest.fixture
async def backend() -> InMemorySearchBackend:
    backend = InMemorySearchBackend()
    await backend.bulk_index(
        [
            make_document(
                "title-hit", "Distributed search", "An overview.", tags=["arch"], metadata={"category": "eng"}
            ),
            make_document(
                "content-hit",
                "Overview",
                "Notes on distributed systems and search.",
                tags=["arch", "ops"],
                metadata={"category": "ops", "year": 2024},
                age_days=5,
            ),
            make_document("unrelated", "Cooking", "Pasta recipes.", tags=["food"], age_days=10),
            make_document("other-tenant", "Distributed search", "Belongs to B.", tenant="tenant-b"),
        ]
    )
    return backend


async def test_title_matches_outrank_content_matches(backend: InMemorySearchBackend) -> None:
    page = await backend.search("tenant-a", SearchQuery(text="distributed search"), fuzzy=False)
    assert [hit.id for hit in page.hits] == ["title-hit", "content-hit"]
    assert page.hits[0].score > page.hits[1].score > 0


async def test_results_never_cross_tenants(backend: InMemorySearchBackend) -> None:
    page_a = await backend.search("tenant-a", SearchQuery(text="belongs"), fuzzy=True)
    page_b = await backend.search("tenant-b", SearchQuery(text="distributed"), fuzzy=False)
    assert page_a.total == 0
    assert [hit.id for hit in page_b.hits] == ["other-tenant"]
    assert await backend.get("tenant-a", "other-tenant") is None


async def test_tag_metadata_and_date_filters(backend: InMemorySearchBackend) -> None:
    by_tag = await backend.search("tenant-a", SearchQuery(text="distributed", tags=("ops",)), fuzzy=False)
    assert [h.id for h in by_tag.hits] == ["content-hit"]

    by_meta = await backend.search(
        "tenant-a", SearchQuery(text="distributed", metadata_filters=(("year", "2024"),)), fuzzy=False
    )
    assert [h.id for h in by_meta.hits] == ["content-hit"]

    recent = await backend.search(
        "tenant-a", SearchQuery(text="distributed", created_after=BASE_TIME + timedelta(days=1)), fuzzy=False
    )
    assert [h.id for h in recent.hits] == ["content-hit"]


async def test_facets_count_matching_documents_only(backend: InMemorySearchBackend) -> None:
    page = await backend.search(
        "tenant-a", SearchQuery(text="distributed", facets=("tags", "metadata.category")), fuzzy=False
    )
    assert {b.value: b.count for b in page.facets["tags"]} == {"arch": 2, "ops": 1}
    assert {b.value: b.count for b in page.facets["metadata.category"]} == {"eng": 1, "ops": 1}


async def test_fuzzy_matching_only_when_enabled(backend: InMemorySearchBackend) -> None:
    exact = await backend.search("tenant-a", SearchQuery(text="distibuted"), fuzzy=False)
    fuzzy = await backend.search("tenant-a", SearchQuery(text="distibuted"), fuzzy=True)
    assert exact.total == 0
    assert fuzzy.total == 2 and fuzzy.fuzzy_applied


async def test_pagination_and_sorting(backend: InMemorySearchBackend) -> None:
    first = await backend.search("tenant-a", SearchQuery(text="distributed", page=1, page_size=1), fuzzy=False)
    second = await backend.search("tenant-a", SearchQuery(text="distributed", page=2, page_size=1), fuzzy=False)
    assert first.total == second.total == 2
    assert first.hits[0].id != second.hits[0].id

    newest = await backend.search("tenant-a", SearchQuery(text="distributed", sort=SortOrder.NEWEST), fuzzy=False)
    oldest = await backend.search("tenant-a", SearchQuery(text="distributed", sort=SortOrder.OLDEST), fuzzy=False)
    assert [h.id for h in newest.hits] == ["content-hit", "title-hit"]
    assert [h.id for h in oldest.hits] == ["title-hit", "content-hit"]


async def test_delete_and_reindex_keep_postings_consistent(backend: InMemorySearchBackend) -> None:
    assert await backend.delete("tenant-a", "title-hit") is True
    assert await backend.delete("tenant-a", "title-hit") is False
    page = await backend.search("tenant-a", SearchQuery(text="distributed"), fuzzy=False)
    assert [h.id for h in page.hits] == ["content-hit"]

    await backend.index(make_document("content-hit", "Renamed", "Nothing relevant anymore."))
    assert (await backend.search("tenant-a", SearchQuery(text="distributed"), fuzzy=False)).total == 0
    assert (await backend.search("tenant-a", SearchQuery(text="renamed"), fuzzy=False)).total == 1


async def test_stopword_only_query_matches_nothing(backend: InMemorySearchBackend) -> None:
    assert (await backend.search("tenant-a", SearchQuery(text="the and of"), fuzzy=True)).total == 0


async def test_highlight_toggle(backend: InMemorySearchBackend) -> None:
    on = await backend.search("tenant-a", SearchQuery(text="systems"), fuzzy=False)
    off = await backend.search("tenant-a", SearchQuery(text="systems", highlight=False), fuzzy=False)
    assert on.hits[0].highlights["content"] == ["Notes on distributed <em>systems</em> and search."]
    assert off.hits[0].highlights == {}
    assert off.hits[0].snippet == "Notes on distributed systems and search."


async def test_health_reports_document_counts(backend: InMemorySearchBackend) -> None:
    health = await backend.health()
    assert health.status == "up"
    assert health.details["documents"] == 4
