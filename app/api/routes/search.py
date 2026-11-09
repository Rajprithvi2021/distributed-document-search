"""GET /search: full-text search with relevance ranking, filters, facets and highlighting."""

from __future__ import annotations

import math
import re
import time
from datetime import datetime

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import enforce_rate_limit, get_container, require_scope
from app.api.routes.documents import ERRORS
from app.api.schemas import SearchResponse
from app.container import Container
from app.core.errors import ValidationFailedError
from app.core.security import SCOPE_SEARCH, Principal
from app.domain.models import FuzzyMode, SearchQuery, SortOrder

FILTER_KEY = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
MAX_FACETS = 5
MAX_FILTERS = 10

router = APIRouter(tags=["Search"], dependencies=[Depends(enforce_rate_limit)], responses=ERRORS)


def _parse_filters(raw: list[str]) -> tuple[tuple[str, str], ...]:
    if len(raw) > MAX_FILTERS:
        raise ValidationFailedError(f"At most {MAX_FILTERS} filters are allowed")
    parsed = []
    for item in raw:
        key, sep, value = item.partition(":")
        if not sep or not FILTER_KEY.fullmatch(key) or not value or len(value) > 256:
            raise ValidationFailedError(
                "filter must look like 'key:value' (metadata key: letters, digits, '_', '-')", details={"filter": item}
            )
        parsed.append((key, value))
    return tuple(parsed)


def _parse_facets(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    facets = tuple(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))
    if len(facets) > MAX_FACETS:
        raise ValidationFailedError(f"At most {MAX_FACETS} facets are allowed")
    for facet in facets:
        key = facet.removeprefix("metadata.")
        if facet != "tags" and (not facet.startswith("metadata.") or not FILTER_KEY.fullmatch(key)):
            raise ValidationFailedError("facets must be 'tags' or 'metadata.<key>'", details={"facet": facet})
    return facets


@router.get("/search", response_model=SearchResponse, summary="Search documents")
async def search_documents(
    q: str = Query(min_length=1, max_length=512, description="Full-text query"),
    tenant: str | None = Query(
        default=None, description="Tenant ID. Alternative to the X-Tenant-ID header (assignment contract)"
    ),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    sort: SortOrder = Query(default=SortOrder.RELEVANCE),
    fuzzy: FuzzyMode = Query(default=FuzzyMode.AUTO, description="auto = fuzzy only if the exact query finds nothing"),
    highlight: bool = Query(default=True),
    tags: list[str] = Query(default=[], description="Only documents having ALL of these tags"),
    filter: list[str] = Query(default=[], description="Metadata filter key:value (repeatable, AND)"),
    facets: str | None = Query(default=None, description="Comma-separated: tags,metadata.<key>"),
    created_after: datetime | None = Query(default=None),
    created_before: datetime | None = Query(default=None),
    principal: Principal = Depends(require_scope(SCOPE_SEARCH)),
    container: Container = Depends(get_container),
) -> SearchResponse:
    started = time.perf_counter()
    text = " ".join(q.split())
    if not text:
        raise ValidationFailedError("q must not be blank")
    if len(tags) > MAX_FILTERS:
        raise ValidationFailedError(f"At most {MAX_FILTERS} tags are allowed")

    query = SearchQuery(
        text=text,
        page=page,
        page_size=page_size,
        sort=sort,
        fuzzy=fuzzy,
        highlight=highlight,
        tags=tuple(sorted({t.strip().lower() for t in tags if t.strip()})),
        metadata_filters=tuple(sorted(_parse_filters(filter))),
        created_after=created_after,
        created_before=created_before,
        facets=_parse_facets(facets),
    )
    outcome = await container.search.search(principal.tenant_id, query)
    page_result = outcome.page
    return SearchResponse(
        query=text,
        page=page,
        page_size=page_size,
        total=page_result.total,
        total_relation=page_result.total_relation,
        total_pages=math.ceil(page_result.total / page_size) if page_result.total else 0,
        results=page_result.hits,
        facets=page_result.facets,
        fuzzy_applied=page_result.fuzzy_applied,
        cache=outcome.cache,
        degraded=outcome.degraded,
        took_ms=round((time.perf_counter() - started) * 1000, 3),
    )
