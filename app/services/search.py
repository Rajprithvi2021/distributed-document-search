"""Search use case: cache lookup → single-flight backend query → stale-if-error fallback."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from app.cache.stores import SearchResultCache
from app.core.errors import BadRequestError, DependencyUnavailableError
from app.core.metrics import CACHE_LOOKUPS, DEGRADED_RESPONSES
from app.domain.models import FuzzyMode, SearchPage, SearchQuery
from app.search.base import SearchBackend

logger = logging.getLogger(__name__)

CacheStatus = Literal["hit", "miss", "stale"]


@dataclass(frozen=True)
class SearchOutcome:
    page: SearchPage
    cache: CacheStatus
    degraded: bool = False


class SearchService:
    def __init__(self, *, backend: SearchBackend, cache: SearchResultCache, max_result_window: int) -> None:
        self._backend = backend
        self._cache = cache
        self._max_result_window = max_result_window
        self._in_flight: dict[str, asyncio.Future[SearchPage]] = {}

    async def search(self, tenant_id: str, query: SearchQuery) -> SearchOutcome:
        if query.page * query.page_size > self._max_result_window:
            raise BadRequestError(
                f"page * page_size may not exceed {self._max_result_window}; narrow the query instead of deep paging",
                details={"max_result_window": self._max_result_window},
            )

        lookup = await self._cache.lookup(tenant_id, query)
        if lookup.page is not None and lookup.fresh:
            return SearchOutcome(page=lookup.page, cache="hit")

        try:
            page = await self._single_flight(lookup.key, lambda: self._execute(tenant_id, query, lookup.key))
        except DependencyUnavailableError:
            if lookup.page is not None:
                # stale-if-error: an older answer beats an error for a read-only query.
                DEGRADED_RESPONSES.inc()
                CACHE_LOOKUPS.labels(cache="search", result="stale").inc()
                logger.warning("search_served_stale", extra={"cache_key": lookup.key})
                return SearchOutcome(page=lookup.page, cache="stale", degraded=True)
            raise
        if lookup.page is not None:  # expired entry refreshed from the backend
            CACHE_LOOKUPS.labels(cache="search", result="miss").inc()
        return SearchOutcome(page=page, cache="miss")

    async def _execute(self, tenant_id: str, query: SearchQuery, cache_key: str) -> SearchPage:
        page = await self._backend.search(tenant_id, query, fuzzy=query.fuzzy is FuzzyMode.ON)
        if query.fuzzy is FuzzyMode.AUTO and page.total == 0:
            # Typo tolerance only when the precise query found nothing.
            page = await self._backend.search(tenant_id, query, fuzzy=True)
        await self._cache.store(cache_key, page, tenant_id)
        return page

    async def _single_flight(self, key: str, factory: Callable[[], Awaitable[SearchPage]]) -> SearchPage:
        """Collapse concurrent identical cache misses into one backend call (stampede protection)."""
        existing = self._in_flight.get(key)
        if existing is not None:
            return await asyncio.shield(existing)

        future: asyncio.Future[SearchPage] = asyncio.get_running_loop().create_future()
        self._in_flight[key] = future
        try:
            result = await factory()
        except asyncio.CancelledError:
            future.cancel()
            raise
        except BaseException as exc:
            future.set_exception(exc)
            future.exception()  # mark retrieved so asyncio doesn't log "exception never retrieved"
            raise
        else:
            future.set_result(result)
            return result
        finally:
            self._in_flight.pop(key, None)
