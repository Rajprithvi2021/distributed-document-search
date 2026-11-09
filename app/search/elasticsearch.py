"""Elasticsearch search backend (production adapter).

Index layout (``ELASTICSEARCH_TENANCY``):

* ``index_per_tenant`` (default): alias ``{prefix}-{tenant}`` → index
  ``{prefix}-{tenant}-v1``. Gives strong isolation, per-tenant lifecycle and
  cheap tenant deletion. Reindexing (mapping change, shard resize) means
  building ``-v2`` and swapping the alias atomically, with zero downtime.
* ``shared_index``: one alias ``{prefix}-shared`` for all tenants, documents
  routed by tenant (one tenant's data on one shard) with composite IDs.
  Scales to many small tenants without exploding the shard count.

In both modes every query also carries a ``tenant_id`` term filter as defence
in depth. Writes use ``require_alias=True``, so a missing alias fails loudly
instead of letting ES auto-create an index with a guessed mapping.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from elasticsearch import (
    ApiError,
    AsyncElasticsearch,
    BadRequestError,
    ConnectionTimeout,
    NotFoundError,
)
from elasticsearch import (
    ConnectionError as ESConnectionError,
)
from elasticsearch.helpers import async_bulk

from app.config import Settings
from app.core.errors import DependencyUnavailableError
from app.core.metrics import BACKEND_LATENCY
from app.domain.models import (
    BulkItemResult,
    DependencyHealth,
    Document,
    FacetBucket,
    SearchHit,
    SearchPage,
    SearchQuery,
    SortOrder,
)

logger = logging.getLogger(__name__)
T = TypeVar("T")

TRANSIENT_STATUSES = frozenset({408, 429, 502, 503, 504})
SOURCE_FIELDS = ["document_id", "title", "tags", "metadata", "created_at", "updated_at"]
SNIPPET_SIZE = 160
FACET_SIZE = 20

ANALYSIS = {
    "filter": {
        "english_stop": {"type": "stop", "stopwords": "_english_"},
        "english_minimal": {"type": "stemmer", "language": "minimal_english"},
    },
    "analyzer": {
        "document_text": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["lowercase", "asciifolding", "english_stop", "english_minimal"],
        }
    },
}

MAPPINGS: dict[str, Any] = {
    "dynamic": "strict",
    "properties": {
        "tenant_id": {"type": "keyword"},
        "document_id": {"type": "keyword"},
        "title": {
            "type": "text",
            "analyzer": "document_text",
            "fields": {"raw": {"type": "keyword", "ignore_above": 512}},
        },
        "content": {"type": "text", "analyzer": "document_text"},
        "tags": {"type": "keyword"},
        # flattened: arbitrary tenant metadata without mapping explosion; supports term filters + terms aggs.
        "metadata": {"type": "flattened"},
        "created_at": {"type": "date"},
        "updated_at": {"type": "date"},
        "created_by": {"type": "keyword"},
    },
}


def index_definition(shards: int, replicas: int) -> dict[str, Any]:
    return {
        "settings": {
            "number_of_shards": shards,
            "number_of_replicas": replicas,
            "refresh_interval": "1s",
            "analysis": ANALYSIS,
        },
        "mappings": MAPPINGS,
    }


def build_search_body(tenant_id: str, query: SearchQuery, *, fuzzy: bool) -> dict[str, Any]:
    """Translate a domain ``SearchQuery`` into ES query DSL (pure function, unit-tested)."""
    multi_match: dict[str, Any] = {
        "query": query.text,
        "fields": ["title^2", "content"],
        "type": "best_fields",
        "tie_breaker": 0.3,
    }
    if fuzzy:
        multi_match.update({"fuzziness": "AUTO", "prefix_length": 1, "max_expansions": 50})

    filters: list[dict[str, Any]] = [{"term": {"tenant_id": tenant_id}}]
    filters += [{"term": {"tags": tag}} for tag in query.tags]
    filters += [{"term": {f"metadata.{key}": value}} for key, value in query.metadata_filters]
    if query.created_after or query.created_before:
        date_range: dict[str, str] = {}
        if query.created_after:
            date_range["gte"] = query.created_after.isoformat()
        if query.created_before:
            date_range["lte"] = query.created_before.isoformat()
        filters.append({"range": {"created_at": date_range}})

    # Phrase proximity boosts precision without excluding partial matches.
    should = [
        {"match_phrase": {"title": {"query": query.text, "boost": 3}}},
        {"match_phrase": {"content": {"query": query.text, "slop": 2, "boost": 1.5}}},
    ]

    body: dict[str, Any] = {
        "query": {"bool": {"must": [{"multi_match": multi_match}], "should": should, "filter": filters}},
        "from": query.offset,
        "size": query.page_size,
        "track_total_hits": True,
        "_source": SOURCE_FIELDS,  # content is never shipped back; snippets come from the highlighter
        # The highlighter always runs because it also produces the snippet; with highlight=false the
        # tags are stripped in parse_search_response.
        "highlight": {
            "encoder": "html",
            "pre_tags": ["<em>"],
            "post_tags": ["</em>"],
            "fields": {
                "title": {"number_of_fragments": 0},
                "content": {"fragment_size": SNIPPET_SIZE, "number_of_fragments": 3, "no_match_size": SNIPPET_SIZE},
            },
        },
    }
    if query.sort is SortOrder.NEWEST:
        body["sort"] = [{"created_at": "desc"}, "_score"]
        body["track_scores"] = True
    elif query.sort is SortOrder.OLDEST:
        body["sort"] = [{"created_at": "asc"}, "_score"]
        body["track_scores"] = True
    else:
        body["sort"] = ["_score", {"created_at": "desc"}]
    if query.facets:
        # Neutral aggregation names (facet_0, ...) so user-chosen field names never need escaping.
        body["aggs"] = {
            f"facet_{i}": {"terms": {"field": facet, "size": FACET_SIZE}} for i, facet in enumerate(query.facets)
        }
    return body


def _strip_tags(fragment: str) -> str:
    return fragment.replace("<em>", "").replace("</em>", "")


def parse_search_response(response: dict[str, Any], query: SearchQuery, *, fuzzy: bool) -> SearchPage:
    hits_section = response.get("hits", {})
    total = hits_section.get("total", {"value": 0, "relation": "eq"})
    hits: list[SearchHit] = []
    for hit in hits_section.get("hits", []):
        source = hit.get("_source", {})
        highlight = hit.get("highlight", {})
        content_fragments = highlight.get("content", [])
        snippet = content_fragments[0] if content_fragments else ""
        highlights: dict[str, list[str]] = {}
        if query.highlight:
            for field_name, fragments in highlight.items():
                tagged = [f for f in fragments if "<em>" in f]  # drop no_match_size filler fragments
                if tagged:
                    highlights[field_name] = tagged
        else:
            snippet = _strip_tags(snippet)
        hits.append(
            SearchHit(
                id=source.get("document_id") or hit["_id"],
                title=source.get("title", ""),
                score=hit.get("_score"),
                snippet=snippet,
                highlights=highlights,
                tags=source.get("tags", []),
                metadata=source.get("metadata", {}),
                created_at=source["created_at"],
            )
        )
    aggregations = response.get("aggregations", {})
    facets = {
        facet: [
            FacetBucket(value=str(bucket.get("key_as_string", bucket["key"])), count=bucket["doc_count"])
            for bucket in aggregations.get(f"facet_{i}", {}).get("buckets", [])
        ]
        for i, facet in enumerate(query.facets)
    }
    return SearchPage(
        total=total.get("value", 0),
        total_relation=total.get("relation", "eq"),
        hits=hits,
        facets=facets,
        took_ms=float(response.get("took", 0)),
        fuzzy_applied=fuzzy,
    )


def _error_type(exc: ApiError) -> str | None:
    body = exc.body
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        return body["error"].get("type")
    return None


def _body(response: Any) -> dict[str, Any]:
    return response.body if hasattr(response, "body") else response


class ElasticsearchBackend:
    name = "elasticsearch"

    def __init__(self, settings: Settings, client: AsyncElasticsearch | None = None) -> None:
        auth = None
        if settings.elasticsearch_username and settings.elasticsearch_password:
            auth = (settings.elasticsearch_username, settings.elasticsearch_password)
        # Transport-level retries are off: retries happen in ResilientSearchBackend,
        # so they're counted by the circuit breaker and never multiply.
        self._client = client or AsyncElasticsearch(
            settings.elasticsearch_url,
            basic_auth=auth,
            request_timeout=settings.elasticsearch_request_timeout_s,
            max_retries=0,
            retry_on_timeout=False,
        )
        self._prefix = settings.elasticsearch_index_prefix
        self._shared = settings.elasticsearch_tenancy == "shared_index"
        self._refresh = settings.elasticsearch_refresh
        self._shards = settings.elasticsearch_shards
        self._replicas = settings.elasticsearch_replicas
        self._known_aliases: set[str] = set()
        self._ensure_lock = asyncio.Lock()

    # -- naming ----------------------------------------------------------------
    def alias_for(self, tenant_id: str) -> str:
        return f"{self._prefix}-shared" if self._shared else f"{self._prefix}-{tenant_id}"

    def es_id(self, tenant_id: str, document_id: str) -> str:
        return f"{tenant_id}:{document_id}" if self._shared else document_id

    def routing(self, tenant_id: str) -> str | None:
        return tenant_id if self._shared else None

    # -- lifecycle -------------------------------------------------------------
    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        await self._client.close()

    # -- error translation -----------------------------------------------------
    async def _call(self, operation: str, fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        started = time.perf_counter()
        try:
            return await fn(*args, **kwargs)
        except (ESConnectionError, ConnectionTimeout) as exc:
            raise DependencyUnavailableError("Elasticsearch is unreachable", dependency="elasticsearch") from exc
        except ApiError as exc:
            if exc.meta.status in TRANSIENT_STATUSES:
                raise DependencyUnavailableError(
                    f"Elasticsearch returned {exc.meta.status}", dependency="elasticsearch"
                ) from exc
            raise
        finally:
            BACKEND_LATENCY.labels(backend=self.name, operation=operation).observe(time.perf_counter() - started)

    # -- index management ------------------------------------------------------
    async def ensure_tenant(self, tenant_id: str) -> str:
        alias = self.alias_for(tenant_id)
        if alias in self._known_aliases:
            return alias
        async with self._ensure_lock:
            if alias not in self._known_aliases:
                await self._create_if_missing(alias)
                self._known_aliases.add(alias)
        return alias

    async def _create_if_missing(self, alias: str) -> None:
        if await self._call("exists_alias", self._client.indices.exists_alias, name=alias):
            return
        definition = index_definition(self._shards, self._replicas)
        try:
            await self._call(
                "create_index",
                self._client.indices.create,
                index=f"{alias}-v1",
                settings=definition["settings"],
                mappings=definition["mappings"],
                aliases={alias: {"is_write_index": True}},
            )
            logger.info("index_created", extra={"alias": alias, "index": f"{alias}-v1"})
        except BadRequestError as exc:
            if _error_type(exc) != "resource_already_exists_exception":  # another replica won the race
                raise

    # -- documents -------------------------------------------------------------
    @staticmethod
    def _source(document: Document) -> dict[str, Any]:
        source = document.model_dump(mode="json")
        source["document_id"] = source.pop("id")
        return source

    @staticmethod
    def _document(source: dict[str, Any]) -> Document:
        data = dict(source)
        data["id"] = data.pop("document_id")
        return Document.model_validate(data)

    async def index(self, document: Document) -> None:
        for attempt in (1, 2):
            alias = await self.ensure_tenant(document.tenant_id)
            try:
                await self._call(
                    "index",
                    self._client.index,
                    index=alias,
                    id=self.es_id(document.tenant_id, document.id),
                    document=self._source(document),
                    routing=self.routing(document.tenant_id),
                    refresh=self._refresh,
                    require_alias=True,
                )
                return
            except NotFoundError:
                # The alias was removed out-of-band (e.g. tenant purge): forget it and recreate once.
                self._known_aliases.discard(alias)
                if attempt == 2:
                    raise

    async def bulk_index(self, documents: list[Document]) -> list[BulkItemResult]:
        if not documents:
            return []
        actions = []
        for document in documents:
            alias = await self.ensure_tenant(document.tenant_id)
            action: dict[str, Any] = {
                "_op_type": "index",
                "_index": alias,
                "_id": self.es_id(document.tenant_id, document.id),
                "_source": self._source(document),
            }
            if routing := self.routing(document.tenant_id):
                action["_routing"] = routing
            actions.append(action)

        _, errors = await self._call(
            "bulk",
            async_bulk,
            self._client,
            actions,
            chunk_size=500,
            max_retries=2,  # helper-level retry of 429-rejected items only
            initial_backoff=0.2,
            raise_on_error=False,
            refresh=self._refresh,
            require_alias=True,
        )
        failures: dict[str, dict[str, Any]] = {}
        for item in errors if isinstance(errors, list) else []:
            detail = next(iter(item.values()))
            failures[detail.get("_id", "")] = detail

        results: list[BulkItemResult] = []
        for document in documents:
            failure = failures.get(self.es_id(document.tenant_id, document.id))
            if failure is None:
                results.append(BulkItemResult(id=document.id, status="indexed"))
            else:
                status = int(failure.get("status", 500))
                reason = failure.get("error", {})
                message = reason.get("reason", "indexing failed") if isinstance(reason, dict) else str(reason)
                results.append(
                    BulkItemResult(
                        id=document.id, status="failed", error=message, retryable=status in TRANSIENT_STATUSES
                    )
                )
        return results

    async def get(self, tenant_id: str, document_id: str) -> Document | None:
        try:
            response = await self._call(
                "get",
                self._client.get,
                index=self.alias_for(tenant_id),
                id=self.es_id(tenant_id, document_id),
                routing=self.routing(tenant_id),
            )
        except NotFoundError:
            return None  # missing document or tenant index: a normal outcome, not a failure
        source = _body(response).get("_source", {})
        if source.get("tenant_id") != tenant_id:
            logger.error("tenant_mismatch_on_read", extra={"document_id": document_id})
            return None
        return self._document(source)

    async def delete(self, tenant_id: str, document_id: str) -> bool:
        try:
            await self._call(
                "delete",
                self._client.delete,
                index=self.alias_for(tenant_id),
                id=self.es_id(tenant_id, document_id),
                routing=self.routing(tenant_id),
                refresh=self._refresh,
            )
        except NotFoundError:
            return False
        return True

    async def search(self, tenant_id: str, query: SearchQuery, *, fuzzy: bool) -> SearchPage:
        body = build_search_body(tenant_id, query, fuzzy=fuzzy)
        kwargs: dict[str, Any] = {
            "query": body["query"],
            "from_": body["from"],
            "size": body["size"],
            "sort": body["sort"],
            "track_total_hits": body["track_total_hits"],
            "source": body["_source"],
            "highlight": body["highlight"],
        }
        if "track_scores" in body:
            kwargs["track_scores"] = body["track_scores"]
        if "aggs" in body:
            kwargs["aggs"] = body["aggs"]
        if routing := self.routing(tenant_id):
            kwargs["routing"] = routing
        try:
            response = await self._call(
                "search", self._client.search, index=self.alias_for(tenant_id), ignore_unavailable=True, **kwargs
            )
        except NotFoundError:
            return SearchPage(total=0, fuzzy_applied=fuzzy)  # tenant has never indexed anything
        return parse_search_response(_body(response), query, fuzzy=fuzzy)

    async def health(self) -> DependencyHealth:
        started = time.perf_counter()
        try:
            response = await self._client.options(request_timeout=2).cluster.health()
            body = _body(response)
            cluster_status = body.get("status", "unknown")
            status = "up" if cluster_status in ("green", "yellow") else "degraded"
            details: dict[str, object] = {
                "backend": self.name,
                "cluster_status": cluster_status,
                "nodes": body.get("number_of_nodes"),
                "tenancy": "shared_index" if self._shared else "index_per_tenant",
            }
        except Exception as exc:  # health must never raise
            status, details = "down", {"backend": self.name, "error": type(exc).__name__}
        return DependencyHealth(
            name="search",
            status=status,
            critical=True,
            latency_ms=round((time.perf_counter() - started) * 1000, 2),
            details=details,
        )
