"""In-memory BM25 search backend.

Used for local development and tests. It's an explicit choice
(``SEARCH_BACKEND=memory``), never a silent fallback. It implements the same
contract as the Elasticsearch adapter: per-tenant inverted indexes, BM25
scoring with a title boost, AUTO fuzziness, filters, facets, sorting and
HTML-safe highlighting.

State is process-local, so it can't be shared between API replicas. That's
why multi-replica deployments use Elasticsearch.
"""

from __future__ import annotations

import heapq
import math
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

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
from app.search.text import analyze, auto_fuzziness, highlight_fragments, leading_snippet, levenshtein

FIELD_BOOSTS = {"title": 2.0, "content": 1.0}
BM25_K1 = 1.2
BM25_B = 0.75
FUZZY_WEIGHT = {1: 0.8, 2: 0.6}  # fuzzy matches score lower than exact matches
FACET_SIZE = 20


@dataclass
class _TenantIndex:
    documents: dict[str, Document] = field(default_factory=dict)
    # field -> term -> {doc_id: term frequency}
    postings: dict[str, dict[str, dict[str, int]]] = field(
        default_factory=lambda: {name: defaultdict(dict) for name in FIELD_BOOSTS}
    )
    lengths: dict[str, dict[str, int]] = field(default_factory=lambda: {name: {} for name in FIELD_BOOSTS})

    def add(self, document: Document) -> None:
        if document.id in self.documents:
            self.remove(document.id)
        self.documents[document.id] = document
        for name, text in (("title", document.title), ("content", document.content)):
            terms = analyze(text)
            self.lengths[name][document.id] = len(terms)
            for term, frequency in Counter(terms).items():
                self.postings[name][term][document.id] = frequency

    def remove(self, document_id: str) -> bool:
        document = self.documents.pop(document_id, None)
        if document is None:
            return False
        for name, text in (("title", document.title), ("content", document.content)):
            self.lengths[name].pop(document_id, None)
            for term in set(analyze(text)):
                postings = self.postings[name].get(term)
                if postings is not None:
                    postings.pop(document_id, None)
                    if not postings:
                        del self.postings[name][term]
        return True


def _scalar(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _field_values(document: Document, facet: str) -> list[str]:
    if facet == "tags":
        return list(document.tags)
    key = facet.removeprefix("metadata.")
    value = document.metadata.get(key)
    if value is None:
        return []
    return [_scalar(v) for v in value] if isinstance(value, list) else [_scalar(value)]


class InMemorySearchBackend:
    name = "memory"

    def __init__(self) -> None:
        self._tenants: dict[str, _TenantIndex] = defaultdict(_TenantIndex)

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    async def index(self, document: Document) -> None:
        self._tenants[document.tenant_id].add(document)

    async def bulk_index(self, documents: list[Document]) -> list[BulkItemResult]:
        for document in documents:
            self._tenants[document.tenant_id].add(document)
        return [BulkItemResult(id=d.id, status="indexed") for d in documents]

    async def get(self, tenant_id: str, document_id: str) -> Document | None:
        index = self._tenants.get(tenant_id)
        return index.documents.get(document_id) if index else None

    async def delete(self, tenant_id: str, document_id: str) -> bool:
        index = self._tenants.get(tenant_id)
        return index.remove(document_id) if index else False

    async def health(self) -> DependencyHealth:
        documents = sum(len(t.documents) for t in self._tenants.values())
        return DependencyHealth(
            name="search",
            status="up",
            critical=True,
            latency_ms=0.0,
            details={"backend": self.name, "tenants": len(self._tenants), "documents": documents},
        )

    # -- search --------------------------------------------------------------
    async def search(self, tenant_id: str, query: SearchQuery, *, fuzzy: bool) -> SearchPage:
        started = time.perf_counter()
        index = self._tenants.get(tenant_id)
        query_terms = list(dict.fromkeys(analyze(query.text)))
        if index is None or not index.documents or not query_terms:
            return SearchPage(total=0, took_ms=_elapsed_ms(started), fuzzy_applied=fuzzy)

        scores, matched_terms = self._score(index, query_terms, fuzzy)
        candidates = [
            index.documents[doc_id] for doc_id in scores if self._passes_filters(index.documents[doc_id], query)
        ]

        facets = self._facets(candidates, query.facets)
        ordered = self._top(candidates, scores, query.sort, query.offset + query.page_size)
        page = ordered[query.offset :]

        hits = [self._to_hit(document, scores[document.id], matched_terms[document.id], query) for document in page]
        return SearchPage(
            total=len(candidates), hits=hits, facets=facets, took_ms=_elapsed_ms(started), fuzzy_applied=fuzzy
        )

    def _expand(self, index: _TenantIndex, term: str, fuzzy: bool) -> list[tuple[str, float]]:
        variants = [(term, 1.0)]
        max_edits = auto_fuzziness(term) if fuzzy else 0
        if not max_edits:
            return variants
        vocabulary = set(index.postings["title"]) | set(index.postings["content"])
        for candidate in vocabulary:
            # prefix_length=1: the first character must match, as in the ES query.
            if candidate == term or candidate[:1] != term[:1]:
                continue
            distance = levenshtein(term, candidate, max_edits)
            if distance <= max_edits:
                variants.append((candidate, FUZZY_WEIGHT[distance]))
        return variants

    def _score(
        self, index: _TenantIndex, query_terms: list[str], fuzzy: bool
    ) -> tuple[dict[str, float], dict[str, set[str]]]:
        scores: dict[str, float] = defaultdict(float)
        matched: dict[str, set[str]] = defaultdict(set)
        total_docs = len(index.documents)
        expansions = {term: self._expand(index, term, fuzzy) for term in query_terms}
        for field_name, boost in FIELD_BOOSTS.items():
            lengths = index.lengths[field_name]
            avg_length = (sum(lengths.values()) / len(lengths)) or 1.0
            for variants in expansions.values():
                for term, weight in variants:
                    postings = index.postings[field_name].get(term)
                    if not postings:
                        continue
                    df = len(postings)
                    idf = math.log(1 + (total_docs - df + 0.5) / (df + 0.5))
                    for doc_id, tf in postings.items():
                        norm = BM25_K1 * (1 - BM25_B + BM25_B * lengths[doc_id] / avg_length)
                        scores[doc_id] += boost * weight * idf * tf * (BM25_K1 + 1) / (tf + norm)
                        matched[doc_id].add(term)
        return scores, matched

    @staticmethod
    def _passes_filters(document: Document, query: SearchQuery) -> bool:
        if query.tags and not set(query.tags).issubset(document.tags):
            return False
        for key, expected in query.metadata_filters:
            if expected not in _field_values(document, f"metadata.{key}"):
                return False
        if query.created_after and document.created_at < query.created_after:
            return False
        return not (query.created_before and document.created_at > query.created_before)

    @staticmethod
    def _facets(documents: list[Document], facets: tuple[str, ...]) -> dict[str, list[FacetBucket]]:
        result: dict[str, list[FacetBucket]] = {}
        for facet in facets:
            counts: Counter[str] = Counter()
            for document in documents:
                counts.update(set(_field_values(document, facet)))
            buckets = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:FACET_SIZE]
            result[facet] = [FacetBucket(value=value, count=count) for value, count in buckets]
        return result

    @staticmethod
    def _top(documents: list[Document], scores: dict[str, float], sort: SortOrder, limit: int) -> list[Document]:
        """Top ``limit`` documents in order: a heap selection, O(n log k), instead of sorting everything."""
        if sort is SortOrder.NEWEST:
            return heapq.nlargest(limit, documents, key=lambda d: (d.created_at, d.id))
        if sort is SortOrder.OLDEST:
            return heapq.nsmallest(limit, documents, key=lambda d: (d.created_at, d.id))
        return heapq.nsmallest(limit, documents, key=lambda d: (-scores[d.id], -d.created_at.timestamp(), d.id))

    @staticmethod
    def _to_hit(document: Document, score: float, terms: set[str], query: SearchQuery) -> SearchHit:
        tags = ("<em>", "</em>") if query.highlight else ("", "")
        content_fragments = highlight_fragments(document.content, terms, pre_tag=tags[0], post_tag=tags[1])
        title_fragments = highlight_fragments(document.title, terms, max_fragments=0, pre_tag=tags[0], post_tag=tags[1])
        highlights: dict[str, list[str]] = {}
        if query.highlight:
            if title_fragments:
                highlights["title"] = title_fragments
            if content_fragments:
                highlights["content"] = content_fragments
        snippet = content_fragments[0] if content_fragments else leading_snippet(document.content)
        return SearchHit(
            id=document.id,
            title=document.title,
            score=round(score, 6),
            snippet=snippet,
            highlights=highlights,
            tags=document.tags,
            metadata=document.metadata,
            created_at=document.created_at,
        )


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 3)
