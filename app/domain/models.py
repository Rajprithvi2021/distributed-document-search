"""Core domain models shared by services and adapters.

These types know nothing about HTTP or a particular search engine. The API
layer maps them to response schemas, and each adapter maps them to its own
storage format.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

MetadataScalar = str | int | float | bool
MetadataValue = MetadataScalar | list[MetadataScalar]


def utcnow() -> datetime:
    return datetime.now(UTC)


class Document(BaseModel):
    """A tenant-owned document, the unit of indexing and retrieval."""

    model_config = ConfigDict(frozen=True)

    id: str
    tenant_id: str
    title: str
    content: str
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, MetadataValue] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    created_by: str | None = None


class SortOrder(str, Enum):
    RELEVANCE = "relevance"
    NEWEST = "newest"
    OLDEST = "oldest"


class FuzzyMode(str, Enum):
    """``auto`` runs the exact query first and retries with fuzziness only if
    nothing matched. Typo tolerance then costs extra only on zero-hit queries."""

    AUTO = "auto"
    ON = "true"
    OFF = "false"


@dataclass(frozen=True)
class SearchQuery:
    text: str
    page: int = 1
    page_size: int = 10
    sort: SortOrder = SortOrder.RELEVANCE
    fuzzy: FuzzyMode = FuzzyMode.AUTO
    highlight: bool = True
    tags: tuple[str, ...] = ()
    metadata_filters: tuple[tuple[str, str], ...] = ()
    created_after: datetime | None = None
    created_before: datetime | None = None
    facets: tuple[str, ...] = ()

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size

    def fingerprint(self) -> str:
        """Stable hash of every parameter that affects the result.

        Used in cache keys, so it must be identical on every API replica. Python's
        built-in ``hash()`` is randomised per process and can't be used here.
        """
        payload = asdict(self)
        payload["text"] = " ".join(self.text.lower().split())
        canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


class SearchHit(BaseModel):
    id: str
    title: str
    score: float | None = None
    snippet: str = ""
    highlights: dict[str, list[str]] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, MetadataValue] = Field(default_factory=dict)
    created_at: datetime


class FacetBucket(BaseModel):
    value: str
    count: int


class SearchPage(BaseModel):
    total: int
    total_relation: Literal["eq", "gte"] = "eq"
    hits: list[SearchHit] = Field(default_factory=list)
    facets: dict[str, list[FacetBucket]] = Field(default_factory=dict)
    took_ms: float = 0.0
    fuzzy_applied: bool = False


class BulkItemResult(BaseModel):
    id: str
    status: Literal["indexed", "pending", "failed"]
    error: str | None = None
    retryable: bool = False


@dataclass
class DependencyHealth:
    name: str
    status: Literal["up", "degraded", "down"]
    critical: bool
    latency_ms: float | None = None
    details: dict[str, object] = field(default_factory=dict)
