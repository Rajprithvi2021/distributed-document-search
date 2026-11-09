"""Request/response contracts (also drive the OpenAPI document at /docs)."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.domain.models import BulkItemResult, Document, FacetBucket, MetadataValue, SearchHit

TAG_PATTERN = re.compile(r"^[\w.:-]{1,64}$")
METADATA_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
MAX_TAGS = 50
MAX_METADATA_KEYS = 50
MAX_METADATA_STRING = 1024
MAX_METADATA_LIST = 50


# --- errors -----------------------------------------------------------------
class ErrorBody(BaseModel):
    code: str = Field(examples=["DOCUMENT_NOT_FOUND"])
    message: str
    request_id: str | None = None
    details: Any = None


class ErrorResponse(BaseModel):
    error: ErrorBody


# Error responses shared by every tenant-scoped route (for the OpenAPI document).
ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": ErrorResponse, "description": "Missing/invalid tenant"},
    401: {"model": ErrorResponse, "description": "Missing/invalid credentials (api_key/jwt modes)"},
    403: {"model": ErrorResponse, "description": "Credentials not valid for tenant, or missing scope"},
    422: {"model": ErrorResponse, "description": "Validation error"},
    429: {"model": ErrorResponse, "description": "Tenant rate limit exceeded"},
    503: {"model": ErrorResponse, "description": "Search engine unavailable"},
}


# --- documents ----------------------------------------------------------------
class DocumentCreateRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "title": "Designing a distributed search service",
                "content": "Elasticsearch shards the inverted index; Redis caches hot queries.",
                "tags": ["architecture", "search"],
                "metadata": {"category": "engineering", "author": "jane", "year": 2024},
            }
        },
    )

    title: str = Field(min_length=1, max_length=512)
    content: str = Field(min_length=1)
    tags: list[str] = Field(default_factory=list, max_length=MAX_TAGS)
    metadata: dict[str, MetadataValue] = Field(default_factory=dict)

    @field_validator("title", "content")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value.strip()

    @field_validator("tags")
    @classmethod
    def _normalise_tags(cls, tags: list[str]) -> list[str]:
        normalised: list[str] = []
        for tag in tags:
            tag = tag.strip().lower()
            if not TAG_PATTERN.fullmatch(tag):
                raise ValueError(f"invalid tag {tag!r}: 1-64 chars of letters, digits, '_', '-', '.', ':'")
            if tag not in normalised:
                normalised.append(tag)
        return normalised

    @field_validator("metadata")
    @classmethod
    def _check_metadata(cls, metadata: dict[str, MetadataValue]) -> dict[str, MetadataValue]:
        if len(metadata) > MAX_METADATA_KEYS:
            raise ValueError(f"at most {MAX_METADATA_KEYS} metadata keys are allowed")
        for key, value in metadata.items():
            if not METADATA_KEY_PATTERN.fullmatch(key):
                raise ValueError(f"invalid metadata key {key!r}: 1-64 chars of letters, digits, '_', '-'")
            values = value if isinstance(value, list) else [value]
            if len(values) > MAX_METADATA_LIST:
                raise ValueError(f"metadata[{key!r}] has more than {MAX_METADATA_LIST} items")
            if any(isinstance(v, str) and len(v) > MAX_METADATA_STRING for v in values):
                raise ValueError(f"metadata[{key!r}] strings are limited to {MAX_METADATA_STRING} chars")
        return metadata


class BulkCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    documents: list[DocumentCreateRequest] = Field(min_length=1, max_length=500)


class DocumentResponse(BaseModel):
    id: str
    tenant_id: str
    title: str
    content: str
    tags: list[str]
    metadata: dict[str, MetadataValue]
    created_at: datetime
    updated_at: datetime
    created_by: str | None
    status: Literal["indexed", "pending"] = Field(
        description="'pending' = accepted and queued for async indexing; not yet searchable"
    )

    @classmethod
    def from_domain(cls, document: Document, status: Literal["indexed", "pending"]) -> DocumentResponse:
        return cls(**document.model_dump(), status=status)


class BulkCreateResponse(BaseModel):
    total: int
    succeeded: int
    failed: int
    items: list[BulkItemResult]


# --- search -------------------------------------------------------------------
class SearchResponse(BaseModel):
    query: str
    page: int
    page_size: int
    total: int
    total_relation: Literal["eq", "gte"]
    total_pages: int
    results: list[SearchHit]
    facets: dict[str, list[FacetBucket]]
    fuzzy_applied: bool = Field(description="True if typo-tolerant matching produced these results")
    cache: Literal["hit", "miss", "stale"]
    degraded: bool = Field(description="True if served from stale cache because the search engine failed")
    took_ms: float = Field(description="Server-side time to answer this request")


# --- health -------------------------------------------------------------------
class DependencyStatus(BaseModel):
    status: Literal["up", "degraded", "down"]
    critical: bool
    latency_ms: float | None
    details: dict[str, Any]


class HealthResponse(BaseModel):
    status: Literal["healthy", "degraded", "unhealthy"]
    version: str
    environment: str
    timestamp: datetime
    dependencies: dict[str, DependencyStatus]
