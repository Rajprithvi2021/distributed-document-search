"""Prometheus metrics.

Labels are deliberately low-cardinality: routes are recorded as their template
(``/documents/{document_id}``), never the raw path, and tenant IDs are not used
as labels (per-tenant usage belongs in logs or a usage pipeline, not in TSDB series).
"""

from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

_LATENCY_BUCKETS = (0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

HTTP_REQUESTS = Counter("http_requests_total", "HTTP requests handled", ["method", "route", "status"])
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP request latency", ["method", "route"], buckets=_LATENCY_BUCKETS
)
BACKEND_LATENCY = Histogram(
    "search_backend_duration_seconds", "Search backend call latency", ["backend", "operation"], buckets=_LATENCY_BUCKETS
)
CACHE_LOOKUPS = Counter("cache_lookups_total", "Cache lookups by result", ["cache", "result"])
CACHE_ERRORS = Counter("cache_errors_total", "Cache operations that failed and were bypassed", ["operation"])
RATE_LIMITED = Counter("rate_limited_requests_total", "Requests rejected by the per-tenant rate limiter")
RATE_LIMIT_FALLBACK = Counter(
    "rate_limit_fallback_total", "Rate-limit decisions made locally because Redis was unavailable"
)
INDEXING_JOBS = Counter("indexing_jobs_total", "Async indexing job outcomes", ["status"])
QUEUE_DEPTH = Gauge("indexing_queue_depth", "Indexing jobs waiting or in flight")
CIRCUIT_STATE = Gauge("circuit_breaker_state", "Circuit state: 0=closed, 1=half_open, 2=open", ["name"])
DEGRADED_RESPONSES = Counter("degraded_responses_total", "Search responses served stale because the backend failed")


def render_latest() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
