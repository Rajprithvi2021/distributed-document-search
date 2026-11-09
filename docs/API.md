# API Reference

Interactive OpenAPI docs: `http://localhost:8000/docs` (disabled when `ENVIRONMENT=production`).

## Conventions

| Topic | Contract |
|---|---|
| Tenant (`AUTH_MODE=header`) | `X-Tenant-ID: <tenant>` header **or** `?tenant=<tenant>` query parameter (`tenant_id` also accepted). If both are sent and differ: `400 TENANT_MISMATCH`. Format: `^[a-z0-9][a-z0-9_-]{0,62}$` |
| `AUTH_MODE=api_key` | `X-API-Key: <key>`. The tenant is derived from the key; a conflicting `X-Tenant-ID` returns `403` |
| `AUTH_MODE=jwt` | `Authorization: Bearer <token>` with claims `sub`, `tenant_id`, `exp`, `aud=document-search`, `scope` (space-separated: `documents:read documents:write search:read`) |
| Request IDs | Send `X-Request-ID` (8–128 chars `[A-Za-z0-9._-]`) to correlate; otherwise one is generated. Always echoed back |
| Rate limits | Per-tenant token bucket. Every tenant-scoped response carries `X-RateLimit-Limit` (burst), `X-RateLimit-Remaining`, `X-RateLimit-Reset` (seconds until full). `429` adds `Retry-After` |
| Errors | `{"error": {"code": "…", "message": "…", "request_id": "…", "details": …}}` |
| Caching | Responses are `Cache-Control: no-store` (tenant data never goes into shared caches) |
| Highlights | HTML-escaped, with matches wrapped in `<em>…</em>`, so they are safe to render |

## Endpoints

### `POST /documents`: index a new document

| Field | Type | Rules |
|---|---|---|
| `title` | string | required, 1–512 chars, trimmed |
| `content` | string | required, non-blank; title + content ≤ `MAX_DOCUMENT_BYTES` (default 1 MiB) |
| `tags` | string[] | optional, ≤ 50, each `[\w.:-]{1,64}`, lowercased and de-duplicated |
| `metadata` | object | optional, ≤ 50 keys `[A-Za-z0-9_-]{1,64}`; values string/number/bool or lists of them |

Header `Idempotency-Key` (optional, ≤ 128 chars, scoped to the tenant, remembered 24 h): a retry with the same key and the same body replays the original response (same document and status) with `200` and `Idempotent-Replayed: true`, even if the document was deleted since. The same key with a different body is `422 IDEMPOTENCY_KEY_REUSED`. While Redis is unavailable keys can't be recorded, so de-duplication is best-effort.

Responses: `201` (indexed, `Location` header) · `202` (async mode, `status: "pending"`) · `200` (replay) · `409` (same key still in progress) · `413` · `422` · `429` · `503`.

```json
{
  "id": "6f1c2a7e-…", "tenant_id": "acme", "title": "Q3 incident review", "content": "…",
  "tags": ["ops"], "metadata": {"severity": "sev1"}, "status": "indexed",
  "created_at": "2024-10-01T12:00:00Z", "updated_at": "2024-10-01T12:00:00Z", "created_by": "anonymous"
}
```

### `POST /documents/bulk`

`{"documents": [ …1–500 document objects… ]}` → `201` all indexed · `202` all queued (async) · `207` partial failure.

```json
{"total": 2, "succeeded": 1, "failed": 1,
 "items": [{"id": "…", "status": "indexed", "error": null, "retryable": false},
           {"id": "…", "status": "failed", "error": "mapper_parsing_exception", "retryable": false}]}
```

### `GET /documents/{id}`

`200` with the document (`status: "pending"` until an async job is indexed) · `404 DOCUMENT_NOT_FOUND`, including when the document belongs to another tenant (existence is never leaked).

### `DELETE /documents/{id}`

`204` · `404`. Removes the document from the index and caches and invalidates the tenant's cached searches. In async mode, a tombstone stops a still-queued job from re-creating it.

### `GET /search`

| Param | Default | Description |
|---|---|---|
| `q` | required | 1–512 chars |
| `tenant` | – | tenant (alternative to the header) |
| `page`, `page_size` | 1, 10 | `page_size` ≤ 100; `page × page_size` ≤ 10,000 (`400` beyond, use filters instead of deep paging) |
| `sort` | `relevance` | `relevance` · `newest` · `oldest` |
| `fuzzy` | `auto` | `auto` = exact first, fuzzy (AUTO edit distance) only if nothing matched · `true` · `false` |
| `highlight` | `true` | include `highlights` |
| `tags` | – | repeatable; documents must have **all** tags |
| `filter` | – | repeatable `key:value` on metadata (AND), e.g. `filter=category:engineering` |
| `facets` | – | comma-separated `tags`, `metadata.<key>` (≤ 5) |
| `created_after`, `created_before` | – | ISO-8601 datetimes |

```json
{
  "query": "circuit breaker", "page": 1, "page_size": 10, "total": 42, "total_relation": "eq", "total_pages": 5,
  "results": [{
    "id": "6f1c…", "title": "Q3 incident review", "score": 7.91,
    "snippet": "<em>Circuit</em> <em>breakers</em> stopped the cascade…",
    "highlights": {"content": ["<em>Circuit</em> <em>breakers</em> stopped…"]},
    "tags": ["ops"], "metadata": {"severity": "sev1"}, "created_at": "2024-10-01T12:00:00Z"
  }],
  "facets": {"tags": [{"value": "ops", "count": 30}]},
  "fuzzy_applied": false, "cache": "miss", "degraded": false, "took_ms": 23.4
}
```

`cache` is `hit` | `miss` | `stale`. `degraded: true` means the search engine was unavailable and this is the last good result for the query.

### Operations

| Endpoint | Purpose | Codes |
|---|---|---|
| `GET /health` | Per-dependency status, latency, circuit state; `healthy` / `degraded` (non-critical down) / `unhealthy` (search down). For monitoring and alerting | 200 · 503 |
| `GET /health/live` | Liveness: the process is up, no dependency calls | 200 |
| `GET /health/ready` | Readiness: started and not draining (`{"ready": true}`). No shared-dependency checks, so an ES outage doesn't empty the load balancer | 200 · 503 |
| `GET /metrics` | Prometheus exposition | 200 |
| `GET /` | Service info and active adapters | 200 |

Behind the compose/nginx gateway, `/health*`, `/metrics` and `/docs` are served only to loopback and private-network clients.

## Error codes

| HTTP | `code` | When |
|---|---|---|
| 400 | `MISSING_TENANT` · `INVALID_TENANT` · `TENANT_MISMATCH` · `BAD_REQUEST` | tenant problems; result window exceeded |
| 401 | `UNAUTHENTICATED` | missing/invalid/expired credentials (`WWW-Authenticate` set) |
| 403 | `FORBIDDEN` | credential not valid for the requested tenant, or missing scope |
| 404 | `DOCUMENT_NOT_FOUND` · `NOT_FOUND` | unknown document / route |
| 405 | `METHOD_NOT_ALLOWED` | |
| 409 | `CONFLICT` | Idempotency-Key in progress |
| 413 | `PAYLOAD_TOO_LARGE` | body > `MAX_REQUEST_BYTES` or document > `MAX_DOCUMENT_BYTES` |
| 422 | `VALIDATION_ERROR` · `IDEMPOTENCY_KEY_REUSED` | schema/parameter validation (`details[]` lists `location`, `message`, `type`) · an Idempotency-Key reused with a different body |
| 429 | `RATE_LIMIT_EXCEEDED` | tenant bucket empty; honour `Retry-After` |
| 500 | `INTERNAL_ERROR` | unexpected; quote `request_id` to support |
| 503 | `SERVICE_UNAVAILABLE` | search engine unreachable or circuit open (`Retry-After` when open) |
