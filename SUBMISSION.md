# Distributed Document Search Service: Technical Assessment Submission

**Candidate:** _[your name]_ · **Repository:** [github.com/Rajprithvi2021/distributed-document-search](https://github.com/Rajprithvi2021/distributed-document-search) · **Stack:** Python 3.12 · FastAPI · Elasticsearch 8 · Redis 7 · Docker Compose · Kubernetes manifests

| Deliverable | Where |
|---|---|
| 1. Architecture design (2–3 pages) | §1, with extended diagrams, sizing and decision records in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) |
| 2. Working prototype | `app/`; run `docker compose up --build`, or `uvicorn app.main:app` with no dependencies · §2 |
| 3. Production readiness analysis | §3 |
| 4. Enterprise experience showcase | §4 |
| AI tool usage note | §5 |
| Sample requests | [`postman/`](postman/document-search.postman_collection.json) (with tests), `sample-requests.http`, `scripts/smoke_test.sh` / `.ps1`, README |
| Bonus: fuzzy, faceted search, highlighting · blue-green · benchmarks · cost | §2 · `deploy/blue-green/`, `deploy/k8s/` · [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) · §3.8 |

---

## 1. Architecture Design

**Targets → design.** 10M+ documents, 1,000+ searches/s at p95 < 500 ms, strict tenant isolation and horizontal scale lead to a sharded Elasticsearch cluster, stateless API replicas, two cache tiers, and tenant scoping enforced at every layer. **Assumptions:** about 5 KB of text per document (≈ 50 GB raw, ≈ 120 GB indexed with one replica); read:write ≈ 20:1; a long tail of small tenants plus a few large ones; search may lag writes by about 1 s; acknowledged writes must never be lost.

### 1.1 High-level architecture

```
 Clients ─HTTPS─▶ Gateway / LB: TLS · WAF · per-IP limit · 10 MB body cap · X-Request-ID
                        │
            ┌───────────▼────────────────────────────────────────────┐
            │ Search API (FastAPI) · stateless · N replicas (HPA)     │
            │ authenticate → tenant → rate limit → scope → services   │
            │ L1 cache (2 s) · circuit breakers · retries with jitter │
            └─────┬────────────────────┬──────────────────────┬──────┘
       search/get │ index (sync)       │ cache · buckets      │ publish (async mode)
       ┌──────────▼────────┐  ┌────────▼─────────┐  ┌─────────▼───────────┐
       │ Elasticsearch     │  │ Redis (L2)       │  │ Queue: Redis Streams│
       │ alias per tenant  │  │ pages · docs     │  │ (prod: Kafka)       │
       │ shards + replicas │  │ token buckets    │  │ consumer group, DLQ │
       └──────────▲────────┘  └──────────────────┘  └─────────┬───────────┘
                  └──────── _bulk ──────── Indexing workers (N) ◀──┘
 Observability: Prometheus · JSON logs (request_id, tenant_id) · OpenTelemetry (prod)
```

### 1.2 Data flows

```
INDEX (sync)  POST → auth · tenant · rate limit · validate
              → ES index via alias (require_alias, retry + breaker) → cache doc
              → INCR tenant cache generation → 201       (never 201 unless stored; else 503)
INDEX (async) POST → cache "pending" copy → XADD job → 202  (GET by id works immediately)
              worker: XAUTOCLAIM stale + XREADGROUP batch → drop tombstoned → ES _bulk
              → XACK │ re-queue retryable │ DLQ permanent → INCR generation of touched tenants
SEARCH        GET → L1 → L2 key search:{tenant}:{generation}:{sha256(params)}
              hit → 200 │ miss → single-flight ES query (filter tenant_id) → store → 200
              ES down / circuit open → last good page (degraded: true), else 503
```

### 1.3 Storage strategy

| Concern | Choice | Why, and the alternative rejected |
|---|---|---|
| Search engine | **Elasticsearch 8** | BM25 ranking, shards and replicas, native fuzzy/highlight/aggregations, snapshots and ILM. *PostgreSQL FTS* is simpler and transactional but scales out poorly and has weaker relevance tuning. It's the right call below about 5M docs, not at 10M+ |
| System of record | Prototype: ES `_source` · **Production:** object storage + metadata DB | ES becomes a rebuildable projection (reindex and DR without depending on ES alone); kept out of the prototype to stay at 3 services |
| Cache | In-process **L1** + **Redis** L2 | Sub-millisecond, atomic Lua, TTLs. *Memcached* lacks Lua and streams |
| Queue | **Redis Streams** → **Kafka** | Consumer groups with no extra service today; replay, per-key ordering and throughput at scale |

**Index design:** custom analyzer (standard → lowercase → asciifolding → stopwords → `minimal_english`); `title^2` + `content`; keyword `tags`; `flattened` metadata (filterable and faceted, no mapping explosion); `dynamic: strict`; 1 s refresh; 10–50 GB shards. Writes go to alias `docs-{tenant}` → `docs-{tenant}-v1`, so a mapping change is reindex + atomic alias swap.

### 1.4 API design

| Endpoint | Purpose | Responses |
|---|---|---|
| `POST /documents` | Index a document (`Idempotency-Key` supported) | 201 sync · 202 async · 200 replay · 413 · 422 · 503 |
| `GET /search?q=&tenant=` | Ranked search + `page`, `page_size`, `sort`, `fuzzy`, `highlight`, `tags`, `filter=k:v`, `facets`, date range | 200 |
| `GET /documents/{id}` | Document details, tenant-scoped | 200 · 404 |
| `DELETE /documents/{id}` | Remove a document | 204 · 404 |
| `POST /documents/bulk` · `GET /health` (+`/live`, `/ready`) · `GET /metrics` | Batch ingest · dependency status · Prometheus | 201/202/207 · 200/503 · 200 |

```http
GET /search?q=circuit+breaker&tenant=acme&facets=tags&page_size=10
```
```json
{"query": "circuit breaker", "total": 42, "total_pages": 5, "cache": "miss",
 "degraded": false, "fuzzy_applied": false, "took_ms": 23.4,
 "results": [{"id": "5b7d…", "title": "Q3 incident review", "score": 7.91,
   "snippet": "<em>Circuit</em> <em>breakers</em> stopped the cascade…", "tags": ["ops"]}],
 "facets": {"tags": [{"value": "ops", "count": 30}]}}
```

Every error uses one envelope, `{"error": {"code", "message", "request_id", "details"}}`, with stable codes (`MISSING_TENANT`, `VALIDATION_ERROR`, `RATE_LIMIT_EXCEEDED`, `SERVICE_UNAVAILABLE`, …). Headers: `X-Request-ID`, `X-RateLimit-*`, `Retry-After`. Highlights are HTML-escaped. Full reference: [`docs/API.md`](docs/API.md).

### 1.5 Consistency model and trade-offs

| Operation | Guarantee | Mechanism |
|---|---|---|
| Write | Durable when acknowledged (201/202); otherwise 503, never silent loss | ES ack · Redis AOF (Kafka `acks=all` in prod) |
| GET by id | Read-your-writes | Document cache + ES real-time GET |
| Search | Eventually consistent: ≈ 1 s, plus queue lag in async mode | ES refresh; `ELASTICSEARCH_REFRESH=wait_for` when read-your-writes is required |
| Cached search after a write | Same replica: immediate · other replicas: ≤ 2 s | Generation bump; L1 TTL bounds staleness |
| Delete vs queued job · retries | No resurrection · no duplicates | Tombstones · server-assigned IDs + `Idempotency-Key` |

Trade-off: **availability for reads, correctness for writes.** During an ES outage, search serves the last good result (flagged `degraded`) while writes fail fast and visibly. The design for an update endpoint (ES external versioning) is in `docs/ARCHITECTURE.md`.

### 1.6 Caching strategy across layers

| Layer | Holds | TTL | Invalidation |
|---|---|---|---|
| Client / CDN | nothing tenant-specific (`Cache-Control: no-store`) | – | – |
| L1 in-process | search pages, generation counters (per-document data skips L1: GET-by-id stays read-your-writes across replicas) | 2 s, LRU 10k | TTL |
| L2 Redis | pages (fresh 60 s, kept 600 s), documents 300 s, idempotency 24 h, tombstones 1 h, token buckets | per key | **Per-tenant generation counter**: O(1), no `KEYS`/`SCAN`, other tenants untouched |
| Elasticsearch | filter bitsets (node query cache), shard request cache, OS page cache | engine | segment refresh |

Single-flight collapses concurrent identical misses into one query; stale-if-error serves expired pages during an outage; Redis sits behind a circuit breaker (outage = slower, not failing); `volatile-lru` means only TTL'd keys are ever evicted, never streams or counters. **Measured (real Elasticsearch):** p50 12.8 ms (miss) vs 3.1 ms (hit), and 91–93% hit ratio on popular queries.

### 1.7 Message queue usage

* **What runs asynchronously:** document ingestion (burst absorption, batched `_bulk`), reindex/backfill, tenant purge, and future enrichment (OCR, embeddings).
* **Semantics:** at-least-once, idempotent consumers. Item failures retry up to 5 attempts, then go to the DLQ. An infrastructure outage re-queues *without* spending attempts, with exponential backoff. Crashed workers' jobs are reclaimed with `XAUTOCLAIM`. Queue depth is a metric used for worker autoscaling.
* **Production:** Kafka topic keyed by `tenant:doc_id` (per-document ordering), RF = 3, 7-day retention for replay. Use a transactional outbox when the source of truth is a database.

### 1.8 Multi-tenancy and data isolation

* **Identity → tenant:** in production the tenant comes from the JWT claim or API key, never from a client-chosen header. The service refuses to start in header mode when `ENVIRONMENT=production`.
* **Validation:** `^[a-z0-9][a-z0-9_-]{0,62}$`, so tenant IDs are safe in index names and keys (no `*`, `,`, `..`).
* **Layout:** index per tenant (default: physical isolation, O(1) offboarding) or a shared index with `routing=tenant` for long-tail tenants; dedicated clusters for regulated tenants. Tenants move between tiers by alias swap.
* **Defence in depth:** every query is filtered by `tenant_id`; every GET verifies the owner; cache keys, rate-limit buckets and logs are tenant-namespaced.
* **Noisy neighbours:** per-tenant token buckets with tier overrides, physical isolation for large tenants, per-IP limits at the edge.

---

## 2. Working Prototype

| Requirement | Implementation |
|---|---|
| `POST /documents`, `GET /search?q=&tenant=`, `GET/DELETE /documents/{id}` | `app/api/routes/`, `app/services/` |
| Multi-tenancy (header or query) | `app/core/security.py`; plus `api_key` and `jwt` (scopes) modes |
| Search engine | `app/search/elasticsearch.py`; `app/search/memory.py` is a BM25 dev/test adapter with the same analyzer |
| Caching layer | `app/cache/`: L1 + L2, generation invalidation, single-flight, stale-if-error |
| Per-tenant rate limiting | `app/ratelimit/`: atomic Redis Lua token bucket, overrides, local fallback |
| Health check with dependency status | `/health` (503 when a critical dependency is down), `/health/live`, `/health/ready` |
| Docker Compose | nginx gateway, 2 API replicas, indexing worker, Elasticsearch, Redis |
| **Bonus:** fuzzy · facets · highlighting | `fuzzy=auto` (fuzzy only if the exact query finds nothing), `terms` facets on tags/metadata, HTML-safe highlights |
| **Bonus:** blue-green · benchmarks · cost | `deploy/blue-green/switch.sh` (health-gated, reversible) + `deploy/k8s/` · `docs/BENCHMARKS.md` · §3.8 |
| Beyond the brief | Async indexing (Redis Streams + worker + DLQ), idempotency keys, circuit breakers, Prometheus metrics, JSON logs, CI |

**Structure:** ports and adapters. `api/` → `services/` → ports (`search/`, `cache/`, `ratelimit/`, `queue/`) → adapters, all wired in one composition root (`container.py`).

**Verification:**
* **161 tests, 94% coverage, ruff-clean.** Covers tenant isolation on every endpoint, rate limits shared by two API instances, cross-replica cache invalidation, the circuit breaker opening, stale-if-error, Redis outage, the async pipeline, and JWT scopes. Redis paths (Lua, Streams) run on fakeredis.
* **`tests/live/`** runs the Elasticsearch and Redis adapters, and the full HTTP stack, against real ES 8.15 and Redis 7.4.
* **Run against a real Elasticsearch 8.15.3:** all 10 live ES tests pass (both tenancy layouts); the HTTP smoke test passes; benchmarks hit p95 32 ms at 314 req/s on cache misses; and an outage test passes (kill ES → health 503, cached queries served `degraded`, writes 503, breaker fails fast in 5 ms → automatic recovery after restart). Details: `docs/BENCHMARKS.md`.
* **Run as the deployed topology** (nginx gateway → 2 API replicas → Elasticsearch 8.15.3 + Redis-protocol server, plus the standalone worker): all 17 live tests pass; smoke tests pass through the gateway in sync and async modes; load is balanced 50/50; the cache and rate limit are shared across replicas (200 on replica A, 429 on replica B); killing a replica loses no requests; a blue-green switch and rollback under live traffic gives 694/694 OK. This found **two bugs, now fixed with regression tests**: per-document cache entries could be served for up to 2 s from another replica's memory after a delete, and the worker's blocking queue read tripped the cache client's socket timeout, which looked like a Redis outage while idle.
* **CI** (`.github/workflows/ci.yml`) runs `tests/live/` against service containers, then runs the actual `docker compose` stack end to end with `scripts/smoke_test.sh`, in both sync and async indexing modes.

---

## 3. Production Readiness Analysis

### 3.1 Scalability: 100x (≈ 1B documents, ≈ 100k searches/s peak)

| Tier | Plan (sizing math in `docs/ARCHITECTURE.md` §3) |
|---|---|
| API | **Measured with real Elasticsearch:** one process sustains 314 req/s on cache misses and 752 req/s on hits, so 1,000 req/s ≈ 6 processes at 60% utilisation (3 pods × 2 workers). At 100x, ≈ 280 pods via HPA on CPU/RPS |
| Elasticsearch | Size by **QPS, not bytes**: about 12 TB indexed; at 70% cache hits ≈ 30k ES QPS → about 60 data nodes (estimate) + dedicated masters/coordinators; hot/warm/cold tiers; ILM rollover by size |
| Tenancy | Hybrid layout (shared routed indices for small tenants, dedicated for large); keep the cluster's total shard count in the low thousands |
| Redis | Redis Cluster (6+ primaries with replicas); separate cache and queue clusters |
| Queue | Kafka, 48+ partitions; workers autoscale on consumer lag |
| Geography | Tenants pinned to a home region (residency); cross-cluster replication for DR and read locality |

### 3.2 Resilience

*Implemented:* circuit breakers on ES and Redis (closed/open/half-open); exponential backoff with full jitter; explicit timeouts; stale-if-error reads; fail-loud writes; local rate-limit fallback; DLQ; idempotency keys; tombstones; liveness/readiness split; graceful shutdown; gateway retries of idempotent requests.
*Production:* multi-AZ ES with allocation awareness; Redis Sentinel/Cluster failover; Kafka RF = 3; per-dependency bulkheads; adaptive concurrency and load shedding; ES search `timeout` for partial results; regular chaos experiments.

### 3.3 Security

* **AuthN:** OIDC/JWT from the IdP (RS256 via JWKS in production; HS256 in the prototype); hashed, rotatable API keys for services; mTLS inside the mesh.
* **AuthZ:** per-route scopes (implemented); the tenant always comes from the token, preventing BOLA (broken object-level authorization), the top OWASP API risk; separate admin plane.
* **Encryption:** TLS 1.2+ at the edge and to ES (`https`) and Redis (`rediss`); KMS-encrypted volumes and snapshots; customer-managed keys via dedicated clusters.
* **API hardening (implemented):** strict schemas, size limits, no query-DSL injection (`multi_match`, never `query_string`), HTML-escaped highlights, security headers, CORS allowlist, no internal error leakage, per-tenant and per-IP limits.
* **Operations:** secrets manager with rotation; non-root, read-only containers; pinned dependencies + image scanning + SBOM; audit log of writes and deletes; GDPR purge by dropping the tenant index and expiring backups.

### 3.4 Observability

* **Metrics (implemented):** HTTP RED per route template, backend latency, cache hit/miss/stale, rate-limit rejections, circuit state, queue depth, indexing outcomes.
* **SLIs/SLOs:** availability, p95/p99 latency, **freshness** (queue age + refresh), DLQ size; alerts on multi-window error-budget burn.
* **Logs (implemented):** JSON with `request_id` and `tenant_id`, propagated from the gateway; no document content in logs.
* **Tracing (production):** OpenTelemetry across API, ES, Redis and Kafka, with W3C `traceparent` carried in queue message headers; tail sampling keeps slow and failed traces.

### 3.5 Performance

* **Query:** scoping clauses in filter context; `_source` filtering (implemented); `index_options: offsets` for long-document highlighting; capped `track_total_hits` for huge tenants; result-window guard (implemented) + `search_after`; routing to avoid fan-out.
* **Indexing:** 5–15 MB bulk batches (worker); during backfills `refresh_interval: 30s` and `replicas: 0`.
* **Index management:** ILM, force-merge read-only indices, `best_compression` on warm tiers, heap ≤ 50% of RAM and below 32 GB.
* **Measured:** profiling halved query cost (memoised token analysis, heap top-k); caching gives about 4x lower p50 on real Elasticsearch (12.8 → 3.1 ms); p95 stays under 500 ms at every tested concurrency. With 10% writes the hit ratio falls from 93% to 20% because each write invalidates the tenant's cache, so the next optimisation is coalescing invalidations to the 1 s refresh interval.

### 3.6 Operations

* **CI/CD:** lint → tests → live ES/Redis tests → compose end-to-end → image scan → deploy.
* **Zero-downtime:** health-gated blue-green (`deploy/blue-green/switch.sh`; Kubernetes Service selector flip), `maxUnavailable: 0`, readiness gates, `preStop` + graceful drain, PodDisruptionBudget; canary with automated analysis for risky changes.
* **Schema changes:** new index version + reindex/replay + atomic alias swap; keep the old version for rollback.
* **Backup and recovery:** ES snapshots to S3 every 30 min (RPO ≤ 30 min; about 0 with cross-cluster replication); point-in-time recovery for the source of truth; RTO ≤ 1 h; quarterly restore drills; runbook per alert.

### 3.7 SLA: 99.95% availability

The error budget is **21.9 min/month**. It is met by: no single point of failure (≥ 3 AZs, N+2 capacity); a minimal critical path (Redis degrades rather than fails; the queue decouples writes); stale-if-error reads; fast detection (synthetic probes, burn-rate alerts); fast mitigation (automated rollback, blue-green flip in seconds); an error-budget policy that freezes releases when the budget is spent; and cross-region DR. The SLI is successful (non-5xx, < 1 s) requests over valid requests, measured at the edge.

### 3.8 Cost optimisation (cloud)

* **Data tiers:** hot SSD for active tenants; warm/cold on cheaper nodes; a frozen tier via searchable snapshots for archives.
* **Caching as a cost lever:** every 10 points of hit ratio removes about 10% of ES QPS, and ES is the dominant cost.
* **Right-sizing:** replicas by tenant tier; avoid small shards; `best_compression` on warm indices.
* **Compute:** savings plans for the steady ES baseline; **spot instances for workers** (stateless, idempotent, at-least-once); API autoscaling down to baseline off-peak.
* **Tenant economics:** usage metering for chargeback and tier pricing; heavy tenants on dedicated, separately billed resources.
* **Observability:** trace sampling, 7-day hot logs, then archive.

---

## 4. Enterprise Experience Showcase

> ✍️ **Written by the candidate from their own career.** Invented experience would be misrepresentation, and interviewers probe these stories. 1–2 paragraphs each, with numbers you can defend.

**4.1 A similar distributed system you built (scale and impact)**
_Context and problem · scale (data, QPS, tenants, team) · your role and key design decisions · measurable impact._

**4.2 A performance optimisation with significant improvement**
_Symptom and how you measured it · root cause · change · before/after numbers · how you prevented regression._

**4.3 A critical production incident you resolved**
_Detection · impact · diagnosis · mitigation vs. permanent fix · follow-up actions · lesson._

**4.4 An architectural decision balancing competing concerns**
_Options · constraints · decision and accepted trade-off · how it was validated · what you'd change._

---

## 5. AI Tool Usage

> ✍️ Adjust to match exactly how you worked.

I used Anthropic's Claude (via Claude Code) as a pair programmer. It mapped every requirement against the original prototype and found its defects (for example, 201 responses for documents that were never indexed, process-local search fallbacks, non-atomic rate limiting, and a container that couldn't start as non-root). It also drafted the ports-and-adapters restructuring, the tests and these documents. I own the design decisions and trade-offs in §1 and §3 and reviewed the code against them. Every number quoted comes from an actual run of the tests, smoke scripts or `scripts/benchmark.py`; capacity figures marked as estimates are planning assumptions to validate under load. AI output was treated as a draft to verify, not as ground truth.

---

## 6. Assumptions and Known Limitations

* The in-memory search backend is for development and tests only (per-process; can't back multiple replicas).
* Benchmarks ran with a single-node Elasticsearch on the same 4-core laptop as the API and the load generator, so they are conservative; production capacity must be confirmed on production-sized hardware.
* No update endpoint; the versioning design for one is documented.
* Local compose runs ES and Redis without TLS (ports bound to localhost); production enables TLS everywhere.
* JWTs use HS256 in the prototype (RS256/JWKS in production); distributed tracing is designed but not wired in, and request IDs provide correlation today.
