# Implementation Plan — Distributed Document Search Service

This file records (1) what the assignment asks for, (2) what the original
`distributed-document-search-main.zip` delivered, (3) the gaps and defects found,
and (4) the plan used to rebuild the project. Section 5 maps every requirement to
the file that satisfies it.

---

## 1. Requirements extracted from the assignment

| # | Deliverable | Required content |
|---|---|---|
| D1 | Architecture design doc (2–3 pages) | HLD diagram, indexing + search data flows, storage strategy, API design + contract examples, consistency model, caching layers, message-queue usage, multi-tenancy/isolation |
| D2 | Working prototype | `POST /documents`, `GET /search?q=&tenant=`, `GET /documents/{id}`, `DELETE /documents/{id}`; multi-tenancy; search engine; caching; per-tenant rate limiting; health check with dependency status; docker-compose |
| D3 | Production readiness | Scalability (100x), resilience, security, observability, performance, operations, 99.95% SLA |
| D4 | Experience showcase | 4 short examples from the candidate's **own** experience |
| S | Submission | README, docker-compose, sample requests (Postman or curl), diagrams, one Markdown/PDF containing D1+D3+D4 + AI-usage note |
| B | Bonus | Fuzzy / faceted / highlighting, blue-green deployment, benchmarks from the prototype, cost optimisation, OSS links |
| E | Evaluation | Architecture, **code quality (clean, modular, testable, error handling)**, scalability, security, production maturity, communication |

---

## 2. Review of the original submission

### 2.1 Structural problems
- All business logic is in `src/services/__init__.py` (500 lines) and all middleware in
  `src/middleware/__init__.py`. Nothing can be swapped or unit-tested in isolation.
- `src/models/database.py` is a dead "DEPRECATED" shim.
- No layering: routes call global singletons (`es_client`, `redis_client`) directly; no dependency injection.
- Only 9 tests, all end-to-end, and they **only pass because of a hidden in-memory fallback** (see 2.2).
- Docs cite files that don't exist (`ARCHITECTURE.md`, `SETUP.md`, `tests/unit/...`) and components that aren't
  built (PostgreSQL, RabbitMQ, `/auth/login`, `/health/live`).

### 2.2 Correctness defects in the code
| Area | Defect | Impact |
|---|---|---|
| Search | Every write is mirrored into a process-local dict; if ES returns 0 hits, search silently answers from that dict | Different answers per API replica; unbounded memory growth; tests prove nothing about ES |
| ES client | `index_document`, `bulk_index`, `create_index` return **success when ES is down** | Silent data loss: the API returns `201` for documents that were never indexed |
| ES client | `NotFoundError` on get/delete marks ES "unavailable" for 5 s | A single 404 disables search for every tenant |
| ES client | 0.25 s request timeout on all operations, including bulk indexing | Real indexing times out under load |
| ES client | `@retry` decorators are no-ops (`stop_after_attempt(1)`, or the exception is swallowed before tenacity sees it) | The "retry logic" the docs describe doesn't exist |
| ES query | No `highlight` or `aggs` in the ES request | Highlighting and facets (bonus) only work in the fake fallback |
| ES mapping | `metadata` is mapped `enabled: false` | Metadata can't be filtered or faceted |
| Cache | Cache key uses Python `hash()` (randomised per process) and leaves out `sort_by` | No cache sharing across replicas; wrong results when only the sort changes |
| Cache | Invalidation uses `KEYS search:{tenant}:*` | O(N) and blocks Redis; a known production anti-pattern |
| Rate limit | Read-then-write (`GET`, then `INCRBY`, then `SETEX`) is not atomic, and every request also pays a `PING` | Over-admits under concurrency; adds latency |
| Rate limit | The `except` branch calls `call_next` a second time | The request can execute twice |
| Tenancy | Tenant ID isn't validated before being put into the ES index name | Input such as `*` or `a,b` produces wildcard or multi-index names |
| Security | `allow_origins=["*"]` together with `allow_credentials=True`; error messages contain raw exception text | CORS misconfiguration; internal details leak to clients |
| Errors | Three different error shapes (`{"detail"}`, `{"error","error_code"}`, a 500 envelope) | Inconsistent API contract |
| Health | Always returns HTTP 200; no latency, no readiness/liveness split | Load balancers can't detect an unhealthy instance |
| Metrics | `/metrics` returns hard-coded zeros | Fake observability |
| Docker | Packages installed to `/root/.local` but the container runs as `appuser` | The container fails at start-up (permission denied on `/root`) |
| Docker | ES has no heap limit and security is on without explicit TLS settings | Can OOM on a laptop or fail HTTP health checks |
| Deps | `pendulum==2.1.2` and `aioredis==2.0.1` are pinned but unused; neither builds/imports on Python 3.12 | `pip install -r requirements.txt` fails on current Python |
| Async | `async def` endpoints call the **synchronous** ES and Redis clients | Blocks the event loop; throughput collapses under concurrency |

### 2.3 Documentation gaps vs. the assignment
- No data-flow diagram for async indexing; message-queue usage is mentioned only in passing.
- The consistency model and caching layers are thin and partly describe behaviour that doesn't exist.
- There's no single submission document containing D1 + D3 + D4 + AI note (content is split across 5 files that contradict each other).
- No performance benchmarks are taken from the prototype, even though the docs quote invented numbers.
- `EXPERIENCE_SHOWCASE.md` contains specific companies and metrics. D4 must be the candidate's own real
  experience, so the new submission provides a structured template to fill in instead of invented stories.
- No Postman collection.

---

## 3. Target design

### 3.1 Principles
1. **Ports and adapters.** Search, cache, rate limiter, and queue are interfaces, each with a production
   adapter (Elasticsearch / Redis / Redis Streams) and an in-memory adapter. Backends are chosen **explicitly**
   by configuration. Nothing falls back silently.
2. **Fail loudly on writes, degrade gracefully on reads.** A write that wasn't stored returns an error
   (`503`). Reads can be served stale from cache, flagged `degraded: true`.
3. **Tenant isolation at every layer**: identity → validated tenant ID → index/routing → mandatory query
   filter → cache keys → rate-limit buckets → metrics/logs.
4. **Everything testable without Docker**: unit tests run against fakes, Redis code runs against `fakeredis` (including the
   Lua scripts), and ES query construction is tested as pure functions.

### 3.2 Directory structure
```
app/
  main.py               app factory (create_app), middleware + router wiring
  config.py             typed settings (pydantic-settings) with validation
  container.py          composition root: builds adapters from settings
  api/                  HTTP layer only (schemas, dependencies, error handlers, routes/)
  core/                 cross-cutting: errors, logging, metrics, resilience, security
  domain/               framework-free models (Document, SearchQuery, SearchPage)
  search/               SearchBackend port + elasticsearch / memory / resilient adapters
  cache/                Cache port + memory / redis / tiered / guarded + search & document caches
  ratelimit/            token bucket: memory + Redis Lua (atomic) with local fallback
  queue/                IndexingQueue port + Redis Streams / memory
  services/             use cases: documents, search, health
  workers/              async indexing worker (python -m app.workers.indexer)
  middleware/           pure ASGI: request context/metrics/security headers, body-size limit
tests/unit/ tests/integration/
scripts/                seed, benchmark, JWT generator, smoke tests (bash + PowerShell)
deploy/                 nginx gateway, blue-green (compose + switch script), Kubernetes manifests
docs/                   API reference, benchmarks, deployment guide
postman/                Postman collection
SUBMISSION.md           single document: architecture + production readiness + experience + AI note
```

### 3.3 Key behaviours
| Capability | Design |
|---|---|
| Tenancy | `AUTH_MODE=header` (assignment: `X-Tenant-ID` header or `?tenant=`), `api_key` (key → tenant), `jwt` (HS256, `tenant_id` + `scope` claims). Tenant ID regex `^[a-z0-9][a-z0-9_-]{0,62}$`. In production, header mode is rejected at start-up. |
| ES layout | `index_per_tenant` (alias `docs-{tenant}` → `docs-{tenant}-v1`, so reindexing is an alias swap) or `shared_index` (routing by tenant, composite IDs). A `tenant_id` filter is always applied as well. |
| Search | `multi_match` over `title^2, content`, phrase-proximity boost, filters (tags, metadata, date range), sort (relevance/newest/oldest), facets (`terms` aggs), HTML-safe highlighting, fuzzy `auto` mode (fuzziness only when the exact query returns nothing), max-window guard. |
| Cache | L1 in-process TTL/LRU (2 s) → L2 Redis. Tenant **generation counter** in the key gives O(1) invalidation. Stale-if-error fallback, single-flight against stampedes. Redis behind a circuit breaker; cache failures never fail a request. |
| Rate limit | Token bucket per tenant (burst + sustained rate), atomic Redis Lua, per-tenant overrides, `X-RateLimit-*` + `Retry-After`, falls back to a per-instance limiter if Redis is down. |
| Async indexing | `INDEXING_MODE=async`: `202 Accepted`, job published to Redis Streams (consumer group, at-least-once), worker bulk-indexes in batches, retries infrastructure failures without consuming attempts, DLQ for poison messages, tombstones stop deleted documents from being re-indexed, pending documents readable via GET. |
| Resilience | Circuit breaker (closed/open/half-open) + exponential backoff with full jitter around every ES call; ES errors classified as transient or permanent. |
| Idempotency | An `Idempotency-Key` header on `POST /documents` replays the original result (SET NX). |
| Observability | Prometheus metrics (HTTP RED, backend latency, cache hit ratio, rate-limit rejections, queue depth, breaker state), JSON logs with request/tenant IDs, `X-Request-ID` propagation. |
| Health | `/health` (dependency detail, `503` when critical dependency down), `/health/live`, `/health/ready`. |
| Errors | One envelope `{"error": {"code","message","request_id","details"}}` for every error. |

---

## 4. Execution plan

| Phase | Work | Exit criteria |
|---|---|---|
| P1 | Plan + gap analysis (this file) | Reviewed against the assignment |
| P2 | Core: config, errors, logging, metrics, resilience, security | Unit tests green |
| P3 | Adapters: memory BM25 search, ES backend + query builder, caches, rate limiters, queues | Unit tests green (fakeredis for Redis paths) |
| P4 | Services, API routes, middleware, worker, container | Integration tests green |
| P5 | Tests: unit + integration incl. isolation, rate limits, degradation, async indexing, multi-instance | All green, coverage reported |
| P6 | Delivery: Dockerfile, compose (ES, Redis, 2 API replicas, worker, nginx), blue-green, k8s, CI, Postman, smoke scripts | YAML/JSON validated |
| P7 | Benchmarks from the running prototype; SUBMISSION.md, README, API.md, DEPLOYMENT.md, BENCHMARKS.md | Numbers come from real runs |
| P8 | Final verification: lint, full test run, live server smoke test | Results recorded below |

---

## 5. Requirement traceability

| Requirement | Where |
|---|---|
| POST /documents | `app/api/routes/documents.py`, `app/services/documents.py` |
| GET /search?q=&tenant= | `app/api/routes/search.py`, `app/services/search.py` |
| GET /documents/{id} | `app/api/routes/documents.py` |
| DELETE /documents/{id} | `app/api/routes/documents.py` |
| Multi-tenancy | `app/core/security.py`, `app/search/elasticsearch.py` (index/alias/routing), cache + rate-limit key namespacing |
| Search engine | `app/search/elasticsearch.py` (production), `app/search/memory.py` (BM25 dev/test adapter) |
| Caching layer | `app/cache/*` |
| Per-tenant rate limiting | `app/ratelimit/*`, `app/api/dependencies.py` |
| Health check with dependency status | `app/services/health.py`, `app/api/routes/system.py` |
| docker-compose | `docker-compose.yml`, `Dockerfile`, `deploy/nginx/nginx.conf` |
| Message queue for async operations | `app/queue/*`, `app/workers/indexer.py` |
| Architecture doc | `SUBMISSION.md` §1 |
| Production readiness | `SUBMISSION.md` §3 |
| Experience showcase | `SUBMISSION.md` §4 (template for the candidate's own experience) |
| AI tool usage note | `SUBMISSION.md` §5 |
| Sample requests | `postman/`, `sample-requests.http`, `scripts/smoke_test.*`, README curl examples |
| Bonus: fuzzy / facets / highlighting | `app/search/*`, `app/services/search.py` |
| Bonus: blue-green | `deploy/blue-green/`, `deploy/k8s/`, `docs/DEPLOYMENT.md` |
| Bonus: benchmarks | `scripts/benchmark.py`, `docs/BENCHMARKS.md` |
| Bonus: cost optimisation | `SUBMISSION.md` §3.8 |

---

## 6. Status

| Phase | Status |
|---|---|
| P1 Plan + gap analysis | ✅ Done |
| P2 Core (config, errors, logging, metrics, resilience, security) | ✅ Done |
| P3 Adapters (memory BM25, ES, caches, rate limiters, queues) | ✅ Done |
| P4 Services, API, middleware, worker, container | ✅ Done |
| P5 Tests | ✅ 161 tests, 94% line coverage, + 17 live tests |
| P6 Delivery (Docker, compose, nginx, blue-green, k8s, CI, Postman, smoke scripts) | ✅ Done; YAML/JSON validated |
| P7 Benchmarks + documentation | ✅ Done; numbers in `docs/BENCHMARKS.md` come from real runs |
| P8 Final verification | ✅ See below |

### Verification results

| Check | Result |
|---|---|
| `pytest` | 159 passed (stable across repeated runs) |
| Coverage (`pytest --cov=app`) | 94% |
| `ruff check` / `ruff format --check` on app, tests, scripts | clean |
| Live HTTP smoke test (`scripts/smoke_test.ps1`) vs. `uvicorn`, sync mode | all checks passed |
| Live HTTP smoke test, async mode (202 → worker → searchable) | all checks passed |
| Benchmark (`scripts/benchmark.py`, 20k docs, concurrency 1/8/32) | 0 errors / 15,000 requests; see `docs/BENCHMARKS.md` |
| `docker-compose.yml`, blue-green compose, k8s manifests, CI workflow | parse as valid YAML; `.env.example` loads through `Settings` |
| `.env.example` and Postman collection | load/parse correctly |

| Architecture section length (SUBMISSION §1 rendered at A4, 10.5 pt) | 2.46 pages (limit 3); no diagram wider than the page |
| Live Redis tests (`tests/live/test_redis_live.py`) | logic verified against in-process fakeredis (4/4); real Redis 7.4 runs in CI |
| **Live Elasticsearch tests vs. real ES 8.15.3** (native Windows build, SHA-512 verified) | **10/10 passed** (both tenancy layouts) |
| HTTP smoke test, API on real ES | all checks passed |
| Benchmark on real ES (20k docs, concurrency 1/8/32) | p95 < 500 ms at every level; 314 req/s uncached / 752 cached at concurrency 8; 0 errors |
| Outage test (kill ES, restart) | health 503 → stale `degraded` reads → writes 503 → breaker fail-fast (5 ms) → automatic recovery |

**Second pass (after self-review against the brief):** architecture section cut to 2–3 pages with the detail moved to
`docs/ARCHITECTURE.md` (sequence diagrams, sizing math, 7 decision records); measured capacity model for
1,000+ searches/s; ES query hardened (no empty highlight tags, neutral facet aggregation names); 16 live tests
for real Elasticsearch/Redis; CI jobs `live` (ES 8.15 + Redis 7.4 service containers) and `e2e` (full
`docker compose` stack + smoke tests in sync and async modes).

**Third pass: deployed-topology testing.** Docker Desktop couldn't run on the authoring machine (WSL
installers are downloaded from GitHub, and GitHub release downloads return 504 on that network), so the compose
topology was reproduced natively: nginx 1.31.8 gateway → 2 API replicas → Elasticsearch 8.15.3 + Memurai 4.1.2
(Redis 7.2-compatible) + standalone `app.workers.indexer`.

| Check | Result |
|---|---|
| Full suite incl. 17 live tests against those services | **178 passed** |
| Gateway load balancing | 5/5 split across replicas |
| Smoke test through gateway, sync and async modes | all checks passed (3/3 sync runs; async passes) |
| Shared state across replicas | cache entry written by B is a hit on A; search staleness ≤ 2 s as designed; rate limit shared (A 200 → B 429) |
| Replica failure | 10/10 requests 200 with replica B killed (nginx retried on A) |
| Async latency (POST 202 → searchable) | 0.5–2.2 s; queue depth 0, no dead letters |
| Blue-green switch + rollback under live traffic | 694/694 requests 200, 0 transport errors, version 1.0.0 → 2.1.0 → 1.0.0 |
| **Bug found → fixed** | GET-by-id returned a deleted document for ≤ 2 s from another replica's L1 → per-document data now bypasses L1 (`test_get_after_delete_on_another_replica_is_404`) |
| **Bug found → fixed** | worker's blocking `XREADGROUP` (1 s) exceeded the 0.5 s socket timeout → idle queue looked like an outage, up to 30 s backoff → dedicated queue connection + `INDEXING_BLOCK_MS` (`test_idle_worker_blocking_read_is_not_treated_as_an_outage`, `test_queue_connection_timeout_covers_the_blocking_read`) |

**Not executed in the authoring environment** (no Docker there): `docker compose up`, `tests/live` against
real engines, the blue-green `switch.sh`, and `kubectl apply`. The CI `live` and `e2e` jobs execute the first
two on every push. Run them (or `docker compose up --build -d && bash scripts/smoke_test.sh`) and confirm
green before submitting.
