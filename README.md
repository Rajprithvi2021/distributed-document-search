# Distributed Document Search Service

[![ci](https://github.com/Rajprithvi2021/distributed-document-search/actions/workflows/ci.yml/badge.svg)](https://github.com/Rajprithvi2021/distributed-document-search/actions/workflows/ci.yml)

Multi-tenant full-text search API: **FastAPI + Elasticsearch + Redis**, built to show enterprise patterns such as tenant isolation, layered caching, per-tenant rate limiting, async indexing through a queue, circuit breakers and zero-downtime deployment.

> 📄 **Reviewers: start with [`SUBMISSION.md`](SUBMISSION.md).** It is the single document with the architecture design, production-readiness analysis, experience showcase and AI-usage note.

| | |
|---|---|
| Required endpoints | `POST /documents` · `GET /search?q=&tenant=` · `GET /documents/{id}` · `DELETE /documents/{id}` · `GET /health` |
| Extras | bulk indexing, async indexing (202 + Redis Streams + worker + DLQ), fuzzy / faceted / highlighted search, idempotency keys, API-key & JWT auth with scopes, Prometheus metrics, readiness/liveness |
| Quality | 169 tests (unit + HTTP integration), **94% coverage**, ruff-clean; plus 18 live tests against real Elasticsearch 8.15 and Redis (all passing); verified as a 2-replica deployment behind nginx, including failover and a zero-downtime blue-green switch; CI runs the tests and the full `docker compose` stack end to end |
| Docs | [Submission (architecture, production readiness)](SUBMISSION.md) · [Extended architecture & decision records](docs/ARCHITECTURE.md) · [API reference](docs/API.md) · [Benchmarks](docs/BENCHMARKS.md) · [Deployment & blue-green](docs/DEPLOYMENT.md) |

## Quick start

**Option A: full stack (Docker).** nginx gateway → 2 API replicas → Elasticsearch 8 + Redis 7, plus an indexing worker. Give Docker at least 4 GB of memory (Elasticsearch runs with a 1 GB heap).

```bash
docker compose up --build -d --wait   # returns when every healthcheck passes (first run: ~1–2 min)
curl http://localhost:8000/health
bash scripts/smoke_test.sh            # Windows: powershell -File scripts\smoke_test.ps1
```

**Option B: no dependencies (in-memory adapters).** Useful for a 30-second look and for development.

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload                      # http://localhost:8000/docs
```

**Run the tests**

```bash
pytest                                  # 169 tests, ~10 s, no Docker needed (live tests auto-skip)
pytest --cov=app --cov-report=term      # coverage
ruff check app tests scripts            # lint

# live tests against the running compose stack's Elasticsearch and Redis (default passwords shown)
ES_TEST_URL=http://localhost:9200 ES_TEST_USERNAME=elastic ES_TEST_PASSWORD=change-me-elastic \
REDIS_TEST_URL=redis://:change-me-redis@localhost:6379/0 pytest tests/live -v
```

## Try it

```bash
# index
curl -s -X POST localhost:8000/documents -H 'X-Tenant-ID: acme' -H 'Content-Type: application/json' \
  -d '{"title":"Distributed search","content":"Elasticsearch shards the index; Redis caches hot queries.","tags":["arch"],"metadata":{"category":"eng"}}'

# search: the assignment contract (tenant as a query parameter)
curl -s 'localhost:8000/search?q=distributed%20search&tenant=acme'

# facets, filters, typo tolerance (fuzzy=auto kicks in when the exact query finds nothing)
curl -s 'localhost:8000/search?q=distibuted&facets=tags,metadata.category&filter=category:eng' -H 'X-Tenant-ID: acme'

# get / delete (another tenant gets 404: isolation)
curl -s localhost:8000/documents/<id> -H 'X-Tenant-ID: acme'
curl -s -X DELETE localhost:8000/documents/<id> -H 'X-Tenant-ID: acme' -o /dev/null -w '%{http_code}\n'
```

There's also a [Postman collection](postman/document-search.postman_collection.json) (with tests; run it with the Collection Runner) and [`sample-requests.http`](sample-requests.http).

## How it's built

```
app/
├── main.py            app factory: routers, middleware, lifespan
├── config.py          typed settings, validated (e.g. header auth refused in production)
├── container.py       composition root: the only place concrete adapters are chosen
├── api/               HTTP layer: schemas, dependencies (auth → rate limit → scope), error envelope, routes/
├── core/              errors, JSON logging, Prometheus metrics, circuit breaker + retry, authentication
├── domain/            framework-free models (Document, SearchQuery, SearchPage)
├── search/            SearchBackend port → elasticsearch.py (prod) · memory.py (BM25 dev/test) · resilient.py
├── cache/             Cache port → memory · redis · tiered L1/L2 · guarded (breaker) · search/document stores
├── ratelimit/         token bucket → atomic Redis Lua (shared) · in-memory (fallback)
├── queue/             IndexingQueue port → Redis Streams (consumer group, XAUTOCLAIM, DLQ) · in-memory
├── services/          use cases: documents, search (single-flight, stale-if-error), health
├── workers/indexer.py async indexing worker (python -m app.workers.indexer)
└── middleware/        pure ASGI: request ID, access log, RED metrics, security headers, body limit
tests/unit · tests/integration · tests/live (real ES/Redis)    scripts/ (smoke, seed, benchmark, JWT)    deploy/ (nginx, blue-green, k8s)
```

Key behaviours, each covered by tests:

| Concern | Behaviour |
|---|---|
| Tenant isolation | Tenant validated (`^[a-z0-9][a-z0-9_-]{0,62}$`) and applied to index/alias, query filter, GET verification, cache keys and rate-limit buckets |
| Writes | Never acknowledged unless stored (503 otherwise); `Idempotency-Key` replays the original response (a different body with the same key is 422); async mode returns 202 and is readable by id immediately; a delete can't be undone by a queued or in-flight index job |
| Search | BM25, `title^2`, phrase boost, filters, sort, facets, HTML-safe highlights, `fuzzy=auto`, deep-paging guard |
| Caching | L1 (2 s) → Redis L2; O(1) per-tenant invalidation via generation counters; results computed inside the ≈ 1 s refresh window are never cached for the full TTL; single-flight; stale-if-error |
| Rate limiting | Per-tenant token bucket (burst + sustained), atomic in Redis on the Redis server clock, per-tenant overrides, local fallback if Redis is down |
| Failure handling | Circuit breakers + jittered retries under a 3 s deadline; Redis outage → still serving; ES outage → stale results or a clean 503, `/health` → 503 for alerting while `/health/ready` keeps replicas in rotation |

## Configuration

All settings are environment variables (see [`.env.example`](.env.example) and `app/config.py`). The important ones:

| Variable | Default | Values |
|---|---|---|
| `SEARCH_BACKEND` | `memory` | `elasticsearch` · `memory` |
| `STATE_BACKEND` | `memory` | `redis` · `memory` (cache, rate limits, queue) |
| `ELASTICSEARCH_TENANCY` | `index_per_tenant` | `index_per_tenant` · `shared_index` (routing by tenant) |
| `ELASTICSEARCH_REFRESH` | `false` | `false` (≈1 s eventual) · `wait_for` (read-your-writes) |
| `INDEXING_MODE` | `sync` | `sync` (201) · `async` (202 + queue + worker) |
| `AUTH_MODE` | `header` | `header` · `api_key` (`API_KEYS`) · `jwt` (`JWT_SECRET`) |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` / `RATE_LIMIT_BURST` | 600 / 100 | per tenant; `RATE_LIMIT_OVERRIDES='{"tenant":rpm}'` |
| `CACHE_SEARCH_TTL_S` / `CACHE_SEARCH_STALE_TTL_S` / `CACHE_LOCAL_TTL_S` | 60 / 600 / 2 | |
| `SEARCH_VISIBILITY_S` | 1.5 | refresh window after a write during which new pages are cached only until it closes |
| `RETRY_DEADLINE_S` | 3.0 | total retry budget per dependency call |

## Benchmarks (from this prototype)

One API process against **real Elasticsearch 8.15.3**, all on one laptop (i5-1135G7), in-process state. Full method, the Redis-state run, the in-memory comparison and an outage/recovery test are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

| Workload (concurrency 8) | req/s | p50 | p95 | cache hit |
|---|---:|---:|---:|---:|
| Cache-miss search | 314 | 25 ms | 32 ms | 0% |
| Popular-query search | 752 | 8 ms | 25 ms | 93% |
| Bulk indexing | 1,070 docs/s | | | |

p95 stays under 500 ms at concurrency 1, 8 and 32 in this run, with 0 errors in 15,000 requests. Redis-backed state (the deployed configuration) costs ≈ 8–18% of throughput in an interleaved A/B; capacity planning discounts for it.
