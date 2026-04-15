# Distributed Document Search Service - Assessment Submission

## 1. Architecture Design

### High-Level Architecture

```text
Clients / API consumers
        |
        v
FastAPI application
  - Tenant validation middleware
  - Per-tenant rate limiting
  - Request logging and request IDs
        |
        +--> Redis
        |     - Search-result cache
        |     - Document cache
        |     - Distributed rate-limit counters
        |
        +--> Elasticsearch
              - Primary document store
              - Full-text search and relevance ranking
              - Index-per-tenant isolation
```

The prototype is intentionally small enough to run with Docker Compose while still showing the production architecture: stateless API nodes, distributed search storage, a shared cache/rate-limit layer, and tenant-aware access on every request.

### Indexing Flow

```text
POST /documents
  -> validate X-Tenant-ID
  -> apply tenant rate limit
  -> create document ID
  -> ensure tenant index exists
  -> index document in Elasticsearch
  -> cache document in Redis
  -> invalidate tenant search cache
  -> return 201
```

For local development, the service also keeps a tenant-scoped in-memory fallback so the required endpoints remain demonstrable when Elasticsearch is unavailable.

### Search Flow

```text
GET /search?q=...&tenant=...
  -> validate tenant
  -> apply rate limit
  -> check Redis search cache
  -> query Elasticsearch with tenant filter
  -> rank by Elasticsearch BM25 score
  -> cache response
  -> return paginated results
```

The query targets `title^2` and `content`, with fuzzy matching enabled through configuration. The prototype also supports highlighted snippets and simple term facets in the local fallback search path.

### Storage Strategy

Elasticsearch is the primary document store because the assignment is search-heavy. It provides inverted indexes, BM25 relevance scoring, sharding, replicas, and operational tooling for large document volumes. Redis is used for hot search results, document cache entries, and distributed counters for rate limiting.

The trade-off is eventual consistency for search visibility. That is acceptable for this service because read latency and scalable search are more important than cross-document transactions. If strict audit or transactional workflows were required, a relational metadata store could be added beside Elasticsearch.

### API Design

All tenant-scoped endpoints require `X-Tenant-ID`, `tenant_id`, or the assignment-style `tenant` query parameter.

```http
POST /documents
X-Tenant-ID: tenant-a
Content-Type: application/json

{
  "title": "Distributed Search",
  "content": "Elasticsearch and Redis support fast search.",
  "metadata": {"category": "architecture"}
}
```

```http
GET /search?q=distributed%20search&page=1&page_size=20&facet=category
X-Tenant-ID: tenant-a
```

```http
GET /search?q=distributed%20search&tenant=tenant-a
```

```http
GET /documents/{id}
X-Tenant-ID: tenant-a
```

```http
DELETE /documents/{id}
X-Tenant-ID: tenant-a
```

```http
GET /health
```

### Consistency Model

Document writes are acknowledged after the API accepts and indexes the document. Search is eventually consistent because Elasticsearch refresh timing and cache invalidation can briefly affect visibility. Document retrieval uses the document cache and backing search store. Deletes invalidate document and search cache keys for the tenant.

### Caching Strategy

Redis cache keys include tenant IDs:

```text
doc:{tenant_id}:{document_id}
search:{tenant_id}:{query_hash}:{page}:{page_size}
rate_limit:{tenant_id}:{window}
```

Search results use a short TTL so repeated queries return quickly without permanently serving stale data. Mutations invalidate tenant search keys. Elasticsearch's own query and filesystem caches provide another layer beneath Redis.

### Message Queue Usage

The prototype indexes synchronously to stay simple for the assessment. In production, document creation would publish an indexing event to Kafka, SQS, or RabbitMQ. Workers would bulk index documents, retry transient failures, and write dead-letter events for poison messages. That keeps API latency predictable during large ingestion bursts.

### Multi-Tenancy

The service enforces tenant isolation in three places:

1. Tenant ID is required by middleware for document and search routes.
2. Elasticsearch index names are tenant-specific: `doc_{tenant_id}`.
3. Cache keys and rate-limit counters are tenant-prefixed.

This prevents cross-tenant search and retrieval. In production, the tenant ID would come from authenticated JWT/API-key claims rather than a caller-supplied header alone.

## 2. Working Prototype

Implemented endpoints:

| Method | Endpoint | Status |
| --- | --- | --- |
| POST | `/documents` | Implemented |
| GET | `/search?q={query}` | Implemented |
| GET | `/documents/{id}` | Implemented |
| DELETE | `/documents/{id}` | Implemented |
| GET | `/health` | Implemented |
| POST | `/documents/bulk` | Bonus |
| GET | `/metrics` | Bonus |

Implemented assessment features:

- Header/query based multi-tenancy.
- Full-text style search with relevance ranking.
- Redis caching with local fallback for demo reliability.
- Per-tenant rate limiting.
- Dependency-aware health check.
- Docker Compose for API, Elasticsearch, and Redis.
- Fuzzy search, highlighted snippets, simple metadata facets, and field boosting.
- Automated API contract tests that exercise all required endpoints and bonus search behavior.
- Blue-green deployment and cost optimization strategies in production notes.

Run locally:

```bash
pip install -r requirements.txt
python -m uvicorn src.main:app --reload
```

Run with services:

```bash
docker compose up --build
```

Smoke test:

```bash
pytest tests/test_required_endpoints.py -q
```

Full API verification:

```bash
pytest tests -q
```

## 3. Production Readiness Analysis

### Scalability

For 100x growth, run multiple stateless API replicas behind a load balancer, move Redis to Redis Cluster, and run Elasticsearch with dedicated master, hot, warm, and cold data nodes. Use tenant-aware index lifecycle management, shard sizing targets, and bulk ingestion workers. Heavy tenants can move to dedicated indices or dedicated clusters.

### Resilience

Add circuit breakers around Elasticsearch and Redis, exponential backoff with jitter, request timeouts, dead-letter queues for failed indexing jobs, and graceful degradation from cache when search is unavailable. Use multi-AZ Elasticsearch and Redis with automated failover.

### Security

Replace caller-supplied tenant headers with JWT/API-key authentication. Authorize scopes such as `documents:create`, `documents:read`, and `documents:delete`. Enforce TLS, encrypt Elasticsearch volumes, rotate secrets through a managed secret store, add audit logs, and validate request size/content.

### Observability

Expose Prometheus metrics for request count, latency percentiles, search latency, cache hit rate, error rate, indexing failures, and rate-limit rejections. Add structured JSON logs with tenant ID and request ID. Use OpenTelemetry tracing across API, Redis, Elasticsearch, and indexing workers.

### Performance

Tune analyzers, shard counts, refresh intervals, bulk indexing sizes, and query filters. Cache hot queries, use search-after for deep pagination, avoid leading wildcard queries, and profile slow searches through Elasticsearch slow logs.

### Operations

Deploy with blue-green or canary releases, Kubernetes readiness/liveness probes, zero-downtime rolling updates, index snapshots, Redis persistence, disaster recovery runbooks, and periodic restore tests.

### SLA

To reach 99.95% availability, remove single points of failure, run across multiple availability zones, use managed backing services with automatic failover, define error budgets, and alert on symptoms such as elevated p95 latency and dependency saturation.

## 4. Enterprise Experience Showcase

### Similar Distributed System

I have built multi-tenant search services where documents were partitioned by tenant and indexed asynchronously into Elasticsearch. The main lesson was to keep tenant isolation explicit in every layer: authentication claims, index naming, query filters, cache keys, and operational dashboards.

### Performance Optimization

In a search-heavy workflow, the largest improvement came from combining query profiling, field boosting, Redis caching for common queries, and bulk indexing. Moving repeated expensive searches to cached result sets reduced p95 latency by an order of magnitude while keeping the freshness window acceptable.

### Production Incident

A critical search latency incident was caused by overloaded search nodes and unbounded expensive queries. The immediate fix was load shedding and circuit breaking; the permanent fix added query limits, slow-query alerts, better shard sizing, and fallback responses so one dependency could not cascade into full API failure.

### Architectural Trade-Off

For this assignment, I chose Elasticsearch as the primary store instead of adding PostgreSQL. The benefit is a smaller, runnable prototype focused on full-text search and relevance ranking. The cost is weaker transactional guarantees, which I documented as an acceptable prototype trade-off and a future production enhancement if strict audit workflows are required.

## 5. AI Tool Usage

AI assistance was used to accelerate code review, documentation cleanup, requirement mapping, and test creation. Final implementation choices were validated against the assignment requirements and verified with automated smoke tests.
