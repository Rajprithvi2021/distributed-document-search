# Benchmarks

Measured with `scripts/benchmark.py` against the running prototype over real HTTP. Raw output: [`benchmark-results-elasticsearch.json`](benchmark-results-elasticsearch.json) (real Elasticsearch) and [`benchmark-results.json`](benchmark-results.json) (in-memory dev adapter).

## Setup

| | |
|---|---|
| Hardware | Laptop, Intel Core i5-1135G7 (4 cores / 8 threads), 7.7 GB RAM, Windows 11. **Everything on one machine:** API, Elasticsearch and the load generator compete for the same 4 cores, so these figures are conservative |
| Server | `uvicorn app.main:app`, **1 process**, `RATE_LIMIT_ENABLED=false`, access log off. (A) `SEARCH_BACKEND=elasticsearch`: Elasticsearch 8.15.3 single node, 1 GB heap, `ELASTICSEARCH_REFRESH=false` (production setting). (B) `SEARCH_BACKEND=memory`: the BM25 dev adapter |
| Load generator | `httpx` async client **on the same machine** (it competes with the server for CPU) |
| Corpus | 20,000 synthetic documents (4 tenants × 5,000), Zipf-distributed vocabulary of 3,000 terms, 80–220 words each |
| Workloads | **cold**: unique queries (common terms + a rare term), so ~0% cache hits · **hot**: 50 popular queries, so ~92% hits · **mixed**: hot queries + 10% `POST /documents` |

## Results (A): real Elasticsearch 8.15.3

Indexing: **20,000 documents in 18.7 s ≈ 1,070 docs/s** through `POST /documents/bulk` (500 docs per request, single node).

| Workload | Concurrency | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Errors | Cache hit ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold search | 1 | 28.5 | 12.8 | 106.1 | 494.4 | 0% | 0.1% |
| hot search | 1 | 236.9 | **3.1** | 10.6 | 17.7 | 0% | 91.2% |
| mixed 90/10 | 1 | 52.9 | 8.8 | 23.6 | 340.8 | 0% | 19.7% |
| cold search | 8 | **313.8** | 25.1 | **32.4** | 39.6 | 0% | 0% |
| hot search | 8 | **751.9** | 8.0 | **24.9** | 46.4 | 0% | 92.5% |
| mixed 90/10 | 8 | 329.9 | 23.6 | 35.4 | 52.0 | 0% | 16.5% |
| cold search | 32 | 223.7 | 86.2 | 439.1 | 668.4 | 0% | 0% |
| hot search | 32 | 477.7 | 42.1 | 205.1 | 320.6 | 0% | 93.3% |
| mixed 90/10 | 32 | 241.7 | 86.9 | 408.8 | 587.0 | 0% | 18.6% |

* **p95 < 500 ms at every concurrency level, with 0 errors in 15,000 requests.** At concurrency 8, cache misses are served at p95 32 ms and 314 req/s by one API process.
* Elasticsearch scoring is about **5x faster than the in-memory adapter** (cold search at concurrency 8: 314 vs 64 req/s), which confirms that scoring belongs in the search engine.
* The concurrency-1 tail (p99 494 ms) is first-query warm-up: JVM JIT, cold OS page cache, and the first query per tenant index. It disappears once warm (concurrency 8 runs after it).
* At concurrency 32 throughput drops, because Elasticsearch, the API and the load generator saturate the same 4 cores. In production these run on separate machines.

### Resilience under a real outage

The Elasticsearch process was killed mid-run and restarted (`CACHE_SEARCH_TTL_S=1`, `BREAKER_RECOVERY_TIMEOUT_S=5`):

| During the outage | Result |
|---|---|
| `/health`, `/health/ready` | **503** `unhealthy` (the load balancer drains the instance); `/health/live` stays 200 |
| Previously cached query | **200, `cache: stale`, `degraded: true`** (stale-if-error) |
| Uncached query | **503** `SERVICE_UNAVAILABLE` (after 2 attempts) |
| `POST /documents` | **503**: rejected loudly, never acknowledged |
| Requests after the breaker opened | **4–6 ms fail-fast**, `Retry-After: 5`, `circuit_breaker_state{name="search"} 2` |
| **After restart** | `/health` 200, circuit **closed** (0), fresh search results, writes 201; no manual intervention |

## Results (B): in-memory dev adapter

Indexing: **20,000 documents in 7.6 s ≈ 2,640 docs/s** through `POST /documents/bulk` (500 docs per request).

| Workload | Concurrency | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Errors | Cache hit ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold search | 1 | 62.7 | 13.1 | 36.3 | 55.9 | 0% | 0.1% |
| hot search | 1 | 200.6 | **3.8** | 11.3 | 24.2 | 0% | 91.2% |
| mixed 90/10 | 1 | 97.2 | 10.5 | 23.3 | 46.8 | 0% | 19.7% |
| cold search | 8 | 64.1 | 113.1 | 236.1 | 292.0 | 0% | 0% |
| hot search | 8 | **366.2** | 10.7 | 82.1 | 150.6 | 0% | 92.6% |
| mixed 90/10 | 8 | 93.4 | 77.3 | 157.7 | 224.4 | 0% | 18.2% |
| cold search | 32 | 66.9 | 459.4 | 709.0 | 818.2 | 0% | 0% |
| hot search | 32 | 271.5 | 82.4 | 338.2 | 478.4 | 0% | 93.4% |
| mixed 90/10 | 32 | 87.7 | 344.0 | 566.5 | 629.2 | 0% | 20.5% |

No errors across 15,000 search/write requests (plus 40 bulk-indexing requests).

## What the numbers show

1. **Caching is the main latency lever.** A cache hit is about 3.4x faster at the median (3.8 ms vs 13.1 ms) and about 3x more throughput per process. This is the reason for two cache tiers in front of the search engine.
2. **A single process saturates, as expected.** Cold-search throughput stays flat at about 65 req/s whatever the concurrency, while latency grows linearly (Little's law: 32 in flight ÷ 67 req/s ≈ 480 ms). One CPU-bound Python process is the bottleneck. In production this is handled by scaling stateless API replicas horizontally, and above all by moving scoring to Elasticsearch, whose BM25 implementation is compiled, sharded and replicated.
3. **p95 < 500 ms holds up to moderate concurrency on one laptop process**: every workload at concurrency 8, and hot search at 32. The assignment target is met by adding replicas, not by making one process faster.
4. **Writes reduce cache effectiveness.** With 10% writes, the hit ratio drops from 93% to about 20%, because each write bumps the tenant's search generation and invalidates all its cached queries. That is correct but coarse. For write-heavy tenants, the next step is to **coalesce invalidations to the search refresh interval** (bump at most once per second per tenant, deferred to the end of the window). Search is already eventually consistent at about 1 s, so this costs no extra staleness.

## Capacity model: reaching 1,000+ searches/s

The API tier is stateless, so throughput scales with process count. With real Elasticsearch, one API process sustained **314 req/s on cache misses and 752 req/s on hits** (concurrency 8, everything on one 4-core laptop). Sizing on the conservative miss-path figure at 60% utilisation:

```
processes = 1,000 req/s ÷ (300 req/s × 0.6) ≈ 6   →  3 pods × 2 uvicorn workers
ES load   = 1,000 req/s × (1 − 0.7 hit ratio) = 300 queries/s → a 3-node cluster (HA minimum)
```

A single laptop already served one Elasticsearch node at 314 uncached queries/s, so 300 queries/s across a 3-node cluster leaves large headroom. Validate on production-sized hardware with the same script.

## Optimisations found by profiling

Profiling a cold query (`cProfile`, 5,000-document tenant) showed:

| Hotspot | Share of time | Fix | Effect |
|---|---:|---|---|
| Re-analysing each hit's text for highlighting | 48% | `functools.lru_cache` on per-token analysis (pure function, bounded vocabulary) | |
| Sorting every candidate to return 10 | 20% | `heapq.nsmallest(offset + page_size)`: O(n log k) top-k | |
| **Total** | | | **16.7 → 8.4 ms per query (−50%)**; cold throughput 27 → 65 req/s |

## Reproduce

```bash
# in-memory backends (no Docker)
RATE_LIMIT_ENABLED=false LOG_LEVEL=WARNING uvicorn app.main:app --port 8000 --no-access-log
python scripts/benchmark.py --url http://localhost:8000 --concurrency 1 8 32 --out docs/benchmark-results.json

# full stack (Elasticsearch + Redis + 2 API replicas behind nginx)
RATE_LIMIT_ENABLED=false docker compose up --build -d   # or raise RATE_LIMIT_* in docker-compose.yml
python scripts/benchmark.py --url http://localhost:8000 --concurrency 8 32 64
```

Benchmark the Compose stack from a separate machine when possible, so the load generator doesn't share CPU with Elasticsearch.
