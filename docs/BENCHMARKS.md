# Benchmarks

Measured with `scripts/benchmark.py` against the running prototype over real HTTP. Raw output:

| Run | Search engine | State (cache, counters) | Power | Raw output |
|---|---|---|---|---|
| **A** | Elasticsearch 8.15.3 | in-process (`STATE_BACKEND=memory`) | mains | [`benchmark-results-elasticsearch.json`](benchmark-results-elasticsearch.json) |
| **A-redis** | Elasticsearch 8.15.3 | Redis (`STATE_BACKEND=redis`, the deployed configuration) | battery, CPU throttled | [`benchmark-results-elasticsearch-redis.json`](benchmark-results-elasticsearch-redis.json) |
| **B** | in-memory BM25 dev adapter | in-process | mains | [`benchmark-results.json`](benchmark-results.json) |

Run A gives the absolute capacity figures. A-redis was measured later, with the laptop on battery, so its absolute numbers are depressed for every configuration; what it establishes is the **cost of Redis-backed state**, measured by an interleaved A/B (below).

## Setup

| | |
|---|---|
| Hardware | Laptop, Intel Core i5-1135G7 (4 cores / 8 threads), 7.7 GB RAM, Windows 11. **Everything on one machine:** API, Elasticsearch, Redis and the load generator compete for the same 4 cores, so these figures are conservative |
| Server | `uvicorn app.main:app`, **1 process**, `RATE_LIMIT_ENABLED=false`, access log off, `ELASTICSEARCH_REFRESH=false` (production setting). Elasticsearch single node, 1 GB heap. Redis: Memurai 4.1 (Redis 7-compatible server for Windows) on the same machine |
| Load generator | `httpx` async client **on the same machine** (it competes with the server for CPU) |
| Corpus | 20,000 synthetic documents (4 tenants × 5,000), Zipf-distributed vocabulary of 3,000 terms, 80–220 words each |
| Workloads | **cold**: unique queries (common terms + a rare term), so ~0% cache hits · **hot**: 50 popular queries, so ~91–93% hits · **mixed**: hot queries + 10% `POST /documents` |

## Results (A): real Elasticsearch 8.15.3

Indexing: **20,000 documents in 18.7 s ≈ 1,070 docs/s** through `POST /documents/bulk` (500 docs per request, single node).

| Workload | Concurrency | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Errors | Cache hit ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold search | 1 | 28.5 | 12.8 | 106.1 | 494.4 | 0% | 0.1% |
| hot search | 1 | 236.9 | 3.1 | 10.6 | 17.7 | 0% | 91.2% |
| mixed 90/10 | 1 | 52.9 | 8.8 | 23.6 | 340.8 | 0% | 19.7% |
| cold search | 8 | **313.8** | 25.1 | **32.4** | 39.6 | 0% | 0% |
| hot search | 8 | **751.9** | 8.0 | **24.9** | 46.4 | 0% | 92.5% |
| mixed 90/10 | 8 | 329.9 | 23.6 | 35.4 | 52.0 | 0% | 16.5% |
| cold search | 32 | 223.7 | 86.2 | 439.1 | 668.4 | 0% | 0% |
| hot search | 32 | 477.7 | 42.1 | 205.1 | 320.6 | 0% | 93.3% |
| mixed 90/10 | 32 | 241.7 | 86.9 | 408.8 | 587.0 | 0% | 18.6% |

* **p95 < 500 ms at every concurrency level, with 0 errors in 15,000 requests.** At concurrency 8, cache misses are served at p95 32 ms and 314 req/s by one API process.
* Elasticsearch scoring is about **5x faster than the in-memory adapter** (cold search at concurrency 8: 314 vs 64 req/s in run B), which confirms that scoring belongs in the search engine.
* The concurrency-1 tail (p99 494 ms) is first-query warm-up: JVM JIT, cold OS page cache, and the first query per tenant index. It disappears once warm (concurrency 8 runs after it).
* At concurrency 32 throughput drops, because Elasticsearch, the API and the load generator saturate the same 4 cores. In production these run on separate machines.

## Results (A-redis): Redis-backed state

**Cost of Redis state, interleaved A/B.** Two API processes on the same Elasticsearch indices at the same time, one with `STATE_BACKEND=redis` and one with `STATE_BACKEND=memory`, given identical query sets in alternation (`scripts/ab_compare.py`, concurrency 8, two rounds):

| Workload | Redis state (req/s · p50) | In-process state (req/s · p50) | Redis cost |
|---|---|---|---:|
| cold search | 101–106 · 56–57 ms | 114–130 · 43–44 ms | ≈ 12–18% throughput |
| hot search | 220–255 · 30–31 ms | 240–275 · 28–29 ms | ≈ 8% throughput |

A Redis round trip from the API measured 0.2 ms; the cost is mostly serialisation and the extra round trips on a miss (generation lookup, page write, refresh-window check). Capacity planning below applies a 20% discount to run A's figures for this.

**Full run with Redis state** (battery power, CPU throttled, so read the ratios rather than the absolute values). Indexing and all phases completed with 0 errors:

| Workload | Concurrency | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Errors | Cache hit ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold search | 1 | 21.8 | 27.5 | 194.5 | 339.7 | 0% | 0.1% |
| hot search | 1 | 109.4 | 6.8 | 24.5 | 34.9 | 0% | 91.2% |
| mixed 90/10 | 1 | 42.2 | 22.7 | 35.7 | 67.7 | 0% | 14.8% |
| cold search | 8 | 114.2 | 69.3 | 79.8 | 102.2 | 0% | 0% |
| hot search | 8 | 231.4 | 30.8 | 71.9 | 83.5 | 0% | 90.9% |
| mixed 90/10 | 8 | 126.1 | 65.5 | 87.0 | 113.9 | 0% | 15.8% |
| cold search | 32 | 118.9 | 261.4 | 315.5 | 457.5 | 0% | 0% |
| hot search | 32 | 147.6 | 137.3 | 622.6 | 1,076.4 | 0% | 91.1% |
| mixed 90/10 | 32 | 117.5 | 251.0 | 556.1 | 1,277.7 | 0% | 17.4% |

On the throttled CPU, one process with 32 requests in flight is past saturation and p95 exceeds 500 ms for hot and mixed traffic. Concurrency 8 stays well under (p95 ≤ 87 ms). The target is met by adding processes, not by loading one process further (see the capacity model).

## Resilience under a real outage

The Elasticsearch process was killed mid-run and restarted. API: one process, Redis state, `CACHE_SEARCH_TTL_S=1`, `BREAKER_RECOVERY_TIMEOUT_S=5`:

| During the outage | Result |
|---|---|
| `/health` | **503** `unhealthy`, `search: down` (alerting) |
| `/health/ready`, `/health/live` | **200**: the replica stays in rotation, because readiness is local; pulling every replica during a shared-dependency outage would turn degraded answers into gateway errors |
| Previously cached query | **200, `cache: stale`, `degraded: true`** (stale-if-error) |
| Uncached query | **503** `SERVICE_UNAVAILABLE` after 2 attempts (4.1 s: Windows takes ≈ 2 s to report a refused localhost connection; the 3 s retry deadline prevented a third) |
| `POST /documents` | **503**: rejected loudly, never acknowledged |
| Requests after the breaker opened | **7–8 ms fail-fast**, `Retry-After: 5`, `circuit_breaker_state{name="search"} 2` |
| **After restart** | `/health` 200 `healthy`, circuit **closed** (0), fresh results, writes 201; no manual intervention |

## Results (B): in-memory dev adapter

Indexing: **20,000 documents in 7.6 s ≈ 2,640 docs/s** through `POST /documents/bulk` (500 docs per request).

| Workload | Concurrency | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Errors | Cache hit ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold search | 1 | 62.7 | 13.1 | 36.3 | 55.9 | 0% | 0.1% |
| hot search | 1 | 200.6 | 3.8 | 11.3 | 24.2 | 0% | 91.2% |
| mixed 90/10 | 1 | 97.2 | 10.5 | 23.3 | 46.8 | 0% | 19.7% |
| cold search | 8 | 64.1 | 113.1 | 236.1 | 292.0 | 0% | 0% |
| hot search | 8 | 366.2 | 10.7 | 82.1 | 150.6 | 0% | 92.6% |
| mixed 90/10 | 8 | 93.4 | 77.3 | 157.7 | 224.4 | 0% | 18.2% |
| cold search | 32 | 66.9 | 459.4 | 709.0 | 818.2 | 0% | 0% |
| hot search | 32 | 271.5 | 82.4 | 338.2 | 478.4 | 0% | 93.4% |
| mixed 90/10 | 32 | 87.7 | 344.0 | 566.5 | 629.2 | 0% | 20.5% |

The BM25 dev adapter scores in Python, so cold-search throughput is flat at about 65 req/s whatever the concurrency, while latency grows with it (Little's law: 32 in flight ÷ 67 req/s ≈ 480 ms). It exists for tests and zero-setup development, not for capacity.

## What the numbers show

1. **Caching is the main latency lever** (run A, concurrency 8): a hit is ≈ 3x faster at the median (8.0 vs 25.1 ms) and a process serves 2.4x more requests (752 vs 314 req/s). Run A-redis shows the same shape (31 vs 69 ms).
2. **Scoring belongs in Elasticsearch**: 314 req/s on cache misses vs 64 for the Python adapter (run B), on the same machine.
3. **p95 < 500 ms holds at concurrency 8 in every run.** At 32 in flight a single process is saturated, and whether p95 stays under 500 ms depends on CPU headroom (yes in run A, no for hot/mixed in throttled run A-redis). The assignment target is met by adding replicas.
4. **Writes reduce cache effectiveness.** With 10% writes to the same four tenants, the hit ratio drops from 91–93% to 15–20%, because each write bumps the tenant's search generation and invalidates all its cached queries. That is correct but coarse. The next step is to **coalesce invalidations to the search refresh interval** (bump at most once per second per tenant, at the end of the window). Search is already eventually consistent at about 1 s, so this costs no extra staleness.
5. **Redis-backed state costs ≈ 8–18% of throughput** in exchange for caches, rate limits and counters shared by every replica.

## Capacity model: reaching 1,000+ searches/s

The API tier is stateless, so throughput scales with process count. Start from run A's cache-miss figure (314 req/s per process), discount it 20% for Redis-backed state (≈ 250 req/s), and plan at 60% utilisation:

```
processes = 1,000 req/s ÷ (250 req/s × 0.6) ≈ 7   →  4 pods × 2 uvicorn workers
ES load   = 1,000 req/s × (1 − hit ratio): 100 / 300 / 800 queries/s at 90% / 70% / 20% hits
          → a 3-node cluster (HA minimum) in every case
```

A single laptop already served one Elasticsearch node at 314 uncached queries/s, so even the 20%-hit case (800 queries/s across 3 nodes) is within reach; validate on production-sized hardware with the same script. At 100x the same arithmetic gives ≈ 670 processes (≈ 335 pods × 2) and the data-node counts in [`ARCHITECTURE.md` §3](ARCHITECTURE.md#3-sizing).

## Optimisations found by profiling (in-memory adapter, run B)

Profiling a cold query against the BM25 dev adapter (`cProfile`, 5,000-document tenant) showed:

| Hotspot | Share of time | Fix | Effect |
|---|---:|---|---|
| Re-analysing each hit's text for highlighting | 48% | `functools.lru_cache` on per-token analysis (pure function, bounded vocabulary) | |
| Sorting every candidate to return 10 | 20% | `heapq.nsmallest(offset + page_size)`: O(n log k) top-k | |
| **Total** | | | **16.7 → 8.4 ms per query (−50%)**; cold throughput 27 → 65 req/s |

## Reproduce

```bash
# (A) one API process on real Elasticsearch + Redis (e.g. the compose stack's, ports 9200/6379)
SEARCH_BACKEND=elasticsearch ELASTICSEARCH_URL=http://localhost:9200 ELASTICSEARCH_USERNAME=elastic \
ELASTICSEARCH_PASSWORD=change-me-elastic STATE_BACKEND=redis REDIS_URL=redis://:change-me-redis@localhost:6379/1 \
RATE_LIMIT_ENABLED=false LOG_LEVEL=WARNING uvicorn app.main:app --port 8010 --no-access-log
python scripts/benchmark.py --url http://localhost:8010 --concurrency 1 8 32 --out docs/benchmark-results-elasticsearch-redis.json

# cost of Redis state: start a second process like the first but with STATE_BACKEND=memory on :8011
python scripts/ab_compare.py http://localhost:8010 http://localhost:8011 --concurrency 8 --rounds 2

# (B) in-memory backends (no Docker)
RATE_LIMIT_ENABLED=false LOG_LEVEL=WARNING uvicorn app.main:app --port 8000 --no-access-log
python scripts/benchmark.py --url http://localhost:8000 --concurrency 1 8 32 --out docs/benchmark-results.json
```

Run on mains power: on battery, laptop CPUs throttle and every figure drops (here by about 3x). To benchmark the whole compose stack through the gateway instead, start it with `RATE_LIMIT_ENABLED=false` **and** raise the gateway's per-IP `limit_req` `rate` in `deploy/nginx/nginx.conf`: a load generator is a single client IP, so the edge flood limit would otherwise be what you measure. Run the load generator on a separate machine when possible, so it doesn't share CPU with Elasticsearch.
