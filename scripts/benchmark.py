"""Load benchmark against a running instance.

    python scripts/benchmark.py --url http://localhost:8000 --docs-per-tenant 5000 --tenants 4 \
        --requests 3000 --concurrency 50 --out docs/benchmark-results.json

Phases:
  1. bulk indexing throughput (POST /documents/bulk, 500 docs per request)
  2. "cold" search: mostly-unique queries -> mostly cache misses (search-engine path)
  3. "hot" search: a small set of popular queries -> mostly cache hits (Redis/L1 path)
  4. mixed: 90% search / 10% writes, which also measures cache invalidation cost

Run the server with rate limiting relaxed (RATE_LIMIT_ENABLED=false) so the limiter doesn't
dominate the measurement. Latencies are client-observed end-to-end over HTTP.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from corpus import Corpus
from seed import seed


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct / 100
    lower, upper = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (k - lower)


async def run_phase(
    client: httpx.AsyncClient, name: str, requests: list[tuple[str, str, dict]], concurrency: int
) -> dict:
    latencies: list[float] = []
    statuses: Counter[int] = Counter()
    cache: Counter[str] = Counter()
    queue: asyncio.Queue = asyncio.Queue()
    for item in requests:
        queue.put_nowait(item)

    async def worker() -> None:
        while not queue.empty():
            method, tenant, payload = queue.get_nowait()
            started = time.perf_counter()
            if method == "search":
                response = await client.get("/search", headers={"X-Tenant-ID": tenant}, params=payload)
            else:
                response = await client.post("/documents", headers={"X-Tenant-ID": tenant}, json=payload)
            latencies.append((time.perf_counter() - started) * 1000)
            statuses[response.status_code] += 1
            if method == "search" and response.status_code == 200:
                cache[response.json()["cache"]] += 1

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - started
    errors = sum(count for status, count in statuses.items() if status >= 400)
    searches = sum(cache.values())
    return {
        "phase": name,
        "requests": len(requests),
        "concurrency": concurrency,
        "throughput_rps": round(len(requests) / elapsed, 1),
        "p50_ms": round(percentile(latencies, 50), 2),
        "p95_ms": round(percentile(latencies, 95), 2),
        "p99_ms": round(percentile(latencies, 99), 2),
        "max_ms": round(max(latencies), 2),
        "mean_ms": round(statistics.fmean(latencies), 2),
        "error_rate": round(errors / len(requests), 4),
        "cache_hit_ratio": round(cache["hit"] / searches, 3) if searches else None,
    }


async def main_async(args: argparse.Namespace) -> dict:
    total_docs = args.tenants * args.docs_per_tenant
    print(f"[1/4] seeding {total_docs} documents ...")
    seed_seconds = await seed(args.url, args.tenants, args.docs_per_tenant)

    corpus = Corpus(seed=7)
    tenants = [f"tenant-{i + 1}" for i in range(args.tenants)]
    popular = [corpus.query() for _ in range(50)]
    rare_terms = corpus.vocabulary[600:]
    phases: list[dict] = []
    max_concurrency = max(args.concurrency)
    limits = httpx.Limits(max_connections=max_concurrency, max_keepalive_connections=max_concurrency)
    async with httpx.AsyncClient(base_url=args.url, timeout=60, limits=limits) as client:
        for _ in range(50):  # warm up connections
            await client.get("/health/live")

        for concurrency in args.concurrency:
            print(f"[2/4] cold search, concurrency {concurrency} (unique queries -> cache misses)")
            cold = [
                ("search", corpus.rng.choice(tenants), {"q": f"{corpus.query()} {corpus.rng.choice(rare_terms)}"})
                for _ in range(args.requests // 2)
            ]
            phases.append(await run_phase(client, "cold_search", cold, concurrency))

            print(f"[3/4] hot search, concurrency {concurrency} (50 popular queries -> cache hits)")
            hot = [
                ("search", corpus.rng.choice(tenants), {"q": corpus.rng.choice(popular)}) for _ in range(args.requests)
            ]
            phases.append(await run_phase(client, "hot_search", hot, concurrency))

            print(f"[4/4] mixed 90% search / 10% write, concurrency {concurrency}")
            mixed = []
            for i in range(args.requests):
                tenant = corpus.rng.choice(tenants)
                if i % 10 == 0:
                    mixed.append(("write", tenant, corpus.document(i)))
                else:
                    mixed.append(("search", tenant, {"q": corpus.rng.choice(popular)}))
            phases.append(await run_phase(client, "mixed_90_10", mixed, concurrency))

        root = (await client.get("/")).json()

    return {
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "processor": platform.processor(),
            "search_backend": root.get("search_backend"),
            "state_backend": root.get("state_backend"),
        },
        "corpus": {"documents": total_docs, "tenants": args.tenants},
        "indexing": {
            "documents": total_docs,
            "seconds": round(seed_seconds, 2),
            "docs_per_second": round(total_docs / seed_seconds),
        },
        "phases": phases,
    }


def print_table(results: dict) -> None:
    print("\n| phase | concurrency | rps | p50 ms | p95 ms | p99 ms | errors | cache hit |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for p in results["phases"]:
        print(
            f"| {p['phase']} | {p['concurrency']} | {p['throughput_rps']} | {p['p50_ms']} | {p['p95_ms']} | "
            f"{p['p99_ms']} | {p['error_rate']:.2%} | {p['cache_hit_ratio']} |"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--tenants", type=int, default=4)
    parser.add_argument("--docs-per-tenant", type=int, default=5000)
    parser.add_argument("--requests", type=int, default=2000, help="requests per phase (cold phase uses half)")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 32])
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    results = asyncio.run(main_async(args))
    print(json.dumps(results, indent=2))
    print_table(results)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
