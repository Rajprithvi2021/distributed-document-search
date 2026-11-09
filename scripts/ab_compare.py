"""Interleaved A/B of two running API processes on the same, already seeded indices.

Both sides get identical query sets in alternation, so machine load (thermal or power
throttling, background work) affects them equally and the *ratio* is trustworthy even
when absolute numbers are not. Seed first with ``scripts/benchmark.py`` against either side.

    python scripts/ab_compare.py http://localhost:8010 http://localhost:8011 --concurrency 8 --rounds 2
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark import run_phase
from corpus import Corpus


async def main_async(args: argparse.Namespace) -> None:
    corpus = Corpus(seed=11)
    tenants = [f"tenant-{i + 1}" for i in range(args.tenants)]
    popular = [corpus.query() for _ in range(50)]
    rare = corpus.vocabulary[600:]
    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    clients = [httpx.AsyncClient(base_url=url, timeout=60, limits=limits) for url in (args.url_a, args.url_b)]
    try:
        for client in clients:
            for _ in range(30):  # warm up connections
                await client.get("/health/live")
            info = (await client.get("/")).json()
            print(f"{client.base_url}  search={info['search_backend']} state={info['state_backend']}")
        for round_no in range(args.rounds):
            cold = [
                ("search", corpus.rng.choice(tenants), {"q": f"{corpus.query()} {corpus.rng.choice(rare)}"})
                for _ in range(args.requests // 2)
            ]
            hot = [
                ("search", corpus.rng.choice(tenants), {"q": corpus.rng.choice(popular)}) for _ in range(args.requests)
            ]
            for name, workload in (("cold", cold), ("hot", hot)):
                for client in clients:
                    result = await run_phase(client, name, list(workload), args.concurrency)
                    print(
                        f"round {round_no} {name:<4} {client.base_url}  {result['throughput_rps']:>7} req/s  "
                        f"p50 {result['p50_ms']:>7} ms  p95 {result['p95_ms']:>7} ms  hit {result['cache_hit_ratio']}"
                    )
    finally:
        for client in clients:
            await client.aclose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url_a")
    parser.add_argument("url_b")
    parser.add_argument("--tenants", type=int, default=4)
    parser.add_argument("--requests", type=int, default=1500, help="hot requests per phase (cold uses half)")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--rounds", type=int, default=2)
    asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
