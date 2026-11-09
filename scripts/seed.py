"""Seed a running instance with synthetic documents via POST /documents/bulk.

python scripts/seed.py --url http://localhost:8000 --tenants 3 --docs-per-tenant 2000
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from corpus import Corpus


async def seed(url: str, tenants: int, docs_per_tenant: int, batch: int = 500) -> float:
    corpus = Corpus()
    started = time.perf_counter()
    async with httpx.AsyncClient(base_url=url, timeout=120) as client:
        for t in range(tenants):
            tenant = f"tenant-{t + 1}"
            for offset in range(0, docs_per_tenant, batch):
                documents = [corpus.document(offset + i) for i in range(min(batch, docs_per_tenant - offset))]
                response = await client.post(
                    "/documents/bulk", headers={"X-Tenant-ID": tenant}, json={"documents": documents}
                )
                response.raise_for_status()
                if response.json()["failed"]:
                    raise SystemExit(f"bulk indexing reported failures: {response.text[:500]}")
            print(f"seeded {docs_per_tenant} documents for {tenant}")
    return time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--tenants", type=int, default=3)
    parser.add_argument("--docs-per-tenant", type=int, default=2000)
    args = parser.parse_args()
    elapsed = asyncio.run(seed(args.url, args.tenants, args.docs_per_tenant))
    total = args.tenants * args.docs_per_tenant
    print(f"indexed {total} documents in {elapsed:.1f}s ({total / elapsed:.0f} docs/s)")


if __name__ == "__main__":
    main()
