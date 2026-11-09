"""Deterministic synthetic corpus shared by seed.py and benchmark.py.

Word frequencies follow a Zipf-like distribution (a few very common terms, a
long tail of rare ones), which is how real text behaves and what makes posting
list sizes, and therefore query cost, realistic.
"""

from __future__ import annotations

import random

TOPICS = [
    "search",
    "index",
    "shard",
    "replica",
    "cache",
    "redis",
    "latency",
    "throughput",
    "tenant",
    "cluster",
    "query",
    "ranking",
    "relevance",
    "queue",
    "worker",
    "kafka",
    "stream",
    "backup",
    "snapshot",
    "failover",
    "circuit",
    "breaker",
    "retry",
    "timeout",
    "gateway",
    "kubernetes",
    "deployment",
    "rollout",
    "canary",
    "metrics",
    "tracing",
    "logging",
    "alerting",
    "budget",
    "capacity",
    "storage",
    "compression",
    "encryption",
    "token",
    "policy",
]
CATEGORIES = ["engineering", "operations", "security", "product", "finance", "legal"]


def _vocabulary(size: int, rng: random.Random) -> list[str]:
    syllables = ["ka", "lo", "mi", "ne", "ra", "su", "ti", "vo", "xe", "zu", "pa", "do", "fi", "gu", "he"]
    words = set(TOPICS)
    while len(words) < size:
        words.add("".join(rng.choice(syllables) for _ in range(rng.randint(2, 4))))
    tail = sorted(words - set(TOPICS))
    rng.shuffle(tail)  # random frequency rank for the synthetic words
    return list(TOPICS) + tail


class Corpus:
    def __init__(self, seed: int = 42, vocabulary_size: int = 3000) -> None:
        self.rng = random.Random(seed)
        self.vocabulary = _vocabulary(vocabulary_size, self.rng)
        self.weights = [1.0 / (rank + 1) ** 1.07 for rank in range(len(self.vocabulary))]

    def words(self, count: int) -> list[str]:
        return self.rng.choices(self.vocabulary, weights=self.weights, k=count)

    def document(self, index: int) -> dict:
        return {
            "title": " ".join(self.words(self.rng.randint(4, 8))).capitalize(),
            "content": " ".join(self.words(self.rng.randint(80, 220))) + ".",
            "tags": sorted(set(self.rng.sample(TOPICS, k=self.rng.randint(1, 3)))),
            "metadata": {"category": self.rng.choice(CATEGORIES), "seq": index},
        }

    def query(self) -> str:
        # Mostly 1-3 term queries drawn from the head and torso of the distribution.
        return " ".join(self.rng.choices(self.vocabulary[:600], weights=self.weights[:600], k=self.rng.randint(1, 3)))
