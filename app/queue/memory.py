"""In-process queue with the same semantics as the Redis Streams adapter (dev/test)."""

from __future__ import annotations

import asyncio
import itertools
from collections import deque

from app.queue.base import IndexingJob, QueuedMessage


class InMemoryQueue:
    name = "memory"

    def __init__(self) -> None:
        self._ids = itertools.count(1)
        self._ready: deque[QueuedMessage] = deque()
        self._in_flight: dict[str, QueuedMessage] = {}
        self.dead_letters: list[tuple[QueuedMessage, str]] = []
        self._available = asyncio.Event()

    async def setup(self) -> None:
        return None

    def _push(self, job: IndexingJob) -> None:
        self._ready.append(QueuedMessage(message_id=str(next(self._ids)), job=job))
        self._available.set()

    async def publish(self, jobs: list[IndexingJob]) -> None:
        for job in jobs:
            self._push(job)

    async def consume(self, consumer: str, count: int, block_ms: int) -> list[QueuedMessage]:
        if not self._ready and block_ms > 0:
            self._available.clear()
            try:
                await asyncio.wait_for(self._available.wait(), timeout=block_ms / 1000)
            except TimeoutError:
                return []
        batch: list[QueuedMessage] = []
        while self._ready and len(batch) < count:
            message = self._ready.popleft()
            self._in_flight[message.message_id] = message
            batch.append(message)
        return batch

    async def ack(self, message_ids: list[str]) -> None:
        for message_id in message_ids:
            self._in_flight.pop(message_id, None)

    async def requeue(self, message: QueuedMessage, error: str, *, count_attempt: bool) -> None:
        await self.ack([message.message_id])
        attempt = message.job.attempt + 1 if count_attempt else message.job.attempt
        self._push(message.job.model_copy(update={"attempt": attempt, "last_error": error}))

    async def dead_letter(self, message: QueuedMessage, error: str) -> None:
        await self.ack([message.message_id])
        self.dead_letters.append((message, error))

    async def depth(self) -> int:
        return len(self._ready) + len(self._in_flight)

    async def dead_letter_depth(self) -> int:
        return len(self.dead_letters)
