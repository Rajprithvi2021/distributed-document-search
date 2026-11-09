"""Queue port for asynchronous indexing.

Delivery is **at-least-once**: a job can be delivered again after a consumer
crash, so the worker's processing must be idempotent. It is, because every
document carries a caller-assigned ID and indexing is an upsert.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from pydantic import BaseModel, Field

from app.domain.models import Document, utcnow


class IndexingJob(BaseModel):
    job_id: str
    document: Document
    attempt: int = 1
    enqueued_at: datetime = Field(default_factory=utcnow)
    last_error: str | None = None


@dataclass(frozen=True)
class QueuedMessage:
    message_id: str
    job: IndexingJob


class IndexingQueue(Protocol):
    name: str

    async def setup(self) -> None: ...

    async def publish(self, jobs: list[IndexingJob]) -> None: ...

    async def consume(self, consumer: str, count: int, block_ms: int) -> list[QueuedMessage]: ...

    async def ack(self, message_ids: list[str]) -> None: ...

    async def requeue(self, message: QueuedMessage, error: str, *, count_attempt: bool) -> None:
        """Ack ``message`` and publish its job again (optionally consuming an attempt)."""
        ...

    async def dead_letter(self, message: QueuedMessage, error: str) -> None: ...

    async def depth(self) -> int: ...

    async def dead_letter_depth(self) -> int: ...
