"""Redis Streams adapter: durable, consumer-group based job queue.

* ``XADD`` publishes. The stream is capped (approximate ``MAXLEN``) so it can't
  grow without bound.
* ``XREADGROUP`` gives each job to exactly one worker in the group, and a job
  stays in the Pending Entries List until ``XACK``.
* ``XAUTOCLAIM`` lets a live worker take over jobs that a crashed worker left
  pending for longer than ``claim_idle_ms``. This is what makes delivery
  at-least-once.
* Poison messages go to a separate dead-letter stream for inspection or replay.

For larger scale the same port maps onto Kafka (partitions keyed by tenant)
or SQS (visibility timeout + redrive policy). See SUBMISSION.md §1.7.
"""

from __future__ import annotations

from datetime import UTC, datetime

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from app.cache.redis import redis_unavailable
from app.queue.base import IndexingJob, QueuedMessage


class RedisStreamQueue:
    name = "redis-streams"

    def __init__(
        self,
        client: Redis,
        *,
        stream: str,
        dead_letter_stream: str,
        group: str,
        claim_idle_ms: int = 60_000,
        max_length: int = 1_000_000,
    ) -> None:
        self._client = client
        self._stream = stream
        self._dlq = dead_letter_stream
        self._group = group
        self._claim_idle_ms = claim_idle_ms
        self._max_length = max_length

    async def setup(self) -> None:
        try:
            await self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):  # group already exists: fine
                raise redis_unavailable(exc) from exc
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def publish(self, jobs: list[IndexingJob]) -> None:
        if not jobs:
            return
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                for job in jobs:
                    pipe.xadd(self._stream, {"job": job.model_dump_json()}, maxlen=self._max_length, approximate=True)
                await pipe.execute()
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    @staticmethod
    def _parse(entries: list) -> list[QueuedMessage]:
        messages = []
        for message_id, fields in entries:
            if not fields:  # entry trimmed or deleted while pending
                continue
            messages.append(QueuedMessage(message_id=message_id, job=IndexingJob.model_validate_json(fields["job"])))
        return messages

    async def consume(self, consumer: str, count: int, block_ms: int) -> list[QueuedMessage]:
        try:
            # 1) Take over jobs abandoned by crashed consumers.
            claimed = await self._client.xautoclaim(
                self._stream, self._group, consumer, min_idle_time=self._claim_idle_ms, start_id="0-0", count=count
            )
            reclaimed = self._parse(claimed[1]) if claimed and len(claimed) > 1 else []
            if reclaimed:
                return reclaimed
            # 2) Read new jobs.
            response = await self._client.xreadgroup(
                self._group, consumer, {self._stream: ">"}, count=count, block=block_ms or None
            )
        except ResponseError as exc:
            if "NOGROUP" in str(exc):  # Redis was not ready at start-up, or the stream was deleted
                await self.setup()
                return []
            raise redis_unavailable(exc) from exc
        except RedisError as exc:
            raise redis_unavailable(exc) from exc
        messages: list[QueuedMessage] = []
        for _stream, entries in response or []:
            messages.extend(self._parse(entries))
        return messages

    async def ack(self, message_ids: list[str]) -> None:
        if not message_ids:
            return
        try:
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.xack(self._stream, self._group, *message_ids)
                pipe.xdel(self._stream, *message_ids)
                await pipe.execute()
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def requeue(self, message: QueuedMessage, error: str, *, count_attempt: bool) -> None:
        attempt = message.job.attempt + 1 if count_attempt else message.job.attempt
        job = message.job.model_copy(update={"attempt": attempt, "last_error": error})
        try:
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.xadd(self._stream, {"job": job.model_dump_json()}, maxlen=self._max_length, approximate=True)
                pipe.xack(self._stream, self._group, message.message_id)
                pipe.xdel(self._stream, message.message_id)
                await pipe.execute()
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def dead_letter(self, message: QueuedMessage, error: str) -> None:
        record = {
            "job": message.job.model_dump_json(),
            "error": error[:2000],
            "failed_at": datetime.now(UTC).isoformat(),
            "source_id": message.message_id,
        }
        try:
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.xadd(self._dlq, record, maxlen=self._max_length, approximate=True)
                pipe.xack(self._stream, self._group, message.message_id)
                pipe.xdel(self._stream, message.message_id)
                await pipe.execute()
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def depth(self) -> int:
        try:
            return int(await self._client.xlen(self._stream))
        except RedisError as exc:
            raise redis_unavailable(exc) from exc

    async def dead_letter_depth(self) -> int:
        try:
            return int(await self._client.xlen(self._dlq))
        except RedisError as exc:
            raise redis_unavailable(exc) from exc
