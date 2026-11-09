"""Circuit breaker and retry-with-backoff primitives.

Typical composition: each retry *attempt* goes through the breaker, so failed
attempts are counted, and once the breaker opens the remaining retries stop
immediately (``CircuitOpenError`` is never retried)::

    await retry_async(lambda: breaker.call(fetch), policy=policy)
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar

from app.core.errors import CircuitOpenError, DependencyUnavailableError
from app.core.metrics import CIRCUIT_STATE

logger = logging.getLogger(__name__)
T = TypeVar("T")


class BreakerState(str, Enum):
    CLOSED = "closed"
    HALF_OPEN = "half_open"
    OPEN = "open"


_GAUGE_VALUE = {BreakerState.CLOSED: 0, BreakerState.HALF_OPEN: 1, BreakerState.OPEN: 2}


class CircuitBreaker:
    """Consecutive-failure circuit breaker.

    * CLOSED: calls pass through; ``failure_threshold`` consecutive dependency
      failures trip the breaker.
    * OPEN: calls fail fast with ``CircuitOpenError`` for ``recovery_timeout_s``.
    * HALF_OPEN: up to ``half_open_max_calls`` trial calls are let through; one
      success closes the circuit, one failure re-opens it.

    Only ``DependencyUnavailableError`` counts as a failure. Caller errors
    (validation, not-found) say nothing about the dependency's health.
    """

    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        recovery_timeout_s: float = 10.0,
        half_open_max_calls: int = 1,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_s = recovery_timeout_s
        self.half_open_max_calls = half_open_max_calls
        self._clock = clock
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._half_open_in_flight = 0
        CIRCUIT_STATE.labels(name=name).set(0)

    @property
    def state(self) -> BreakerState:
        if self._state is BreakerState.OPEN and self._clock() - self._opened_at >= self.recovery_timeout_s:
            return BreakerState.HALF_OPEN
        return self._state

    def retry_after_s(self) -> float:
        if self._state is not BreakerState.OPEN:
            return 0.0
        return max(0.0, self.recovery_timeout_s - (self._clock() - self._opened_at))

    async def call(self, fn: Callable[..., Awaitable[T]], *args: object, **kwargs: object) -> T:
        self._before_call()
        try:
            result = await fn(*args, **kwargs)
        except DependencyUnavailableError:
            self._on_failure()
            raise
        except BaseException:
            self._release_trial_slot()
            raise
        self._on_success()
        return result

    # -- state transitions -------------------------------------------------
    def _before_call(self) -> None:
        state = self.state
        if state is BreakerState.OPEN:
            raise CircuitOpenError(
                f"{self.name} circuit is open",
                dependency=self.name,
                headers={"Retry-After": str(max(1, round(self.retry_after_s())))},
            )
        if state is BreakerState.HALF_OPEN:
            if self._half_open_in_flight >= self.half_open_max_calls:
                raise CircuitOpenError(f"{self.name} circuit is half-open (trial in progress)", dependency=self.name)
            if self._state is not BreakerState.HALF_OPEN:
                self._set_state(BreakerState.HALF_OPEN)
            self._half_open_in_flight += 1

    def _on_success(self) -> None:
        self._release_trial_slot()
        self._failures = 0
        if self._state is not BreakerState.CLOSED:
            logger.info("circuit_closed", extra={"breaker": self.name})
            self._set_state(BreakerState.CLOSED)

    def _on_failure(self) -> None:
        if self._state is BreakerState.HALF_OPEN:
            self._half_open_in_flight = 0
            self._trip()
            return
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._trip()

    def _release_trial_slot(self) -> None:
        if self._half_open_in_flight:
            self._half_open_in_flight -= 1

    def _trip(self) -> None:
        self._opened_at = self._clock()
        self._failures = 0
        logger.warning("circuit_opened", extra={"breaker": self.name, "recovery_timeout_s": self.recovery_timeout_s})
        self._set_state(BreakerState.OPEN)

    def _set_state(self, state: BreakerState) -> None:
        self._state = state
        CIRCUIT_STATE.labels(name=self.name).set(_GAUGE_VALUE[state])


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    base_delay_s: float = 0.05
    max_delay_s: float = 1.0


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    operation: str = "call",
) -> T:
    """Retry transient failures with exponential backoff and full jitter.

    Full jitter (``uniform(0, base * 2**n)``) spreads retries from many clients
    so they don't hit a recovering dependency all at once.
    """
    attempt = 1
    while True:
        try:
            return await fn()
        except CircuitOpenError:
            raise
        except DependencyUnavailableError as exc:
            if attempt >= policy.attempts:
                raise
            ceiling = min(policy.max_delay_s, policy.base_delay_s * (2 ** (attempt - 1)))
            delay = random.uniform(0, ceiling)
            logger.info(
                "retrying_transient_failure",
                extra={"operation": operation, "attempt": attempt, "delay_s": round(delay, 4), "error": exc.message},
            )
            await sleep(delay)
            attempt += 1
