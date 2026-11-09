import asyncio

import pytest

from app.core.errors import CircuitOpenError, DependencyUnavailableError
from app.core.resilience import BreakerState, CircuitBreaker, RetryPolicy, retry_async
from tests.fakes import FakeClock


class Dependency:
    def __init__(self) -> None:
        self.calls = 0
        self.failing = True

    async def __call__(self) -> str:
        self.calls += 1
        if self.failing:
            raise DependencyUnavailableError("down", dependency="dep")
        return "ok"


async def test_breaker_opens_after_threshold_and_fails_fast() -> None:
    clock, dep = FakeClock(), Dependency()
    breaker = CircuitBreaker("dep", failure_threshold=3, recovery_timeout_s=10, clock=clock)
    for _ in range(3):
        with pytest.raises(DependencyUnavailableError):
            await breaker.call(dep)
    assert breaker.state is BreakerState.OPEN

    with pytest.raises(CircuitOpenError) as excinfo:
        await breaker.call(dep)
    assert dep.calls == 3  # not called while open
    assert excinfo.value.headers["Retry-After"] == "10"


async def test_half_open_trial_closes_or_reopens() -> None:
    clock, dep = FakeClock(), Dependency()
    breaker = CircuitBreaker("dep", failure_threshold=1, recovery_timeout_s=5, clock=clock)
    with pytest.raises(DependencyUnavailableError):
        await breaker.call(dep)

    clock.advance(5)
    assert breaker.state is BreakerState.HALF_OPEN
    with pytest.raises(DependencyUnavailableError):
        await breaker.call(dep)  # trial fails -> open again
    assert breaker.state is BreakerState.OPEN

    clock.advance(5)
    dep.failing = False
    assert await breaker.call(dep) == "ok"
    assert breaker.state is BreakerState.CLOSED


async def test_half_open_admits_a_single_concurrent_trial() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker("dep", failure_threshold=1, recovery_timeout_s=1, clock=clock)
    with pytest.raises(DependencyUnavailableError):
        await breaker.call(Dependency())
    clock.advance(1)

    release = asyncio.Event()

    async def slow() -> str:
        await release.wait()
        return "ok"

    trial = asyncio.create_task(breaker.call(slow))
    await asyncio.sleep(0)
    with pytest.raises(CircuitOpenError):
        await breaker.call(slow)
    release.set()
    assert await trial == "ok"
    assert breaker.state is BreakerState.CLOSED


async def test_caller_errors_do_not_trip_the_breaker() -> None:
    breaker = CircuitBreaker("dep", failure_threshold=1)

    async def bad_request() -> None:
        raise ValueError("caller bug")

    for _ in range(3):
        with pytest.raises(ValueError):
            await breaker.call(bad_request)
    assert breaker.state is BreakerState.CLOSED


async def test_retry_recovers_from_transient_failures_with_bounded_jitter() -> None:
    attempts, delays = 0, []

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise DependencyUnavailableError("blip")
        return "ok"

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    result = await retry_async(flaky, policy=RetryPolicy(attempts=3, base_delay_s=0.1, max_delay_s=1), sleep=fake_sleep)
    assert result == "ok" and attempts == 3
    assert 0 <= delays[0] <= 0.1 and 0 <= delays[1] <= 0.2  # full jitter within exponential ceiling


async def test_retry_gives_up_and_never_retries_open_circuits_or_caller_errors() -> None:
    dep = Dependency()
    with pytest.raises(DependencyUnavailableError):
        await retry_async(dep, policy=RetryPolicy(attempts=3, base_delay_s=0), sleep=_no_sleep)
    assert dep.calls == 3

    calls = 0

    async def open_circuit() -> None:
        nonlocal calls
        calls += 1
        raise CircuitOpenError("open")

    with pytest.raises(CircuitOpenError):
        await retry_async(open_circuit, policy=RetryPolicy(attempts=5), sleep=_no_sleep)
    assert calls == 1

    async def caller_error() -> None:
        nonlocal calls
        calls += 1
        raise KeyError("x")

    with pytest.raises(KeyError):
        await retry_async(caller_error, policy=RetryPolicy(attempts=5), sleep=_no_sleep)
    assert calls == 2


async def test_retry_stops_when_the_deadline_is_spent() -> None:
    """Slow failures (timeouts) must not multiply into attempts x request timeout of latency."""
    clock = FakeClock(start=0.0)
    calls = 0

    async def slow_failure() -> None:
        nonlocal calls
        calls += 1
        clock.advance(0.6)  # e.g. a request that timed out
        raise DependencyUnavailableError("timeout")

    with pytest.raises(DependencyUnavailableError):
        await retry_async(
            slow_failure, policy=RetryPolicy(attempts=10, base_delay_s=0, deadline_s=1.0), sleep=_no_sleep, clock=clock
        )
    assert calls == 2


async def _no_sleep(_: float) -> None:
    return None
