import fakeredis
import pytest

from app.core.resilience import BreakerState, CircuitBreaker
from app.ratelimit.base import BucketConfig, TenantRateLimitPolicy
from app.ratelimit.memory import InMemoryTokenBucket
from app.ratelimit.redis import RedisTokenBucket
from tests.conftest import fake_redis, make_settings
from tests.fakes import FakeClock

BUCKET = BucketConfig(capacity=3, refill_per_s=1.0)


async def test_memory_bucket_allows_burst_then_refills() -> None:
    clock = FakeClock()
    limiter = InMemoryTokenBucket(clock=clock)
    decisions = [await limiter.acquire("t", BUCKET) for _ in range(4)]
    assert [d.allowed for d in decisions] == [True, True, True, False]
    assert [d.remaining for d in decisions[:3]] == [2, 1, 0]
    assert decisions[3].retry_after_s == pytest.approx(1.0)

    clock.advance(1.0)
    assert (await limiter.acquire("t", BUCKET)).allowed
    assert not (await limiter.acquire("t", BUCKET)).allowed


async def test_buckets_are_independent_per_key() -> None:
    limiter = InMemoryTokenBucket(clock=FakeClock())
    for _ in range(3):
        await limiter.acquire("tenant-a", BUCKET)
    assert not (await limiter.acquire("tenant-a", BUCKET)).allowed
    assert (await limiter.acquire("tenant-b", BUCKET)).allowed


def test_policy_applies_per_tenant_overrides() -> None:
    policy = TenantRateLimitPolicy(
        make_settings(rate_limit_requests_per_minute=600, rate_limit_burst=100, rate_limit_overrides={"premium": 6000})
    )
    assert policy.bucket_for("basic") == BucketConfig(capacity=100, refill_per_s=10.0)
    assert policy.bucket_for("premium") == BucketConfig(capacity=1000, refill_per_s=100.0)


async def test_redis_lua_bucket_is_shared_across_replicas() -> None:
    server = fakeredis.FakeServer()
    now = [1_000_000]
    breaker = CircuitBreaker("redis")
    replica_1 = RedisTokenBucket(fake_redis(server), breaker=breaker, clock_ms=lambda: now[0])
    replica_2 = RedisTokenBucket(fake_redis(server), breaker=breaker, clock_ms=lambda: now[0])

    results = [
        await replica_1.acquire("t", BUCKET),
        await replica_2.acquire("t", BUCKET),
        await replica_1.acquire("t", BUCKET),
        await replica_2.acquire("t", BUCKET),
    ]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert results[3].retry_after_s == pytest.approx(1.0)

    now[0] += 1500  # 1.5 s -> 1.5 tokens
    assert (await replica_1.acquire("t", BUCKET)).allowed
    assert not (await replica_2.acquire("t", BUCKET)).allowed


async def test_redis_outage_falls_back_to_local_bucket_and_opens_circuit() -> None:
    server = fakeredis.FakeServer()
    breaker = CircuitBreaker("redis", failure_threshold=2)
    limiter = RedisTokenBucket(fake_redis(server), breaker=breaker, fallback=InMemoryTokenBucket(clock=FakeClock()))
    server.connected = False

    decisions = [await limiter.acquire("t", BUCKET) for _ in range(4)]
    assert [d.allowed for d in decisions] == [True, True, True, False]  # still limited, locally
    assert breaker.state is BreakerState.OPEN
