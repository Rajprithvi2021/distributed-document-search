import fakeredis

from app.cache.layers import GuardedCache, TieredCache
from app.cache.memory import MemoryCache
from app.cache.redis import RedisCache
from app.cache.stores import DocumentCache, SearchResultCache
from app.core.resilience import BreakerState, CircuitBreaker
from app.domain.models import SearchPage, SearchQuery
from tests.conftest import fake_redis
from tests.fakes import FakeClock, make_document


async def test_memory_cache_ttl_lru_and_counters() -> None:
    clock = FakeClock()
    cache = MemoryCache(max_items=2, clock=clock)
    await cache.set("a", "1", ttl_s=10)
    await cache.set("b", "2", ttl_s=10)
    await cache.get("a")  # a becomes most recently used
    await cache.set("c", "3", ttl_s=10)
    assert await cache.get("b") is None and await cache.get("a") == "1"

    clock.advance(11)
    assert await cache.get("a") is None

    for _ in range(5):
        await cache.incr("gen")
    for i in range(10):
        await cache.set(f"k{i}", "x", ttl_s=10)
    assert await cache.get("gen") == "5"  # counters are never evicted

    assert await cache.add("nx", "first", ttl_s=10) is True
    assert await cache.add("nx", "second", ttl_s=10) is False
    assert await cache.get("nx") == "first"


async def test_tiered_cache_serves_l1_and_keeps_counters_consistent() -> None:
    clock = FakeClock()
    l2 = MemoryCache()
    tiered = TieredCache(MemoryCache(clock=clock), l2, l1_ttl_s=2)

    await l2.set("k", "v", ttl_s=60)
    assert await tiered.get("k") == "v"
    await l2.delete("k")
    assert await tiered.get("k") == "v"  # served from L1 ...
    clock.advance(2.1)
    assert await tiered.get("k") is None  # ... for at most l1_ttl_s

    await tiered.incr("gen")
    assert await tiered.get("gen") == "1"
    await tiered.incr("gen")  # must drop the L1 copy of the counter
    assert await tiered.get("gen") == "2"


async def test_guarded_cache_swallows_failures_and_trips_breaker() -> None:
    server = fakeredis.FakeServer()
    breaker = CircuitBreaker("redis", failure_threshold=2)
    cache = GuardedCache(RedisCache(fake_redis(server)), breaker)
    await cache.set("k", "v", ttl_s=10)
    assert await cache.get("k") == "v"

    server.connected = False
    assert await cache.get("k") is None
    await cache.set("k", "v2", ttl_s=10)  # no exception
    assert breaker.state is BreakerState.OPEN
    assert await cache.add("k", "v", ttl_s=10) is True  # fail open
    assert await cache.incr("gen") == 0


async def test_search_cache_generation_invalidation_and_staleness() -> None:
    clock = FakeClock()
    cache = SearchResultCache(MemoryCache(), fresh_ttl_s=60, stale_ttl_s=600, clock=clock)
    query = SearchQuery(text="redis")

    lookup = await cache.lookup("tenant-a", query)
    assert lookup.page is None
    await cache.store(lookup.key, SearchPage(total=1), "tenant-a")

    assert (await cache.lookup("tenant-a", query)).fresh
    clock.advance(61)
    stale = await cache.lookup("tenant-a", query)
    assert stale.page is not None and not stale.fresh

    await cache.invalidate_tenant("tenant-a")
    assert (await cache.lookup("tenant-a", query)).page is None
    assert (await cache.lookup("tenant-b", query)).page is None


async def test_pages_cached_inside_the_refresh_window_expire_when_it_closes() -> None:
    """Regression: with ES refresh=false a write is searchable only ~1 s after its ack. A page computed
    in that window must not stay fresh for the full 60 s TTL under the new generation."""
    clock = FakeClock()
    cache = SearchResultCache(MemoryCache(), fresh_ttl_s=60, stale_ttl_s=600, visibility_s=1.5, clock=clock)
    query = SearchQuery(text="redis")

    await cache.invalidate_tenant("tenant-a")  # the write was acknowledged
    clock.advance(0.2)
    lookup = await cache.lookup("tenant-a", query)
    await cache.store(lookup.key, SearchPage(total=0), "tenant-a")  # computed before the refresh
    assert (await cache.lookup("tenant-a", query)).fresh

    clock.advance(1.4)  # 1.6 s after the write: the refresh window has closed
    assert not (await cache.lookup("tenant-a", query)).fresh

    clock.advance(10)  # pages computed after the window get the normal TTL
    later = await cache.lookup("tenant-a", query)
    await cache.store(later.key, SearchPage(total=1), "tenant-a")
    clock.advance(30)
    assert (await cache.lookup("tenant-a", query)).fresh


def test_query_fingerprint_is_stable_and_sensitive() -> None:
    base = SearchQuery(text="Redis  Cache")
    assert base.fingerprint() == SearchQuery(text="redis cache").fingerprint()
    assert base.fingerprint() != SearchQuery(text="redis cache", page=2).fingerprint()
    assert base.fingerprint() != SearchQuery(text="redis cache", highlight=False).fingerprint()


async def test_document_cache_states_tombstones_and_idempotency() -> None:
    cache = DocumentCache(MemoryCache(), ttl_s=60)
    document = make_document("d1")
    await cache.put(document, state="pending")
    cached = await cache.get("tenant-a", "d1")
    assert cached is not None and cached.state == "pending"
    assert await cache.get("tenant-b", "d1") is None

    await cache.tombstone("tenant-a", "d1")
    assert await cache.is_tombstoned("tenant-a", "d1")
    assert not await cache.is_tombstoned("tenant-b", "d1")

    assert await cache.claim_idempotency_key("tenant-a", "key-1", "fp-1") is None
    in_progress = await cache.claim_idempotency_key("tenant-a", "key-1", "fp-1")
    assert in_progress is not None and in_progress.state == "in_progress"
    assert await cache.claim_idempotency_key("tenant-b", "key-1", "fp-1") is None  # keys are per tenant

    await cache.complete_idempotency_key("tenant-a", "key-1", "fp-1", document, "indexed")
    done = await cache.claim_idempotency_key("tenant-a", "key-1", "fp-1")
    assert done is not None and done.state == "done" and done.document == document and done.status == "indexed"
