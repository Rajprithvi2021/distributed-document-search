"""Rate limiter port and per-tenant policy.

Token bucket semantics: a bucket holds up to ``capacity`` tokens (the burst)
and refills continuously at ``refill_per_s`` (the sustained rate). Each request
spends one token. This allows short bursts while bounding long-run throughput,
and avoids the 2x burst a fixed-window counter allows at window boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from app.config import Settings


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    retry_after_s: float  # time until enough tokens exist for this request (0 if allowed)
    reset_after_s: float  # time until the bucket is full again


@dataclass(frozen=True)
class BucketConfig:
    capacity: int
    refill_per_s: float


class RateLimiter(Protocol):
    async def acquire(self, key: str, bucket: BucketConfig, cost: int = 1) -> RateLimitDecision: ...


class TenantRateLimitPolicy:
    """Default limits plus per-tenant overrides (e.g. a premium tier)."""

    def __init__(self, settings: Settings) -> None:
        self._default_rpm = settings.rate_limit_requests_per_minute
        self._default_burst = settings.rate_limit_burst
        self._overrides = dict(settings.rate_limit_overrides)

    def bucket_for(self, tenant_id: str) -> BucketConfig:
        rpm = self._overrides.get(tenant_id, self._default_rpm)
        # Overrides scale the burst proportionally to the sustained rate.
        capacity = max(1, round(self._default_burst * rpm / self._default_rpm))
        return BucketConfig(capacity=capacity, refill_per_s=rpm / 60.0)


def compute_decision(tokens: float, bucket: BucketConfig, cost: int, allowed: bool) -> RateLimitDecision:
    deficit = 0.0 if allowed else max(0.0, cost - tokens)
    return RateLimitDecision(
        allowed=allowed,
        limit=bucket.capacity,
        remaining=max(0, int(tokens)),
        retry_after_s=deficit / bucket.refill_per_s,
        reset_after_s=max(0.0, bucket.capacity - tokens) / bucket.refill_per_s,
    )
