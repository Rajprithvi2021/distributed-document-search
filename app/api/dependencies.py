"""FastAPI dependencies: container access, authentication, scopes, rate limiting.

Resolution order for tenant-scoped routes:
    authenticate (401/400) → rate limit per tenant (429) → scope check (403) → handler
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable

from fastapi import Depends, Request, Response

from app.container import Container
from app.core.errors import PermissionDeniedError, RateLimitExceededError
from app.core.logging import tenant_id_var
from app.core.metrics import RATE_LIMITED
from app.core.security import Principal


def get_container(request: Request) -> Container:
    return request.app.state.container


async def get_principal(request: Request, container: Container = Depends(get_container)) -> Principal:
    principal = container.authenticator.authenticate(request.headers, request.query_params)
    tenant_id_var.set(principal.tenant_id)
    request.state.tenant_id = principal.tenant_id  # read by the access-log middleware
    return principal


async def enforce_rate_limit(
    response: Response,
    principal: Principal = Depends(get_principal),
    container: Container = Depends(get_container),
) -> None:
    if not container.settings.rate_limit_enabled:
        return
    bucket = container.rate_limit_policy.bucket_for(principal.tenant_id)
    decision = await container.rate_limiter.acquire(f"tenant:{principal.tenant_id}", bucket)
    headers = {
        "X-RateLimit-Limit": str(decision.limit),
        "X-RateLimit-Remaining": str(decision.remaining),
        "X-RateLimit-Reset": str(math.ceil(decision.reset_after_s)),
    }
    if not decision.allowed:
        RATE_LIMITED.inc()
        raise RateLimitExceededError(
            f"Tenant rate limit exceeded ({bucket.capacity} burst, {round(bucket.refill_per_s * 60)}/min sustained)",
            headers={**headers, "Retry-After": str(max(1, math.ceil(decision.retry_after_s)))},
        )
    response.headers.update(headers)


def require_scope(scope: str) -> Callable[..., Awaitable[Principal]]:
    async def checker(principal: Principal = Depends(get_principal)) -> Principal:
        if not principal.has_scope(scope):
            raise PermissionDeniedError(f"Missing required scope '{scope}'", details={"required_scope": scope})
        return principal

    return checker
