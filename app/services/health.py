"""Aggregated dependency health.

* ``healthy``: every dependency is up.
* ``degraded``: a non-critical dependency (cache / rate-limit store / queue)
  is down. The service still answers correctly, just slower or less precisely.
* ``unhealthy``: a critical dependency (the search engine) is down. Readiness
  fails so the load balancer stops routing traffic to this instance.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal

from app.domain.models import DependencyHealth, utcnow

HealthCheck = Callable[[], Awaitable[DependencyHealth]]
OverallStatus = Literal["healthy", "degraded", "unhealthy"]


@dataclass
class HealthReport:
    status: OverallStatus
    dependencies: list[DependencyHealth]
    checked_at: object = field(default_factory=utcnow)

    @property
    def ready(self) -> bool:
        return self.status != "unhealthy"


class HealthService:
    def __init__(self, checks: dict[str, tuple[HealthCheck, bool]], *, timeout_s: float = 2.0) -> None:
        self._checks = checks  # name -> (check, critical)
        self._timeout_s = timeout_s

    async def _run(self, name: str, check: HealthCheck, critical: bool) -> DependencyHealth:
        started = time.perf_counter()
        try:
            result = await asyncio.wait_for(check(), timeout=self._timeout_s)
            result.name, result.critical = name, critical
            return result
        except Exception as exc:
            return DependencyHealth(
                name=name,
                status="down",
                critical=critical,
                latency_ms=round((time.perf_counter() - started) * 1000, 2),
                details={"error": type(exc).__name__},
            )

    async def check(self) -> HealthReport:
        results = await asyncio.gather(
            *(self._run(name, check, critical) for name, (check, critical) in self._checks.items())
        )
        if any(r.critical and r.status == "down" for r in results):
            status: OverallStatus = "unhealthy"
        elif any(r.status != "up" for r in results):
            status = "degraded"
        else:
            status = "healthy"
        return HealthReport(status=status, dependencies=list(results))
