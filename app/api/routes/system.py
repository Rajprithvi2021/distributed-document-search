"""Operational endpoints: service info, health (detailed / liveness / readiness) and metrics."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response

from app.api.dependencies import get_container
from app.api.schemas import DependencyStatus, HealthResponse
from app.container import Container
from app.core.metrics import render_latest
from app.domain.models import utcnow

router = APIRouter(tags=["Operations"])


@router.get("/", summary="Service information")
async def root(container: Container = Depends(get_container)) -> dict[str, object]:
    settings = container.settings
    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment,
        "search_backend": container.raw_search_backend.name,
        "state_backend": settings.state_backend,
        "indexing_mode": settings.indexing_mode,
        "auth_mode": settings.auth_mode,
        "docs": "/docs" if settings.enable_docs else None,
    }


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check with dependency status",
    responses={503: {"model": HealthResponse, "description": "A critical dependency is down"}},
)
async def health(container: Container = Depends(get_container)) -> JSONResponse:
    report = await container.health.check()
    body = HealthResponse(
        status=report.status,
        version=container.settings.app_version,
        environment=container.settings.environment,
        timestamp=utcnow(),
        dependencies={
            dep.name: DependencyStatus(
                status=dep.status, critical=dep.critical, latency_ms=dep.latency_ms, details=dep.details
            )
            for dep in report.dependencies
        },
    )
    return JSONResponse(status_code=200 if report.ready else 503, content=body.model_dump(mode="json"))


@router.get("/health/live", summary="Liveness probe (process is up; no dependency checks)")
async def live() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/health/ready", summary="Readiness probe (critical dependencies reachable)")
async def ready(container: Container = Depends(get_container)) -> JSONResponse:
    report = await container.health.check()
    return JSONResponse(
        status_code=200 if report.ready else 503,
        content={"ready": report.ready, "status": report.status},
    )


@router.get("/metrics", summary="Prometheus metrics", response_class=Response)
async def metrics() -> Response:
    payload, content_type = render_latest()
    return Response(content=payload, media_type=content_type)
