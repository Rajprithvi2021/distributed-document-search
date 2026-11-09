"""Application factory.

Run locally (no external services; in-memory adapters):
    uvicorn app.main:app --reload

Run against Elasticsearch + Redis: set SEARCH_BACKEND / STATE_BACKEND (see .env.example)
or use ``docker compose up``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.errors import register_exception_handlers
from app.api.routes import documents, search, system
from app.config import Settings, get_settings
from app.container import Container
from app.core.logging import configure_logging
from app.middleware.body_limit import BodySizeLimitMiddleware
from app.middleware.request_context import RequestContextMiddleware

logger = logging.getLogger(__name__)

DESCRIPTION = """
Multi-tenant full-text document search.

**Tenant identification** (`AUTH_MODE=header`, the default): send `X-Tenant-ID: <tenant>` or `?tenant=<tenant>`.
In `api_key` / `jwt` modes the tenant is derived from the credential.

**Rate limits**: per-tenant token bucket; see `X-RateLimit-*` response headers and `Retry-After` on 429.

**Errors**: every error is `{"error": {"code", "message", "request_id", "details"}}`.
"""


def create_app(settings: Settings | None = None, container: Container | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_json)
    container = container or Container(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await container.startup()
        try:
            yield
        finally:
            await container.shutdown()

    docs_enabled = settings.enable_docs and not settings.is_production
    app = FastAPI(
        title="Distributed Document Search",
        version=settings.app_version,
        description=DESCRIPTION,
        lifespan=lifespan,
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json" if docs_enabled else None,
    )
    app.state.container = container
    app.state.settings = settings

    register_exception_handlers(app)
    app.include_router(system.router)
    app.include_router(documents.router)
    app.include_router(search.router)

    # Middleware: the last one added runs first (outermost).
    if settings.cors_allow_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_allow_origins,
            allow_methods=["GET", "POST", "DELETE"],
            allow_headers=[
                "Authorization",
                "Content-Type",
                "X-Tenant-ID",
                "X-API-Key",
                "Idempotency-Key",
                "X-Request-ID",
            ],
            expose_headers=[
                "X-Request-ID",
                "X-RateLimit-Limit",
                "X-RateLimit-Remaining",
                "X-RateLimit-Reset",
                "Retry-After",
            ],
        )
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=settings.max_request_bytes)
    app.add_middleware(RequestContextMiddleware, hsts=settings.is_production)
    return app


app = create_app()
