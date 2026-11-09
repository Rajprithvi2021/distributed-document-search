"""Outermost application middleware.

* Accepts a well-formed inbound ``X-Request-ID`` (e.g. from the gateway) or
  generates one, exposes it to logs via a contextvar and echoes it in the response.
* Records RED metrics per route *template* and writes one structured access log line.
* Adds security headers (and HSTS in production).
* Last line of defence: an unhandled exception becomes a clean 500 envelope
  carrying the request ID, never a stack trace.
"""

from __future__ import annotations

import logging
import re
import time
import uuid

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.api.errors import error_response
from app.core.logging import request_id_var, tenant_id_var
from app.core.metrics import HTTP_LATENCY, HTTP_REQUESTS

logger = logging.getLogger("app.access")
_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{8,128}$")
_UNMATCHED = "unmatched"


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp, *, hsts: bool = False) -> None:
        self.app = app
        self.security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
            "Cache-Control": "no-store",
        }
        if hsts:
            self.security_headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        inbound = dict(scope.get("headers") or []).get(b"x-request-id", b"").decode("latin-1")
        request_id = inbound if _REQUEST_ID.fullmatch(inbound) else uuid.uuid4().hex
        rid_token = request_id_var.set(request_id)
        tenant_token = tenant_id_var.set(None)
        started = time.perf_counter()
        status_code = 500
        response_started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                headers = MutableHeaders(scope=message)
                headers["X-Request-ID"] = request_id
                headers["X-Response-Time-Ms"] = f"{(time.perf_counter() - started) * 1000:.2f}"
                for name, value in self.security_headers.items():
                    headers.setdefault(name, value)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            logger.exception("unhandled_exception", extra={"path": scope.get("path")})
            if response_started:
                raise
            response = error_response(500, "INTERNAL_ERROR", "An internal error occurred")
            await response(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - started
            route = scope.get("route")
            route_path = getattr(route, "path", _UNMATCHED)
            method = scope.get("method", "")
            HTTP_REQUESTS.labels(method=method, route=route_path, status=str(status_code)).inc()
            HTTP_LATENCY.labels(method=method, route=route_path).observe(duration)
            if route_path not in ("/metrics", "/health/live"):
                tenant = (scope.get("state") or {}).get("tenant_id")
                logger.info(
                    "request",
                    extra={
                        "method": method,
                        "path": scope.get("path"),
                        "route": route_path,
                        "status": status_code,
                        "duration_ms": round(duration * 1000, 2),
                        **({"tenant_id": tenant} if tenant else {}),
                    },
                )
            tenant_id_var.reset(tenant_token)
            request_id_var.reset(rid_token)
