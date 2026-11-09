"""Reject oversized request bodies before they are read into memory.

``Content-Length`` is checked up front. The gateway (nginx
``client_max_body_size``) enforces the same limit for chunked uploads, so the
API is protected both behind the gateway and when exposed directly.
"""

from __future__ import annotations

from starlette.types import ASGIApp, Receive, Scope, Send

from app.api.errors import error_response


class BodySizeLimitMiddleware:
    def __init__(self, app: ASGIApp, *, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope.get("method") in ("POST", "PUT", "PATCH"):
            raw = dict(scope.get("headers") or []).get(b"content-length")
            try:
                length = int(raw) if raw is not None else 0
            except ValueError:
                length = 0
            if length > self.max_bytes:
                response = error_response(
                    413,
                    "PAYLOAD_TOO_LARGE",
                    f"Request body exceeds {self.max_bytes} bytes",
                    details={"limit_bytes": self.max_bytes},
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
