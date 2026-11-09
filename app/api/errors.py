"""Uniform error rendering.

Every error, whether it's ours, a validation failure, a routing 404/405 or an
unexpected exception (handled in the request-context middleware), is returned as::

    {"error": {"code": "...", "message": "...", "request_id": "...", "details": ...}}

Internal exception text is never sent to clients; it's logged with the request ID instead.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.errors import AppError
from app.core.logging import request_id_var

logger = logging.getLogger(__name__)

_HTTP_CODES = {
    400: "BAD_REQUEST",
    401: "UNAUTHENTICATED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    413: "PAYLOAD_TOO_LARGE",
    415: "UNSUPPORTED_MEDIA_TYPE",
    429: "RATE_LIMIT_EXCEEDED",
}


def error_payload(code: str, message: str, details: Any = None) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "request_id": request_id_var.get(), "details": details}}


def error_response(
    status_code: int, code: str, message: str, details: Any = None, headers: dict[str, str] | None = None
) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=error_payload(code, message, details), headers=headers)


async def _app_error(_: Request, exc: AppError) -> JSONResponse:
    log = logger.warning if exc.status_code >= 500 else logger.info
    log("request_failed", extra={"error_code": exc.code, "status": exc.status_code, "error": exc.message})
    return error_response(exc.status_code, exc.code, exc.message, exc.details, exc.headers)


async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = [
        {
            "location": ".".join(str(part) for part in err.get("loc", ())),
            "message": err.get("msg"),
            "type": err.get("type"),
        }
        for err in exc.errors()
    ]
    return error_response(422, "VALIDATION_ERROR", "Request validation failed", details)


async def _http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    code = _HTTP_CODES.get(exc.status_code, "HTTP_ERROR")
    message = exc.detail if isinstance(exc.detail, str) else code.replace("_", " ").capitalize()
    return error_response(exc.status_code, code, message, headers=getattr(exc, "headers", None))


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppError, _app_error)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, _validation_error)  # type: ignore[arg-type]
    app.add_exception_handler(StarletteHTTPException, _http_error)  # type: ignore[arg-type]
