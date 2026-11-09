"""Structured logging with request/tenant correlation.

``request_id`` and ``tenant_id`` live in context variables, so every log line
written while handling a request is tagged with them automatically, with no
need to thread them through function signatures.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from datetime import UTC, datetime

request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)
tenant_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("tenant_id", default=None)

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}
_HANDLER_MARK = "_docsearch_handler"


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get() or "-"
        record.tenant_id = tenant_id_var.get() or "-"
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            if key in ("request_id", "tenant_id") and value == "-":
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", json_logs: bool = True) -> None:
    """Install one stdout handler on the root logger (idempotent)."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _HANDLER_MARK, False):
            root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    setattr(handler, _HANDLER_MARK, True)
    handler.addFilter(ContextFilter())
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-7s %(name)s [req=%(request_id)s tenant=%(tenant_id)s] %(message)s"
            )
        )
    root.addHandler(handler)
    root.setLevel(level.upper())

    # Third-party loggers that are noisy at INFO.
    for noisy in ("uvicorn.access", "elastic_transport.transport", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
