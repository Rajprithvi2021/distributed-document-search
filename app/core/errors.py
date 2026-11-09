"""Domain error hierarchy.

Every error the service raises intentionally is an ``AppError`` subclass with a
stable machine-readable ``code`` and an HTTP status. The API layer renders all
of them in one envelope (see ``app.api.errors``), so clients see a single,
predictable error contract.
"""

from __future__ import annotations

from typing import Any


class AppError(Exception):
    status_code: int = 500
    code: str = "INTERNAL_ERROR"
    default_message: str = "An internal error occurred"

    def __init__(
        self,
        message: str | None = None,
        *,
        details: Any = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.message = message or self.default_message
        self.details = details
        self.headers = headers or {}
        super().__init__(self.message)


class ValidationFailedError(AppError):
    status_code = 422
    code = "VALIDATION_ERROR"
    default_message = "Request validation failed"


class BadRequestError(AppError):
    status_code = 400
    code = "BAD_REQUEST"
    default_message = "Bad request"


class MissingTenantError(AppError):
    status_code = 400
    code = "MISSING_TENANT"
    default_message = "Tenant is required: send the X-Tenant-ID header or the 'tenant' query parameter"


class InvalidTenantError(AppError):
    status_code = 400
    code = "INVALID_TENANT"
    default_message = "Tenant ID must match ^[a-z0-9][a-z0-9_-]{0,62}$"


class TenantMismatchError(AppError):
    status_code = 400
    code = "TENANT_MISMATCH"
    default_message = "X-Tenant-ID header and 'tenant' query parameter disagree"


class AuthenticationError(AppError):
    status_code = 401
    code = "UNAUTHENTICATED"
    default_message = "Authentication required"


class PermissionDeniedError(AppError):
    status_code = 403
    code = "FORBIDDEN"
    default_message = "You do not have permission to perform this action"


class DocumentNotFoundError(AppError):
    status_code = 404
    code = "DOCUMENT_NOT_FOUND"
    default_message = "Document not found"


class ConflictError(AppError):
    status_code = 409
    code = "CONFLICT"
    default_message = "The request conflicts with the current state"


class IdempotencyKeyReusedError(AppError):
    status_code = 422
    code = "IDEMPOTENCY_KEY_REUSED"
    default_message = "This Idempotency-Key was already used with a different request body"


class PayloadTooLargeError(AppError):
    status_code = 413
    code = "PAYLOAD_TOO_LARGE"
    default_message = "Request payload is too large"


class RateLimitExceededError(AppError):
    status_code = 429
    code = "RATE_LIMIT_EXCEEDED"
    default_message = "Rate limit exceeded for this tenant"


class DependencyUnavailableError(AppError):
    """A backing service (search engine, cache, queue) is unreachable or overloaded.

    This is the *transient* error class: retries and circuit breakers act on it.
    """

    status_code = 503
    code = "SERVICE_UNAVAILABLE"
    default_message = "A backing service is temporarily unavailable"

    def __init__(self, message: str | None = None, *, dependency: str = "unknown", **kwargs: Any) -> None:
        self.dependency = dependency
        super().__init__(message, **kwargs)


class CircuitOpenError(DependencyUnavailableError):
    """Raised without calling the dependency because its circuit breaker is open."""

    default_message = "Service temporarily unavailable (circuit open); retry shortly"
