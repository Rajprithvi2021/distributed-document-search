"""Authentication and tenant resolution.

Three modes, selected by ``AUTH_MODE``:

* ``header``: the assignment's contract. The tenant comes from the
  ``X-Tenant-ID`` header or the ``tenant`` query parameter. It is trusted
  as-is, so this mode is rejected when ``ENVIRONMENT=production``.
* ``api_key``: ``X-API-Key`` is mapped to a tenant server-side. A caller can't
  choose another tenant; a conflicting ``X-Tenant-ID`` is a 403.
* ``jwt``: ``Authorization: Bearer <HS256 token>`` with ``sub``, ``tenant_id``,
  ``exp`` and a space-separated ``scope`` claim, as issued by an IdP.

Whatever the mode, the resolved tenant ID is validated against a strict
pattern. That makes it safe to embed in index names, cache keys and log fields
(no ``*``, ``,``, ``/`` or uppercase, which could otherwise turn into ES
wildcard or multi-index expressions).
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

import jwt

from app.config import Settings
from app.core.errors import (
    AuthenticationError,
    InvalidTenantError,
    MissingTenantError,
    PermissionDeniedError,
    TenantMismatchError,
)

TENANT_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")

SCOPE_DOCUMENTS_READ = "documents:read"
SCOPE_DOCUMENTS_WRITE = "documents:write"
SCOPE_SEARCH = "search:read"
ALL_SCOPES = frozenset({SCOPE_DOCUMENTS_READ, SCOPE_DOCUMENTS_WRITE, SCOPE_SEARCH})


@dataclass(frozen=True)
class Principal:
    """The authenticated caller and the tenant all its operations are scoped to."""

    tenant_id: str
    subject: str
    auth_method: str
    scopes: frozenset[str] = field(default=ALL_SCOPES)

    def has_scope(self, scope: str) -> bool:
        return "*" in self.scopes or scope in self.scopes


def validate_tenant_id(raw: str) -> str:
    if not isinstance(raw, str) or not TENANT_ID_PATTERN.fullmatch(raw):
        raise InvalidTenantError()
    return raw


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


class Authenticator:
    def __init__(self, settings: Settings) -> None:
        self._mode = settings.auth_mode
        self._jwt_secret = settings.jwt_secret
        self._jwt_algorithm = settings.jwt_algorithm
        self._jwt_audience = settings.jwt_audience
        self._jwt_issuer = settings.jwt_issuer
        # Keep only digests in memory and compare digests in constant time.
        self._api_keys: list[tuple[bytes, str]] = [
            (_digest(key), validate_tenant_id(tenant)) for key, tenant in settings.api_keys.items()
        ]

    @property
    def mode(self) -> str:
        return self._mode

    def authenticate(self, headers: Mapping[str, str], query: Mapping[str, str]) -> Principal:
        requested = self._requested_tenant(headers, query)
        if self._mode == "header":
            if requested is None:
                raise MissingTenantError()
            subject = headers.get("x-user-id") or "anonymous"
            return Principal(tenant_id=requested, subject=subject[:128], auth_method="header")
        if self._mode == "api_key":
            return self._authenticate_api_key(headers, requested)
        return self._authenticate_jwt(headers, requested)

    # -- helpers -------------------------------------------------------------
    @staticmethod
    def _requested_tenant(headers: Mapping[str, str], query: Mapping[str, str]) -> str | None:
        header_value = headers.get("x-tenant-id")
        query_value = query.get("tenant") or query.get("tenant_id")
        if header_value is not None and query_value is not None and header_value != query_value:
            raise TenantMismatchError()
        value = header_value if header_value is not None else query_value
        if value is None:
            return None
        return validate_tenant_id(value.strip())

    @staticmethod
    def _enforce_requested(requested: str | None, actual: str) -> None:
        if requested is not None and requested != actual:
            raise PermissionDeniedError(
                "Credentials are not valid for the requested tenant", details={"requested_tenant": requested}
            )

    def _authenticate_api_key(self, headers: Mapping[str, str], requested: str | None) -> Principal:
        key = headers.get("x-api-key")
        if not key:
            raise AuthenticationError("Missing X-API-Key header", headers={"WWW-Authenticate": "ApiKey"})
        candidate = _digest(key)
        tenant: str | None = None
        for stored, stored_tenant in self._api_keys:  # no early exit: constant work per request
            if hmac.compare_digest(candidate, stored):
                tenant = stored_tenant
        if tenant is None:
            raise AuthenticationError("Invalid API key", headers={"WWW-Authenticate": "ApiKey"})
        self._enforce_requested(requested, tenant)
        return Principal(tenant_id=tenant, subject=f"api-key:{candidate.hex()[:12]}", auth_method="api_key")

    def _authenticate_jwt(self, headers: Mapping[str, str], requested: str | None) -> Principal:
        authorization = headers.get("authorization", "")
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise AuthenticationError("Missing bearer token", headers={"WWW-Authenticate": "Bearer"})
        try:
            claims = jwt.decode(
                token,
                self._jwt_secret,
                algorithms=[self._jwt_algorithm],
                audience=self._jwt_audience,
                issuer=self._jwt_issuer,
                options={"require": ["exp", "sub", "tenant_id"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise AuthenticationError("Token has expired", headers={"WWW-Authenticate": "Bearer"}) from exc
        except jwt.InvalidTokenError as exc:
            raise AuthenticationError("Invalid token", headers={"WWW-Authenticate": "Bearer"}) from exc

        tenant = validate_tenant_id(str(claims["tenant_id"]))
        self._enforce_requested(requested, tenant)
        scopes = frozenset(str(claims.get("scope", "")).split())
        return Principal(tenant_id=tenant, subject=str(claims["sub"])[:128], auth_method="jwt", scopes=scopes)
