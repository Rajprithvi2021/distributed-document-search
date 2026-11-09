import time

import jwt
import pytest
from pydantic import ValidationError

from app.config import Settings
from app.core.errors import (
    AuthenticationError,
    InvalidTenantError,
    MissingTenantError,
    PermissionDeniedError,
    TenantMismatchError,
)
from app.core.security import Authenticator
from tests.conftest import make_settings

SECRET = "s" * 40


def token(**claims) -> str:
    payload = {
        "sub": "user-1",
        "tenant_id": "tenant-a",
        "aud": "document-search",
        "exp": int(time.time()) + 60,
        "scope": "search:read",
    } | claims
    return jwt.encode(payload, SECRET, algorithm="HS256")


# --- header mode -----------------------------------------------------------------
def test_header_mode_resolves_tenant_from_header_or_query() -> None:
    auth = Authenticator(make_settings())
    assert auth.authenticate({"x-tenant-id": "tenant-a"}, {}).tenant_id == "tenant-a"
    assert auth.authenticate({}, {"tenant": "tenant-b"}).tenant_id == "tenant-b"
    assert auth.authenticate({}, {"tenant_id": "tenant-c"}).tenant_id == "tenant-c"


@pytest.mark.parametrize("bad", ["Tenant-A", "*", "a,b", "../etc", "-leading", "a" * 64, "", "tenant a"])
def test_header_mode_rejects_unsafe_tenant_ids(bad: str) -> None:
    with pytest.raises(InvalidTenantError):
        Authenticator(make_settings()).authenticate({"x-tenant-id": bad}, {})


def test_header_mode_missing_and_conflicting_tenant() -> None:
    auth = Authenticator(make_settings())
    with pytest.raises(MissingTenantError):
        auth.authenticate({}, {})
    with pytest.raises(TenantMismatchError):
        auth.authenticate({"x-tenant-id": "tenant-a"}, {"tenant": "tenant-b"})


# --- api key mode ------------------------------------------------------------------
def test_api_key_mode_derives_tenant_from_key() -> None:
    auth = Authenticator(make_settings(auth_mode="api_key", api_keys={"key-a": "tenant-a", "key-b": "tenant-b"}))
    principal = auth.authenticate({"x-api-key": "key-b"}, {})
    assert principal.tenant_id == "tenant-b" and principal.auth_method == "api_key"
    assert "key-b" not in principal.subject  # never log raw keys

    with pytest.raises(AuthenticationError):
        auth.authenticate({}, {})
    with pytest.raises(AuthenticationError):
        auth.authenticate({"x-api-key": "wrong"}, {})
    with pytest.raises(PermissionDeniedError):  # header can't widen access
        auth.authenticate({"x-api-key": "key-a", "x-tenant-id": "tenant-b"}, {})
    assert auth.authenticate({"x-api-key": "key-a", "x-tenant-id": "tenant-a"}, {}).tenant_id == "tenant-a"


# --- jwt mode ------------------------------------------------------------------------
def jwt_auth() -> Authenticator:
    return Authenticator(make_settings(auth_mode="jwt", jwt_secret=SECRET))


def test_jwt_mode_reads_tenant_and_scopes() -> None:
    principal = jwt_auth().authenticate({"authorization": f"Bearer {token()}"}, {})
    assert principal.tenant_id == "tenant-a" and principal.subject == "user-1"
    assert principal.has_scope("search:read") and not principal.has_scope("documents:write")


@pytest.mark.parametrize(
    "bad_token",
    [
        token(exp=int(time.time()) - 10),
        jwt.encode(
            {"sub": "u", "tenant_id": "tenant-a", "aud": "document-search", "exp": int(time.time()) + 60},
            "x" * 40,
            algorithm="HS256",
        ),
        token(aud="someone-else"),
        jwt.encode({"sub": "u", "aud": "document-search", "exp": int(time.time()) + 60}, SECRET, algorithm="HS256"),
        "not-a-jwt",
    ],
    ids=["expired", "wrong-secret", "wrong-audience", "missing-tenant-claim", "garbage"],
)
def test_jwt_mode_rejects_invalid_tokens(bad_token: str) -> None:
    with pytest.raises(AuthenticationError):
        jwt_auth().authenticate({"authorization": f"Bearer {bad_token}"}, {})


def test_jwt_mode_requires_bearer_and_matching_tenant() -> None:
    with pytest.raises(AuthenticationError):
        jwt_auth().authenticate({}, {})
    with pytest.raises(PermissionDeniedError):
        jwt_auth().authenticate({"authorization": f"Bearer {token()}"}, {"tenant": "tenant-b"})
    with pytest.raises(InvalidTenantError):
        jwt_auth().authenticate({"authorization": f"Bearer {token(tenant_id='BAD*')}"}, {})


# --- configuration validation ---------------------------------------------------------
@pytest.mark.parametrize(
    "overrides",
    [
        {"environment": "production", "auth_mode": "header"},
        {"environment": "production", "auth_mode": "api_key", "api_keys": {"k": "t"}, "cors_allow_origins": ["*"]},
        {"auth_mode": "jwt"},
        {"auth_mode": "jwt", "jwt_secret": "too-short"},
        {"auth_mode": "api_key"},
        {"cache_search_ttl_s": 100, "cache_search_stale_ttl_s": 10},
    ],
)
def test_unsafe_or_inconsistent_configuration_is_rejected(overrides: dict) -> None:
    with pytest.raises(ValidationError):
        make_settings(**overrides)


def test_queue_connection_timeout_covers_the_blocking_read() -> None:
    """Regression: the worker's blocking XREADGROUP must fit inside the queue client's socket timeout."""
    from app.container import Container

    settings = make_settings(state_backend="redis", redis_socket_timeout_s=0.5, indexing_block_ms=3000)
    container = Container(settings)
    queue_timeout = container.queue_redis.connection_pool.connection_kwargs["socket_timeout"]
    cache_timeout = container.redis.connection_pool.connection_kwargs["socket_timeout"]
    assert queue_timeout > settings.indexing_block_ms / 1000
    assert cache_timeout == 0.5  # cache calls keep failing fast


def test_complex_settings_parse_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RATE_LIMIT_OVERRIDES", '{"premium": 6000}')
    monkeypatch.setenv("SEARCH_BACKEND", "elasticsearch")
    settings = Settings(_env_file=None)
    assert settings.rate_limit_overrides == {"premium": 6000}
    assert settings.search_backend == "elasticsearch"
