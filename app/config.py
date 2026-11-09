"""Typed application settings.

Every value can be overridden with an environment variable of the same name
(case-insensitive), e.g. ``SEARCH_BACKEND=elasticsearch``. Complex values
(dicts/lists) are given as JSON, e.g. ``API_KEYS='{"key-a": "tenant-a"}'``.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Application -------------------------------------------------------
    app_name: str = "distributed-document-search"
    app_version: str = __version__
    environment: Literal["development", "test", "staging", "production"] = "development"
    log_level: str = "INFO"
    log_json: bool = True
    enable_docs: bool = True

    # --- Backend selection (explicit, never a silent fallback) --------------
    search_backend: Literal["elasticsearch", "memory"] = "memory"
    state_backend: Literal["redis", "memory"] = "memory"  # cache, rate limits, queue

    # --- Elasticsearch -------------------------------------------------------
    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_username: str | None = None
    elasticsearch_password: str | None = None
    elasticsearch_request_timeout_s: float = 5.0
    elasticsearch_index_prefix: str = Field(default="docs", pattern=r"^[a-z][a-z0-9-]{0,20}$")
    elasticsearch_tenancy: Literal["index_per_tenant", "shared_index"] = "index_per_tenant"
    elasticsearch_shards: int = Field(default=1, ge=1)
    elasticsearch_replicas: int = Field(default=0, ge=0)
    # "false" = eventual (≈1s refresh), "wait_for" = read-your-writes for search.
    elasticsearch_refresh: Literal["false", "wait_for", "true"] = "false"

    # --- Redis ---------------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"
    redis_socket_timeout_s: float = 0.5
    redis_max_connections: int = 200

    # --- Caching -------------------------------------------------------------
    cache_search_ttl_s: int = Field(default=60, ge=1)  # served as "fresh"
    cache_search_stale_ttl_s: int = Field(default=600, ge=1)  # kept for stale-if-error
    cache_document_ttl_s: int = Field(default=300, ge=1)
    cache_local_ttl_s: float = Field(default=2.0, ge=0)  # L1 in-process; 0 disables
    cache_local_max_items: int = Field(default=10_000, ge=1)

    # --- Rate limiting (token bucket per tenant) -----------------------------
    rate_limit_enabled: bool = True
    rate_limit_requests_per_minute: int = Field(default=600, ge=1)  # sustained rate
    rate_limit_burst: int = Field(default=100, ge=1)  # bucket capacity
    rate_limit_overrides: dict[str, int] = Field(default_factory=dict)  # tenant -> rpm

    # --- Indexing ------------------------------------------------------------
    indexing_mode: Literal["sync", "async"] = "sync"
    indexing_stream: str = "indexing:jobs"
    indexing_dlq_stream: str = "indexing:dlq"
    indexing_consumer_group: str = "indexers"
    indexing_batch_size: int = Field(default=100, ge=1, le=1000)
    indexing_max_attempts: int = Field(default=5, ge=1)
    indexing_claim_idle_ms: int = Field(default=60_000, ge=1000)
    indexing_block_ms: int = Field(default=1000, ge=0, le=10_000)  # worker's blocking wait for new jobs
    run_embedded_worker: bool = True  # run the indexer inside the API process (dev only)

    # --- Resilience ----------------------------------------------------------
    breaker_failure_threshold: int = Field(default=5, ge=1)
    breaker_recovery_timeout_s: float = Field(default=10.0, gt=0)
    retry_attempts: int = Field(default=3, ge=1)
    retry_base_delay_s: float = Field(default=0.05, ge=0)
    retry_max_delay_s: float = Field(default=1.0, ge=0)

    # --- Security ------------------------------------------------------------
    auth_mode: Literal["header", "api_key", "jwt"] = "header"
    api_keys: dict[str, str] = Field(default_factory=dict)  # api key -> tenant id
    jwt_secret: str | None = None
    jwt_algorithm: Literal["HS256", "HS384", "HS512"] = "HS256"
    jwt_audience: str | None = "document-search"
    jwt_issuer: str | None = None
    cors_allow_origins: list[str] = Field(default_factory=list)
    max_request_bytes: int = Field(default=10 * 1024 * 1024, ge=1024)
    max_document_bytes: int = Field(default=1024 * 1024, ge=1)

    # --- Search --------------------------------------------------------------
    search_max_result_window: int = Field(default=10_000, ge=1)

    @model_validator(mode="after")
    def _validate(self) -> Settings:
        if self.auth_mode == "jwt" and not self.jwt_secret:
            raise ValueError("AUTH_MODE=jwt requires JWT_SECRET")
        if self.auth_mode == "jwt" and self.jwt_secret and len(self.jwt_secret) < 32:
            raise ValueError("JWT_SECRET must be at least 32 characters")
        if self.auth_mode == "api_key" and not self.api_keys:
            raise ValueError("AUTH_MODE=api_key requires API_KEYS")
        if self.environment == "production":
            if self.auth_mode == "header":
                raise ValueError("AUTH_MODE=header trusts a caller-supplied tenant and is not allowed in production")
            if "*" in self.cors_allow_origins:
                raise ValueError("Wildcard CORS origins are not allowed in production")
        if self.cache_search_stale_ttl_s < self.cache_search_ttl_s:
            raise ValueError("CACHE_SEARCH_STALE_TTL_S must be >= CACHE_SEARCH_TTL_S")
        return self

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


@lru_cache
def get_settings() -> Settings:
    return Settings()
