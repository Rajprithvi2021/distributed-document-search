"""
Configuration management module.
Handles environment variables and application settings with proper validation.
"""

from pydantic_settings import BaseSettings
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Application
    app_name: str = "distributed-document-search"
    app_version: str = "1.0.0"
    app_env: str = "development"
    debug: bool = False
    log_level: str = "INFO"
    
    def __init__(self, **data):
        # Handle DEBUG environment variable which might be set to non-boolean values
        import os
        debug_env = os.getenv('DEBUG', 'false').lower()
        if debug_env in ('true', '1', 'yes', 'on'):
            data['debug'] = True
        elif debug_env in ('false', '0', 'no', 'off', 'release'):
            data['debug'] = False
        super().__init__(**data)
    
    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 4
    
    # Elasticsearch
    elasticsearch_hosts: str = "http://localhost:9200"
    elasticsearch_username: str = "elastic"
    elasticsearch_password: str = "changeme"
    elasticsearch_timeout: int = 1
    elasticsearch_retries: int = 3
    
    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_cache_ttl: int = 3600  # 1 hour
    redis_max_connections: int = 50
    
    # Rate Limiting
    rate_limit_enabled: bool = True
    rate_limit_default: int = 1000
    rate_limit_period: int = 60
    
    # Security
    secret_key: str = "your-super-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expiration_hours: int = 24
    
    # Multi-tenancy
    tenant_isolation_mode: str = "row_level"  # or 'index_per_tenant'
    enable_tenant_validation: bool = True
    
    # Monitoring
    prometheus_enabled: bool = True
    opentelemetry_enabled: bool = False
    jaeger_agent_host: str = "localhost"
    jaeger_agent_port: int = 6831
    
    # Feature Flags
    enable_fuzzy_search: bool = True
    enable_faceted_search: bool = True
    enable_highlighting: bool = True
    enable_async_indexing: bool = True
    
    # Performance
    search_page_size_default: int = 20
    search_page_size_max: int = 100
    index_batch_size: int = 1000
    index_bulk_timeout: int = 30
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
