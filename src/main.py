"""
Main FastAPI application entry point.
Initializes the application with all middleware, routes, and configuration.
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from src.config import settings
from src.api import router
from src.middleware import (
    TenantValidationMiddleware, 
    RateLimitingMiddleware,
    RequestLoggingMiddleware
)
from src.utils import setup_logging, logger, es_client, redis_client

# Setup logging
setup_logging(log_level=settings.log_level, json_format=False)
logger_instance = logging.getLogger(__name__)


# ============================================================================
# Lifespan Management
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle.
    Handles startup and shutdown operations.
    """
    # Startup
    logger_instance.info(f"Starting {settings.app_name} v{settings.app_version}")
    
    try:
        # Initialize Elasticsearch client
        logger_instance.info("Elasticsearch client ready")
        logger_instance.info("Redis client ready")
        
    except Exception as e:
        logger_instance.error(f"Startup error: {str(e)}")
        raise
    
    yield
    
    # Shutdown
    logger_instance.info(f"Shutting down {settings.app_name}")
    try:
        es_client.close()
        redis_client.close()
        logger_instance.info("Service shutdown complete")
    except Exception as e:
        logger_instance.error(f"Shutdown error: {str(e)}")


# ============================================================================
# Application Factory
# ============================================================================

def create_app() -> FastAPI:
    """
    Create and configure FastAPI application.
    
    Returns:
        Configured FastAPI instance
    """
    # Create app with lifespan
    app = FastAPI(
        title=settings.app_name,
        description="Distributed Document Search Service - Enterprise-Grade Prototype",
        version=settings.app_version,
        lifespan=lifespan
    )
    
    # ========================================================================
    # Middleware Setup
    # ========================================================================
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*", "X-Tenant-ID"],
    )
    
    # Custom middleware in reverse order (last added = first executed)
    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(RateLimitingMiddleware)
    app.add_middleware(TenantValidationMiddleware)
    
    # ========================================================================
    # Route Registration
    # ========================================================================
    
    app.include_router(router, tags=["API"])
    
    # ========================================================================
    # Exception Handlers
    # ========================================================================
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Handle unexpected exceptions."""
        request_id = getattr(request.state, "request_id", "unknown")
        logger_instance.error(f"[{request_id}] Unhandled exception: {str(exc)}", exc_info=True)
        
        return JSONResponse(
            status_code=500,
            content={
                "error": "INTERNAL_SERVER_ERROR",
                "error_code": "500",
                "message": "An internal server error occurred",
                "request_id": request_id
            }
        )
    
    # ========================================================================
    # OpenAPI Documentation
    # ========================================================================
    
    def custom_openapi():
        """Customize OpenAPI schema."""
        if app.openapi_schema:
            return app.openapi_schema
        
        openapi_schema = get_openapi(
            title=settings.app_name,
            version=settings.app_version,
            description="""
            ## Overview
            A distributed document search service demonstrating enterprise-grade architectural patterns.
            
            ## Key Features
            - **Multi-Tenancy**: Row-level isolation at database and index level
            - **Full-Text Search**: Powered by Elasticsearch with relevance ranking
            - **Horizontal Scalability**: Stateless API design with distributed cache
            - **Fault Tolerance**: Circuit breakers, retry logic, graceful degradation
            - **Rate Limiting**: Per-tenant rate limiting with Redis
            - **Caching**: Multi-layer caching (Redis, in-memory)
            
            ## Authentication
            All endpoints require tenant identification via:
            - Header: `X-Tenant-ID` (recommended)
            - Query Param: `tenant_id`
            
            ## Rate Limiting
            - Default: 1000 requests per 60 seconds per tenant
            - Headers: `X-Rate-Limit-Limit`, `X-Rate-Limit-Remaining`, `X-Rate-Limit-Reset`
            """,
            routes=app.routes,
        )
        
        openapi_schema["info"]["x-logo"] = {
            "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
        }
        
        app.openapi_schema = openapi_schema
        return app.openapi_schema
    
    app.openapi = custom_openapi
    
    return app


# ============================================================================
# Application Instance
# ============================================================================

app = create_app()


# ============================================================================
# Root Endpoint
# ============================================================================

@app.get("/", tags=["Info"])
async def root():
    """Root endpoint - service information."""
    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "environment": settings.app_env,
        "docs_url": "/docs",
        "openapi_url": "/openapi.json"
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        workers=settings.workers,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )
