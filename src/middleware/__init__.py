"""
Middleware for authentication, tenant validation, and request tracking.
"""

from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from uuid import uuid4
import time
import logging
from src.config import settings
from src.utils import redis_client

logger = logging.getLogger(__name__)
_LOCAL_RATE_LIMITS = {}


class TenantValidationMiddleware(BaseHTTPMiddleware):
    """Middleware to validate tenant from request headers or path."""
    
    async def dispatch(self, request: Request, call_next):
        """Validate tenant and add to request state."""
        # Extract tenant_id from header or query parameter
        tenant_id = (
            request.headers.get("X-Tenant-ID")
            or request.query_params.get("tenant_id")
            or request.query_params.get("tenant")
        )
        
        if not tenant_id and settings.enable_tenant_validation:
            # Allow health check endpoint without tenant
            if request.url.path not in ["/", "/health", "/metrics", "/docs", "/redoc", "/openapi.json"]:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": "Missing tenant ID", "error_code": "MISSING_TENANT"}
                )
        
        # Add tenant_id to request state
        request.state.tenant_id = tenant_id
        request.state.request_id = str(uuid4())
        request.state.start_time = time.time()
        
        # Process request
        response = await call_next(request)
        
        # Add timing header
        duration = (time.time() - request.state.start_time) * 1000
        response.headers["X-Request-Duration-Ms"] = str(duration)
        response.headers["X-Request-ID"] = request.state.request_id
        
        logger.debug(
            f"Request {request.state.request_id}: {request.method} {request.url.path} "
            f"- Tenant: {tenant_id} - Duration: {duration:.2f}ms"
        )
        
        return response


class RateLimitingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for rate limiting per tenant using token bucket algorithm.
    Uses Redis for distributed rate limiting across multiple instances.
    """
    
    async def dispatch(self, request: Request, call_next):
        """Apply rate limiting based on tenant ID."""
        tenant_id = getattr(request.state, "tenant_id", "anonymous")
        
        if not settings.rate_limit_enabled or not tenant_id:
            return await call_next(request)
        
        # Get rate limit
        limit = settings.rate_limit_default
        period = settings.rate_limit_period
        
        # Rate limit key
        rate_limit_key = f"rate_limit:{tenant_id}:{int(time.time() // period)}"
        
        try:
            # Check rate limit (with timeout to prevent blocking)
            use_local_counter = not redis_client._verify_connection()
            current_count = 0
            if use_local_counter:
                current_count = _LOCAL_RATE_LIMITS.get(rate_limit_key, 0)
            else:
                result = redis_client.get(rate_limit_key)
                current_count = result if result is not None else 0
            
            if current_count >= limit:
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "error": "Rate limit exceeded",
                        "error_code": "RATE_LIMIT_EXCEEDED",
                        "message": f"Rate limit: {limit} requests per {period}s"
                    }
                )
            
            # Increment counter
            if use_local_counter:
                _LOCAL_RATE_LIMITS[rate_limit_key] = current_count + 1
            else:
                redis_client.increment(rate_limit_key)
                redis_client.setex(rate_limit_key, period, current_count + 1)
            
            # Add rate limit headers
            response = await call_next(request)
            response.headers["X-Rate-Limit-Limit"] = str(limit)
            response.headers["X-Rate-Limit-Remaining"] = str(max(0, limit - current_count - 1))
            response.headers["X-Rate-Limit-Reset"] = str(int(time.time()) + period)
            
            return response
        except Exception as e:
            logger.error(f"Unhandled error in rate limiting: {str(e)}")
            # If anything goes wrong, pass the request through
            return await call_next(request)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for structured request logging."""
    
    async def dispatch(self, request: Request, call_next):
        """Log request details."""
        request_id = getattr(request.state, "request_id", str(uuid4()))
        tenant_id = getattr(request.state, "tenant_id", "unknown")
        start_time = time.time()
        
        # Process request
        response = await call_next(request)
        
        # Log request details
        duration_ms = (time.time() - start_time) * 1000
        
        log_context = {
            "request_id": request_id,
            "tenant_id": tenant_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
        }
        
        if response.status_code >= 400:
            logger.warning(f"Request failed: {log_context}")
        else:
            logger.info(f"Request completed: {log_context}")
        
        return response
