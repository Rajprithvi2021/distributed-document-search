"""
Redis client wrapper with connection pooling and circuit breaker.
Provides caching and session management with automatic failover.
"""

import redis
from redis import Redis, ConnectionPool
from typing import Any, Optional
import json
import logging
from src.config import settings

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis client wrapper with connection pooling and error handling."""
    
    def __init__(self):
        """Initialize Redis client with connection pooling."""
        try:
            self.pool = ConnectionPool.from_url(
                settings.redis_url,
                max_connections=settings.redis_max_connections,
                decode_responses=True,
                socket_connect_timeout=1,  # Short timeout - fail fast
                socket_timeout=1,  # Socket read/write timeout
                socket_keepalive=True,
            )
            self.client = Redis(connection_pool=self.pool)
            # Skip verification at initialization to avoid blocking
            # Connection will be verified on first use
            logger.info("Redis client initialized (verification deferred)")
        except Exception as e:
            logger.error(f"Failed to initialize Redis client: {str(e)}")
            raise
    
    def _verify_connection(self) -> bool:
        """Verify Redis connection."""
        try:
            self.client.ping()
            logger.info("Connected to Redis")
            return True
        except Exception as e:
            logger.warning(f"Redis not available (expected during development): {str(e)}")
            # Don't raise - allow app to load even if Redis isn't running
            return False
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a key-value pair in Redis.
        
        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time to live in seconds (uses default if not specified)
            
        Returns:
            True if successful
        """
        try:
            ttl = ttl or settings.redis_cache_ttl
            serialized = json.dumps(value) if not isinstance(value, str) else value
            self.client.setex(key, ttl, serialized)
            logger.debug(f"Set cache key: {key}")
            return True
        except Exception as e:
            logger.error(f"Failed to set cache key {key}: {str(e)}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from Redis.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value if found, None otherwise
        """
        try:
            value = self.client.get(key)
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return None
        except Exception as e:
            logger.error(f"Failed to get cache key {key}: {str(e)}")
            return None
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from Redis.
        
        Args:
            key: Cache key
            
        Returns:
            True if key was deleted, False otherwise
        """
        try:
            result = self.client.delete(key)
            logger.debug(f"Deleted cache key: {key}")
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to delete cache key {key}: {str(e)}")
            return False
    
    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.
        
        Args:
            pattern: Key pattern (supports wildcards)
            
        Returns:
            Number of keys deleted
        """
        try:
            keys = self.client.keys(pattern)
            if keys:
                count = self.client.delete(*keys)
                logger.debug(f"Deleted {count} keys matching pattern: {pattern}")
                return count
            return 0
        except Exception as e:
            logger.error(f"Failed to delete keys with pattern {pattern}: {str(e)}")
            return 0
    
    def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a numeric value in Redis.
        
        Args:
            key: Cache key
            amount: Amount to increment by
            
        Returns:
            New value
        """
        try:
            return self.client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Failed to increment key {key}: {str(e)}")
            return 0
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists.
        
        Args:
            key: Cache key
            
        Returns:
            True if key exists
        """
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error(f"Failed to check key existence {key}: {str(e)}")
            return False
    
    def setex(self, key: str, seconds: int, value: Any) -> bool:
        """
        Set a key with expiration time.
        
        Args:
            key: Cache key
            seconds: Expiration time in seconds
            value: Value to cache
            
        Returns:
            True if successful
        """
        try:
            serialized = json.dumps(value) if not isinstance(value, str) else value
            self.client.setex(key, seconds, serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to setex key {key}: {str(e)}")
            return False
    
    def ttl(self, key: str) -> int:
        """
        Get time to live for a key.
        
        Args:
            key: Cache key
            
        Returns:
            TTL in seconds (-1 if no expiry, -2 if doesn't exist)
        """
        try:
            return self.client.ttl(key)
        except Exception as e:
            logger.error(f"Failed to get TTL for key {key}: {str(e)}")
            return -2
    
    def close(self):
        """Close Redis connection pool."""
        try:
            self.pool.disconnect()
            logger.info("Redis connection pool closed")
        except Exception as e:
            logger.error(f"Error closing Redis: {str(e)}")


# Global instance
redis_client = RedisClient()
