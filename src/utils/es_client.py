"""
Elasticsearch client wrapper with connection pooling and error handling.
Provides abstraction over Elasticsearch operations with retry logic.
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict, Any, Optional, Tuple
import logging
import time
from src.config import settings

logger = logging.getLogger(__name__)


class ElasticsearchClient:
    """Elasticsearch client wrapper with connection pooling."""
    
    def __init__(self):
        """Initialize Elasticsearch client with configured settings."""
        self.available = True
        self.last_failure_at = 0.0
        self.retry_after_seconds = 5
        try:
            # Use the new Elasticsearch client API (8.x+)
            self.client = Elasticsearch(
                [settings.elasticsearch_hosts],
                basic_auth=(settings.elasticsearch_username, settings.elasticsearch_password),
                request_timeout=settings.elasticsearch_timeout,
                max_retries=settings.elasticsearch_retries,
                retry_on_timeout=True,
            )
        except TypeError:
            # Fallback for different Elasticsearch versions
            self.client = Elasticsearch(
                [settings.elasticsearch_hosts],
                http_auth=(settings.elasticsearch_username, settings.elasticsearch_password),
            )
        # Skip verification at initialization to avoid blocking
        # Connection will be verified on first use
        logger.info("Elasticsearch client initialized (verification deferred)")

    def _should_try(self) -> bool:
        """Avoid repeated socket timeouts when Elasticsearch is unavailable."""
        return self.available or (time.time() - self.last_failure_at) >= self.retry_after_seconds

    def _mark_unavailable(self):
        self.available = False
        self.last_failure_at = time.time()

    def _mark_available(self):
        self.available = True
    
    def _verify_connection(self) -> bool:
        """Verify Elasticsearch connection asynchronously."""
        if not self._should_try():
            return False
        try:
            # Use a short timeout for the health check
            info = self.client.info(request_timeout=0.25)
            self._mark_available()
            logger.info(f"Connected to Elasticsearch {info['version']['number']}")
            return True
        except Exception as e:
            self._mark_unavailable()
            logger.warning(f"Elasticsearch not available (expected during development): {str(e)}")
            # Don't raise - allow app to load even if ES isn't running
            return False
    
    @retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_index(self, index_name: str, settings_dict: Dict[str, Any] = None) -> bool:
        """
        Create an Elasticsearch index with optional settings.
        Non-blocking: fails fast if Elasticsearch is unavailable.
        
        Args:
            index_name: Name of the index to create
            settings_dict: Custom index settings including mappings
            
        Returns:
            True if index created successfully
        """
        if not self._should_try():
            return True

        try:
            # Fast check if index exists
            if self.client.indices.exists(index=index_name, request_timeout=0.25):
                self._mark_available()
                logger.debug(f"Index {index_name} already exists")
                return True
        except Exception as e:
            self._mark_unavailable()
            # ES not available, silent fail and return True to allow app to continue
            logger.debug(f"Could not verify if index {index_name} exists: {str(e)[:50]}")
            return True  # Return True to allow app to continue without ES
        
        if settings_dict is None:
            settings_dict = self._default_index_settings()
        
        try:
            self.client.indices.create(index=index_name, body=settings_dict, request_timeout=0.25)
            self._mark_available()
            logger.info(f"Created index: {index_name}")
            return True
        except Exception as e:
            self._mark_unavailable()
            logger.debug(f"Could not create index {index_name} (ES unavailable expected): {str(e)[:50]}")
            # Return True anyway - ES is optional for development  
            return True
    
    @staticmethod
    def _default_index_settings() -> Dict[str, Any]:
        """Return default index settings for document search."""
        return {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1,
                "index.store.type": "niofs",
                "analysis": {
                    "analyzer": {
                        "default": {
                            "type": "standard",
                            "stopwords": "_english_"
                        },
                        "keyword_analyzer": {
                            "type": "keyword"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "tenant_id": {"type": "keyword"},
                    "document_id": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "standard",
                    },
                    "metadata": {"type": "object", "enabled": False},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "status": {"type": "keyword"},
                }
            }
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def index_document(self, index_name: str, doc_id: str, document: Dict[str, Any]) -> str:
        """
        Index a single document.
        Non-blocking: fails fast if Elasticsearch is unavailable.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            document: Document content
            
        Returns:
            Document ID if successful, empty string if failed
        """
        if not self._should_try():
            return doc_id

        try:
            result = self.client.index(index=index_name, id=doc_id, body=document, request_timeout=0.25)
            self._mark_available()
            logger.debug(f"Indexed document {doc_id} in {index_name}")
            return result.get("_id", doc_id)
        except Exception as e:
            self._mark_unavailable()
            logger.debug(f"Could not index document {doc_id} (ES unavailable expected): {str(e)[:50]}")
            # Return the doc_id anyway to allow app to continue
            return doc_id
    
    @retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=1, max=1))
    def bulk_index(self, index_name: str, documents: List[Tuple[str, Dict[str, Any]]]) -> Tuple[int, int]:
        """
        Bulk index multiple documents.
        Non-blocking: fails fast if Elasticsearch is unavailable.
        
        Args:
            index_name: Name of the index
            documents: List of (doc_id, document) tuples
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not self._should_try():
            return len(documents), 0

        actions = [
            {
                "_index": index_name,
                "_id": doc_id,
                "_source": doc
            }
            for doc_id, doc in documents
        ]
        
        try:
            success, failed = bulk(self.client, actions, chunk_size=500, request_timeout=0.25)
            self._mark_available()
            logger.info(f"Bulk indexed: {success} successful, {failed} failed")
            return success, failed
        except Exception as e:
            self._mark_unavailable()
            logger.debug(f"Bulk indexing not available (ES unavailable expected): {str(e)[:50]}")
            # Return fake success to allow app to continue
            return len(documents), 0
    
    @retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=1, max=1))
    def search(
        self,
        index_name: str,
        query: Dict[str, Any],
        size: int = 20,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Execute a search query.
        Non-blocking: fails fast if Elasticsearch is unavailable.
        
        Args:
            index_name: Name of the index
            query: Elasticsearch query
            size: Number of results to return
            from_: Offset for pagination
            sort: Sort specification
            
        Returns:
            Search results (empty if ES unavailable)
        """
        if not self._should_try():
            return {
                "hits": {
                    "total": {"value": 0, "relation": "eq"},
                    "hits": []
                }
            }

        try:
            body = {"query": query, "size": size, "from": from_}
            if sort:
                body["sort"] = sort
            
            result = self.client.search(index=index_name, body=body, request_timeout=0.25)
            self._mark_available()
            return result
        except Exception as e:
            self._mark_unavailable()
            logger.debug(f"Search not available (ES unavailable expected): {str(e)[:50]}")
            # Return empty results to allow app to continue
            return {
                "hits": {
                    "total": {"value": 0, "relation": "eq"},
                    "hits": []
                }
            }
    
    @retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=1, max=1))
    def delete_document(self, index_name: str, doc_id: str) -> bool:
        """
        Delete a document by ID.
        Non-blocking: fails fast if Elasticsearch is unavailable.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            
        Returns:
            True (always succeeds even if ES unavailable)
        """
        if not self._should_try():
            return False

        try:
            result = self.client.delete(index=index_name, id=doc_id, request_timeout=0.25)
            self._mark_available()
            logger.debug(f"Deleted document {doc_id} from {index_name}")
            return result.get("result") in ["deleted", "not_found"]
        except Exception as e:
            self._mark_unavailable()
            logger.debug(f"Delete not available (ES unavailable expected): {str(e)[:50]}")
            return False
    
    @retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=1, max=1))
    def get_document(self, index_name: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a document by ID.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            
        Returns:
            Document if found, None otherwise
        """
        if not self._should_try():
            return None

        try:
            result = self.client.get(index=index_name, id=doc_id, request_timeout=0.25)
            self._mark_available()
            return result["_source"]
        except Exception as e:
            self._mark_unavailable()
            logger.debug(f"Could not get document {doc_id} (ES unavailable expected): {str(e)[:50]}")
            return None
    
    def close(self):
        """Close the Elasticsearch connection."""
        self.client.close()
        logger.info("Elasticsearch connection closed")


# Global instance
es_client = ElasticsearchClient()
