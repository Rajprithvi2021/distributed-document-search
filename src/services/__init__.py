"""
Core business logic services for document management and search.
"""

import logging
import re
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from src.utils import es_client, redis_client, logger
from src.config import settings

logger = logging.getLogger(__name__)


_IN_MEMORY_DOCUMENTS: Dict[str, Dict[str, Dict[str, Any]]] = {}
_IN_MEMORY_SEARCH_CACHE: Dict[str, Dict[str, Any]] = {}


def _tenant_docs(tenant_id: str) -> Dict[str, Dict[str, Any]]:
    """Return the prototype fallback store for a tenant."""
    return _IN_MEMORY_DOCUMENTS.setdefault(tenant_id, {})


def _invalidate_search_cache(tenant_id: str) -> None:
    """Invalidate cached search results for one tenant."""
    for key in list(_IN_MEMORY_SEARCH_CACHE):
        if key.startswith(f"search:{tenant_id}:"):
            del _IN_MEMORY_SEARCH_CACHE[key]
    redis_client.delete_pattern(f"search:{tenant_id}:*")


def _score_document(document: Dict[str, Any], query_text: str) -> float:
    """Simple relevance score for the local prototype fallback."""
    terms = [term.lower() for term in re.findall(r"\w+", query_text)]
    if not terms:
        return 0.0

    title = document.get("title", "").lower()
    content = document.get("content", "").lower()
    score = 0.0
    for term in terms:
        score += title.count(term) * 2.0
        score += content.count(term)

    return score


def _snippet(content: str, query_text: str, size: int = 180) -> str:
    """Build a small snippet around the first matching query term."""
    lowered = content.lower()
    for term in re.findall(r"\w+", query_text.lower()):
        idx = lowered.find(term)
        if idx >= 0:
            start = max(0, idx - 50)
            return content[start:start + size]
    return content[:size]


class TenantService:
    """Service for tenant management and isolation."""
    
    @staticmethod
    def validate_tenant(tenant_id: str) -> bool:
        """
        Validate that tenant exists and is active.
        Simplified for prototype: any tenant_id is valid.
        In production, would validate against tenant registry.
        
        Args:
            tenant_id: Tenant ID to validate
            
        Returns:
            True if tenant is valid and active
        """
        # For prototype: accept any non-empty tenant_id
        # In production: would check tenant registry
        return bool(tenant_id and tenant_id.strip())


class IndexService:
    """Service for document indexing and index management."""
    
    TENANT_INDEX_PREFIX = "doc_"
    
    @classmethod
    def get_index_name(cls, tenant_id: str) -> str:
        """Get Elasticsearch index name for a tenant."""
        return f"{cls.TENANT_INDEX_PREFIX}{tenant_id}".lower()
    
    @classmethod
    def ensure_tenant_index(cls, tenant_id: str) -> bool:
        """
        Ensure Elasticsearch index exists for tenant.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            True if index exists or was created successfully
        """
        index_name = cls.get_index_name(tenant_id)
        
        try:
            es_client.create_index(index_name)
            logger.info(f"Index ensured for tenant {tenant_id}: {index_name}")
            return True
        except Exception as e:
            logger.error(f"Error ensuring index for tenant {tenant_id}: {str(e)}")
            return False
    
    @classmethod
    def index_document(cls, tenant_id: str, document_id: str, title: str, content: str, 
                      metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Index a document in Elasticsearch.
        
        Args:
            tenant_id: Tenant ID (for isolation)
            document_id: Document ID
            title: Document title
            content: Document content
            metadata: Optional metadata
            
        Returns:
            True if indexing successful
        """
        index_name = cls.get_index_name(tenant_id)
        
        now = datetime.utcnow().isoformat()
        doc_body = {
            "tenant_id": tenant_id,
            "document_id": document_id,
            "title": title,
            "content": content,
            "status": "active",
            "indexed": True,
            "created_by": "system",
            "created_at": now,
            "updated_at": now,
        }
        
        if metadata:
            doc_body["metadata"] = metadata
        
        try:
            es_client.index_document(index_name, document_id, doc_body)
            _tenant_docs(tenant_id)[document_id] = doc_body
            _invalidate_search_cache(tenant_id)
            
            # Cache document for quick retrieval
            cache_key = f"doc:{tenant_id}:{document_id}"
            redis_client.set(cache_key, doc_body, ttl=settings.redis_cache_ttl)
            
            logger.info(f"Indexed document {document_id} for tenant {tenant_id}")
            return True
        except Exception as e:
            _tenant_docs(tenant_id)[document_id] = doc_body
            _invalidate_search_cache(tenant_id)
            logger.error(f"Error indexing document {document_id}; stored in local fallback: {str(e)}")
            return False
    
    @classmethod
    def bulk_index(cls, tenant_id: str, documents: List[Tuple[str, Dict[str, Any]]]) -> Tuple[int, int]:
        """
        Bulk index multiple documents.
        
        Args:
            tenant_id: Tenant ID
            documents: List of (doc_id, doc_data) tuples
            
        Returns:
            Tuple of (success_count, failure_count)
        """
        index_name = cls.get_index_name(tenant_id)
        
        # Add tenant_id to all documents
        bulk_docs = [
            (doc_id, {**doc_data, "tenant_id": tenant_id})
            for doc_id, doc_data in documents
        ]
        
        try:
            success, failed = es_client.bulk_index(index_name, bulk_docs)
            for doc_id, doc_data in bulk_docs:
                _tenant_docs(tenant_id)[doc_id] = doc_data
            _invalidate_search_cache(tenant_id)
            logger.info(f"Bulk indexed for tenant {tenant_id}: {success} success, {failed} failed")
            return success, failed
        except Exception as e:
            for doc_id, doc_data in bulk_docs:
                _tenant_docs(tenant_id)[doc_id] = doc_data
            _invalidate_search_cache(tenant_id)
            logger.error(f"Error bulk indexing for tenant {tenant_id}: {str(e)}")
            return len(documents), 0


class SearchService:
    """Service for document search operations."""
    
    @staticmethod
    def build_search_query(query_text: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build Elasticsearch query with filtering.
        
        Args:
            query_text: Search query text
            filters: Optional filters to apply
            
        Returns:
            Elasticsearch query body
        """
        must_clauses = [
            {
                "multi_match": {
                    "query": query_text,
                    "fields": ["title^2", "content"],
                    "fuzziness": "AUTO" if settings.enable_fuzzy_search else 0
                }
            }
        ]
        
        filter_clauses = []
        if filters:
            for field, value in filters.items():
                if field == "status":
                    filter_clauses.append({"term": {"status": value}})
                elif field == "created_after":
                    filter_clauses.append({"range": {"created_at": {"gte": value}}})
        
        query_body = {
            "bool": {
                "must": must_clauses,
                "filter": filter_clauses if filter_clauses else []
            }
        }
        
        return query_body
    
    @staticmethod
    def search(tenant_id: str, query_text: str, page: int = 1, page_size: int = 20,
               filters: Optional[Dict[str, Any]] = None, sort_by: str = "_score") -> Dict[str, Any]:
        """
        Execute search query.
        
        Args:
            tenant_id: Tenant ID
            query_text: Search query
            page: Page number (1-indexed)
            page_size: Results per page
            filters: Optional filtering
            sort_by: Sort field
            
        Returns:
            Search results with metadata
        """
        # Check cache for query
        cache_key = f"search:{tenant_id}:{hash(query_text)}:{page}:{page_size}"
        if cached := redis_client.get(cache_key):
            logger.debug(f"Cache hit for search query: {cache_key}")
            return cached
        if cache_key in _IN_MEMORY_SEARCH_CACHE:
            logger.debug(f"In-memory cache hit for search query: {cache_key}")
            return _IN_MEMORY_SEARCH_CACHE[cache_key]
        
        index_name = IndexService.get_index_name(tenant_id)
        
        # Add tenant filter for isolation
        query_body = SearchService.build_search_query(query_text, filters or {})
        query_body["bool"]["filter"].append({"term": {"tenant_id": tenant_id}})
        
        from_offset = (page - 1) * page_size
        
        try:
            start_time = datetime.utcnow()
            
            results = es_client.search(
                index_name=index_name,
                query=query_body,
                size=page_size,
                from_=from_offset,
                sort=[{sort_by: {"order": "desc"}}] if sort_by else None
            )
            
            duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Process results
            hits = results.get("hits", {})
            total_hits = hits.get("total", {}).get("value", 0)
            es_hits = hits.get("hits", [])

            if total_hits == 0 and _tenant_docs(tenant_id):
                return SearchService._search_in_memory(
                    tenant_id=tenant_id,
                    query_text=query_text,
                    page=page,
                    page_size=page_size,
                    started_at=start_time,
                    cache_key=cache_key,
                )
            
            search_results = {
                "query": query_text,
                "total_results": total_hits,
                "page": page,
                "page_size": page_size,
                "total_pages": (total_hits + page_size - 1) // page_size,
                "results": [
                    {
                        "document_id": hit["_id"],
                        "title": hit["_source"].get("title", ""),
                        "relevance_score": float(hit.get("_score", 0)),
                        "snippet": hit["_source"].get("content", "")[:200],
                        "metadata": hit["_source"].get("metadata")
                    }
                    for hit in es_hits
                ],
                "execution_time_ms": duration_ms
            }
            
            # Cache results for 5 minutes
            redis_client.set(cache_key, search_results, ttl=300)
            _IN_MEMORY_SEARCH_CACHE[cache_key] = search_results
            
            logger.info(f"Search for tenant {tenant_id}: '{query_text}' returned {total_hits} results in {duration_ms:.2f}ms")
            return search_results
            
        except Exception as e:
            logger.error(f"Search error for tenant {tenant_id}: {str(e)}")
            return SearchService._search_in_memory(
                tenant_id=tenant_id,
                query_text=query_text,
                page=page,
                page_size=page_size,
                started_at=datetime.utcnow(),
                cache_key=cache_key,
            )

    @staticmethod
    def _search_in_memory(
        tenant_id: str,
        query_text: str,
        page: int,
        page_size: int,
        started_at: datetime,
        cache_key: str,
    ) -> Dict[str, Any]:
        scored = []
        for document in _tenant_docs(tenant_id).values():
            if document.get("status") != "active":
                continue
            score = _score_document(document, query_text)
            if score > 0:
                scored.append((score, document))

        scored.sort(key=lambda item: item[0], reverse=True)
        total_hits = len(scored)
        start = (page - 1) * page_size
        paged = scored[start:start + page_size]
        duration_ms = (datetime.utcnow() - started_at).total_seconds() * 1000

        search_results = {
            "query": query_text,
            "total_results": total_hits,
            "page": page,
            "page_size": page_size,
            "total_pages": (total_hits + page_size - 1) // page_size,
            "results": [
                {
                    "document_id": document["document_id"],
                    "title": document.get("title", ""),
                    "relevance_score": float(score),
                    "snippet": _snippet(document.get("content", ""), query_text),
                    "metadata": document.get("metadata"),
                }
                for score, document in paged
            ],
            "execution_time_ms": duration_ms,
        }

        redis_client.set(cache_key, search_results, ttl=300)
        _IN_MEMORY_SEARCH_CACHE[cache_key] = search_results
        return search_results
    
    @staticmethod
    def get_document(tenant_id: str, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve document details.
        
        Args:
            tenant_id: Tenant ID
            document_id: Document ID
            
        Returns:
            Document data or None if not found
        """
        # Check cache first
        cache_key = f"doc:{tenant_id}:{document_id}"
        if cached := redis_client.get(cache_key):
            return cached
        if document_id in _tenant_docs(tenant_id):
            return _tenant_docs(tenant_id)[document_id]
        
        index_name = IndexService.get_index_name(tenant_id)
        
        try:
            doc = es_client.get_document(index_name, document_id)
            
            if doc and doc.get("tenant_id") == tenant_id:
                # Cache the document
                redis_client.set(cache_key, doc, ttl=settings.redis_cache_ttl)
                return doc
            
            return None
        except Exception as e:
            logger.error(f"Error retrieving document {document_id}: {str(e)}")
            return None
    
    @staticmethod
    def delete_document(tenant_id: str, document_id: str) -> bool:
        """
        Delete a document.
        
        Args:
            tenant_id: Tenant ID
            document_id: Document ID
            
        Returns:
            True if deletion successful
        """
        index_name = IndexService.get_index_name(tenant_id)
        
        try:
            result = es_client.delete_document(index_name, document_id)
            local_deleted = _tenant_docs(tenant_id).pop(document_id, None) is not None
            
            # Invalidate cache
            cache_key = f"doc:{tenant_id}:{document_id}"
            redis_client.delete(cache_key)
            _invalidate_search_cache(tenant_id)
            
            logger.info(f"Deleted document {document_id} from tenant {tenant_id}")
            return bool(result or local_deleted)
        except Exception as e:
            local_deleted = _tenant_docs(tenant_id).pop(document_id, None) is not None
            _invalidate_search_cache(tenant_id)
            logger.error(f"Error deleting document {document_id}: {str(e)}")
            return local_deleted
