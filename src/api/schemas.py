"""
Pydantic models for request/response validation and OpenAPI documentation.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid


class TenantInfo(BaseModel):
    """Tenant information."""
    tenant_id: str
    name: str
    status: str = "active"
    created_at: datetime


class DocumentCreate(BaseModel):
    """Request model for creating a document."""
    title: str = Field(..., min_length=1, max_length=500, description="Document title")
    content: str = Field(..., min_length=1, description="Document content")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Custom metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "title": "Sample Document",
                "content": "This is a sample document content for indexing.",
                "metadata": {"author": "John Doe", "category": "tech"}
            }
        }


class DocumentUpdate(BaseModel):
    """Request model for updating a document."""
    title: Optional[str] = Field(None, max_length=500)
    content: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DocumentResponse(BaseModel):
    """Response model for document retrieval."""
    document_id: str
    title: str
    content: str
    status: str
    tenant_id: str
    indexed: bool
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str]
    metadata: Optional[Dict[str, Any]]
    
    class Config:
        from_attributes = True


class DocumentMetadata(BaseModel):
    """Metadata for search results."""
    document_id: str
    title: str
    status: str
    score: float = Field(description="Elasticsearch relevance score")


class SearchRequest(BaseModel):
    """Request model for search queries."""
    query: str = Field(..., min_length=1, description="Search query")
    page: int = Field(default=1, ge=1, description="Page number for pagination")
    page_size: int = Field(default=20, ge=1, le=100, description="Results per page")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    sort_by: Optional[str] = Field(default="_score", description="Sort field")
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "python distributed systems",
                "page": 1,
                "page_size": 20,
                "filters": {"status": "active"}
            }
        }


class SearchResult(BaseModel):
    """Single search result."""
    document_id: str
    title: str
    relevance_score: float
    snippet: Optional[str] = None
    highlights: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]]


class SearchResponse(BaseModel):
    """Response model for search results."""
    query: str
    total_results: int
    page: int
    page_size: int
    total_pages: int
    results: List[SearchResult]
    facets: Optional[Dict[str, Dict[str, int]]] = None
    execution_time_ms: float = Field(description="Query execution time in milliseconds")
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "python distributed systems",
                "total_results": 42,
                "page": 1,
                "page_size": 20,
                "total_pages": 3,
                "results": [
                    {
                        "document_id": "doc-123",
                        "title": "Distributed Systems Design",
                        "relevance_score": 0.95,
                        "snippet": "...systems are designed to handle..."
                    }
                ],
                "execution_time_ms": 124.5
            }
        }


class BulkDocumentCreate(BaseModel):
    """Request model for bulk document creation."""
    documents: List[DocumentCreate] = Field(..., min_items=1, max_items=1000)


class BulkDocumentResponse(BaseModel):
    """Response model for bulk document creation."""
    total: int
    created: int
    failed: int
    errors: Optional[List[Dict[str, Any]]] = None


class HealthCheckResponse(BaseModel):
    """Database response model for health check."""
    status: str
    version: str
    dependencies: Dict[str, Dict[str, str]]
    timestamp: datetime


class RateLimitInfo(BaseModel):
    """Rate limit information in response header."""
    limit: int
    remaining: int
    reset_at: datetime


class ErrorResponse(BaseModel):
    """Standard error response model."""
    error: str
    error_code: str
    message: str
    timestamp: datetime
    request_id: Optional[str] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "INVALID_QUERY",
                "error_code": "400",
                "message": "Search query cannot be empty",
                "timestamp": "2024-01-15T10:30:00Z"
            }
        }
