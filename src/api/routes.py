"""
REST API routes for the document search service.
Implements all required endpoints with proper error handling.
"""

from fastapi import APIRouter, Request, HTTPException, status, Query, Depends
from datetime import datetime
import uuid
import logging

from src.api.schemas import (
    DocumentCreate, DocumentResponse, SearchResponse, 
    HealthCheckResponse, BulkDocumentCreate, BulkDocumentResponse
)
from src.services import TenantService, IndexService, SearchService
from src.config import settings
from src.utils import es_client, redis_client

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================================
# Health & Status Endpoints
# ============================================================================

@router.get("/health", response_model=HealthCheckResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint with dependency status.
    Returns status of all critical dependencies (with non-blocking checks).
    """
    dependencies_status = {
        "elasticsearch": {"status": "healthy" if es_client._verify_connection() else "degraded"},
        "redis": {"status": "healthy" if redis_client._verify_connection() else "degraded"}
    }
    
    status_code = "healthy" if all(
        dependency["status"] == "healthy"
        for dependency in dependencies_status.values()
    ) else "degraded"
    
    return HealthCheckResponse(
        status=status_code,
        version=settings.app_version,
        dependencies=dependencies_status,
        timestamp=datetime.utcnow()
    )


# ============================================================================
# Document Management Endpoints
# ============================================================================

@router.post("/documents", response_model=DocumentResponse, status_code=status.HTTP_201_CREATED, tags=["Documents"])
async def create_document(
    request: Request,
    doc_req: DocumentCreate
):
    """
    Create and index a new document.
    
    - **title**: Document title (required)
    - **content**: Document content (required)
    - **metadata**: Optional custom metadata
    
    Returns the created document with ID.
    """
    tenant_id = request.state.tenant_id or "anonymous"
    request_id = request.state.request_id
    
    # Create document ID
    document_id = str(uuid.uuid4())
    
    try:
        IndexService.ensure_tenant_index(tenant_id)
        indexed = IndexService.index_document(
            tenant_id=tenant_id,
            document_id=document_id,
            title=doc_req.title,
            content=doc_req.content,
            metadata=doc_req.metadata,
        )
        logger.info(f"[{request_id}] Created document {document_id} for tenant {tenant_id}")
        now = datetime.utcnow()
        
        return DocumentResponse(
            document_id=document_id,
            title=doc_req.title,
            content=doc_req.content,
            status="active",
            tenant_id=tenant_id,
            indexed=indexed,
            created_at=now,
            updated_at=now,
            created_by=request.headers.get("X-User-ID", "system"),
            metadata=doc_req.metadata
        )
        
    except Exception as e:
        logger.error(f"[{request_id}] Error creating document: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create document: {str(e)}"
        )


@router.post("/documents/bulk", response_model=BulkDocumentResponse, status_code=status.HTTP_201_CREATED, tags=["Documents"])
async def bulk_create_documents(
    request: Request,
    bulk_req: BulkDocumentCreate
):
    """
    Bulk create and index multiple documents.
    Maximum 1000 documents per request.
    """
    tenant_id = request.state.tenant_id or "anonymous"
    request_id = request.state.request_id
    
    try:
        documents = []
        now = datetime.utcnow().isoformat()
        for doc in bulk_req.documents:
            document_id = str(uuid.uuid4())
            documents.append((
                document_id,
                {
                    "document_id": document_id,
                    "title": doc.title,
                    "content": doc.content,
                    "metadata": doc.metadata,
                    "status": "active",
                    "indexed": True,
                    "created_by": request.headers.get("X-User-ID", "system"),
                    "created_at": now,
                    "updated_at": now,
                },
            ))

        IndexService.ensure_tenant_index(tenant_id)
        created, failed = IndexService.bulk_index(tenant_id, documents)
        errors = [] if failed == 0 else [{"message": f"{failed} documents failed to index"}]
        logger.info(f"[{request_id}] Bulk created {created} documents for tenant {tenant_id}")
        
        return BulkDocumentResponse(
            total=len(bulk_req.documents),
            created=created,
            failed=failed,
            errors=errors if errors else None
        )
        
    except Exception as e:
        logger.error(f"[{request_id}] Bulk creation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Bulk creation failed: {str(e)}"
        )


@router.get("/documents/{document_id}", response_model=DocumentResponse, tags=["Documents"])
async def get_document(
    request: Request,
    document_id: str
):
    """
    Retrieve document details by ID.
    Only returns documents belonging to the requesting tenant.
    """
    tenant_id = request.state.tenant_id or "anonymous"
    request_id = request.state.request_id
    
    try:
        document = SearchService.get_document(tenant_id, document_id)
        if not document:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document not found for this tenant"
            )

        logger.debug(f"[{request_id}] Retrieved document {document_id}")
        
        return DocumentResponse(
            document_id=document_id,
            title=document.get("title", ""),
            content=document.get("content", ""),
            status=document.get("status", "active"),
            tenant_id=tenant_id,
            indexed=bool(document.get("indexed", True)),
            created_at=document.get("created_at", datetime.utcnow()),
            updated_at=document.get("updated_at", datetime.utcnow()),
            created_by=document.get("created_by", "system"),
            metadata=document.get("metadata")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Error retrieving document {document_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve document: {str(e)}"
        )


@router.delete("/documents/{document_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Documents"])
async def delete_document(
    request: Request,
    document_id: str
):
    """
    Delete a document.
    Only allows deletion of documents belonging to the requesting tenant.
    """
    tenant_id = request.state.tenant_id or "anonymous"
    request_id = request.state.request_id
    
    try:
        deleted = SearchService.delete_document(tenant_id, document_id)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document not found for this tenant"
            )
        logger.info(f"[{request_id}] Deleted document {document_id}")
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Error deleting document {document_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete document: {str(e)}"
        )


# ============================================================================
# Search Endpoint
# ============================================================================

@router.get("/search", response_model=SearchResponse, tags=["Search"])
async def search(
    request: Request,
    q: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
    sort_by: str = Query("_score", description="Sort field")
):
    """
    Full-text search across documents.
    Results are filtered by tenant for isolation.
    
    - **q**: Search query (required)
    - **page**: Page number for pagination (default: 1)
    - **page_size**: Number of results per page (default: 20, max: 100)
    - **sort_by**: Field to sort by (default: _score for relevance)
    """
    tenant_id = request.state.tenant_id or "anonymous"
    request_id = request.state.request_id
    
    try:
        results = SearchService.search(
            tenant_id=tenant_id,
            query_text=q,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
        )
        logger.info(f"[{request_id}] Search for '{q}': {results['total_results']} results")
        return SearchResponse(**results)
        
    except Exception as e:
        logger.error(f"[{request_id}] Search error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )


# ============================================================================
# Error Handlers
# ============================================================================

@router.get("/metrics", tags=["Monitoring"])
async def get_metrics():
    """
    Get basic metrics about the service.
    For production, this should integrate with Prometheus.
    """
    return {
        "uptime_seconds": 0,
        "total_requests": 0,
        "total_errors": 0,
        "cache_hit_rate": 0.0,
        "average_response_time_ms": 0.0
    }
