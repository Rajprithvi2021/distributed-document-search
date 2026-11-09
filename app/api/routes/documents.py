"""Document endpoints: POST /documents, POST /documents/bulk, GET/DELETE /documents/{id}."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Path, Response, status

from app.api.dependencies import enforce_rate_limit, get_container, require_scope
from app.api.schemas import (
    BulkCreateRequest,
    BulkCreateResponse,
    DocumentCreateRequest,
    DocumentResponse,
    ErrorResponse,
)
from app.container import Container
from app.core.security import SCOPE_DOCUMENTS_READ, SCOPE_DOCUMENTS_WRITE, Principal
from app.services.documents import DocumentInput

DOCUMENT_ID = Path(pattern=r"^[A-Za-z0-9_-]{1,64}$", description="Document ID (UUID)")
ERRORS = {
    400: {"model": ErrorResponse, "description": "Missing/invalid tenant"},
    401: {"model": ErrorResponse, "description": "Missing/invalid credentials (api_key/jwt modes)"},
    403: {"model": ErrorResponse, "description": "Credentials not valid for tenant, or missing scope"},
    422: {"model": ErrorResponse, "description": "Validation error"},
    429: {"model": ErrorResponse, "description": "Tenant rate limit exceeded"},
    503: {"model": ErrorResponse, "description": "Search engine unavailable"},
}

router = APIRouter(
    prefix="/documents", tags=["Documents"], dependencies=[Depends(enforce_rate_limit)], responses=ERRORS
)


def _to_input(payload: DocumentCreateRequest) -> DocumentInput:
    return DocumentInput(title=payload.title, content=payload.content, tags=payload.tags, metadata=payload.metadata)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=DocumentResponse,
    summary="Index a new document",
    responses={
        200: {"model": DocumentResponse, "description": "Idempotent replay of an earlier request"},
        202: {"model": DocumentResponse, "description": "Accepted for asynchronous indexing (INDEXING_MODE=async)"},
        409: {"model": ErrorResponse, "description": "Same Idempotency-Key still in progress"},
        413: {"model": ErrorResponse, "description": "Document too large"},
    },
)
async def create_document(
    payload: DocumentCreateRequest,
    response: Response,
    principal: Principal = Depends(require_scope(SCOPE_DOCUMENTS_WRITE)),
    container: Container = Depends(get_container),
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key", max_length=128, description="Replays the original result on retry"
    ),
) -> DocumentResponse:
    result = await container.documents.create(principal, _to_input(payload), idempotency_key=idempotency_key)
    if result.replayed:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replayed"] = "true"
    elif result.status == "pending":
        response.status_code = status.HTTP_202_ACCEPTED
    response.headers["Location"] = f"/documents/{result.document.id}"
    return DocumentResponse.from_domain(result.document, result.status)


@router.post(
    "/bulk",
    status_code=status.HTTP_201_CREATED,
    response_model=BulkCreateResponse,
    summary="Index up to 500 documents in one request",
    responses={202: {"model": BulkCreateResponse, "description": "Accepted for asynchronous indexing"}},
)
async def bulk_create_documents(
    payload: BulkCreateRequest,
    response: Response,
    principal: Principal = Depends(require_scope(SCOPE_DOCUMENTS_WRITE)),
    container: Container = Depends(get_container),
) -> BulkCreateResponse:
    result = await container.documents.bulk_create(principal, [_to_input(d) for d in payload.documents])
    if result.items and all(item.status == "pending" for item in result.items):
        response.status_code = status.HTTP_202_ACCEPTED
    elif result.failed:
        response.status_code = status.HTTP_207_MULTI_STATUS
    return BulkCreateResponse(
        total=len(result.items), succeeded=result.succeeded, failed=result.failed, items=result.items
    )


@router.get(
    "/{document_id}",
    response_model=DocumentResponse,
    summary="Retrieve document details",
    responses={404: {"model": ErrorResponse, "description": "No such document for this tenant"}},
)
async def get_document(
    document_id: str = DOCUMENT_ID,
    principal: Principal = Depends(require_scope(SCOPE_DOCUMENTS_READ)),
    container: Container = Depends(get_container),
) -> DocumentResponse:
    document, state = await container.documents.get(principal.tenant_id, document_id)
    return DocumentResponse.from_domain(document, state)


@router.delete(
    "/{document_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    response_class=Response,
    summary="Remove a document",
    responses={404: {"model": ErrorResponse, "description": "No such document for this tenant"}},
)
async def delete_document(
    document_id: str = DOCUMENT_ID,
    principal: Principal = Depends(require_scope(SCOPE_DOCUMENTS_WRITE)),
    container: Container = Depends(get_container),
) -> None:
    await container.documents.delete(principal.tenant_id, document_id)
