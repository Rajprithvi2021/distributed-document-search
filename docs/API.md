# API Documentation
## Distributed Document Search Service

**Version**: 1.0.0  
**Base URL**: `http://localhost:8000` (local) or `https://api.docstore.com` (production)  
**Authentication**: JWT Bearer Token or API Key (header: `Authorization: Bearer {token}`)

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Endpoints](#endpoints)
4. [Request/Response Examples](#request--response-examples)
5. [Error Handling](#error-handling)
6. [Rate Limiting](#rate-limiting)
7. [Pagination](#pagination)

---

## Overview

The Document Search Service provides a RESTful API for indexing and searching documents with enterprise features including:
- Multi-tenant data isolation
- Full-text search with relevance ranking
- Bulk operations for batch indexing
- Distributed caching and rate limiting
- Comprehensive health monitoring

### Key Features

- **Multi-Tenancy**: Tenant isolation via `X-Tenant-ID` header
- **Full-Text Search**: Elasticsearch-powered search with fuzzy matching
- **Relevance Scoring**: BM25 algorithm with field-level boosting
- **Pagination**: Cursor and offset-based pagination
- **Caching**: Automatic result caching with TTL
- **Rate Limiting**: Per-tenant rate limits (default: 1000 req/min)

---

## Authentication

### Header-Based Tenant Identification

All endpoints require tenant identification via headers:

```bash
curl -H "X-Tenant-ID: tenant-123" http://localhost:8000/documents
```

### Optional: JWT Authentication (For Production)

```bash
# Get token
curl -X POST http://localhost:8000/auth/login \
  -d '{"username": "user", "password": "pass"}'

# Use token
curl -H "Authorization: Bearer eyJh..." http://localhost:8000/documents
```

---

## Endpoints

### 1. Health & Status

#### GET /health

Check service and dependency health.

**Request:**
```bash
curl http://localhost:8000/health
```

**Response** (200 OK):
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "dependencies": {
    "postgresql": {
      "status": "healthy"
    },
    "elasticsearch": {
      "status": "healthy",
      "version": "8.11.0"
    },
    "redis": {
      "status": "healthy"
    }
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Status Codes:**
- `200 OK`: Service healthy
- `503 Service Unavailable`: Dependency down

---

### 2. Document Management

#### POST /documents

Create and index a new document.

**Request:**
```bash
curl -X POST http://localhost:8000/documents \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-123" \
  -d '{
    "title": "Introduction to Distributed Systems",
    "content": "Distributed systems are computer systems whose components are located on different networked computers...",
    "metadata": {
      "author": "John Doe",
      "category": "technology",
      "tags": ["distributed", "systems"]
    }
  }'
```

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| title | string | Yes | Document title (1-500 chars) |
| content | string | Yes | Document content |
| metadata | object | No | Custom metadata |
| X-Tenant-ID | header | Yes | Tenant identifier |

**Response** (201 Created):
```json
{
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "title": "Introduction to Distributed Systems",
  "content": "Distributed systems are computer systems...",
  "status": "active",
  "tenant_id": "tenant-123",
  "indexed": true,
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z",
  "created_by": "system",
  "metadata": {
    "author": "John Doe",
    "category": "technology"
  }
}
```

**Error Responses:**
```bash
# 400 Bad Request - Validation error
{
  "error": "VALIDATION_ERROR",
  "error_code": "400",
  "message": "title must be at least 1 character"
}

# 403 Forbidden - Invalid tenant
{
  "error": "INVALID_TENANT",
  "error_code": "403",
  "message": "Tenant tenant-999 not found"
}

# 429 Too Many Requests - Rate limit exceeded
{
  "error": "RATE_LIMIT_EXCEEDED",
  "error_code": "429",
  "message": "Rate limit: 1000 requests per 60s"
}
```

---

#### POST /documents/bulk

Bulk create multiple documents.

**Request:**
```bash
curl -X POST http://localhost:8000/documents/bulk \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-123" \
  -d '{
    "documents": [
      {
        "title": "Document 1",
        "content": "Content 1...",
        "metadata": {}
      },
      {
        "title": "Document 2",
        "content": "Content 2..."
      }
    ]
  }'
```

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| documents | array | Yes | Array of documents (1-1000) |
| documents[].title | string | Yes | Document title |
| documents[].content | string | Yes | Document content |
| documents[].metadata | object | No | Optional metadata |

**Response** (201 Created):
```json
{
  "total": 2,
  "created": 2,
  "failed": 0,
  "errors": null
}
```

**Constraints:**
- Maximum 1000 documents per request
- Maximum request size: 10 MB
- Timeout: 30 seconds

---

#### GET /documents/{id}

Retrieve document details.

**Request:**
```bash
curl -H "X-Tenant-ID: tenant-123" \
  http://localhost:8000/documents/550e8400-e29b-41d4-a716-446655440000
```

**Parameters:**
| Parameter | Type | Location | Description |
|-----------|------|----------|-------------|
| id | string | path | Document ID (UUID) |
| X-Tenant-ID | string | header | Tenant identifier |

**Response** (200 OK):
```json
{
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "title": "Introduction to Distributed Systems",
  "content": "...",
  "status": "active",
  "tenant_id": "tenant-123",
  "indexed": true,
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z",
  "created_by": "system",
  "metadata": {}
}
```

**Error Responses:**
```bash
# 404 Not Found
{
  "error": "NOT_FOUND",
  "error_code": "404",
  "message": "Document not found"
}

# 403 Forbidden - Tenant mismatch
{
  "error": "UNAUTHORIZED",
  "error_code": "403",
  "message": "Access denied to document"
}
```

---

#### DELETE /documents/{id}

Delete a document.

**Request:**
```bash
curl -X DELETE -H "X-Tenant-ID: tenant-123" \
  http://localhost:8000/documents/550e8400-e29b-41d4-a716-446655440000
```

**Response** (204 No Content):
```
[Empty body]
```

**Side Effects:**
- Document removed from Elasticsearch index
- Document marked as deleted in PostgreSQL (audit trail)
- Document removed from Redis cache
- Audit log entry created

---

### 3. Search

#### GET /search

Execute full-text search.

**Request:**
```bash
curl "http://localhost:8000/search?q=distributed%20systems&page=1&page_size=20&sort_by=_score" \
  -H "X-Tenant-ID: tenant-123"
```

**Parameters:**
| Parameter | Type | Location | Default | Description |
|-----------|------|----------|---------|-------------|
| q | string | query | - | Search query (required, min 1 char) |
| page | integer | query | 1 | Page number (≥1) |
| page_size | integer | query | 20 | Results per page (1-100) |
| sort_by | string | query | _score | Sort field (_score for relevance) |
| X-Tenant-ID | string | header | - | Tenant identifier (required) |

**Response** (200 OK):
```json
{
  "query": "distributed systems",
  "total_results": 42,
  "page": 1,
  "page_size": 20,
  "total_pages": 3,
  "results": [
    {
      "document_id": "550e8400-e29b-41d4-a716-446655440000",
      "title": "Introduction to Distributed Systems",
      "relevance_score": 0.95,
      "snippet": "Distributed systems are computer systems whose components...",
      "metadata": {
        "author": "John Doe",
        "category": "technology"
      }
    },
    {
      "document_id": "550e8400-e29b-41d4-a716-446655440001",
      "title": "Building Scalable Systems",
      "relevance_score": 0.87,
      "snippet": "When building distributed systems at scale, reliability...",
      "metadata": null
    }
  ],
  "execution_time_ms": 124.5
}
```

**Query Examples:**

```bash
# Basic search
curl "http://localhost:8000/search?q=python" \
  -H "X-Tenant-ID: tenant-123"

# Multi-term search (AND by default)
curl "http://localhost:8000/search?q=python%20distributed%20systems" \
  -H "X-Tenant-ID: tenant-123"

# Phrase search
curl "http://localhost:8000/search?q=%22distributed%20systems%22" \
  -H "X-Tenant-ID: tenant-123"

# Pagination (get page 2)
curl "http://localhost:8000/search?q=python&page=2&page_size=10" \
  -H "X-Tenant-ID: tenant-123"

# Sort by newest first
curl "http://localhost:8000/search?q=python&sort_by=created_at" \
  -H "X-Tenant-ID: tenant-123"
```

**Performance:**
- Cached results: ~100ms (sub-second)
- Fresh search: ~300ms (p95) to ~500ms (p99)
- Maximum results: 10,000

---

### 4. Metrics

#### GET /metrics

Get service metrics (Prometheus format).

**Request:**
```bash
curl http://localhost:8000/metrics
```

**Response** (200 OK):
```
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",endpoint="/search",status="200"} 15234.0
http_requests_total{method="POST",endpoint="/documents",status="201"} 3421.0

# HELP http_request_duration_seconds HTTP request duration
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{method="GET",endpoint="/search",le="0.05"} 8234.0
...
```

---

## Request & Response Examples

### Example 1: Complete Workflow

```bash
#!/bin/bash
TENANT="tenant-123"
BASE_URL="http://localhost:8000"

# 1. Create a document
RESPONSE=$(curl -s -X POST $BASE_URL/documents \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT" \
  -d '{
    "title": "Kafka Tutorial",
    "content": "Apache Kafka is a distributed event streaming service...",
    "metadata": {"type": "tutorial"}
  }')

DOC_ID=$(echo $RESPONSE | jq -r '.document_id')
echo "Created document: $DOC_ID"

# 2. Wait for indexing
sleep 1

# 3. Search for the document
curl -s "$BASE_URL/search?q=kafka" \
  -H "X-Tenant-ID: $TENANT" | jq '.results'

# 4. Retrieve document
curl -s "$BASE_URL/documents/$DOC_ID" \
  -H "X-Tenant-ID: $TENANT" | jq '.title'

# 5. Delete document
curl -s -X DELETE "$BASE_URL/documents/$DOC_ID" \
  -H "X-Tenant-ID: $TENANT"

echo "Workflow complete"
```

### Example 2: Bulk Indexing

```bash
#!/bin/bash
TENANT="tenant-123"
BASE_URL="http://localhost:8000"

# Bulk create documents
curl -X POST $BASE_URL/documents/bulk \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT" \
  -d '{
    "documents": [
      {
        "title": "Document 1",
        "content": "This is the first document about....",
        "metadata": {"category": "tech"}
      },
      {
        "title": "Document 2",
        "content": "This is the second document about.....",
        "metadata": {"category": "business"}
      }
    ]
  }' | jq .
```

---

## Error Handling

### Standard Error Response

All errors follow this format:

```json
{
  "error": "ERROR_CODE",
  "error_code": "HTTP_STATUS",
  "message": "Human-readable description",
  "request_id": "uuid-for-support",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|------------|-------------|
| VALIDATION_ERROR | 400 | Request validation failed |
| MISSING_TENANT | 400 | Missing X-Tenant-ID header |
| INVALID_TENANT | 403 | Tenant not found or disabled |
| UNAUTHORIZED | 403 | Access denied |
| NOT_FOUND | 404 | Document not found |
| RATE_LIMIT_EXCEEDED | 429 | Rate limit exceeded |
| INTERNAL_SERVER_ERROR | 500 | Server error |
| SERVICE_UNAVAILABLE | 503 | Dependency unavailable |

### Retry Strategy

For production, implement exponential backoff:

```python
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[408, 429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retries)
session.mount('http://', adapter)
session.mount('https://', adapter)

response = session.get('http://localhost:8000/search?q=test')
```

---

## Rate Limiting

### Limits

- **Default**: 1000 requests per 60 seconds per tenant
- **Burst**: Up to 100 requests per second (short-term)
- **Sustained**: ~16-17 requests per second (long-term average)

### Headers

All responses include rate limit headers:

```
X-Rate-Limit-Limit: 1000           # Total requests allowed
X-Rate-Limit-Remaining: 999         # Requests remaining
X-Rate-Limit-Reset: 1705318260      # Unix timestamp when limit resets
```

### Sample Response Headers

```bash
HTTP/1.1 200 OK
X-Rate-Limit-Limit: 1000
X-Rate-Limit-Remaining: 987
X-Rate-Limit-Reset: 1705318260

[Response body]
```

---

## Pagination

### Offset-Based Pagination

Default pagination method:

```bash
# Page 1 (first 20 results)
curl "http://localhost:8000/search?q=python&page=1&page_size=20"

# Page 2 (next 20 results)
curl "http://localhost:8000/search?q=python&page=2&page_size=20"

# Page N
curl "http://localhost:8000/search?q=python&page=N&page_size=20"
```

**Constraints:**
- `page`: Minimum 1
- `page_size`: 1-100 (default 20)
- Maximum offset: 10,000 (limit × (page-1))

### Response Metadata

Every paginated response includes:

```json
{
  "page": 1,
  "page_size": 20,
  "total_results": 42,
  "total_pages": 3,
  "results": [...]
}
```

---

## Performance Tips

### 1. Use Caching

Frequently used queries are cached automatically (5-minute TTL). Cache hit rate is ~85-90% in production.

### 2. Optimize Page Size

```bash
# Slow: Too many results
page_size=100  # 500ms response time

# Fast: Optimal size
page_size=20   # 120ms response time
```

Suggested page sizes:
- UI Results: 20
- API Aggregation: 50
- Exports: 100-500

### 3. Batch Operations

For creating many documents, use bulk endpoint:

```bash
# Slow: 1000 individual requests = High latency
POST /documents (×1000)

# Fast: Single bulk request
POST /documents/bulk with 1000 documents = ~2 seconds total
```

### 4. Search Query Optimization

```bash
# Slow: Complex search
q=("distributed" OR "systems") AND (python OR java)  # Not supported

# Fast: Simple multi-term search
q=distributed systems python  # AND operator
```

---

## Support & Debugging

### Request ID Tracking

Every API request includes a unique `request_id` for support:

```
X-Request-ID: 550e8400-e29b-41d4-a716-446655440000
X-Request-Duration-Ms: 145.23
```

Include `X-Request-ID` when reporting issues.

### API Documentation

- **Interactive Docs**: http://localhost:8000/docs (Swagger UI)
- **OpenAPI JSON**: http://localhost:8000/openapi.json
- **ReDoc**: http://localhost:8000/redoc

### Status Page

Check service status:
- **Health**: http://localhost:8000/health
- **Status Dashboard**: http://status.docstore.com (production)
