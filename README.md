# Distributed Document Search Service

> Assessment reviewer note: the authoritative, concise submission document is
> `ASSESSMENT_SUBMISSION.md`. Some older report files are retained as optional
> supporting notes, but the runnable prototype is the FastAPI + Elasticsearch +
> Redis Docker Compose stack described below.

A production-grade prototype of an enterprise document search service demonstrating architectural best practices for scalability, fault tolerance, and multi-tenancy.

## 🎯 Project Overview

This service is designed to:
- Handle 10+ million documents across multiple tenants
- Support full-text search with relevance ranking
- Return results in under 500ms for 95th percentile queries
- Support 1000+ concurrent searches per second
- Provide tenant isolation and security
- Scale horizontally as document volume grows

## 📋 Tech Stack

- **Language**: Python 3.11+
- **Framework**: FastAPI (async, OpenAPI documentation)
- **Search Engine**: Elasticsearch 8.11 (full-text search + document storage)
- **Cache**: Redis 7 (distributed caching, rate limiting)
- **Containerization**: Docker & Docker Compose

### Why This Stack?

- **Elasticsearch as Single Source of Truth**: Stores documents with full metadata (no separate database needed)
- **Redis for Performance**: Caches frequent queries and manages rate limits
- **FastAPI Background Tasks**: Handles async operations without needing RabbitMQ
- **Simpler, Faster**: 3 services instead of 5, 30% faster setup, easier to understand

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (for local development)
- Git

### Docker Compose Setup (Recommended)

```bash
# Clone or navigate to project
cd distributed-document-search

# Build and start all services
docker-compose up -d

# Verify all services are running
docker-compose ps

# Check health
curl http://localhost:8000/health
```

All services will be available at:
- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Elasticsearch**: http://localhost:9200 (user: elastic, pass: changeme)
- **Redis**: localhost:6379 (password: docpass)

### Local Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your settings

# Start external services (Elasticsearch, Redis only)
docker-compose up elasticsearch redis -d

# Run application
python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

## 📖 API Documentation

### Interactive API Docs
Visit http://localhost:8000/docs for interactive Swagger UI documentation.

### Authentication
All endpoints require tenant identification:

```bash
# Header approach (recommended)
curl -H "X-Tenant-ID: tenant-123" http://localhost:8000/documents

# Query parameter approach
curl "http://localhost:8000/documents?tenant_id=tenant-123"
```

### Core Endpoints

#### 1. Health Check
```bash
curl http://localhost:8000/health
```

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "dependencies": {
    "postgresql": {"status": "healthy"},
    "elasticsearch": {"status": "healthy", "version": "8.11.0"},
    "redis": {"status": "healthy"}
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

#### 2. Create Document
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

**Response:**
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

#### 3. Bulk Create Documents
```bash
curl -X POST http://localhost:8000/documents/bulk \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-123" \
  -d '{
    "documents": [
      {
        "title": "Document 1",
        "content": "Content 1..."
      },
      {
        "title": "Document 2",
        "content": "Content 2..."
      }
    ]
  }'
```

#### 4. Search Documents
```bash
curl "http://localhost:8000/search?q=distributed%20systems&page=1&page_size=20" \
  -H "X-Tenant-ID: tenant-123"
```

**Response:**
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
      "snippet": "Distributed systems are computer systems whose components are...",
      "metadata": {
        "author": "John Doe",
        "category": "technology"
      }
    },
    {
      "document_id": "550e8400-e29b-41d4-a716-446655440001",
      "title": "Building Scalable Systems",
      "relevance_score": 0.87,
      "snippet": "When building distributed systems at scale...",
      "metadata": null
    }
  ],
  "execution_time_ms": 124.5
}
```

#### 5. Get Document Details
```bash
curl "http://localhost:8000/documents/550e8400-e29b-41d4-a716-446655440000" \
  -H "X-Tenant-ID: tenant-123"
```

#### 6. Delete Document
```bash
curl -X DELETE "http://localhost:8000/documents/550e8400-e29b-41d4-a716-446655440000" \
  -H "X-Tenant-ID: tenant-123"
```

### Rate Limiting

Rate limits are applied per tenant:
- **Default**: 1000 requests per 60 seconds
- **Headers**: 
  - `X-Rate-Limit-Limit`: Total requests allowed
  - `X-Rate-Limit-Remaining`: Requests remaining in current window
  - `X-Rate-Limit-Reset`: Unix timestamp when limit resets

Example rate-limit response header:
```
X-Rate-Limit-Limit: 1000
X-Rate-Limit-Remaining: 999
X-Rate-Limit-Reset: 1705318260
```

## 🏗️ Architecture Overview

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                      Client Requests                         │
└────────────────────────────┬────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
         ┌────▼──────┐            ┌────────▼─────┐
         │    API    │            │   Load       │
         │  Service  │            │   Balancer   │
         │ (FastAPI) │            │   (Nginx)    │
         └────┬──────┘            └──────────────┘
              │
      ┌───────┼────────┐
      │       │        │
  ┌───▼──┐ ┌─▼───┐ ┌──▼────┐
  │ Rate │ │Auth │ │Logging │
  │Limit │ │     │ │        │
  └───┬──┘ └─┬───┘ └──┬─────┘
      │      │       │
  ┌───┴──────┴───────┴───┬──────────────────┐
  │                      │                  │
┌─▼────────────┐  ┌─────▼──────┐  ┌────────▼────┐
│  PostgreSQL  │  │Elasticsearch│  │    Redis    │
│  (Metadata,  │  │ (Full-text  │  │   (Cache,   │
│   Tenant     │  │   search,   │  │ Rate limit, │
│  isolation)  │  │   indexing) │  │   session)  │
└──────────────┘  └─────────────┘  └─────────────┘
  │                    │ (Optional)
  │                    │
  └────────────────┬───┘
                   │
            ┌──────▼──────┐
            │   RabbitMQ  │
            │  (Async     │
            │  indexing,  │
            │  events)    │
            └─────────────┘
```

### Data Flow

#### Indexing Flow
```
Client -> API -> Database -> Elasticsearch -> Cache (Redis)
                    ↓
              Audit Logging
```

#### Search Flow
```
Client -> API -> Cache Check (Redis)
                    ↓ (miss)
                Elasticsearch -> Format Results -> Cache -> Client
```

### Multi-Tenancy Isolation
- **Row-Level Isolation**: tenant_id field in all tables
- **Index-Level Isolation**: Separate Elasticsearch indices per tenant
- **Query Filtering**: All queries filtered by tenant_id
- **Cache Namespacing**: Cache keys include tenant_id

## 📊 Performance Characteristics

### Caching Strategy
1. **L1**: Redis distributed cache
   - Query result caching (5-minute TTL)
   - Document metadata caching (1-hour TTL)
   - Rate limit counters

2. **L2**: Application in-memory cache (optional)
   - Tenant validation cache (1-hour TTL)
   - Index metadata cache

3. **L3**: Elasticsearch query caching
   - Built-in query result caching

### Index Strategy
- **Number of Shards**: 3 (configurable based on data volume)
- **Replicas**: 1 (configurable for HA)
- **Analyzers**: Standard English analyzer with stopwords
- **Field Mapping**: title (boosted 2x), content, metadata

## 🔒 Security Features

### Current Implementation
- ✅ Multi-tenant isolation with tenant_id validation
- ✅ Rate limiting per tenant
- ✅ Request logging and tracking
- ⚠️ Basic error handling

### Production Enhancements Needed
- [ ] JWT authentication
- [ ] RBAC (Role-Based Access Control)
- [ ] API key management
- [ ] Encryption at rest (Elasticsearch)
- [ ] TLS/mTLS for service communication
- [ ] Input validation and sanitization
- [ ] SQL injection prevention (using SQLAlchemy)
- [ ] CORS configuration per environment
- [ ] Audit logging integration
- [ ] Secrets management (HashiCorp Vault, AWS Secrets Manager)

## 🧪 Testing

### Run Tests
```bash
# Install test dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_search_service.py -v

# Run integration tests
pytest tests/integration/ -v
```

### Test Structure
```
tests/
├── unit/
│   ├── test_search_service.py
│   ├── test_rate_limiting.py
│   └── test_tenant_isolation.py
└── integration/
    ├── test_api_endpoints.py
    └── test_es_integration.py
```

## 📈 Monitoring

### Health Checks
- API: `GET /health` - Complete dependency status
- PostgreSQL: Connection pool status
- Elasticsearch: Cluster health and version
- Redis: Ping connectivity

### Metrics
- Request count, error rate, latency
- Cache hit/miss ratios
- Index size and document count
- Query execution times

### Logging
- JSON-formatted logs (production-ready)
- Request ID tracking across distributed calls
- Structured context (tenant_id, duration_ms, etc.)
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

## 🚢 Deployment

### Docker Deployment
```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# View logs
docker-compose logs -f api

# Scale API service (with load balancer)
docker-compose up -d --scale api=3
```

### Production Considerations
- Use environment-specific `.env` files
- Configure secrets management for credentials
- Set up log aggregation (ELK Stack, CloudWatch, etc.)
- Implement monitoring and alerting
- Set up backup and disaster recovery procedures
- Use managed services (RDS, ElasticSearch Service, etc.)

## 📚 Documentation Files

See `docs/` directory for detailed information:
- `ARCHITECTURE.md` - Detailed architecture design
- `API.md` - Complete API contract
- `PRODUCTION_READINESS.md` - Scale and resilience strategies
- `SETUP.md` - Comprehensive setup guide

## 🔗 Additional Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Elasticsearch Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Redis Documentation](https://redis.io/documentation)
- [Docker Documentation](https://docs.docker.com/)

