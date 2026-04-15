# Enterprise Experience Showcase
## Distributed Document Search Service

---

## 1. Building a Large-Scale Distributed Search System

### Background

At **TechCorp Analytics** (2021-2023), I led the architectural redesign of our document management platform serving 50+ enterprise customers with 500M+ documents across multiple data centers.

### Challenge

The legacy system used centralized Solr instances, causing:
- Bottlenecks during peak query times (stock market close)
- Long indexing latency (8+ hours for daily updates)
- Single points of failure causing 2-3 major outages/quarter
- No multi-tenancy isolation (data leakage risk)
- Scaling horizontally was cost-prohibitive ($50K/month per region)

### Solution

Architected and implemented a distributed search service using Elasticsearch clusters:

**Key Decisions:**
1. **Index-per-tenant strategy** instead of shared indices
   - Isolation: Complete dataset separation
   - Performance: Each tenant's queries hit their index
   - Scaling: Add tenants without impacting others
   - Cost: Efficient for varying document volumes

2. **Multi-tier caching** (Redis + local cache)
   - L1: Redis (distributed, 5min TTL for queries)
   - L2: Local memory cache (1min TTL for hot queries)
   - L3: Elasticsearch built-in cache
   - Result: 87% cache hit rate during peak hours

3. **Event-driven indexing** via RabbitMQ
   - Decoupled indexing from API requests
   - Bulk operations (10K docs/batch)
   - Automatic retry with exponential backoff
   - Reduced indexing tail latency by 60%

### Impact & Scale

**Performance Improvements:**
- **P95 Search Latency**: 1200ms → 120ms (10x improvement)
- **Indexing Throughput**: 1K docs/sec → 50K docs/sec (50x improvement)
- **Query Indexing Latency**: 8 hours → 5 minutes (96x improvement)

**Operational Impact:**
- **Availability**: 95% → 99.8% (eliminated cascading failures)
- **Major Incidents**: 2-3/quarter → 2/year (90% reduction)
- **Infrastructure Cost**: -$35K/month through optimization

**Scale Achieved:**
- 500M+ documents indexed
- 1000+ concurrent searches/sec during peak
- 40+ tenant instances
- 99.8% availability SLA (3 nines)

**Key Technologies:** Elasticsearch 6.x, Redis Cluster, RabbitMQ, Java/Spring Boot, PostgreSQL, Nginx load balancing

---

## 2. Critical Production Incident: Lightning-Fast Response

### Scenario

At **DataFlow Systems** (2022), we experienced a critical production incident in our real-time data pipeline serving financial institutions.

### The Problem

**Time: 2:15 AM Monday** - On-call alert: "Search latency > 5 seconds" (SLA: <500ms)

**Initial Symptoms:**
- User complaints: "Search results taking 30+ seconds"
- Alert: Elasticsearch CPU at 95%
- Monitoring: Query queue depth 50K+ (normal: <100)
- Market impact: Trading halt risk (data unavailable)

**Timeline:**
- 02:17: Incident declared (SEV1)
- 02:20: Root cause analysis initiated
- 02:25: Temporary fix deployed
- 02:35: Full resolution

### My Response (as On-Call Lead)

**Step 1: Immediate Triage (2 mins)**
```
1. Check dashboard: Elasticsearch 95% CPU
2. Verify: All shards healthy, no failures
3. Conclusion: Not cluster failure, but overload
```

**Step 2: Quick Diagnosis (3 mins)**
```
Problem identified: Full cluster GC (garbage collection)
- Elasticsearch JVM heap pressure
- Old generation objects not being cleaned
- GC pauses: 5-8 seconds (stops all queries)

Evidence:
- GC logs: 47 full GCs in last 10 minutes
- Heap usage: 92% of 32GB
- Query response: Paused during GC
```

**Step 3: Emergency Mitigation (5 mins)**
```
Action 1: Circuit breaker activation
- Reject new queries above 50K queue depth
- Preserve cluster stability
- Users get error instead of timeout

Action 2: Memory pressure relief
- Disable query cache (100GB), free instant memory
- Reduce thread pool (from 32 to 8)
- Market resumed: ~2min impact

Action 3: Load shedding
- Route 30% of queries to read replicas in secondary DC
- Reduce ES cluster load by 30%
```

**Step 4: Root Cause Analysis (during mitigation)**
```
Findings:
1. Hourly batch job started at 2:00 AM (scheduled task)
2. 5M new documents indexed (instead of normal 1M)
3. Elasticsearch didn't create new segments efficiently
4. Query cache grew to 200GB (misconfiguration)

Cause: Data import job had bug - imported 5x normal data
Configuration: Cache size limits not enforced
```

**Step 5: Permanent Fix (deployed 05:15 AM)**
```python
# Change 1: Enforce cache limits
{
  "settings": {
    "indices.memory": {
      "index_buffer_size": "10%",  # Was unlimited
      "max_cache_percentage": "25%"  # Was 50%
    }
  }
}

# Change 2: Improve monitoring
@scheduler.scheduled(interval=30s)
async def check_gc_pressure():
    if gc_pause_time > 1000ms:
        trigger_alert("GC pause detected")
        circuit_breaker.open()

# Change 3: Add rate limiting for batch jobs
@rate_limiter(max_per_minute=100K)
async def bulk_index():
    pass

# Change 4: Backup infrastructure
- Replicate 5% of queries to secondary cluster
- Validate results match primary
- Option to fail over if needed
```

### Impact & Learnings

**Outcome:**
- **Total Downtime**: 2 minutes (vs potential 30+ min)
- **Data Loss**: Zero
- **Customer SLA**: Honored (within 500ms P95)
- **Recovery**: 13-minute incident (industry avg: 45 min)

**Key Decisions & Tradeoffs:**
1. **Reject unpatible (circuit breaker)** vs. "hope it recovers"
   - Better: Fail loudly, fast recovery
   - Risk: Temporary user-facing errors
   - Resolution: Brief error + status page

2. **Load shedding to secondary DC** vs. "wait for recovery"
   - Better: 70% service availability
   - Risk: Potential secondary cluster overload
   - Mitigation: Monitored secondary health closely
   - Validation: Ran 8 hours before fully trusting

3. **Disable query cache** vs. "rolling restart"
   - Better: Immediate recovery (2 min vs 20 min)
   - Trade: Slightly slower queries (100ms increase)
   - Temporary: Cache re-enabled after 4 hours

**Post-Incident Actions:**
1. **Chaos engineering test** - Verify recovery procedures
2. **Monitoring enhancement** - GC pause detection < 500ms
3. **Configuration audit** - Resource limits enforced
4. **Team training** - 2-hour incident response workshop
5. **Cost optimization** - Reduced cost by moving batch to off-peak (2 hours earlier)

**Metrics from incident:**
- **MTTR** (Mean Time To Recovery): 13 minutes
- **MTTD** (Mean Time To Detect): 2 minutes
- **Incident Frequency**: From 1-2/quarter to 1-2/year

---

## 3. Performance Optimization: 10x Query Latency Improvement

### Project

At **InsightDB Inc.** (2023), I worked on optimizing a slow analytics dashboard aggregating 50M+ events.

### Initial Problem

**Symptoms:**
- Dashboard loads: 12-15 seconds (unacceptable UX)
- User complaints: "Dashboard freezes on load"
- Backend: 5-7 seconds just for data queries
- P95 latency: 18 seconds
- P99 latency: 30+ seconds

### Root Cause Analysis

**Phase 1: Profiling (2 hours)**
```
Entry Point: Dashboard loads 6 concurrent queries

Query 1: "Last 30 days sales by region"
- Elasticsearch scan: 5M+ documents
- Latency: 2.8s (dominated by IO)

Query 2: "Top 100 products by revenue"
- Database aggregation: GROUP BY product
- Latency: 1.2s (memory pressure)

Query 3: "Customer cohort analysis"
- Complex joins: 3 tables, 15M rows
- Latency: 2.1s (network + CPU)

Query 4: "Anomaly detection results"
- Cache miss (pre-warmed incorrectly)
- Latency: 0.8s (recalculated)

Query 5-6: Supporting data
- Latency: 0.5s each
```

**Total**: 2.8 + 1.2 + 2.1 + 0.8 + 0.5 + 0.5 = 7.9s

### Optimization Strategy

**1. Query Optimization (Target: -40%)**

```sql
-- BEFORE: 1.2s (full table scan)
SELECT product_id, SUM(revenue) as total
FROM sales
WHERE region = ? AND date > ?
GROUP BY product_id
ORDER BY total DESC
LIMIT 100;

-- AFTER: 0.18s (indexed aggregation)
-- Add composite index
CREATE INDEX idx_region_date_product ON sales(region, date DESC, product_id);

-- Add materialized view
CREATE MATERIALIZED VIEW mv_sales_by_product_daily AS
SELECT 
  region,
  DATE(date) as date,
  product_id,
  SUM(revenue) as total,
  COUNT(*) as count
FROM sales
GROUP BY region, DATE(date), product_id;

-- Refreshed hourly via scheduled job
-- Query time: 0.18s (pre-aggregated data)
```

**2. Pre-computation & Caching (Target: -35%)**

```python
# Cache expensive aggregations
@scheduler.scheduled(interval=timedelta(hours=1))
async def warm_cache():
    # Query 1: Pre-compute regional metrics
    regions = await db.get_regions()
    for region in regions:
        data = await compute_regional_sales(region)  # 2.8s to compute
        await cache.set(f"regional_sales:{region}", data, ttl=3600)
    
    # Query 2 & 3: Pre-compute cohorts
    cohorts = await compute_customer_cohorts()  # 3.3s to compute
    await cache.set("customer_cohorts", cohorts, ttl=3600)
    
    # Query 4: Pre-compute anomalies
    # Note: Anomaly detection takes 5 minutes, compute async separately
    await anomaly_detector.schedule_update()

# Dashboard requests now hit cache
# Cache hit rate: 95% during business hours
# Cache miss fallback: 0.5s (from 3.8s before)
```

**3. Request Parallelization (Target: -30%)**

```javascript
// BEFORE: Sequential queries
const response1 = await fetch('/api/regional-sales?region=' + region);
const response2 = await fetch('/api/top-products');
const response3 = await fetch('/api/cohort-analysis');
const response4 = await fetch('/api/anomalies');
// Total time: 7.9 seconds

// AFTER: Parallel queries
const [response1, response2, response3, response4] = await Promise.all([
  fetch('/api/regional-sales?region=' + region),
  fetch('/api/top-products'),
  fetch('/api/cohort-analysis'),
  fetch('/api/anomalies')
]);
// Total time: 1.8 seconds (limited by slowest query)
```

**4. Data Reduction & Filtering (Target: -20%)**

```elasticsearch
// BEFORE: Query all 50M documents
{
  "query": {
    "range": { "timestamp": { "gte": "30 days ago" } }
  },
  "aggs": { "by_region": { "terms": { "field": "region" } } }
}
// Scanned: 50M documents, 2.8s

// AFTER: Pre-filtered by index
{
  "query": {
    "bool": {
      "filter": [
        { "range": { "timestamp": { "gte": "30 days ago" } } },
        { "range": { "timestamp": { "lte": "today" } } }
      ]
    }
  },
  "aggs": { "by_region": { "terms": { "size": 50 } } }
}
// + Add index with timestamp routing
// Scanned: 1.2M documents, 0.3s
```

### Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Dashboard Load Time | 12.5s | 1.2s | **10.4x** |
| P95 Latency | 18s | 2.1s | **8.6x** |
| P99 Latency | 30s | 3.2s | **9.4x** |
| Query 1 (Regional) | 2.8s | 0.15s (cache) | **18.7x** |
| Query 2 (Products) | 1.2s | 0.18s (indexed) | **6.7x** |
| Query 3 (Cohorts) | 2.1s | 0.25s (cache) | **8.4x** |
| Database CPU | 78% | 12% | **6.5x lower** |
| Cache Hit Rate | 0% | 95% | **95%** |

### Key Lessons

1. **Measure First**: Profiling revealed 40% of time was parallelizable (quick win)
2. **Maximize Cache**: Pre-computation reduced latency 35% with minimal complexity
3. **Index Strategy**: Proper indexing gave 18.7x improvement on largest query
4. **Parallel Execution**: Was worth frontend complexity (2-line change)
5. **Monitoring**: Set up percentile-based SLOs, not just averages

### Implementation Impact

- **User Experience**: Dashboard now loads in 1.2 seconds (delightful)
- **Infrastructure**: CPU usage 12% (freed capacity for growth)
- **Cost**: Fewer queries on database = smaller instance (-30% cost)
- **Scale**: Could now handle 10x more concurrent users

---

## 4. Architectural Decision: Building Block-Based Microservices

### Challenge

At **CloudScale Systems** (2020-2021), we had monolithic product service (2M+ lines of code, 500+ developers) causing:
- 45-minute deploy times
- Testing took 6+ hours
- One team's change broke another team's feature
- Scaling required scaling everything

### Decision: Modular Microservices

**Considered Alternatives:**

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Monolith (keep) | Simple ops, shared code | Can't scale parts, slow deploys | ❌ Ruled out |
| Full Microservices | Independent scaling, deploys | Operational complexity, distributed systems | ⚠️ Considered but risky |
| **Modular Monolith** | Best of both | Needs discipline | ✅ Chosen |
| Serverless | Autoscaling, low ops | Vendor lock-in, cold starts | ⚠️ For spikes only |

**Architecture Decision:**

```
Monolith (before)
├── User Service (100K lines)
├── Order Service (250K lines)
├── Payment Service (80K lines)
├── Inventory Service (200K lines)
├── Search Service (50K lines)
└── Analytics Service (180K lines)
    [All tightly coupled]

Modular Monolith (after)
├── User Service (50K lines, independent module)
│   ├── API layer (Flask)
│   ├── Business logic
│   ├── Data access (SQLAlchemy)
│   └── Tests
│
├── Order Service (100K lines, independent module)
│   ├── API layer
│   ├── Events (publish: OrderCreated, OrderShipped)
│   └── Tests
│
├── Search Service (40K lines, can be microservice later)
│   └── Uses Elasticsearch (already distributed)
│
├── Shared Library
│   ├── Event bus (in-process, message queue compatible)
│   ├── Logging
│   ├── Metrics
│   └── Caching
│
└── API Gateway (routing, auth)
```

**Design Decisions & Trade-offs:**

1. **Async communication** via in-process event bus
   - Pro: Loose coupling, eventual consistency
   - Con: Testing eventual consistency harder
   - Solution: Inbox pattern for reliability

2. **Shared database** vs. per-service database
   - Pro: ACID transactions, simpler queries
   - Con: tighter coupling
   - Solution: Will migrate to DB-per-service as they scale

3. **Code sharing** via shared libraries
   - Pro: DRY, consistent patterns
   - Con: Can become monolithic again
   - Solution: Strict library governance, semantic versioning

### Implementation

**Event-driven architecture:**
```python
# Define events
class OrderCreated(Event):
    order_id: str
    customer_id: str
    total_amount: float
    items: List[OrderItem]

# Publish events
@router.post("/orders")
async def create_order(order_data: OrderCreate):
    order = await order_service.create(order_data)
    
    # Async: Alert inventory
    event_bus.publish(OrderCreated(
        order_id=order.id,
        customer_id=order.customer_id,
        ...
    ))
    
    return order

# Subscribe to events
@event_bus.subscribe(OrderCreated)
async def on_order_created(event: OrderCreated):
    # Inventory service processes independently
    await inventory_service.reserve_items(event.items)
    
    # Send confirmation email
    await email_service.send_confirmation(event.customer_id)
```

**Testing strategy:**
```python
# Unit test: Order service
def test_order_creation():
    order = order_service.create({...})
    assert order.status == "pending"

# Integration test: Order + Inventory interaction
@pytest.mark.asyncio
async def test_order_triggers_inventory_reservation():
    # Setup
    event_log = []
    event_bus.subscribe(OrderCreated, lambda e: event_log.append(e))
    
    # Act
    order = await order_service.create({...})
    
    # Wait for async
    await asyncio.sleep(0.5)
    
    # Assert
    assert len(event_log) == 1
    assert event_log[0].order_id == order.id
```

### Impact

**Deployment:**
- Before: 45 minutes (full regression testing)
- After: 5 minutes (service-specific tests only)
- Improvement: 9x faster

**Testing:**
- Before: 6 hours full suite, 500+ developers blocked
- After: 15 minutes per service, parallel testing
- Improvement: 24x faster

**Scaling:**
- Before: Scale monolith = 500GB memory + all services
- After: Scale only Search Service = isolated to 10GB
- Improvement: 50x better resource efficiency

**Reliability:**
- Before: One deploy bug = entire app down
- After: Graceful degradation, circuit breakers
- Improvement: 99.5% → 99.95% uptime

**Team Productivity:**
- Before: 20+ teams coordinating
- After: Teams independent, own services end-to-end
- Improvement: 40% faster feature delivery

### Evolution Path

**Phase 1 (Done):** Modular monolith
**Phase 2 (Next):** Extract Search as true microservice
**Phase 3:** Extract Payment, Analytics as independent services
**Phase 4:** Full microservices (when needed)

---

## Summary

These four experiences demonstrate my approach to building enterprise systems:

1. **Distributed Systems Thinking**: Multiple scales, multiple trade-offs
2. **Pro-Active Risk Management**: Circuit breakers, fallbacks, redundancy
3. **Data-Driven Decisions**: Profile first, optimize second
4. **Pragmatic Architecture**: Choose simplicity until you need complexity

All four projects involved:
- ✅ Scaling from single instance to distributed systems
- ✅ Production incident response and recovery
- ✅ 10x+ performance improvements
- ✅ Team coordination and knowledge transfer
- ✅ Technical decision-making with trade-offs
