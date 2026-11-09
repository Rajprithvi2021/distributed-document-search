# Deployment Guide

## 1. Local full stack (Docker Compose)

Give Docker at least **4 GB of memory** (Elasticsearch alone runs with a 1 GB heap).

```bash
docker compose up --build -d --wait   # gateway :8000 → 2 API replicas → Elasticsearch + Redis, plus indexer;
                                      # --wait returns once every healthcheck passes (first run: ~1–2 min)
bash scripts/smoke_test.sh            # or: powershell -File scripts\smoke_test.ps1
```

| Variant | Command |
|---|---|
| Async indexing (202 + Redis Streams + worker) | `INDEXING_MODE=async docker compose up --build -d` |
| API-key auth | `AUTH_MODE=api_key API_KEYS='{"key-a":"tenant-a"}' docker compose up -d` (add `API_KEYS` to `x-app-env`) |
| Benchmark without throttling | `RATE_LIMIT_ENABLED=false docker compose up -d`, and raise the gateway's per-IP `limit_req` `rate` in `deploy/nginx/nginx.conf` (a load generator is one IP, so the edge limit would otherwise cap it) |
| More API replicas | `docker compose up -d --scale api=4` (the nginx resolver picks them up within 10 s) |
| Tear down incl. data | `docker compose down -v` |

Ports are bound to `127.0.0.1` for Elasticsearch (9200) and Redis (6379) so they're never exposed on the LAN. Change the default passwords through `ELASTIC_PASSWORD` / `REDIS_PASSWORD`. The gateway serves `/health*`, `/metrics` and the OpenAPI docs only to loopback and private-network clients.

## 2. Blue-green with Docker Compose (bonus)

`deploy/blue-green/` runs two API colours behind one nginx. `switch.sh` performs a **health-gated** cut-over:

1. polls `api-<target>:8000/health/ready` until the new colour has started (aborts with traffic unchanged otherwise);
2. smoke-tests the target directly with a real search, which proves its Elasticsearch and Redis connections;
3. rewrites `upstreams/active.conf` and validates it with `nginx -t` (restores the previous file on failure);
4. `nginx -s reload` is a graceful reload: in-flight requests finish on the old colour while new requests go to the new one.

```bash
cd deploy/blue-green
docker build -t docsearch-api:2.0 ../..
BLUE_TAG=docsearch-api:2.0 GREEN_TAG=docsearch-api:2.0 GREEN_VERSION=2.1.0 \
  docker compose -f docker-compose.bluegreen.yml up -d
./switch.sh green     # cut over (seconds)
./switch.sh blue      # instant rollback
```

Both colours share Elasticsearch and Redis. This works because the API is stateless and index changes are **expand/contract**: a new mapping goes into a new index version behind the alias, and old fields stay readable until both colours have moved on.

## 3. Kubernetes

```bash
kubectl apply -f deploy/k8s/base.yaml        # namespace, config, secret (example), service, indexer, PDB
kubectl apply -f deploy/k8s/api-blue.yaml    # live colour
kubectl apply -f deploy/k8s/api-green.yaml   # candidate colour
kubectl -n docsearch rollout status deploy/docsearch-api-green
kubectl -n docsearch port-forward deploy/docsearch-api-green 8080:8000 &  # smoke-test the candidate
bash scripts/smoke_test.sh http://localhost:8080
# cut over: flip the Service selector
kubectl -n docsearch patch service docsearch-api -p '{"spec":{"selector":{"app":"docsearch-api","color":"green"}}}'
# rollback
kubectl -n docsearch patch service docsearch-api -p '{"spec":{"selector":{"app":"docsearch-api","color":"blue"}}}'
```

Zero-downtime checklist (already in the manifests):

| Mechanism | Setting |
|---|---|
| No capacity dip during rollouts | `maxUnavailable: 0`, `maxSurge: 1` |
| Traffic only to ready pods | readiness → `/health/ready`: started and not draining. Deliberately **no** dependency checks: ES and Redis are shared, so a dependency-based probe would pull every pod out at once during an ES outage, and clients would get gateway errors instead of stale results and clean 503s. `/health` (503 when ES is down) feeds monitoring and alerting instead. Liveness → `/health/live` |
| Clean drain | readiness fails as soon as shutdown starts, then `preStop: sleep 5` (LB deregistration) + uvicorn `--timeout-graceful-shutdown 20` + `terminationGracePeriodSeconds: 30` |
| Voluntary-disruption floor | `PodDisruptionBudget minAvailable: 2` |
| AZ failure tolerance | `topologySpreadConstraints` on `topology.kubernetes.io/zone` |
| Autoscaling | HPA on CPU (65%), 3–30 replicas (in the manifests); scaling workers on queue lag with KEDA is planned, not in the manifests |
| Least privilege | non-root UID 10001, `readOnlyRootFilesystem`, no privilege escalation |

For progressive delivery, replace the manual selector flip with Argo Rollouts (blue-green or canary with Prometheus-driven automated analysis on 5xx rate and p95 latency).

## 4. Index changes without downtime

1. Create `docs-{tenant}-v2` with the new mapping/settings.
2. `_reindex` from v1, or replay from the system of record / Kafka.
3. Atomically move the alias: `POST _aliases {"actions":[{"remove":{"index":"docs-t-v1","alias":"docs-t"}},{"add":{"index":"docs-t-v2","alias":"docs-t","is_write_index":true}}]}`.
4. Keep v1 until the release is confirmed, then delete it.

## 5. Backup and disaster recovery

| Data | Mechanism | Target |
|---|---|---|
| Elasticsearch | SLM snapshots to S3 every 30 min (and daily retained 30 days); cross-cluster replication to the DR region for critical tenants | RPO ≤ 30 min (≈ 0 with CCR), RTO ≤ 1 h |
| System of record (production) | DB point-in-time recovery + versioned object storage | RPO ≤ 5 min |
| Redis | AOF (queue durability); cache and buckets are rebuildable | – |
| Kafka (production) | RF=3, 7-day retention: indexes can be rebuilt by replay | – |

Example SLM policy:

```json
PUT _slm/policy/every-30-min
{ "schedule": "0 */30 * * * ?", "name": "<docs-{now/m}>", "repository": "s3-backups",
  "config": { "indices": ["docs-*"], "include_global_state": false },
  "retention": { "expire_after": "30d", "min_count": 48, "max_count": 2000 } }
```

Restores are rehearsed quarterly: restore into a scratch cluster, run the smoke test, record the achieved RTO.
