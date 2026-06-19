# Scale Plan — Signals Service at 10 000 RPS

## 1. Data Model & Indexes

### Current schema
```sql
CREATE TABLE signals (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id          TEXT NOT NULL,
  type             TEXT NOT NULL,
  payload          TEXT NOT NULL,
  idempotency_key  TEXT UNIQUE,          -- DB-level uniqueness = atomic idempotency
  created_at       INTEGER NOT NULL
);
CREATE INDEX idx_user_created ON signals(user_id, created_at);
```

### At scale
- **Partition** the `signals` table by `user_id` hash (16–64 shards) so each shard handles ~600 RPS, well within PostgreSQL's per-connection throughput.
- **Idempotency keys** move to a dedicated `idempotency_keys` table (short TTL, 24 h) with a `UNIQUE (key)` constraint — keeps the hot write path narrow.
- **Archive** `signals` older than 90 days to cold storage (S3 Parquet) via a nightly pg_dump partition swap.
- **Connection pool**: PgBouncer in transaction mode, 20 connections per app pod → 2 000 concurrent queries.

---

## 2. Idempotency Across Instances

### Single-instance (current)
`idempotency_key TEXT UNIQUE` in SQLite provides an atomic guarantee via the database engine's B-tree lock. Concurrent inserts with the same key will have exactly one succeed; the loser gets `SQLITE_CONSTRAINT_UNIQUE`, then reads and returns the winner's row.

### Multi-instance (production)
1. **Shared relational DB** (PostgreSQL): same `UNIQUE` constraint — each node issues `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING *` (atomic upsert). If `RETURNING` is empty, the caller does `SELECT … WHERE idempotency_key = ?`. This eliminates all check-then-insert races across nodes.
   ```sql
   INSERT INTO signals (user_id, type, payload, idempotency_key, created_at)
   VALUES ($1,$2,$3,$4,$5)
   ON CONFLICT (idempotency_key) DO NOTHING
   RETURNING *;
   ```
2. **Idempotency-key TTL**: expire keys after 24 h using a `expires_at` column + periodic DELETE cron.
3. **In-flight deduplication** (optional, defence-in-depth): Redis `SET NX PX 10000` on the idempotency key → only one node proceeds with the DB write during the 10 s window. All others wait briefly then fall through to the `SELECT` path.

---

## 3. Rate Limiting Across Instances

### Single-instance (current)
In-process `Map<userId, {windowStart, count}>` — O(1), zero latency, safe within Node's single-threaded event loop.

### Multi-instance (production)
Replace the in-memory map with **Redis** using the atomic sliding-window pattern:

```lua
-- Lua script executed atomically in Redis
local key = KEYS[1]          -- e.g. "rl:user123"
local now  = tonumber(ARGV[1])
local win  = tonumber(ARGV[2])  -- 60000 ms
local lim  = tonumber(ARGV[3])  -- 5

redis.call('ZREMRANGEBYSCORE', key, 0, now - win)
local count = redis.call('ZCARD', key)
if count < lim then
  redis.call('ZADD', key, now, now .. math.random())
  redis.call('PEXPIRE', key, win)
  return 1   -- allowed
end
return 0     -- rate-limited
```

- **Sorted-set sliding window** — accurate across all app nodes.
- Redis EVAL guarantees atomicity (no TOCTOU race).
- Fallback: if Redis is unavailable, fail-open (allow request) and log the miss.
- Alternative: **token-bucket** via Redis + Lua for smoother bursting.

---

## 4. Observability — Logs / Metrics / Alerts

| Layer | Tool | Key signals |
|-------|------|-------------|
| Structured logs | `pino` (Fastify default) → stdout → Datadog / CloudWatch | Request id, userId, idempotency key, latency, status |
| Metrics | `prom-client` sidecar → Prometheus | `http_requests_total{status}`, `rate_limit_hits_total`, `db_retries_total`, `db_errors_total`, p50/p95/p99 latency |
| Distributed traces | OpenTelemetry → Jaeger / Tempo | Full request trace incl. DB query spans |
| Alerts | PagerDuty via Prometheus AlertManager | p99 latency > 200 ms, error rate > 1%, rate-limit spike > 10× baseline, DB connection exhaustion |

---

## 5. Failure Modes & Retry Strategy

### DB transient failures (SQLITE_BUSY / connection timeout)
- **Retry helper** in `signals.js`: up to 3 attempts, exponential backoff starting at 50 ms, full jitter (`delay + rand(0, delay)`), gives max wait of ~350 ms before surfacing 503.
- Idempotency guarantee during retries: because the UNIQUE constraint is at the DB level, a retried `INSERT` either succeeds once or gets `CONSTRAINT_UNIQUE` — **never creates a duplicate row**.

### DB hard failure (node crash, failover)
- PostgreSQL streaming replication (1 primary, 2 replicas). Automatic failover via `pg_auto_failover` or Amazon RDS Multi-AZ. Typical failover time: 20–30 s.
- During failover window: app returns `503 db_unavailable`; clients with idempotency keys retry safely.

### Partial outages
- **Circuit breaker** (e.g. `opossum`) around DB calls: open after 5 consecutive failures, half-open after 30 s.
- Redis failure for rate-limiting: fail-open — log the miss, allow the request, alert the on-call team.

### Payload validation
- Bad requests (missing `userId`, `type`, `payload`) rejected at 400 before any DB/Redis touch.

---

## 6. 10 000 RPS Architecture Sketch

```
                      ┌──────────────────────────┐
                      │   AWS ALB / Cloudflare   │  ← TLS termination, DDoS shield
                      └─────────────┬────────────┘
                                    │ 10 000 req/s
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
        ┌─────▼──────┐       ┌──────▼─────┐       ┌──────▼─────┐
        │  App Pod 1  │       │  App Pod 2  │       │  App Pod N  │
        │ Fastify/Node│       │ Fastify/Node│  ...  │ Fastify/Node│
        │  4 vCPU     │       │  4 vCPU     │       │  4 vCPU     │
        └──────┬──────┘       └──────┬──────┘       └──────┬──────┘
               │                     │                     │
        ┌──────▼─────────────────────▼─────────────────────▼──────┐
        │              Redis Cluster (rate limit + idem TTL)        │
        │         3 primaries × 3 replicas  (~1M ops/s)             │
        └──────────────────────────┬───────────────────────────────┘
                                   │
        ┌──────────────────────────▼───────────────────────────────┐
        │           PgBouncer (transaction pool, 200 connections)   │
        └──────────────────────────┬───────────────────────────────┘
                                   │
        ┌──────────────────────────▼───────────────────────────────┐
        │          PostgreSQL Primary (r6g.4xlarge, 16 vCPU)        │
        │  + 2 Read Replicas for GET /v1/signals                    │
        └──────────────────────────────────────────────────────────┘
```

### Sizing at 10 000 RPS

| Component | Count | Instance | Est. cost/mo |
|-----------|-------|----------|--------------|
| App pods (EKS) | 10 × (auto-scaled) | `c6i.xlarge` (4 vCPU, 8 GB) | ~$700 |
| Redis Cluster | 3+3 nodes | `cache.r6g.large` | ~$450 |
| PostgreSQL primary | 1 | `db.r6g.4xlarge` (RDS Multi-AZ) | ~$800 |
| PgBouncer sidecar | per pod | within pod | ~$0 |
| ALB + data transfer | — | — | ~$200 |
| **Total** | | | **~$2 150/mo** |

### Throughput math
- Each Node.js pod (single-threaded): ~1 500–2 000 RPS for I/O-bound workloads.
- 7 pods × 1 500 = **10 500 RPS** with 3 pods spare for rolling deployments.
- PostgreSQL with PgBouncer: 200 connections × ~50 TPS per connection = **10 000 writes/s** comfortably.
- Redis: single node handles ~200 000 ops/s; cluster is overkill but gives HA.

### Horizontal scaling trigger
- HPA on `cpu > 60%` or `requests_in_flight > 1 200` per pod.
- Scale-down cool-down: 5 minutes to avoid thrashing.

---

## 7. Deployment & Zero-Downtime

- **Kubernetes** rolling updates (`maxUnavailable: 0`, `maxSurge: 1`).
- DB migrations run as a pre-deploy Kubernetes Job (`migrate.js`) with idempotent SQL (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX CONCURRENTLY`).
- **Feature flags** (LaunchDarkly) to gate new endpoints without re-deploy.
