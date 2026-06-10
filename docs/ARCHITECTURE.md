# Flint — Architecture Document
> *Quietly igniting your payload, every job has a spark.*

---

## Table of Contents

1. [System Overview](#system-overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Component Breakdown](#component-breakdown)
4. [Data Flow](#data-flow)
5. [Database Design](#database-design)
6. [Heap-Based Priority Queue](#heap-based-priority-queue)
7. [Timing Wheel — Alternative Algorithm](#timing-wheel--alternative-algorithm)
8. [Algorithm Tradeoffs & Benchmark](#algorithm-tradeoffs--benchmark)
9. [DAG Workflow Engine](#dag-workflow-engine)
10. [Worker Architecture](#worker-architecture)
11. [Retry & Backoff System](#retry--backoff-system)
12. [Dead Letter Queue](#dead-letter-queue)
13. [Starvation Prevention](#starvation-prevention)
14. [Cancellation Design](#cancellation-design)
15. [Duplicate Protection](#duplicate-protection)
16. [Recurring Jobs](#recurring-jobs)
17. [Live Updates — SSE](#live-updates--sse)
18. [Logging Architecture](#logging-architecture)
19. [Security](#security)
20. [Infrastructure Overview](#infrastructure-overview)

---

## System Overview

Flint is a background job scheduling system. It accepts jobs from a REST API, queues them by priority and schedule, processes them through independent workers, and tracks every state transition. It is built to handle failure — retries, dead letters, and alerting are first-class concerns, not afterthoughts.

Flint is composed of four independently running processes that share a PostgreSQL database and a Redis instance:

- **API server** — accepts and exposes job data over HTTP
- **Worker processes** — poll the queue and execute jobs
- **Scheduler process** — watches for due jobs and manages recurring execution and priority aging
- **Frontend** — Next.js dashboard that reflects system state in real-time

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Client (Browser)                           │
│                     Next.js — flint.muizzyranking.com               │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ HTTPS
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                            Nginx                                    │
│                     Reverse Proxy + SSL                             │
  api.flint.muizzyranking.com → :8000    flint.muizzyranking.com → :3000    │
└────────────────┬──────────────────────────────────┬─────────────────┘
                 │                                  │
                 ▼                                  ▼
┌───────────────────────────┐          ┌────────────────────────────┐
│       FastAPI App         │          │       Next.js App          │
│       Port :8000          │          │       Port :3000           │
│                           │          │                            │
│  REST API (versioned)     │          │  Dashboard                 │
│  SSE Stream               │          │  Jobs Table                │
│  Swagger Docs             │          │  Create Job Form           │
│  Auth (API Key)           │          │  DLQ View                  │
└──────────┬────────────────┘          │  Settings Panel            │
           │                           │  Bin / Restore             │
           │                           │  Logs Viewer               │
           │                           │  Benchmark View            │
           │                           └────────────────────────────┘
           │
    ┌──────┴──────────────────────────────────────┐
    │                                             │
    ▼                                             ▼
┌────────────────┐                    ┌───────────────────────┐
│  PostgreSQL    │                    │        Redis          │
│  Primary Store │                    │  Priority Queue       │
│                │                    │  Pub/Sub (SSE)        │
│  jobs          │                    │  Worker Registry      │
│  job_deps      │◄───────────────────│  Strategy Config      │
│  job_logs      │                    └───────────────────────┘
│  settings      │                              ▲
└────────┬───────┘                              │
         │                                      │
         │         ┌────────────────────────────┘
         │         │
         ▼         ▼
┌──────────────────────────────────────────────────────────┐
│                     Worker Processes                     │
│                                                          │
│   Worker-1                       Worker-2                │
│   ┌─────────────────┐            ┌─────────────────┐    │
│   │ Poll Queue      │            │ Poll Queue      │    │
│   │ Claim Job (DB)  │            │ Claim Job (DB)  │    │
│   │ Execute Handler │            │ Execute Handler │    │
│   │ Update Status   │            │ Update Status   │    │
│   │ Publish Event   │            │ Publish Event   │    │
│   └─────────────────┘            └─────────────────┘    │
└──────────────────────────────────────────────────────────┘
         ▲
         │
┌────────┴──────────────┐
│   Scheduler Process   │
│                       │
│  Due Job Loop         │  ← pushes scheduled jobs to queue when due
│  Aging Loop           │  ← decrements effective_priority on old jobs
│  Recurrence Handler   │  ← creates next run for recurring jobs
└───────────────────────┘
```

---

## Component Breakdown

### FastAPI Application

The API server is the entry point for all external interaction. It is responsible for:

- Receiving job creation requests and writing them to PostgreSQL
- Exposing job status, logs, DLQ, and settings over REST
- Streaming real-time job events to connected clients via SSE
- Authenticating all requests via API key
- Providing Swagger documentation at `/api/v1/docs`

The API server does **not** process jobs. It writes jobs to the database and returns. All execution happens asynchronously in the worker processes.

### Worker Processes

Workers run independently. They share no memory with the API server — their only communication channel is the PostgreSQL database and Redis queue.

Each worker:
- Polls the active queue (heap or timing wheel, based on current `scheduler_strategy` setting) for the next job
- Atomically claims the job in PostgreSQL
- Executes the appropriate handler
- Checks a cooperative cancellation flag at key checkpoints
- Updates job status and publishes an SSE event on completion or failure
- Handles retries and DLQ transitions

Two workers run by default. They operate independently and cannot pick up the same job due to the atomic claim mechanism.

### Scheduler Process

The scheduler is a single long-running process with two internal loops:

**Due Job Loop** — runs every second. Queries PostgreSQL for `pending` jobs where `scheduled_at <= NOW()` and whose `worker_id IS NULL`. Pushes eligible jobs onto the active Redis queue. This is how both future-scheduled and immediate jobs enter the execution pipeline.

**Aging Loop** — runs every 30 seconds. Finds jobs that have been waiting longer than their priority threshold and decrements their `effective_priority`. This prevents low-priority jobs from waiting indefinitely.

### PostgreSQL

The primary source of truth. All job state lives here. Redis is a cache and coordination layer — PostgreSQL is what is trusted.

Key design decisions:
- UUID primary keys throughout for safe distributed generation
- Soft delete via `deleted_at` — data is never lost by default
- `effective_priority` stored as a float column so the aging process can update it in-place
- `cancellation_requested` flag on the jobs table for cooperative cancellation
- `worker_id` on the jobs table acts as an optimistic lock for duplicate protection

### Redis

Redis serves three purposes:

1. **Priority queue backing store** — a sorted set (`flint:queue`) stores job IDs scored by `effective_priority`. Workers pop from this set to know which job to claim next.
2. **Pub/Sub channel** — the `flint:events` channel carries job status change events to the SSE endpoint, which forwards them to connected browser clients.
3. **Worker registry** — each worker writes a heartbeat key (`flint:worker:<id>`) with a TTL. The API reads these keys to report active workers.

### Mailhog

A local SMTP server that catches all outgoing email. Used as the mock external email service for the `send_email` job handler and for DLQ alert emails. Its web UI (port 8025) allows inspecting delivered emails during development.

---

## Data Flow

### Job Creation Flow

```
Client POST /api/v1/jobs
    ↓
FastAPI validates request
    ↓
Job written to PostgreSQL (status: pending, scheduled_at set)
    ↓
If dependency_ids provided → rows inserted into job_dependencies
    ↓
Job is NOT pushed to queue yet
    ↓
Scheduler's due-job loop picks it up when scheduled_at <= NOW()
  AND all dependencies are completed (or no dependencies exist)
    ↓
Job pushed to Redis sorted set (flint:queue)
    ↓
Worker pops job_id from Redis
    ↓
Worker atomically claims job in PostgreSQL
    ↓
Handler executes
    ↓
Status updated → event published to Redis pub/sub
    ↓
SSE endpoint broadcasts to connected browser clients
    ↓
Frontend updates without page refresh
```

### Retry Flow

```
Handler raises exception
    ↓
Worker increments retry_count
    ↓
retry_count < max_retries?
  YES → calculate backoff delay with jitter
        set status = pending, next_retry_at = now + delay
        job re-enters queue after delay
  NO  → send to DLQ (status = failed, is_dlq = true)
        check DLQ count against threshold
        if count >= threshold → fire alert email
```

### DAG Flow

```
Job C depends on Job B which depends on Job A

Job A created → status: pending (in queue)
Job B created → status: pending (NOT in queue, waiting on A)
Job C created → status: pending (NOT in queue, waiting on B)

Job A completes
    ↓
DAG service checks: who depends on A?
    → Job B. Are all of B's dependencies complete? Yes.
    → Push Job B onto queue

Job B completes
    ↓
DAG service checks: who depends on B?
    → Job C. Are all of C's dependencies complete? Yes.
    → Push Job C onto queue

Job C completes → DAG workflow done
```

---

## Database Design

### Entity Relationships

```
jobs (1) ──────────────────── (many) job_dependencies
  job_id = jobs.id (the waiting job)
  depends_on_id = jobs.id (the prerequisite job)

jobs (1) ──────────────────── (many) job_logs
  job_id = jobs.id

settings (standalone key-value store)
```

### Key Design Decisions

**Why `effective_priority` as a separate column from `priority`?**
The raw `priority` (1, 2, 3) is the user-assigned value. `effective_priority` is the scheduler's working value — it starts equal to `priority` and is decremented by the aging process. Keeping them separate means that the original priority can always be reported while the scheduler works with the aged value internally.

**Why soft delete?**
Job history is valuable. Soft delete lets the system maintain a bin with restore capability while keeping normal queries clean via `WHERE deleted_at IS NULL`.

**Why JSONB for `payload`?**
Job payloads are handler-specific and vary in structure. JSONB gives full flexibility without requiring a schema change for each new handler type. PostgreSQL's JSONB indexing also allows future querying on payload fields if needed.

**Why store `interval_seconds` as a bigint?**
Recurring intervals are stored in seconds regardless of how they were expressed (minutes, hours, days, etc). This eliminates parsing on every recurrence calculation. The conversion happens once at job creation.

---

## Heap-Based Priority Queue

### What a Min-Heap Is

A min-heap is a complete binary tree where every parent node has a value less than or equal to its children. In Python's `heapq` module, this is implemented as a list where for any element at index `i`, its children are at indices `2i+1` and `2i+2`.

The invariant means the smallest element is always at index 0 — accessible in O(1). Inserting a new element or removing the minimum takes O(log n) due to the "sift" operation that restores the heap invariant.

### How Flint Uses the Heap

Flint's heap stores entries as tuples:

```
(effective_priority, scheduled_at, created_at, job_id)
```

Python compares tuples lexicographically left-to-right. So `heappop` always returns the entry with:
1. The lowest `effective_priority` (most urgent by priority)
2. Ties broken by earliest `scheduled_at` (longest waiting job wins)
3. Ties broken by earliest `created_at` (oldest job wins)
4. Ties broken by `job_id` (deterministic, not meaningful)

### Mutable Priorities — Lazy Deletion

The heap does not support O(1) removal or re-keying of arbitrary elements. When the aging process lowers a job's `effective_priority`, or when a job is cancelled, we cannot efficiently find and update its position in the heap.

Flint uses the **lazy deletion pattern**:
- When a job needs to be removed or re-scored, its heap entry is marked as `__removed__` in place
- A new entry with the updated score is pushed onto the heap
- On `pop`, entries marked `__removed__` are silently discarded until a valid entry is found

This means the heap may temporarily contain stale entries, but correctness is maintained because valid entries always shadow stale ones.

### Redis Sorted Set as Persistent Backing

The in-memory heap is ephemeral — it is rebuilt when a worker process restarts. The Redis sorted set (`flint:queue`) acts as the persistent queue. When a worker starts, it loads pending job IDs from Redis and reconstructs its local heap.

The Redis sorted set uses `effective_priority` as the score. `ZADD` for insert is O(log n). `ZRANGE ... LIMIT 1` for peek is O(log n). `ZREM` for consume is O(log n). This is consistent with heap performance.

---

## Timing Wheel — Alternative Algorithm

### What a Timing Wheel Is

A timing wheel is a circular array of time slots. Each slot holds a list of jobs due at that time. A pointer advances by one slot every tick (e.g. every second). Jobs in the current slot are executed, and the pointer moves on.

```
Slot 0  [job_A, job_B]
Slot 1  []
Slot 2  [job_C]
Slot 3  []
...
Slot 3599 [job_Z]
   ↑
current pointer (advances 1 slot per second)
```

When the pointer advances past slot 3599 it wraps back to slot 0 — hence "wheel".

### Flint's Timing Wheel Design

- **Wheel size:** 3600 slots (1 hour of second-resolution coverage)
- **Tick interval:** 1 second
- **Overflow:** Jobs scheduled more than 3600 seconds in the future are held in an overflow dictionary keyed by `scheduled_at` timestamp. Each tick, the scheduler checks if any overflow jobs have come within the wheel's range and inserts them.
- **Priority within a slot:** Jobs in the same slot are sorted by `effective_priority`. The timing wheel does not natively support cross-slot priority ordering — it is purely time-ordered at the slot level.

### Why This Is the Right Alternative

The timing wheel is architecturally different from the heap. The heap prioritises by urgency score; the timing wheel prioritises by time. They excel at different things. Benchmarking them against each other reveals a genuine tradeoff, not just an implementation difference.

---

## Algorithm Tradeoffs & Benchmark

### Theoretical Comparison

| Property | Heap | Timing Wheel |
|---|---|---|
| Insert | O(log n) | O(1) |
| Pop next job | O(log n) | O(1) amortized |
| Re-score (aging) | O(log n) | O(n) worst case |
| Priority ordering | Exact, global | Per-slot only |
| Far-future scheduling | Natural | Requires overflow dict |
| Memory | O(n) | O(W + n), W = wheel size |
| Starvation prevention | Natural via score | Requires explicit priority sort per slot |
| Best workload | Priority-heavy, mixed scheduling | High-volume, time-based, similar priorities |

### Expected Benchmark Results

At `n = 10,000` jobs with random priorities and scheduled times:

The timing wheel is expected to win on raw insert and pop throughput because O(1) insert vs O(log n) is a real difference at scale. At n=10,000, log(10,000) ≈ 13. At n=1,000,000, log(1,000,000) ≈ 20. The gap widens at scale.

However, when re-scoring is frequent (as it is in Flint due to aging), the timing wheel's O(n) worst-case removal cost erodes its throughput advantage. The heap's lazy deletion pattern handles re-scoring efficiently.

**Conclusion:** The heap is the correct primary algorithm for Flint because priority ordering and re-scoring are first-class requirements. The timing wheel would win in a pure time-based scheduling workload with uniform priorities.

### Running the Benchmark

```bash
# Run the benchmark script directly
python -m benchmark.runner --n 10000

# Or via the API endpoint
POST /api/v1/benchmark/run
{ "n": 10000, "algorithm": "both" }
```

### Switching Algorithms at Runtime

The active scheduling algorithm is stored in the `settings` table under the key `scheduler_strategy`. Workers and the scheduler read this setting on every poll cycle. Switching from `heap` to `timing_wheel` (or back) via `PATCH /api/v1/settings` takes effect on the next poll cycle — no restart required. Jobs currently in the queue remain there; the new strategy governs job selection going forward.

---

## DAG Workflow Engine

### What a DAG Is

A Directed Acyclic Graph (DAG) is a graph where edges have direction and there are no cycles. In Flint, nodes are jobs and edges are dependencies. "Job B depends on Job A" means there is a directed edge from A to B. Acyclic means you cannot have A depend on B while B depends on A — directly or transitively.

### How Flint Implements It

Dependencies are stored in the `job_dependencies` table:

```
job_id        → the job that is waiting
depends_on_id → the job that must complete first
```

A job with dependencies is created with `status = pending` but is **not pushed to the queue**. It becomes eligible for the queue only when all its `depends_on_id` jobs have `status = completed`.

After every job completion, the DAG service:
1. Queries for all jobs that list the completed job as a dependency
2. For each dependent, counts how many of its total dependencies are now `completed`
3. If the count equals the total dependency count, the job is unblocked and pushed to the queue

### Cascade Failure

When a job fails permanently (exhausts retries, goes to DLQ):
- All jobs that depend on it (directly or transitively) are marked `cancelled`
- The cancellation reason is recorded: `"Cancelled: dependency job <id> failed permanently"`
- The cascade recurses through the full downstream graph

### Cascade Retry (Option A)

When a DLQ job that has downstream dependents is manually retried:
- The job is reset to `pending` with `retry_count = 0`
- All downstream `cancelled` jobs whose `last_error` contains `"dependency"` are also reset to `pending`
- The cascade recurses through the full downstream graph
- Once the retried root job completes, the normal DAG unblocking flow resumes

This means retrying a root job automatically re-arms the entire workflow without manual intervention on each dependent job.

### Cycle Prevention

The API validates at job creation time that adding the specified `dependency_ids` would not create a cycle. This is done via a depth-first search from each proposed dependency back through its own dependencies, checking if the new job's ID would be encountered.

---

## Worker Architecture

### Independence

Workers share no memory with the API server or with each other. They communicate exclusively through:
- **PostgreSQL** — reading job state, writing status updates
- **Redis** — reading from the priority queue, publishing SSE events, writing heartbeats

This means workers can be scaled, restarted, or replaced without affecting the API server.

### Poll Loop

Each worker runs a tight async loop:

```
while running:
    job_id = pop from Redis queue
    if job_id is None:
        sleep(WORKER_POLL_INTERVAL)
        continue

    claimed = atomic_claim(job_id)
    if not claimed:
        continue    ← another worker got it

    check cancellation flag
    execute handler
    update status
    publish SSE event
    handle recurrence if applicable
```

### Graceful Shutdown

Workers listen for `SIGTERM` and `SIGINT`. On receiving either:
1. Stop accepting new jobs from the queue
2. Allow the current job to complete or reach its next cancellation checkpoint
3. Deregister from Redis worker registry
4. Exit cleanly

This ensures no job is left in `processing` state with no worker responsible for it.

### Worker Registry

Each worker writes a heartbeat to Redis every 30 seconds:

```
SET flint:worker:<worker_id> "active" EX 60
```

The key expires after 60 seconds. If a worker dies without deregistering, its key expires and the API's worker list reflects it as gone within 60 seconds. The scheduler has a cleanup process that resets any jobs with a `worker_id` pointing to a dead worker back to `pending`.

---

## Retry & Backoff System

### Backoff Formula

```
delay = 5^(attempt - 1) * random(0.5, 1.5)

Attempt 1: 5^0 = 1  × jitter → ~0.5s to ~1.5s
Attempt 2: 5^1 = 5  × jitter → ~2.5s to ~7.5s
Attempt 3: 5^2 = 25 × jitter → ~12.5s to ~37.5s
```

The jitter multiplier (`random(0.5, 1.5)`) is "full jitter" — it prevents thundering herd problems where many failed jobs retry at exactly the same time and overload the downstream service.

### Why 3 Retries

Three retries covers the most common transient failure patterns (network blip, temporary service outage, rate limiting) without allowing a broken job to consume queue capacity indefinitely. After three attempts, the failure is considered systematic and moves to the DLQ for human inspection.

### Max Retries is Configurable Per Job

The `max_retries` field on the job defaults to 3 but can be set per job at creation time. The retry logic always reads `job.max_retries`, not a hardcoded constant.

---

## Dead Letter Queue

The DLQ is not a separate table. It is a view of the `jobs` table where `is_dlq = true`. This keeps the data model simple and means DLQ jobs retain their full history (all logs, retry counts, original payload).

### DLQ Threshold Alert

The threshold is stored in `settings.dlq_threshold` (default: 5). After every DLQ insertion, the system counts all current DLQ jobs. If the count meets or exceeds the threshold, a Jinja-rendered HTML email is sent to all addresses in `settings.alert_emails`.

The alert email includes:
- Current DLQ count and configured threshold
- A table of the most recent 10 DLQ jobs: ID, type, error message, retry count, failure time
- A direct link to the DLQ dashboard

The threshold and recipient list are configurable at runtime via `PATCH /api/v1/settings` with no restart required.

---

## Starvation Prevention

### The Problem

Without starvation prevention, a sustained flow of high-priority jobs (priority 1) can indefinitely block medium (priority 2) and low (priority 3) jobs from ever executing, even if those jobs have been waiting for hours.

### Flint's Solution — Priority Aging

The aging process runs every 30 seconds. It decrements `effective_priority` for jobs that have been waiting past their threshold:

```
Medium priority (2) waiting > 2 minutes:
    effective_priority -= 0.1 per aging cycle
    After ~10 cycles (~5 min): effective_priority reaches 1.0 (same as High)

Low priority (3) waiting > 5 minutes:
    effective_priority -= 0.1 per aging cycle
    After ~20 cycles (~10 min total): effective_priority reaches 1.0
```

`effective_priority` is floored at 1.0 — a job can never exceed High priority, but it can reach it.

The heap uses `effective_priority` as its primary sort key. As a low-priority job ages, it rises in the heap and eventually gets picked up. Starvation is mathematically bounded — any job will reach `effective_priority = 1.0` within a predictable time window regardless of queue pressure.

---

## Cancellation Design

### Pending Jobs

A cancellation request on a `pending` job takes effect immediately. The job's status is set to `cancelled` and it is removed from the queue. If it has dependents, cascade cancellation propagates downstream.

### Processing Jobs

**Design decision:** Cooperative cancellation via a database flag.

When a cancellation is requested on a `processing` job:
1. The API sets `cancellation_requested = true` on the job row
2. The worker checks this flag at defined checkpoints within the handler
3. On detecting `true`, the worker stops processing, performs any available cleanup, logs the event, and sets status to `cancelled`

**Documented behaviour:** *"A cancellation request on a processing job is honoured at the next checkpoint within the handler, not immediately. The job may continue executing its current atomic step before stopping. Side effects that have already occurred (e.g. an HTTP request that was already sent) cannot be rolled back."*

### Why Cooperative Over Forceful

Forceful termination (killing the process/thread) risks leaving external resources in inconsistent state — a webhook partially delivered, a file half-written, a database transaction uncommitted. Cooperative cancellation allows the handler to reach a safe stopping point before exiting.

---

## Duplicate Protection

Two workers polling simultaneously may both see the same job ID in the Redis queue. Flint prevents them from both processing it via an atomic SQL UPDATE:

```sql
UPDATE jobs
SET status = 'processing', worker_id = :worker_id, started_at = NOW()
WHERE id = :job_id
  AND status = 'pending'
  AND worker_id IS NULL
  AND deleted_at IS NULL
RETURNING id;
```

PostgreSQL guarantees this UPDATE is atomic at the row level. If two workers execute this query for the same job simultaneously, exactly one will receive a row back. The other receives zero rows and discards the job. No explicit distributed locks, no Redis SETNX, no semaphores — the database atomicity is sufficient and correct.

---

## Recurring Jobs

When a recurring job completes:
1. The worker reads `interval_seconds` from the completed job
2. If non-null, it creates a new job with identical `type`, `payload`, `priority`, and `interval_seconds`
3. The new job's `scheduled_at` is set to `NOW() + interval_seconds`
4. The new job is written to PostgreSQL with `status = pending`
5. The scheduler picks it up when its `scheduled_at` is due

The original job is marked `completed` and retained in history. Each recurrence is a new job with a new UUID. This means the full history of recurring executions is preserved and queryable.

---

## Live Updates — SSE

Server-Sent Events (SSE) is a one-way HTTP protocol where the server streams events to the client over a persistent connection. The client uses the browser-native `EventSource` API.

### Why SSE Over WebSockets

The UI only needs to receive updates from the server — it never needs to push data through the live channel. SSE is the correct tool for one-directional server-to-client streaming. WebSockets would add bidirectional overhead for no benefit.

### Event Flow

```
Worker completes job
    ↓
Worker publishes to Redis: PUBLISH flint:events '{"job_id": "...", "status": "completed"}'
    ↓
FastAPI SSE endpoint is subscribed to flint:events via Redis pub/sub
    ↓
Event forwarded to all connected EventSource clients as:
data: {"job_id": "...", "status": "completed"}
    ↓
Frontend JavaScript updates the jobs table row in place
```

### Nginx SSE Configuration

Nginx buffers responses by default. SSE streams must have buffering disabled to reach the client in real-time:

```nginx
location /api/v1/sse/ {
    proxy_pass http://api:8000;
    proxy_buffering off;
    proxy_cache off;
    proxy_set_header Connection '';
    proxy_http_version 1.1;
    chunked_transfer_encoding on;
}
```

---

## Logging Architecture

All log output is structured JSON produced by `structlog`. Every log entry contains at minimum:

```json
{
  "timestamp": "2026-06-09T10:00:00.000Z",
  "level": "info",
  "event": "job_completed",
  "logger": "worker.processor",
  "job_id": "abc-123",
  "worker_id": "worker-1",
  "duration_ms": 142
}
```

### Outputs

1. **stdout** — captured by Docker and available via `docker logs`
2. **Rotating file** — written to `logs/flint.log`, rotated at 10MB, 5 backups retained

### Log Viewer

The API exposes `GET /api/v1/logs` which reads from the log file and returns paginated, filterable log entries. The frontend has a Logs page that displays these. This gives the visibility into system events without SSH access.

---

## Security

### API Key Authentication

All API endpoints require the `X-API-Key` header. The key is stored in the `.env` file and validated on every request via a FastAPI dependency. Invalid or missing keys return HTTP 401.

The API key is a single shared secret suitable for a backend-to-backend or internal tool context. It is not a multi-tenant user authentication system.

### HTTPS

All traffic is terminated at Nginx with a TLS certificate issued by Let's Encrypt via Certbot. HTTP traffic is redirected to HTTPS. The certificate auto-renews via a cron job.

### No Credentials in Logs

Structlog processors are configured to scrub sensitive fields (e.g. `api_key`, `password`, `smtp_password`) before writing log output.

---

## Infrastructure Overview

```
VPS (Ubuntu 24 LTS)
│
├── Nginx (port 80, 443)
│
├── Docker Engine
│   └── Docker Compose
│       ├── api            (port 8000, internal)
│       ├── worker-1       (no port, internal)
│       ├── worker-2       (no port, internal)
│       ├── scheduler      (no port, internal)
│       ├── postgres       (port 5432, internal)
│       ├── redis          (port 6379, internal)
│       └── mailhog        (port 8025, internal only)
│
├── Certbot (Let's Encrypt SSL)
│   └── cron: 0 0 * * * certbot renew --quiet
│
└── GitHub Actions CI/CD
    └── On push to main:
        SSH into VPS
        git pull origin main
        docker compose up -d --build
```

### Nginx Routing

```nginx
# api.flint.muizzyranking.com → FastAPI
server {
    listen 443 ssl;
    server_name api.flint.muizzyranking.com;

    location / {
        proxy_pass http://api:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    location /api/v1/sse/ {
        proxy_pass http://api:8000;
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 3600s;
    }
}

# flint.muizzyranking.com → Next.js
server {
    listen 443 ssl;
    server_name flint.muizzyranking.com;

    location / {
        proxy_pass http://frontend:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Scaling

To add more workers, add a new service to `docker-compose.yml`:

```yaml
worker-3:
  build: .
  command: python -m worker.worker
  env_file: .env
  environment:
    WORKER_ID: worker-3
```

No configuration changes to the API, scheduler, or database are needed. Workers are stateless and the duplicate protection mechanism handles any number of concurrent workers correctly.
