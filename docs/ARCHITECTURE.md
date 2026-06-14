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
7. [Algorithm Tradeoffs & Benchmark](#algorithm-tradeoffs--benchmark)
8. [DAG Workflow Engine](#dag-workflow-engine)
9. [Worker Architecture](#worker-architecture)
10. [Retry & Backoff System](#retry--backoff-system)
11. [Dead Letter Queue](#dead-letter-queue)
12. [Starvation Prevention](#starvation-prevention)
13. [Cancellation Design](#cancellation-design)
14. [Duplicate Protection](#duplicate-protection)
15. [Recurring Jobs](#recurring-jobs)
16. [Live Updates — SSE](#live-updates--sse)
17. [Worker Registry](#worker-registry)
18. [Logging Architecture](#logging-architecture)
19. [Security](#security)
20. [Infrastructure Overview](#infrastructure-overview)

---

## System Overview

Flint is a background job scheduling system. It accepts jobs from a REST API, queues them by priority and schedule, processes them through an in-memory heap, and tracks every state transition. It is built to handle failure — retries, dead letters, and alerting are first-class concerns, not afterthoughts.

Flint is split across two hosting environments:

**EC2 instance (backend)** — runs the API server and worker process. The frontend is not hosted here to avoid exhausting the EC2 free tier storage limit.

- **API server** — accepts and exposes job data over HTTP. Writes to PostgreSQL only. Never touches the queue.
- **Worker process** — owns the HeapQueue. Runs the scheduler, aging, and worker coroutines together as async tasks inside a single process.

**Vercel (frontend)** — the Next.js dashboard is deployed to Vercel. It communicates with the API server over HTTPS and connects to the SSE stream directly from the browser.

**Nginx** runs on the EC2 host directly (not inside Docker) as a reverse proxy, routing HTTPS traffic into the Dockerised API and worker containers.

Redis is used exclusively for two things: SSE pub/sub (job status events to the browser) and the worker heartbeat/control system. Redis is never used as a job queue.

Email is handled by a **Mailhog server** running as a Docker container, acting as the SMTP mock for the `send_email` job handler and DLQ alert emails.

---

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Client (Browser)                             │
└──────────────┬──────────────────────────────────────┬───────────────┘
               │ HTTPS (API + SSE)                    │ HTTPS (UI)
               ▼                                      ▼
┌──────────────────────────────┐       ┌──────────────────────────────┐
│  EC2 Instance (Ubuntu 24)    │       │  Vercel                      │
│                              │       │                              │
│  ┌────────────────────────┐  │       │  Next.js Dashboard           │
│  │  Nginx  (on host)      │  │       │  app.yourdomain.com          │
│  │  Reverse proxy + SSL   │  │       │                              │
│  │  api.yourdomain.com    │  │       │  Dashboard / Jobs / DLQ      │
│  │  → Docker :8000        │  │       │  Settings / Logs / Benchmark │
│  └───────────┬────────────┘  │       └──────────────────────────────┘
│              │               │
│   Docker Compose             │
│   ┌──────────▼────────────┐  │
│   │    FastAPI App        │  │
│   │    Port :8000         │  │
│   │                       │  │
│   │  REST API (versioned) │  │
│   │  SSE Stream           │  │
│   │  Swagger Docs         │  │
│   │  Auth (API Key)       │  │
│   └──────────┬────────────┘  │
│              │               │
│   ┌──────────▼────────────┐  │   ┌────────────────────────────┐
│   │     PostgreSQL        │  │   │          Redis             │
│   │     Port :5432        │  │   │      Port :6379            │
│   │                       │  │   │                            │
│   │  jobs                 │  │   │  SSE pub/sub               │
│   │  job_dependencies     │  │   │  flint:events channel      │
│   │  job_logs             │  │   │                            │
│   │  settings             │  │   │  Worker heartbeats         │
│   └──────────┬────────────┘  │   │  flint:workers:<id>        │
│              │               │   │                            │
│   ┌──────────▼────────────┐  │   │  Control channels          │
│   │   Worker Process      ├──┼───┤  flint:worker:control:<id> │
│   │   (python -m          │  │   └────────────────────────────┘
│   │    worker.main)       │  │
│   │                       │  │
│   │  ┌─────────────────┐  │  │
│   │  │   HeapQueue     │  │  │
│   │  │  (shared in     │  │  │
│   │  │   memory)       │  │  │
│   │  └──┬──────────┬───┘  │  │
│   │     │push()    │pop() │  │
│   │  ┌──▼──────┐ ┌▼────┐  │  │
│   │  │Scheduler│ │Wrkr1│  │  │
│   │  │  task   │ │Wrkr2│  │  │
│   │  └─────────┘ └─────┘  │  │
│   │  ┌──────────────────┐  │  │
│   │  │   Aging task     │  │  │
│   │  └──────────────────┘  │  │
│   └───────────────────────┘  │
│                              │
│   ┌───────────────────────┐  │
│   │       Mailhog         │  │
│   │  SMTP :1025           │  │
│   │  Web UI :8025         │  │
│   └───────────────────────┘  │
└──────────────────────────────┘
```

---

## Component Breakdown

### FastAPI Application

The API server is the entry point for all external interaction. It is responsible for:

- Receiving job creation requests and writing them to PostgreSQL
- Exposing job status, logs, DLQ, settings, and worker state over REST
- Streaming real-time job events to connected clients via SSE
- Authenticating all requests via API key
- Providing Swagger documentation at `/api/v1/docs`

The API server does **not** process jobs and does **not** interact with the HeapQueue. It writes jobs to PostgreSQL and returns. The worker process picks them up independently.

### Worker Process

A single process (`python -m worker.main`) that owns the HeapQueue and runs all execution-related async tasks together:

- **Scheduler task** — polls PostgreSQL every second for due pending jobs, pushes them into the HeapQueue
- **Worker-1 task** — pops from the HeapQueue, claims jobs in PostgreSQL, executes handlers
- **Worker-2 task** — same as Worker-1, operating concurrently on the same shared queue
- **Aging task** — every 30 seconds, decrements `effective_priority` on long-waiting jobs and updates their scores in the heap
- **WorkerRegistry** — maintains heartbeat keys in Redis so the API can list and control workers

The number of worker coroutines is controlled by the `WORKER_COUNT` environment variable (default: 2).

### HeapQueue — The Real Queue

The `HeapQueue` is the authoritative job dispatcher. It is an in-memory min-heap that lives inside the worker process. The scheduler pushes job IDs into it; worker coroutines pop from it. There is no Redis sorted set, no Celery broker, no external queue — the heap is the queue.

The `asyncio.Lock` inside `HeapQueue` ensures concurrent `pop()` calls from multiple worker coroutines are safe. The PostgreSQL atomic claim is the secondary safety net.

### PostgreSQL

The primary source of truth. All job state lives here permanently. Key design decisions:

- UUID primary keys throughout for safe distributed generation
- Soft delete via `deleted_at` — data is never lost by default
- `effective_priority` stored as a float column so the aging process can update it in-place
- `cancellation_requested` flag for cooperative cancellation
- `worker_id` on the jobs table acts as an optimistic lock for duplicate protection

### Redis

Redis serves exactly two purposes in Flint:

1. **SSE pub/sub** — the `flint:events` channel carries job status change events from the worker process to the FastAPI SSE endpoint, which forwards them to browser clients
2. **Worker registry** — each worker coroutine writes a heartbeat key (`flint:worker:<id>`) with a 60-second TTL. The API reads these keys to list active workers. Control commands (stop/restart) are published to per-worker channels (`flint:worker:control:<id>`)

Redis is **not** used as a job queue. No sorted sets, no `ZADD`, no `ZPOPMIN`.

### Mailhog

A local SMTP server that catches all outgoing email. Used for the `send_email` job handler and for DLQ alert emails. Its web UI (port 8025) lets you inspect delivered emails during development.

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
SSE event published to Redis: {"job_id": "...", "status": "pending"}
    ↓
API returns 201 — job is now in PostgreSQL, not yet in the heap
    ↓
Scheduler task (in worker process) polls PostgreSQL every second
    ↓
When scheduled_at <= NOW and all dependencies completed:
    → queue.push(job_id, effective_priority, scheduled_at, created_at)
    ↓
Worker coroutine calls queue.pop()
    → receives job_id from the heap (lowest score = most urgent)
    ↓
Worker atomically claims job in PostgreSQL
    (UPDATE WHERE status='pending' AND worker_id IS NULL)
    ↓
Handler executes (webhook / email / log_processing)
    ↓
Status updated in PostgreSQL
    ↓
SSE event published to Redis → browser receives live update
```

### Retry Flow

```
Handler raises exception
    ↓
processor.handle_failure() called
    ↓
retry_count < max_retries?
  YES → set status='pending', next_retry_at=now+delay
        asyncio.create_task(_requeue_after_delay())
          → sleep(delay)
          → queue.push(job_id, ...)   ← pushed back into the heap
  NO  → send_to_dlq()
          set status='failed', is_dlq=True
          check threshold → fire alert email if needed
```

### DAG Flow

```
Job C depends on B, B depends on A.

Scheduler polls PostgreSQL:
  - Job A: no deps, due now → queue.push(A)
  - Job B: dep on A not completed → skip
  - Job C: dep on B not completed → skip

Worker pops A → executes → marks completed
    ↓
dag_service.on_job_completed(A) called
    → checks who depends on A → finds B
    → all B's deps completed? YES
    → queue.push(B)
    ↓
Worker pops B → executes → marks completed
    ↓
dag_service.on_job_completed(B) called
    → finds C, all deps met → queue.push(C)
    ↓
Worker pops C → executes → done
```

---

## Database Design

### Entity Relationships

```
jobs (1) ──── (many) job_dependencies
  job_id        → the waiting job
  depends_on_id → the prerequisite job

jobs (1) ──── (many) job_logs
  job_id → jobs.id

settings → standalone key-value store
```

### Key Design Decisions

**`effective_priority` vs `priority`**
The raw `priority` (1, 2, 3) is the user-assigned value and never changes. `effective_priority` starts equal to `priority` and is decremented by the aging process over time. Keeping them separate means the user always sees the original priority while the scheduler works with the aged value.

**Why JSONB for `payload`**
Job payloads are handler-specific and vary in structure. JSONB gives full flexibility without requiring a schema change for each new handler type.

**Why store `interval_seconds` as bigint**
Recurring intervals are stored in seconds regardless of how they were expressed. The conversion from human-readable format (5m, 1h, 1d) happens once at job creation. Recurrence math is then a simple integer addition.

---

## Heap-Based Priority Queue

### What a Min-Heap Is

A min-heap is a complete binary tree where every parent node has a value less than or equal to its children. Python's `heapq` implements this as a list where for element at index `i`, children are at `2i+1` and `2i+2`. The smallest element is always at index 0 — accessible in O(1). Insert and remove-min take O(log n).

### How Flint Uses the Heap

Each job in the heap is a tuple:

```
(effective_priority, scheduled_at, created_at, job_id)
```

Python compares tuples lexicographically left-to-right, so `heappop` always returns the entry with:
1. Lowest `effective_priority` (most urgent by priority score)
2. Earliest `scheduled_at` (breaks ties — job scheduled sooner wins)
3. Earliest `created_at` (breaks ties — older job wins)
4. Lexicographically smallest `job_id` (deterministic final tiebreaker)

### Lazy Deletion for Re-scoring

The heap does not support O(1) removal of arbitrary elements. When the aging process lowers a job's `effective_priority`, Flint uses the **lazy deletion pattern**:

- The old entry is marked `__removed__` in-place (O(1))
- A new entry with the updated score is pushed (O(log n))
- On `pop()`, entries marked `__removed__` are silently discarded

This avoids rebuilding the heap and keeps re-scoring at O(log n).

### Why There Is No Redis Sorted Set

A Redis sorted set also provides O(log n) insert and pop. However, using Redis as the queue creates a fundamental mismatch: the heap's sort key is a 4-tuple `(effective_priority, scheduled_at, created_at, job_id)` but Redis only stores a single float score. This means:

- `scheduled_at` and `created_at` tie-breaking is lost
- Aging re-scores would require updating Redis scores separately from the heap
- The queue would be split across two systems with no single owner

By keeping the heap entirely in-process, Flint has one source of truth for job ordering. The scheduler pushes into it; workers pop from it; the aging process updates it. No synchronisation overhead, no split-brain risk.

### Shared Queue, Multiple Workers

The `HeapQueue` is instantiated once in `worker/main.py` and passed to all worker coroutines as a shared reference. The `asyncio.Lock` inside `HeapQueue` serialises concurrent `pop()` calls — only one worker can pop at a time, eliminating the possibility of two workers receiving the same job ID from the heap.

The PostgreSQL atomic claim is a second safety net for cases where the lock could theoretically be bypassed (e.g. after a retry re-queues a job that is briefly visible to both workers).

---

## Algorithm Tradeoffs & Benchmark

Flint includes a `TimingWheel` implementation alongside the `HeapQueue` for benchmarking and comparison purposes. The timing wheel is **not used in job dispatch** — it exists to demonstrate the algorithmic tradeoff and satisfy the spec requirement of implementing and benchmarking an alternative scheduling algorithm.

See `BENCHMARK.md` for full results, methodology, and analysis.

### Summary

| Property | Heap | Timing Wheel |
|---|---|---|
| Insert | O(log n) | O(1) |
| Pop next | O(log n) | O(1) amortised |
| Re-score (aging) | O(log n) via lazy deletion | O(W) scan |
| Priority ordering | Exact, global | Per-slot only |
| Far-future jobs | Natural | Requires overflow dict |
| Why Flint uses it | Priority + aging are first-class | High-volume time-based only |

The heap is the correct choice for Flint because priority ordering and re-scoring (starvation prevention) are first-class requirements. The timing wheel would win on raw throughput in a system with uniform priorities and no aging.

---

## DAG Workflow Engine

### What a DAG Is

A Directed Acyclic Graph (DAG) where nodes are jobs and edges are dependencies. "Job B depends on Job A" means there is a directed edge A→B. Acyclic means no job can depend on itself directly or transitively.

### How Flint Implements It

Dependencies are stored in the `job_dependencies` table:

```
job_id        → the job that is waiting
depends_on_id → the job that must complete first
```

A job with dependencies is created with `status = pending` but the **scheduler skips it** until all its dependencies have `status = completed`. The scheduler checks unmet dependencies on every poll cycle using a COUNT query.

After every job completion, `dag_service.on_job_completed()` finds all jobs that depended on it, checks if their remaining dependencies are met, and pushes newly unblocked jobs into the heap.

### Cascade Failure and Cascade Retry

When a job fails permanently (goes to DLQ), `dag_service.on_job_failed()` BFS-traverses the downstream graph and marks all dependent pending jobs as `cancelled` with a clear reason message.

When an engineer manually retries a DLQ job, `dag_service.on_dag_root_retried()` BFS-traverses the same graph and resets all auto-cancelled downstream jobs back to `pending`. Once the retried root job completes, the normal DAG unblocking flow resumes automatically.

### Cycle Prevention

At job creation, `dag_service.check_cycle()` performs a DFS from each proposed dependency through its own upstream dependencies. If the new job's ID is encountered, a `DependencyCycleException` is raised before any rows are inserted.

---

## Worker Architecture

### Single Process, Multiple Coroutines

All execution logic runs in one process started with `python -m worker.main`. Inside this process, `asyncio.gather()` runs the following concurrent tasks:

- One scheduler task
- One aging task
- N worker coroutines (default 2, controlled by `WORKER_COUNT`)

Because these are all async tasks sharing one event loop, they can share the `HeapQueue` instance directly in memory with no IPC, no serialisation, and no network round-trips.

### Worker Lifecycle

```
worker/main.py starts
    ↓
Creates HeapQueue, WorkerRegistry
    ↓
Scheduler task: initial sweep pushes all due pending jobs
    ↓
Worker coroutines start, register in Redis via WorkerRegistry
    ↓
Worker loop:
    job_id = await queue.pop()      ← from HeapQueue
    registry.set_busy(worker_id)
    claimed = await claim_in_db()   ← atomic PostgreSQL UPDATE
    if not claimed: continue        ← another coroutine got it
    execute handler
    update status in PostgreSQL
    publish SSE event via Redis
    registry.set_idle(worker_id)
    ↓
On SIGTERM/SIGINT:
    shutdown_event.set()
    all tasks exit their loops cleanly
    WorkerRegistry deregisters all workers
    process exits
```

### Graceful Shutdown

Workers listen for `SIGTERM` and `SIGINT` via `asyncio`'s `add_signal_handler`. On receiving either signal, `shutdown_event` is set. All tasks check this event on each loop iteration and exit cleanly after finishing their current job. No job is left in `processing` state with no one responsible for it.

---

## Retry & Backoff System

### Backoff Formula

```
delay = 5^(attempt - 1) × uniform(0.5, 1.5)

Attempt 1: base=1s  → range [0.5s,  1.5s]
Attempt 2: base=5s  → range [2.5s,  7.5s]
Attempt 3: base=25s → range [12.5s, 37.5s]
```

The jitter multiplier (`uniform(0.5, 1.5)`) is full jitter — it prevents thundering herd where many failed jobs all retry simultaneously and re-overwhelm the downstream service.

### Re-queuing After Retry

After a failed job's state is updated in PostgreSQL, the processor creates a background async task:

```python
asyncio.create_task(_requeue_after_delay(job, next_retry_at, delay))
```

This task sleeps for `delay` seconds then calls `queue.push()` to put the job back into the heap at the correct scheduled time. The scheduler's next poll would also pick it up, but the direct re-push is faster and more precise.

---

## Dead Letter Queue

The DLQ is not a separate table. It is a view of the `jobs` table where `is_dlq = True`. Jobs retain their full history — all logs, retry counts, and original payload — when they move to the DLQ.

### Threshold Alert

The threshold is stored in `settings.dlq_threshold` (default: 5). After every DLQ insertion, the system counts current DLQ jobs. If count meets or exceeds the threshold, a branded HTML email is rendered via Jinja2 and sent to all addresses in `settings.alert_emails` via Mailhog.

Both the threshold and recipient list are configurable at runtime via `PATCH /api/v1/settings` with no restart required.

---

## Starvation Prevention

### The Problem

Without starvation prevention, a sustained flow of high-priority jobs can indefinitely block medium and low priority jobs from executing, even after hours of waiting.

### Aging Solution

The aging task runs every `AGING_INTERVAL` seconds (default 30s). It decrements `effective_priority` for jobs waiting past their threshold:

```
Medium priority (2) waiting > 2 minutes:
    effective_priority -= 0.1 per cycle
    After 10 cycles (~5 min): reaches 1.0 → competes with High priority

Low priority (3) waiting > 5 minutes:
    effective_priority -= 0.1 per cycle
    After 20 cycles (~10 min total): reaches 1.0
```

`effective_priority` is floored at 1.0 — a job can never exceed High priority, but it can reach it. After updating the DB, the aging task calls `queue.update_priority()` on the heap for each affected job, using lazy deletion to re-score without rebuilding the heap.

**Starvation is mathematically bounded**: any job will reach `effective_priority = 1.0` within a predictable time window regardless of queue pressure.

---

## Cancellation Design

### Pending Jobs

Immediate. The job status is set to `cancelled` in PostgreSQL. If it is in the heap, the scheduler's next `queue.contains()` check will skip it, and even if a worker pops it, the atomic claim will fail (status is no longer `pending`).

### Processing Jobs

Cooperative cancellation via a database flag. When cancellation is requested on a processing job:

1. The API sets `cancellation_requested = True` on the job row
2. The worker processor checks this flag at defined checkpoints: before execution, after execution, and inside long-running handlers at natural pause points
3. On detecting `True`, the worker stops, logs the event, and sets status to `cancelled`

**Documented behaviour:** *A cancellation request on a processing job is honoured at the next checkpoint within the handler, not immediately. The job may complete its current atomic step before stopping. Side effects that have already occurred (e.g. an HTTP request that was already sent) cannot be rolled back.*

---

## Duplicate Protection

Two worker coroutines calling `queue.pop()` simultaneously cannot receive the same job ID because the `asyncio.Lock` inside `HeapQueue` serialises all pop operations.

Even so, the atomic PostgreSQL claim is retained as a second safety net:

```sql
UPDATE jobs
SET status = 'processing', worker_id = :worker_id, started_at = NOW()
WHERE id = :job_id
  AND status = 'pending'
  AND worker_id IS NULL
  AND deleted_at IS NULL
RETURNING id;
```

If two coroutines somehow attempt to claim the same job simultaneously, exactly one UPDATE succeeds. The other receives zero rows and discards the job. PostgreSQL row-level atomicity guarantees this without any application-level locks.

---

## Recurring Jobs

When a recurring job completes:

1. The processor reads `interval_seconds` from the completed job
2. Creates a new Job row with identical `type`, `payload`, `priority`, and `interval_seconds`
3. Sets the new job's `scheduled_at = NOW() + interval_seconds`
4. If the interval is very short and the next run is already due, pushes it directly into the heap
5. Otherwise the scheduler picks it up on its next poll

Each recurrence is a new job with a new UUID. The full history of recurring executions is preserved and queryable.

---

## Live Updates — SSE

Server-Sent Events (SSE) is a one-way HTTP protocol where the server streams events to the client over a persistent connection. The client uses the browser-native `EventSource` API — no library needed.

### Event Flow

```
Worker coroutine completes a job
    ↓
_publish_sse({"job_id": "...", "status": "completed"})
    → PUBLISH flint:events <json> via Redis
    ↓
FastAPI SSE endpoint subscribed to flint:events
    ↓
Event forwarded as:
data: {"job_id": "...", "status": "completed"}\n\n
    ↓
Browser EventSource receives event
    ↓
Zustand store updates liveUpdates[job_id]
    ↓
Jobs table row re-renders with new status badge
```

### Why SSE Over WebSockets

The UI only needs to receive updates from the server — it never pushes data through the live channel. SSE is the correct tool for one-directional streaming. WebSockets would add bidirectional overhead for no benefit.

### Nginx Configuration for SSE

```nginx
location /api/v1/sse/ {
    proxy_pass http://api:8000;
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 3600s;
    proxy_set_header Connection '';
    proxy_http_version 1.1;
}
```

`proxy_buffering off` is critical — without it, Nginx holds the SSE stream in a buffer and events never reach the browser.

---

## Worker Heartbeat and Control

Each worker coroutine manages its own Redis state directly — no separate registry class is used. The state and control logic lives inside `run_worker()` in `worker/worker.py`.

### State Keys

Each worker writes its status to `flint:workers:<worker_id>` as a plain string with a TTL:

```
flint:workers:worker-1  →  "idle"   (TTL: 60s)
flint:workers:worker-1  →  "busy"   (TTL: 60s, set when job is picked up)
flint:workers:worker-1  →  "active" (TTL: 60s, refreshed by heartbeat loop)
```

The key expires automatically if the process dies without deregistering — no stale state.

### Heartbeat

A `_heartbeat_loop` coroutine runs inside each worker task. It refreshes the Redis key every `HEARTBEAT_INTERVAL` seconds (not every second — the loop sleeps using `asyncio.wait_for` with a timeout):

```python
async def _heartbeat_loop():
    while not local_stop.is_set() and not shutdown_event.is_set():
        await redis.set(worker_key, "active", ex=WORKER_TTL)
        try:
            await asyncio.wait_for(
                asyncio.shield(local_stop.wait()),
                timeout=HEARTBEAT_INTERVAL
            )
        except TimeoutError:
            pass
```

The worker also writes to the key on job state transitions (`"busy"` when picking up a job, `"idle"` when finishing) — these are event-driven updates, not part of the timed heartbeat.

### Control Commands

A `_control_loop` coroutine subscribes to `flint:worker:control:<worker_id>` via Redis pub/sub. The API publishes `"stop"` or `"restart"` to this channel. On receiving the command, the worker sets `local_stop` to trigger a clean exit from the poll loop.

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
2. **Rotating file** — `logs/flint.log`, rotated at 10MB, 5 backups retained

Sensitive fields (`api_key`, `password`, `smtp_password`) are scrubbed by a structlog processor before any output is written.

---

## Security

### API Key Authentication

All API endpoints require the `X-API-Key` header. The key is stored in `.env` and validated on every request via a FastAPI dependency. Invalid or missing keys return HTTP 401. The SSE endpoint is the only exemption — browsers cannot set custom headers in `EventSource`.

### HTTPS

All traffic is terminated at Nginx with a TLS certificate issued by Let's Encrypt via Certbot. HTTP is redirected to HTTPS. The certificate auto-renews via a cron job.

---

## Infrastructure Overview

```
EC2 Instance (Ubuntu 24 LTS)
│
├── Nginx  (installed on host, not in Docker)
│   ├── SSL termination via Certbot + Let's Encrypt
│   ├── api.yourdomain.com → http://localhost:8000  (FastAPI)
│   └── Proxy buffering disabled for SSE endpoint
│
├── Docker Compose (backend services only)
│   ├── api       (FastAPI,   port 8000 internal)
│   ├── worker    (HeapQueue + scheduler + workers, no port)
│   ├── postgres  (port 5432 internal)
│   ├── redis     (port 6379 internal — SSE + heartbeats only)
│   └── mailhog   (SMTP :1025 internal, Web UI :8025 internal)
│
├── Certbot (Let's Encrypt SSL)
│   └── cron: 0 3 * * * certbot renew --quiet
│
└── GitHub Actions CI/CD
    └── On push to main:
        SSH into EC2
        git pull origin main
        docker compose up -d --build

Vercel (separate, no EC2 involvement)
└── Next.js frontend
    └── app.yourdomain.com
    └── Deployed via Vercel GitHub integration
```

### Nginx on Host

Nginx runs directly on the EC2 instance rather than inside Docker. This keeps the reverse proxy outside the container network, giving it direct access to SSL certificate files managed by Certbot on the host without volume mount complexity.

```nginx
# /etc/nginx/sites-available/flint-api
server {
    listen 80;
    server_name api.yourdomain.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name api.yourdomain.com;

    ssl_certificate     /etc/letsencrypt/live/api.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.yourdomain.com/privkey.pem;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # SSE: disable buffering so events reach the browser immediately
    location /api/v1/sse/ {
        proxy_pass http://localhost:8000;
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 3600s;
        proxy_set_header Connection '';
        proxy_http_version 1.1;
    }
}
```

### Docker Compose Services

```yaml
worker:
  build: .
  command: python -m worker.main
  env_file: .env
  environment:
    WORKER_COUNT: "2"      # increase to add more worker coroutines
```

To scale workers, increase `WORKER_COUNT`. No new containers needed — the single worker process spawns more async coroutines. All share the same HeapQueue instance in memory.

### Frontend — Vercel

The Next.js frontend is deployed to Vercel independently from the EC2 instance. The EC2 free tier does not have enough storage to comfortably host both the backend stack and a Next.js build. Vercel handles the frontend build, deployment, and global CDN distribution. The frontend connects to `api.yourdomain.com` for all API calls and SSE.
