<div align="center">

# 🔥 Flint

### *Quietly igniting your payload, every job has a spark.*

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis&logoColor=white)](https://redis.io)
[![License](https://img.shields.io/badge/License-MIT-orange.svg)](LICENSE)

A production-grade background job scheduling system built from scratch.
Priority queuing, DAG workflows, retry logic, dead letter queue, and real-time updates —
no Celery, no external queue broker.

**[Live API](https://api.yourdomain.com/api/v1/docs)** · **[Frontend Repo](https://github.com/yourusername/flint-frontend)** · **[Architecture](./ARCHITECTURE.md)** · **[Benchmark](./BENCHMARK.md)**

</div>

---

## Table of Contents

- [What is Flint?](#what-is-flint)
- [Why Build This From Scratch?](#why-build-this-from-scratch)
- [System Architecture](#system-architecture)
- [Core Features](#core-features)
  - [Heap-Based Priority Queue](#heap-based-priority-queue)
  - [DAG Workflow Engine](#dag-workflow-engine)
  - [Retry System with Backoff](#retry-system-with-backoff)
  - [Dead Letter Queue](#dead-letter-queue)
  - [Starvation Prevention](#starvation-prevention)
  - [Cooperative Cancellation](#cooperative-cancellation)
  - [Recurring Jobs](#recurring-jobs)
  - [Real-Time Updates via SSE](#real-time-updates-via-sse)
  - [Duplicate Protection](#duplicate-protection)
- [Algorithm Benchmark](#algorithm-benchmark)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [API Reference](#api-reference)
- [Job Handlers](#job-handlers)
- [Getting Started](#getting-started)
- [Running Tests](#running-tests)
- [Deployment](#deployment)
- [Environment Variables](#environment-variables)
- [Frontend](#frontend)
- [Design Decisions](#design-decisions)

---

## What is Flint?

Flint is a background job scheduler built entirely from first principles. It accepts jobs over a REST API, queues them in an in-memory min-heap by priority and schedule, processes them through independent worker coroutines, and tracks every state transition in PostgreSQL.

It handles the full lifecycle of a background job:

```
pending → processing → completed
                    ↘ failed → retry (up to 3x with exponential backoff + jitter) → DLQ
                    ↘ cancelled
```

Jobs can depend on other jobs (DAG), recur on a flexible interval, be scheduled for the future, and be monitored in real-time through a live dashboard. Failed jobs land in a dead letter queue with full error details, where engineers can inspect and retry them manually.

This is not a wrapper around an existing queue library. There is no Celery, no RQ, no Dramatiq. The heap is the queue. The scheduler is a coroutine. The workers are async tasks. Every piece is built and owned.

---

## Why Build This From Scratch?

Using Celery or a managed queue (SQS, Cloud Tasks) is the right call for most production systems. This project exists to demonstrate what lives *underneath* those abstractions:

- How a priority queue actually works and what makes it correct under concurrent access
- How to implement retry backoff with jitter to prevent thundering herd
- How DAG dependency resolution works without a framework
- How to design cooperative cancellation that is safe for long-running handlers
- How starvation prevention (priority aging) keeps low-priority jobs from waiting forever
- How atomic database operations replace distributed locks for duplicate protection
- How SSE, pub/sub, and a shared in-memory queue wire together into a real-time system

Every design decision in Flint has a documented reason. See [ARCHITECTURE.md](./ARCHITECTURE.md) and [Design Decisions](#design-decisions).

---

## System Architecture

Flint runs as two independently deployable processes on an EC2 instance, with the frontend on Vercel:

```
┌─────────────────────────────────────────────────────────────────┐
│  EC2 Instance (Ubuntu 24 LTS)                                   │
│                                                                 │
│  Nginx (on host) ──→ FastAPI :8000 ──→ PostgreSQL :5432        │
│                           │                                     │
│                      Redis :6379                                │
│                      (SSE pub/sub + worker heartbeats)          │
│                           │                                     │
│  Worker Process ──────────┘                                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  HeapQueue  (shared in-memory across all tasks)          │   │
│  │                                                          │   │
│  │  Scheduler task  →  queue.push(job_id, ...)              │   │
│  │  Worker-1 task   ←  queue.pop()  →  claim in DB          │   │
│  │  Worker-2 task   ←  queue.pop()  →  claim in DB          │   │
│  │  Aging task      →  queue.update_priority(job_id, ...)   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Mailhog  (SMTP mock — :1025 SMTP, :8025 Web UI)               │
└─────────────────────────────────────────────────────────────────┘

Vercel  →  Next.js dashboard  (app.yourdomain.com)
```

**API process** — FastAPI application. Accepts job creation requests, writes to PostgreSQL, publishes SSE events via Redis. Never touches the HeapQueue.

**Worker process** (`python -m worker.main`) — a single Python process running multiple async tasks that share one `HeapQueue` instance in memory. The scheduler polls PostgreSQL every second and pushes due jobs into the heap. Worker coroutines pop from the heap, claim jobs atomically in PostgreSQL, and execute handlers. The aging task decrements priority scores on long-waiting jobs. All tasks communicate through the shared heap — no IPC, no serialisation overhead.

**Redis** — used for exactly two things: SSE pub/sub (job status events streamed to browsers) and worker heartbeat keys with TTL (so the API can list and control active workers). Redis is **not** the job queue.

**PostgreSQL** — source of truth. All job state, logs, dependency relationships, and settings live here permanently.

**Nginx** — runs on the EC2 host directly, not inside Docker. Routes HTTPS traffic to the Dockerised API. `proxy_buffering off` is set on the SSE endpoint so events reach the browser immediately.

**Frontend on Vercel** — the Next.js dashboard is deployed separately to avoid exhausting EC2 free tier storage. See [Frontend](#frontend).

Full architecture documentation: [ARCHITECTURE.md](./ARCHITECTURE.md)

---

## Core Features

### Heap-Based Priority Queue

The `HeapQueue` (`app/queues/heap_queue.py`) is an in-memory min-heap where each entry is a 4-tuple:

```python
(effective_priority, scheduled_at, created_at, job_id)
```

Python's `heapq` compares tuples lexicographically, so `heappop()` always returns the most urgent job:

1. Lowest `effective_priority` (1.0 = High, 2.0 = Medium, 3.0 = Low — decremented by aging over time)
2. Earliest `scheduled_at` breaks priority ties
3. Earliest `created_at` breaks remaining ties
4. `job_id` is the deterministic final tiebreaker

The queue uses **lazy deletion** for re-scoring: when the aging process lowers a job's priority, the old entry is marked `__removed__` in-place (O(1)) and a new entry is pushed (O(log n)). Stale entries are silently discarded on the next `pop()`. This avoids rebuilding the heap entirely.

```python
@dataclass(order=True)
class HeapEntry:
    effective_priority: float
    scheduled_at: float
    created_at: float
    job_id: str = field(compare=True)
```

The `asyncio.Lock` inside `HeapQueue` serialises concurrent `pop()` calls from multiple worker coroutines — two workers cannot receive the same job ID from the heap.

**Complexity:** O(log n) insert · O(log n) pop · O(1) lazy remove mark · O(1) size

### DAG Workflow Engine

Jobs can declare dependencies on other jobs. A job does not enter the heap until all its dependencies have `status = completed`.

```json
{
  "type": "send_email",
  "payload": { "to": "user@example.com", "subject": "Report Ready" },
  "dependency_ids": ["uuid-of-generate-report-job", "uuid-of-upload-file-job"]
}
```

Dependency edges are stored in `job_dependencies`. On every job completion, `dag_service.on_job_completed()` queries for downstream jobs whose remaining unmet dependency count is now zero and pushes them into the heap.

**Cycle detection** — enforced at creation time via DFS through the existing dependency graph. A `DependencyCycleException` is raised before any rows are written.

**Cascade failure** — when a job is permanently failed (moved to DLQ), all downstream pending jobs are automatically cancelled via BFS traversal of the graph. No orphaned jobs waiting indefinitely.

**Cascade retry (Option A)** — when an engineer manually retries a DLQ root job, all downstream auto-cancelled jobs are automatically reset to `pending` via BFS. The full workflow re-runs from the fixed root with no manual intervention on each downstream job.

```
Generate Report → Upload File → Send Email
     ↓ fails          ↓               ↓
   → DLQ          cancelled       cancelled

Engineer retries Generate Report:
     ↓ reset          ↓               ↓
   pending          pending         pending   ← cascade reset automatically
```

### Retry System with Backoff

Failed jobs retry automatically up to `max_retries` (default 3, configurable per job at creation). Each retry uses **exponential backoff with full jitter**:

```python
def calculate_next_retry_delay(attempt: int) -> float:
    base = math.pow(5, attempt - 1)             # 1, 5, 25
    return round(base * random.uniform(0.5, 1.5), 2)

# Attempt 1 → base=1s   → range [0.5s,  1.5s]
# Attempt 2 → base=5s   → range [2.5s,  7.5s]
# Attempt 3 → base=25s  → range [12.5s, 37.5s]
```

The `uniform(0.5, 1.5)` jitter multiplier prevents thundering herd — if 50 jobs all fail at the same moment, they will not all retry at the same moment.

After the backoff sleep, the job is re-pushed directly into the heap via `queue.push()`. The scheduler's next DB poll would also discover it — whichever path enqueues it first, the atomic PostgreSQL claim prevents double-processing.

### Dead Letter Queue

After exhausting `max_retries`, a job is moved to the Dead Letter Queue:

- `status = 'failed'`, `is_dlq = True`, `last_error` contains the full error string
- The DLQ is **not a separate table** — it is a filtered view of `jobs` (`WHERE is_dlq = TRUE`)
- Full history is preserved: all log entries, retry counts, original payload

**Threshold alerting** — when the DLQ count meets or exceeds `dlq_threshold` (default 5, runtime-configurable), a branded HTML email (Jinja2 template, dark theme, Flint orange) is sent to all configured recipients via Mailhog. The email includes a table of the 10 most recent failed jobs with error summaries and a direct link to the DLQ dashboard page.

**Manual retry** — `POST /api/v1/dlq/{id}/retry` resets the job to `pending` with `retry_count = 0` and re-evaluates its DAG. If it fails again, it goes back to DLQ. No limit on manual retries.

### Starvation Prevention

Without intervention, a sustained stream of high-priority jobs permanently starves low-priority ones. Flint prevents this through **priority aging**.

The aging task (`worker/aging.py`) runs every 30 seconds and decrements `effective_priority` on long-waiting jobs:

| Priority | Wait threshold | Decrement | Cycles to reach High (1.0) |
|---|---|---|---|
| Medium (2.0) | 2 minutes | 0.1 per 30s | 10 cycles (~5 min total wait) |
| Low (3.0) | 5 minutes | 0.1 per 30s | 20 cycles (~10 min total wait) |

`effective_priority` is floored at `1.0` — a job can reach High priority but never exceed it.

After the DB update, the aging task calls `queue.update_priority()` on the heap for each affected job, using lazy deletion to re-score without rebuilding the heap. Starvation is **mathematically bounded** — any job reaches `effective_priority = 1.0` within a predictable time window regardless of queue pressure.

### Cooperative Cancellation

**Pending jobs** — immediately cancelled. Status set to `cancelled`, cascade cancellation propagates to all downstream DAG dependents.

**Processing jobs** — cooperative via a database flag. The API sets `cancellation_requested = True`. The worker processor checks this flag at defined checkpoints:

1. Before calling `handler.execute()`
2. After `handler.execute()` returns (before marking completed)
3. Inside long-running handlers at natural pause points

**Documented behaviour:** *A cancellation request on a processing job is honoured at the next checkpoint, not immediately. Side effects that have already occurred (e.g. an HTTP request already sent) cannot be rolled back.*

This is safer than forceful termination (kill signal), which would leave external resources in inconsistent state — a webhook half-delivered, a file half-written, a database transaction uncommitted.

### Recurring Jobs

Any job can recur by specifying an `interval`:

```json
{
  "type": "log_processing",
  "payload": { "source": "app-server", "lines": ["..."] },
  "interval": "1h"
}
```

**Supported interval formats:** `30s` · `5m` · `2h` · `1d` · `1mo` · `1y` — any `<number><unit>` combination. Stored internally as total seconds.

When a recurring job completes, the processor creates a new job row with identical `type`, `payload`, `priority`, and `interval_seconds`, with `scheduled_at = now + interval_seconds`. Each recurrence is a distinct job with its own UUID and full history — no mutation of the original.

### Real-Time Updates via SSE

The dashboard reflects status changes without page refresh. When a worker updates a job:

```python
await redis.publish("flint:events", json.dumps({
    "job_id": "abc-123",
    "status": "completed",
    "worker_id": "worker-1",
    "duration_ms": 142
}))
```

The FastAPI SSE endpoint (`GET /api/v1/sse/stream`) subscribes to `flint:events` and forwards messages to all connected browser clients via `text/event-stream`. The frontend's Zustand store applies live updates to the jobs table in place — the status badge updates and briefly pulses to signal the live change.

SSE was chosen over WebSockets because updates flow strictly server → client. WebSockets would add bidirectional protocol overhead for no benefit here.

**Critical Nginx config** — `proxy_buffering off` must be set on the SSE location block. Without it, Nginx holds events in its buffer and they never reach the browser.

### Duplicate Protection

Two independent layers prevent a job from being processed twice:

**Layer 1 — HeapQueue lock**
The `asyncio.Lock` inside `HeapQueue` serialises all `pop()` calls. Two coroutines cannot receive the same job ID from the heap simultaneously.

**Layer 2 — Atomic PostgreSQL claim**
Even if two coroutines somehow pop the same ID, only one can win:

```sql
UPDATE jobs
SET status = 'processing', worker_id = :worker_id, started_at = NOW()
WHERE id = :job_id
  AND status = 'pending'
  AND worker_id IS NULL
  AND deleted_at IS NULL
RETURNING id;
```

PostgreSQL row-level locking guarantees exactly one `UPDATE` wins. The other gets zero rows back and discards the job. No application-level locks, no Redis `SETNX`, no semaphores — database atomicity is sufficient and correct.

---

## Algorithm Benchmark

Flint implements a `TimingWheel` alongside the `HeapQueue` for benchmarking. The timing wheel is not used in production dispatch — it demonstrates the architectural alternative and satisfies the requirement of implementing and benchmarking a second scheduling algorithm.

**Real benchmark results (n = 10,000 jobs, random priorities and scheduled times):**

```
=======================================================
  FLINT BENCHMARK RESULTS  (n=10,000)
=======================================================

  HEAP
    Insert :      27.46 ms
    Pop    :      64.56 ms
    Total  :      92.02 ms

  TIMING WHEEL
    Insert :      20.19 ms
    Pop    :     662.87 ms
    Total  :     683.06 ms

  Winner (total time): HEAP  (7.4× faster at this scale)
=======================================================
```

The timing wheel's insert is marginally faster (O(1) vs O(log n)) but its drain loop must advance through all 3,600 wheel slots — an O(W) fixed cost that dominates at typical queue sizes. The heap wins decisively for Flint's workload because priority ordering and frequent re-scoring (aging) are first-class requirements the timing wheel cannot satisfy efficiently.

Full analysis and multi-scale results: [BENCHMARK.md](./BENCHMARK.md)

```bash
# Run the benchmark yourself
python -m benchmark.runner --n 10000

# Or via the API
POST /api/v1/benchmark/run
{ "n": 10000, "algorithm": "both" }
```

---

## Tech Stack

| Concern | Technology | Reason |
|---|---|---|
| Framework | **FastAPI** | Async-native, automatic OpenAPI, clean dependency injection |
| Language | **Python 3.12** | `asyncio`, `heapq`, type hints, clean async ecosystem |
| Database | **PostgreSQL 16** | ACID transactions, row-level locking for atomic claims, JSONB payloads |
| Cache / Pub-sub | **Redis 7** | SSE event bus + worker heartbeats. Not the job queue |
| ORM | **SQLAlchemy 2.x async** | Typed models, async sessions, Alembic integration |
| Migrations | **Alembic** | Schema versioning, data migrations (settings seed) |
| Validation | **Pydantic v2** | Request/response schemas, field validators, settings |
| Email mock | **Mailhog** | Real SMTP server that catches all outgoing mail in dev |
| Email sending | **aiosmtplib** | Async SMTP — non-blocking delivery from handlers |
| HTML email | **Jinja2** | Branded dark-theme alert emails for DLQ notifications |
| HTTP client | **httpx** | Async HTTP for the webhook delivery handler |
| Logging | **structlog** | Structured JSON logs, sensitive field scrubbing |
| Server | **Gunicorn + Uvicorn** | Production ASGI, multiple workers |
| Reverse proxy | **Nginx (on host)** | SSL termination, SSE buffering disabled |
| Containers | **Docker + Compose** | Backend services only; Nginx on host |
| SSL | **Certbot + Let's Encrypt** | Free, auto-renewing TLS |
| CI/CD | **GitHub Actions** | SSH deploy on push to main |
| Testing | **pytest + pytest-asyncio** | Async unit and integration tests |
| Frontend | **Next.js on Vercel** | Separate repo — [flint-frontend](https://github.com/yourusername/flint-frontend) |

---

## Project Structure

```
flint-backend/
│
├── app/                            # FastAPI application
│   ├── main.py                     # App entry point, CORS, exception handlers, lifespan
│   ├── config.py                   # pydantic-settings — all env vars with defaults
│   ├── dependencies.py             # FastAPI DI providers: get_db(), get_redis()
│   │
│   ├── api/v1/                     # Versioned REST API
│   │   ├── router.py               # Mounts all routers under /api/v1
│   │   ├── jobs.py                 # Create, list, get, cancel, soft-delete
│   │   ├── dlq.py                  # List DLQ, retry, remove
│   │   ├── bin.py                  # List bin, restore, hard-delete
│   │   ├── settings.py             # Get/update runtime settings
│   │   ├── workers.py              # List workers, stop, restart
│   │   ├── logs.py                 # Structured job event log viewer
│   │   ├── benchmark.py            # Trigger benchmark runs via API
│   │   ├── dashboard.py            # Aggregated stats endpoint
│   │   └── sse.py                  # Server-Sent Events stream (auth-exempt)
│   │
│   ├── core/
│   │   ├── logger.py               # structlog setup, JSON output, field scrubbing
│   │   ├── exceptions.py           # Domain exception hierarchy (all carry HTTP status_code)
│   │   ├── response.py             # FlintResponse[T] envelope + builder functions
│   │   └── security.py             # X-API-Key header verification dependency
│   │
│   ├── db/
│   │   ├── base.py                 # DeclarativeBase + BaseModel (UUID PK, timestamps)
│   │   ├── session.py              # Async engine, session factory, get_db()
│   │   └── migrations/             # Alembic — full schema + settings seed data
│   │
│   ├── models/
│   │   ├── job.py                  # Job ORM model + JobStatus/JobType/JobPriority enums
│   │   ├── job_dependency.py       # DAG edge table (job_id → depends_on_id)
│   │   ├── job_log.py              # Structured event log per job
│   │   └── setting.py              # Runtime key-value settings store
│   │
│   ├── schemas/
│   │   ├── job.py                  # JobCreate (with parse_interval), JobResponse, filters
│   │   ├── dlq.py                  # DLQ response shapes
│   │   ├── setting.py              # SettingsUpdate with field-level validators
│   │   ├── log.py                  # LogEntryResponse
│   │   ├── benchmark.py            # BenchmarkRequest, BenchmarkResult, AlgorithmResult
│   │   └── response.py             # FlintResponse[T], Meta, ErrorDetail (generic)
│   │
│   ├── services/
│   │   ├── job_service.py          # Job CRUD, cancel, bin management, dashboard stats
│   │   ├── dag_service.py          # Cycle detection (DFS), unblocking, cascade ops (BFS)
│   │   ├── dlq_service.py          # DLQ transitions, threshold check, manual retry
│   │   ├── setting_service.py      # Settings CRUD, typed getters with safe fallbacks
│   │   └── alert_service.py        # DLQ email alerts — Jinja2 render + aiosmtplib send
│   │
│   ├── queues/
│   │   ├── base.py                 # Abstract BaseQueue interface
│   │   ├── heap_queue.py           # Min-heap with lazy deletion — production dispatcher
│   │   └── timing_wheel.py         # Circular buffer, 3600 slots — benchmark only
│   │
│   ├── handlers/
│   │   ├── base.py                 # Abstract BaseHandler (execute → result | raises)
│   │   ├── webhook.py              # Real HTTP via httpx — 4xx/5xx/timeout all raise
│   │   ├── email.py                # Real SMTP via aiosmtplib → Mailhog
│   │   ├── log_processor.py        # Parse lines → structured summary with error stats
│   │   └── __init__.py             # HANDLER_REGISTRY dict + get_handler() factory
│   │
│   └── templates/emails/
│       ├── base.html               # Branded dark-theme transactional email layout
│       └── dlq_alert.html          # DLQ alert: count, threshold, job table, CTA button
│
├── worker/
│   ├── main.py                     # Entry point — creates HeapQueue, starts all tasks
│   ├── worker.py                   # Worker coroutine: pop → claim → process, heartbeat
│   ├── processor.py                # Full job lifecycle: claim, execute, retry, DLQ, recur
│   └── aging.py                    # Starvation prevention: decrement + heap.update_priority
│
├── scheduler/
│   └── scheduler.py                # Polls PostgreSQL → queue.push() for due pending jobs
│
├── benchmark/
│   └── runner.py                   # CLI + API-callable benchmark script
│
├── tests/
│   ├── conftest.py                 # Async test DB engine, test client, seeded_settings fixture
│   ├── unit/                       # No DB, no network — pure logic
│   │   ├── test_heap_queue.py      # Pop order, lazy deletion, re-scoring, perf (64 tests)
│   │   ├── test_timing_wheel.py    # Slots, overflow, drain, per-slot priority
│   │   ├── test_retry.py           # Backoff ranges verified over 50 samples each
│   │   ├── test_dag.py             # Cycle detection, parse_interval, cascade math
│   │   ├── test_starvation.py      # Aging thresholds, floor, bounded convergence proof
│   │   └── test_handlers.py        # All three handlers, all error paths, mocked externals
│   └── integration/                # Real test DB, full FastAPI test client
│       ├── test_jobs_api.py        # CRUD, filters, auth, 17 test cases
│       ├── test_dlq_api.py         # DLQ lifecycle, retry, remove, edge cases
│       ├── test_settings_api.py    # All settings paths, all validation error cases
│       ├── test_worker.py          # Atomic claim race, success, retry, DLQ, cancellation
│       └── test_sse.py             # SSE headers, auth exemption, full bin lifecycle
│
├── logs/                           # Rotating JSON log output (gitignored except .gitkeep)
├── ARCHITECTURE.md                 # Full system design with diagrams and rationale
├── BENCHMARK.md                    # Algorithm comparison — real numbers, analysis
├── DEPLOYMENT.md                   # VPS setup, Docker, Nginx, SSL, CI/CD walkthrough
├── requirements.txt
├── requirements-dev.txt
├── alembic.ini
├── pytest.ini
└── Dockerfile
```

---

## API Reference

All endpoints live under `/api/v1`. Every response follows a consistent envelope:

```json
{
  "message": "Human-readable description of what happened",
  "data": {},
  "errors": [],
  "meta": { "page": 1, "limit": 20, "total": 143 }
}
```

**Authentication** — every request (except `GET /health` and `GET /api/v1/sse/stream`) requires:
```
X-API-Key: <your-api-key>
```

Interactive documentation at `/api/v1/docs` (Swagger UI).

### Jobs

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/jobs` | Create a job with type, payload, priority, schedule, interval, dependencies |
| `GET` | `/api/v1/jobs` | List jobs — filter by `status`, `type`, `priority`, `search`. Paginated |
| `GET` | `/api/v1/jobs/{id}` | Full job detail with event logs and dependency IDs |
| `PATCH` | `/api/v1/jobs/{id}/cancel` | Cancel pending (immediate) or processing (cooperative) |
| `DELETE` | `/api/v1/jobs/{id}` | Soft-delete a terminal job — moves to bin |

### Dead Letter Queue

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/dlq` | List DLQ jobs with full error details. Paginated |
| `POST` | `/api/v1/dlq/{id}/retry` | Reset to pending, re-evaluate DAG, re-queue |
| `DELETE` | `/api/v1/dlq/{id}` | Soft-delete from DLQ — moves to bin |

### Bin

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/bin` | List soft-deleted jobs. Paginated |
| `PATCH` | `/api/v1/bin/{id}/restore` | Restore — clears `deleted_at` |
| `DELETE` | `/api/v1/bin/{id}` | Permanent hard delete — irreversible |

### Settings (runtime, no restart required)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/settings` | All current settings as `{key: value}` |
| `PATCH` | `/api/v1/settings` | Update `dlq_threshold`, `alert_emails`, `scheduler_strategy` |

### Workers

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/workers` | Active workers — ID, status (`idle`/`busy`/`stopping`), TTL |
| `POST` | `/api/v1/workers/{id}/stop` | Publish `stop` to worker's Redis control channel |
| `POST` | `/api/v1/workers/{id}/restart` | Publish `restart` to worker's Redis control channel |

### Other

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/logs` | Structured job event logs. Filter by `event`, `job_id`. Paginated |
| `GET` | `/api/v1/dashboard/stats` | Job counts by status + DLQ count |
| `POST` | `/api/v1/benchmark/run` | Run heap vs timing wheel. Body: `{n, algorithm}` |
| `GET` | `/api/v1/sse/stream` | SSE event stream. No auth required |
| `GET` | `/health` | Health check — returns `{"status": "ok"}` |

---

## Job Handlers

Three handlers are implemented. Each performs real execution with real failure modes — not stubs that return 200.

### `webhook_delivery`

Makes an actual HTTP request using `httpx.AsyncClient` with a 10-second timeout and max 3 redirects. Raises on 4xx, 5xx, timeout, and connection error — each producing a distinct, descriptive error message stored in the DLQ log.

```json
{
  "type": "webhook_delivery",
  "payload": {
    "url": "https://webhook.site/your-id",
    "method": "POST",
    "headers": { "X-Custom": "value" },
    "body": { "event": "user.created", "user_id": "abc-123" }
  }
}
```

### `send_email`

Constructs a proper `MIMEMultipart("alternative")` message with plain text always attached and optional HTML. Delivers via `aiosmtplib` to Mailhog. Raises on SMTP errors so the retry system handles transient mail server issues.

```json
{
  "type": "send_email",
  "payload": {
    "to": "user@example.com",
    "subject": "Your report is ready",
    "body": "The monthly report has been generated.",
    "html": "<p>The monthly report has been <strong>generated</strong>.</p>"
  }
}
```

### `log_processing`

Parses raw log lines in `<timestamp> <LEVEL> <message>` format. Counts occurrences per level, identifies top error messages by frequency, computes error rate, and flags CRITICAL presence. Raises if no lines are parseable — triggers retry.

```json
{
  "type": "log_processing",
  "payload": {
    "source": "app-server-01",
    "lines": [
      "2026-06-09T10:00:00Z INFO  Request received path=/api/users",
      "2026-06-09T10:00:01Z ERROR Database connection timeout after 30s",
      "2026-06-09T10:00:02Z ERROR Database connection timeout after 30s"
    ]
  }
}
```

Returns a structured summary:
```json
{
  "source": "app-server-01",
  "total_lines": 3,
  "parsed_lines": 3,
  "parse_errors": 0,
  "level_counts": { "INFO": 1, "ERROR": 2 },
  "top_errors": [{ "message": "Database connection timeout after 30s", "count": 2 }],
  "has_critical": false,
  "error_rate_pct": 66.67
}
```

**Adding a new handler** requires exactly two changes: implement `BaseHandler.execute()` in a new file and add one entry to `HANDLER_REGISTRY` in `app/handlers/__init__.py`.

---

## Getting Started

### Prerequisites

- Python 3.12+
- PostgreSQL 16
- Redis 7
- Docker + Docker Compose (for running dependencies)

### 1. Clone and set up the environment

```bash
git clone https://github.com/yourusername/flint-backend.git
cd flint-backend

python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Start dependencies

```bash
docker compose up -d postgres redis mailhog
```

Mailhog web UI: http://localhost:8025 — inspect all outgoing emails here.

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env — at minimum set API_KEY, DATABASE_URL, REDIS_URL
```

### 4. Run database migrations

```bash
alembic upgrade head
```

Creates all tables and seeds default settings: `dlq_threshold=5`, `alert_emails=[]`, `scheduler_strategy=heap`.

### 5. Start the API server

```bash
uvicorn app.main:app --reload --port 8000
```

Swagger UI → http://localhost:8000/api/v1/docs

### 6. Start the worker process

```bash
python -m worker.main
```

Starts the scheduler task, aging task, and two worker coroutines — all sharing one `HeapQueue` in memory.

### 7. Create your first job

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "type": "webhook_delivery",
    "payload": {
      "url": "https://webhook.site/your-id",
      "body": { "hello": "from Flint" }
    },
    "priority": 1
  }'
```

The scheduler picks it up within one poll interval (1 second by default), pushes it into the heap, a worker pops it, claims it in PostgreSQL, and executes the webhook request. Watch it move through statuses in the Swagger UI or check the logs.

---

## Running Tests

### Unit tests — no external dependencies required

```bash
pytest tests/unit/ -v
```

64 tests covering: heap queue correctness (pop order, lazy deletion, re-scoring, deduplication, performance), timing wheel behaviour (slots, overflow, drain, priority within slot), backoff ranges verified over 50 random samples each, DAG cycle detection and cascade math, starvation convergence proof, and all three handler error paths with mocked externals.

### Integration tests — requires live PostgreSQL

```bash
# Ensure test DB is running
docker compose up -d postgres

TEST_DATABASE_URL=postgresql+asyncpg://flint:flint@localhost:5432/flint_test \
pytest tests/integration/ -v
```

Covers: full job CRUD with all filter combinations, auth (missing key, wrong key), DLQ full lifecycle, all settings validation error paths, atomic worker claim race condition (two concurrent sessions, exactly one wins), cooperative cancellation, recurring job creation, and SSE endpoint headers.

### Full suite with coverage

```bash
pytest tests/ \
  --cov=app --cov=worker --cov=scheduler \
  --cov-report=term-missing \
  --cov-report=html
```

---

## Deployment

Full step-by-step guide: [DEPLOYMENT.md](./DEPLOYMENT.md)

**Summary:**

1. Provision EC2 (Ubuntu 24 LTS)
2. Install Nginx on the host, Docker + Compose inside the instance
3. Point DNS A record → EC2 IP
4. Obtain SSL: `certbot certonly --standalone -d api.yourdomain.com`
5. Configure Nginx with `proxy_buffering off` on the SSE location
6. Clone repo, configure `.env`, run `docker compose up -d`
7. Run migrations: `docker compose exec api alembic upgrade head`
8. Add GitHub Actions secrets (`VPS_HOST`, `VPS_USER`, `VPS_SSH_KEY`) for CI/CD

The Next.js frontend is deployed to Vercel separately — see [Frontend](#frontend).

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `API_KEY` | *(required)* | Secret for `X-API-Key` header authentication |
| `DATABASE_URL` | *(required)* | PostgreSQL async URL: `postgresql+asyncpg://user:pass@host/db` |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection (SSE + worker heartbeats only) |
| `SMTP_HOST` | `mailhog` | SMTP server hostname |
| `SMTP_PORT` | `1025` | SMTP server port |
| `SMTP_FROM` | `flint@flint.local` | From address for outgoing emails |
| `WORKER_COUNT` | `2` | Number of worker coroutines in the worker process |
| `WORKER_POLL_INTERVAL` | `1.0` | Seconds to sleep when the heap is empty |
| `SCHEDULER_POLL_INTERVAL` | `1.0` | Seconds between scheduler PostgreSQL polls |
| `AGING_INTERVAL` | `30.0` | Seconds between aging cycles |
| `MEDIUM_PRIORITY_AGE_THRESHOLD` | `120` | Seconds before medium jobs start aging (2 min) |
| `LOW_PRIORITY_AGE_THRESHOLD` | `300` | Seconds before low jobs start aging (5 min) |
| `AGING_DECREMENT` | `0.1` | Priority score decrement per aging cycle |
| `LOG_LEVEL` | `INFO` | structlog level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_FILE` | `logs/flint.log` | Rotating log file path (10MB, 5 backups) |

**Runtime settings** (stored in PostgreSQL, changeable via `PATCH /api/v1/settings` with no restart):

| Key | Default | Description |
|---|---|---|
| `dlq_threshold` | `5` | DLQ job count that triggers an alert email |
| `alert_emails` | `[]` | JSON array of alert email recipients |
| `scheduler_strategy` | `heap` | Algorithm selection for the benchmark endpoint |

---

## Frontend

The Flint dashboard is a separate repository deployed to Vercel. The frontend is not hosted on the EC2 instance to avoid exhausting free tier storage limits.

**Repository:** [github.com/yourusername/flint-frontend](https://github.com/yourusername/flint-frontend)
**Live:** [app.yourdomain.com](https://app.yourdomain.com)

**Stack:** Next.js 14 (App Router) · TypeScript · Tailwind CSS · shadcn/ui · Zustand · React Query · Recharts

**Pages:**
- `/dashboard` — live stat cards, status bar chart, worker panel with stop/restart
- `/jobs` — searchable filterable table, live status badges, inline cancel/delete
- `/jobs/new` — dynamic create form — payload fields change per handler type
- `/jobs/:id` — detail view with event timeline, dependency links, payload JSON, cancel button
- `/dlq` — failed jobs with expandable errors, inline retry and remove, threshold warning
- `/bin` — soft-deleted jobs with restore and permanent delete
- `/settings` — DLQ threshold, email tag input, strategy toggle, worker controls
- `/logs` — event log viewer with event type and job ID filters
- `/benchmark` — run and visualise heap vs timing wheel with live bar chart

**Architecture:** All API calls are proxied through Next.js API routes, which inject `X-API-Key` server-side so the key is never exposed to the browser. The SSE connection is opened directly from the browser via `EventSource` to `api.yourdomain.com/api/v1/sse/stream` — Next.js API routes cannot proxy streaming connections reliably.

---

## Design Decisions

### Why not Celery?

Celery is the right tool for most Python job queue use cases in production. Flint exists to demonstrate what lives underneath that abstraction — the heap, the retry logic, the atomic claim, the DAG traversal, the starvation prevention. Using Celery would delegate away exactly the mechanisms this project is built to showcase.

### Why asyncio instead of threads or processes for workers?

The worker process runs concurrent tasks (scheduler, aging, N workers) that all share one `HeapQueue` instance. Sharing data between OS threads requires careful locking. Between processes it requires IPC or a shared external store. With `asyncio`, everything runs in one thread on one event loop — the shared heap is just a Python object. The `asyncio.Lock` inside `HeapQueue` handles concurrent access at a fraction of the overhead of a threading lock. Context switching between async tasks is also far cheaper than OS thread context switches.

### Why is the HeapQueue in-process rather than in Redis?

Redis sorted sets provide O(log n) insert and `ZPOPMIN`. But the heap's sort key is a 4-tuple `(effective_priority, scheduled_at, created_at, job_id)` — Redis only stores a single float score per member. Using Redis as the queue would silently discard `scheduled_at` and `created_at` tie-breaking, and aging re-scores would require two-system updates with no atomic guarantee. With the in-process heap, there is one owner, one source of truth for job ordering, and zero network round-trips per queue operation. Redis remains in the system solely for the two things it is genuinely better at: pub/sub messaging and expiring TTL keys.

### Why atomic PostgreSQL claim instead of Redis SETNX?

The job state already lives in PostgreSQL. The atomic claim is a single `UPDATE ... WHERE status = 'pending' AND worker_id IS NULL ... RETURNING id`. PostgreSQL row-level locking makes this correct without any additional coordination layer. Adding a Redis lock would introduce a second distributed system with its own failure modes (lock expiry, network partitions) for zero additional correctness benefit. The database atomicity is sufficient.

### Why SSE instead of WebSockets?

Status updates flow in one direction: server → browser. SSE is the correct HTTP primitive for one-directional streaming — it is a plain HTTP response, works through standard proxies (with `proxy_buffering off`), and uses the browser-native `EventSource` API with automatic reconnection. WebSockets would add a stateful bidirectional protocol handshake for a use case that never sends data from browser to server through the live channel.

### Why soft delete instead of hard delete?

Job history is operationally valuable. Engineers investigating a production failure three days later need to see cancelled and failed jobs. Soft delete (`deleted_at IS NOT NULL`) preserves data accessible in the bin while keeping all standard queries clean via a `WHERE deleted_at IS NULL` filter. Hard delete is available from the bin when data genuinely needs to be purged.

### Why a flat key-value settings table instead of a config file or environment variables?

Settings like `dlq_threshold` and `alert_emails` must be changeable at runtime with zero downtime. A config file requires a process restart or a file-watch mechanism. Environment variables require a container restart. A database row is readable by any process at any time and writable via a single API call, instantly effective on the next read.

### Why DAG cascade retry resets all downstream cancelled jobs (Option A)?

The alternative (Option B) would leave downstream jobs cancelled and require engineers to manually retry each one after fixing the root cause. This defeats the purpose of the dependency graph — if you declare that C depends on B which depends on A, you should only have to fix A and re-run; the graph structure should handle the rest. Option A makes the cascade retry the default behaviour, which is consistent with how the dependency declaration itself works.

---

<div align="center">

Built with Python, FastAPI, PostgreSQL, and a real heap.

*Flint — Quietly igniting your payload, every job has a spark.*

</div>
