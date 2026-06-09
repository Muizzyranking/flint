# Flint — Implementation Guide
> *Quietly igniting your payload, every job has a spark.*

This document is the complete technical implementation reference for Flint. It covers every module, database schema, API endpoint, worker logic, and internal system in enough detail to build the full system from scratch.

---

## Table of Contents

1. [Repository Structure](#repository-structure)
2. [Technology Stack](#technology-stack)
3. [Database Schema](#database-schema)
4. [Base Models & Conventions](#base-models--conventions)
5. [Configuration & Settings](#configuration--settings)
6. [Job System](#job-system)
7. [Priority Queue — Heap](#priority-queue--heap)
8. [Priority Queue — Timing Wheel](#priority-queue--timing-wheel)
9. [Worker System](#worker-system)
10. [Scheduler Process](#scheduler-process)
11. [Job Handlers](#job-handlers)
12. [Retry System](#retry-system)
13. [Dead Letter Queue](#dead-letter-queue)
14. [DAG Workflow](#dag-workflow)
15. [Starvation Prevention](#starvation-prevention)
16. [Cancellation](#cancellation)
17. [Duplicate Protection](#duplicate-protection)
18. [Recurring Jobs](#recurring-jobs)
19. [Logging](#logging)
20. [SSE — Live Updates](#sse--live-updates)
21. [API Endpoints](#api-endpoints)
22. [Authentication](#authentication)
23. [Benchmark System](#benchmark-system)
24. [Email Alerts](#email-alerts)
25. [Testing](#testing)
26. [Environment Variables](#environment-variables)
27. [Docker Compose](#docker-compose)

---

## Repository Structure

```
flint-backend/
├── app/
│   ├── __init__.py
│   ├── main.py                        # FastAPI app entry point
│   ├── config.py                      # Settings and env vars
│   ├── dependencies.py                # FastAPI dependency injection
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   └── v1/
│   │       ├── __init__.py
│   │       ├── router.py              # Mounts all v1 routers
│   │       ├── jobs.py                # Job CRUD endpoints
│   │       ├── workers.py             # Worker control endpoints
│   │       ├── dlq.py                 # DLQ endpoints
│   │       ├── settings.py            # Settings endpoints
│   │       ├── logs.py                # Log viewer endpoints
│   │       ├── benchmark.py           # Benchmark endpoints
│   │       ├── bin.py                 # Bin (soft deleted jobs)
│   │       └── sse.py                 # SSE stream endpoint
│   │
│   ├── core/
│   │   ├── __init__.py
│   │   ├── security.py                # API key validation
│   │   ├── logger.py                  # Structured logger setup
│   │   ├── exceptions.py              # Custom exception classes
│   │   └── response.py                # Standard API response builder
│   │
│   ├── db/
│   │   ├── __init__.py
│   │   ├── base.py                    # SQLAlchemy base + BaseModel
│   │   ├── session.py                 # Async DB session factory
│   │   └── migrations/                # Alembic migrations
│   │       ├── env.py
│   │       ├── script.py.mako
│   │       └── versions/
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   ├── job.py                     # Job ORM model
│   │   ├── job_dependency.py          # DAG dependency ORM model
│   │   ├── job_log.py                 # Job log ORM model
│   │   ├── dlq.py                     # DLQ ORM model
│   │   └── setting.py                 # Settings ORM model
│   │
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── job.py                     # Job Pydantic schemas
│   │   ├── dlq.py                     # DLQ Pydantic schemas
│   │   ├── setting.py                 # Setting Pydantic schemas
│   │   ├── log.py                     # Log Pydantic schemas
│   │   ├── benchmark.py               # Benchmark Pydantic schemas
│   │   └── response.py                # Standard response schema
│   │
│   ├── services/
│   │   ├── __init__.py
│   │   ├── job_service.py             # Job business logic
│   │   ├── dlq_service.py             # DLQ business logic
│   │   ├── setting_service.py         # Settings business logic
│   │   ├── dag_service.py             # DAG resolution logic
│   │   └── alert_service.py           # DLQ email alert logic
│   │
│   ├── queues/
│   │   ├── __init__.py
│   │   ├── base.py                    # Abstract queue interface
│   │   ├── heap_queue.py              # Heap-based priority queue
│   │   └── timing_wheel.py            # Timing wheel implementation
│   │
│   ├── handlers/
│   │   ├── __init__.py
│   │   ├── base.py                    # Abstract handler interface
│   │   ├── webhook.py                 # Webhook delivery handler
│   │   ├── email.py                   # Email simulation handler
│   │   └── log_processor.py           # Log processing handler
│   │
│   └── templates/
│       └── emails/
│           ├── dlq_alert.html         # DLQ alert Jinja template
│           └── base.html              # Base email template
│
├── worker/
│   ├── __init__.py
│   ├── worker.py                      # Worker entry point
│   ├── processor.py                   # Job processing logic
│   └── aging.py                       # Starvation prevention aging loop
│
├── scheduler/
│   ├── __init__.py
│   └── scheduler.py                   # Scheduler entry point
│
├── benchmark/
│   ├── __init__.py
│   └── runner.py                      # Benchmark script
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                    # Shared fixtures
│   ├── unit/
│   │   ├── test_heap_queue.py
│   │   ├── test_timing_wheel.py
│   │   ├── test_retry.py
│   │   ├── test_dag.py
│   │   ├── test_starvation.py
│   │   └── test_handlers.py
│   └── integration/
│       ├── test_jobs_api.py
│       ├── test_dlq_api.py
│       ├── test_settings_api.py
│       ├── test_worker.py
│       └── test_sse.py
│
├── logs/                              # Log file output directory
├── .env.example
├── .env
├── alembic.ini
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

---

## Technology Stack

| Concern | Technology |
|---|---|
| Backend framework | FastAPI |
| Language | Python 3.12 |
| Primary database | PostgreSQL 16 |
| In-memory / broker | Redis 7 |
| ORM | SQLAlchemy 2.x (async) |
| Migrations | Alembic |
| Validation | Pydantic v2 |
| Email (SMTP mock) | Mailhog |
| Email sending | aiosmtplib |
| HTML email templates | Jinja2 |
| HTTP client (webhook handler) | httpx (async) |
| Structured logging | structlog |
| Server | Uvicorn + Gunicorn |
| Containerization | Docker + Docker Compose |
| Reverse proxy | Nginx |
| SSL | Certbot + Let's Encrypt |
| Testing | pytest + pytest-asyncio |
| API docs | Swagger (FastAPI native) |

---

## Database Schema

### Conventions
- All primary keys are UUIDs (`uuid4`)
- All tables have `id`, `created_at`, `updated_at`
- Soft delete uses `deleted_at` (nullable timestamp)
- `deleted_at IS NULL` is applied to all standard queries as a default filter

---

### Table: `jobs`

```sql
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            VARCHAR(100) NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    priority        SMALLINT NOT NULL DEFAULT 2
                        CHECK (priority IN (1, 2, 3)),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                        CHECK (status IN (
                            'pending', 'processing',
                            'completed', 'failed', 'cancelled'
                        )),
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    interval_seconds BIGINT NULL,             -- NULL means non-recurring
    retry_count     SMALLINT NOT NULL DEFAULT 0,
    max_retries     SMALLINT NOT NULL DEFAULT 3,
    next_retry_at   TIMESTAMPTZ NULL,
    last_error      TEXT NULL,
    effective_priority FLOAT NOT NULL DEFAULT 2.0,  -- used by heap, decremented by aging
    cancellation_requested BOOLEAN NOT NULL DEFAULT FALSE,
    worker_id       VARCHAR(100) NULL,        -- ID of worker currently processing
    started_at      TIMESTAMPTZ NULL,
    completed_at    TIMESTAMPTZ NULL,
    is_dlq          BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at      TIMESTAMPTZ NULL,         -- soft delete
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_status ON jobs(status) WHERE deleted_at IS NULL;
CREATE INDEX idx_jobs_scheduled_at ON jobs(scheduled_at) WHERE deleted_at IS NULL;
CREATE INDEX idx_jobs_effective_priority ON jobs(effective_priority) WHERE deleted_at IS NULL;
CREATE INDEX idx_jobs_is_dlq ON jobs(is_dlq) WHERE deleted_at IS NULL;
CREATE INDEX idx_jobs_deleted_at ON jobs(deleted_at);
```

**Field notes:**
- `interval_seconds` stores the recurring interval as total seconds. A flexible interval input (n-seconds, n-minutes, n-hours, n-days, n-months, n-years) is converted to seconds at creation time and stored here. `NULL` means the job is not recurring.
- `effective_priority` is a float that starts equal to `priority` and is decremented by the aging process. Lower = higher urgency. The heap uses this value, not the raw `priority`.
- `cancellation_requested` is the cooperative cancellation flag. Workers check this at checkpoints.
- `worker_id` is set when a worker picks up the job. Used for duplicate protection.
- `is_dlq` marks whether the job is currently in the dead letter queue.

---

### Table: `job_dependencies`

```sql
CREATE TABLE job_dependencies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    depends_on_id   UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(job_id, depends_on_id)
);

CREATE INDEX idx_job_dependencies_job_id ON job_dependencies(job_id);
CREATE INDEX idx_job_dependencies_depends_on_id ON job_dependencies(depends_on_id);
```

**Field notes:**
- `job_id` is the job that is waiting.
- `depends_on_id` is the job that must complete first.
- A job can have multiple dependencies. All must be `completed` before the job enters the heap.

---

### Table: `job_logs`

```sql
CREATE TABLE job_logs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event       VARCHAR(50) NOT NULL,   -- created, started, retry_attempted, failed, cancelled, completed
    message     TEXT NOT NULL,
    metadata    JSONB NULL,             -- extra structured context
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_job_logs_job_id ON job_logs(job_id);
CREATE INDEX idx_job_logs_event ON job_logs(event);
CREATE INDEX idx_job_logs_created_at ON job_logs(created_at);
```

---

### Table: `settings`

```sql
CREATE TABLE settings (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key         VARCHAR(100) NOT NULL UNIQUE,
    value       TEXT NOT NULL,
    description TEXT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed default settings
INSERT INTO settings (key, value, description) VALUES
    ('dlq_threshold', '5', 'Number of DLQ jobs that triggers an alert email'),
    ('alert_emails', '[]', 'JSON array of emails to receive DLQ alerts'),
    ('scheduler_strategy', 'heap', 'Active scheduling algorithm: heap or timing_wheel');
```

---

## Base Models & Conventions

### SQLAlchemy Base Model (`app/db/base.py`)

```python
import uuid
from datetime import datetime, timezone
from sqlalchemy import DateTime, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class BaseModel(Base):
    __abstract__ = True

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
```

### Standard API Response (`app/core/response.py`)

Every API endpoint returns this shape:

```python
from typing import Any, Generic, TypeVar, Optional
from pydantic import BaseModel

T = TypeVar("T")

class Meta(BaseModel):
    page: Optional[int] = None
    limit: Optional[int] = None
    total: Optional[int] = None

class ErrorDetail(BaseModel):
    field: Optional[str] = None
    message: str

class FlintResponse(BaseModel, Generic[T]):
    message: str
    data: Optional[T] = None
    errors: list[ErrorDetail] = []
    meta: Optional[Meta] = None
```

**Example success (paginated):**
```json
{
  "message": "Jobs retrieved successfully",
  "data": [...],
  "errors": [],
  "meta": {
    "page": 1,
    "limit": 20,
    "total": 143
  }
}
```

**Example error:**
```json
{
  "message": "Validation failed",
  "data": null,
  "errors": [
    { "field": "priority", "message": "must be 1, 2, or 3" },
    { "field": "type", "message": "field is required" }
  ],
  "meta": null
}
```

---

## Configuration & Settings

### Environment Variables (`app/config.py`)

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # App
    APP_NAME: str = "Flint"
    APP_ENV: str = "development"
    DEBUG: bool = False

    # API
    API_KEY: str                          # Required, no default

    # Database
    DATABASE_URL: str                     # PostgreSQL async URL
    REDIS_URL: str = "redis://localhost:6379/0"

    # Mailhog SMTP
    SMTP_HOST: str = "mailhog"
    SMTP_PORT: int = 1025
    SMTP_FROM: str = "flint@flint.local"

    # Worker
    WORKER_POLL_INTERVAL: float = 1.0     # seconds between polls
    WORKER_ID: str = ""                   # Set per worker instance

    # Scheduler
    SCHEDULER_POLL_INTERVAL: float = 1.0  # seconds between due-job checks

    # Aging
    AGING_INTERVAL: float = 30.0          # seconds between aging runs
    MEDIUM_PRIORITY_AGE_THRESHOLD: int = 120   # 2 minutes in seconds
    LOW_PRIORITY_AGE_THRESHOLD: int = 300      # 5 minutes in seconds
    AGING_DECREMENT: float = 0.1          # how much effective_priority drops per aging run

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/flint.log"

    class Config:
        env_file = ".env"

settings = Settings()
```

---

## Job System

### Job Types

```python
class JobType(str, Enum):
    SEND_EMAIL = "send_email"
    WEBHOOK_DELIVERY = "webhook_delivery"
    LOG_PROCESSING = "log_processing"
```

### Job Status Flow

```
pending → processing → completed
                    → failed → (retry) → pending
                                       → dlq (after max retries)
                    → cancelled
```

### Priority Levels

```python
class JobPriority(int, Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3
```

### Recurring Interval Parsing

The API accepts flexible human-readable intervals. They are converted to total seconds and stored in `interval_seconds`.

```python
# Accepted interval formats
# "30s"     → 30 seconds
# "5m"      → 300 seconds
# "2h"      → 7200 seconds
# "1d"      → 86400 seconds
# "1mo"     → 2592000 seconds (30 days)
# "1y"      → 31536000 seconds (365 days)

import re

INTERVAL_MULTIPLIERS = {
    's': 1,
    'm': 60,
    'h': 3600,
    'd': 86400,
    'mo': 2592000,
    'y': 31536000,
}

def parse_interval(interval_str: str) -> int:
    """
    Parse a flexible interval string into total seconds.
    Raises ValueError if the format is invalid.
    """
    pattern = r'^(\d+)(s|mo|m|h|d|y)$'
    match = re.match(pattern, interval_str.strip().lower())
    if not match:
        raise ValueError(
            f"Invalid interval format: '{interval_str}'. "
            "Expected format: <number><unit> where unit is one of: "
            "s, m, h, d, mo, y"
        )
    value, unit = int(match.group(1)), match.group(2)
    return value * INTERVAL_MULTIPLIERS[unit]
```

### Job Pydantic Schemas (`app/schemas/job.py`)

```python
from uuid import UUID
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field, field_validator
from app.models.job import JobStatus, JobType, JobPriority

class JobCreate(BaseModel):
    type: JobType
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: JobPriority = JobPriority.MEDIUM
    scheduled_at: Optional[datetime] = None        # defaults to now if not provided
    interval: Optional[str] = None                 # e.g. "5m", "1h", "2d"
    dependency_ids: Optional[list[UUID]] = None    # list of job IDs this job depends on

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v):
        if v is not None:
            parse_interval(v)   # raises ValueError if invalid
        return v

class JobResponse(BaseModel):
    id: UUID
    type: str
    payload: dict[str, Any]
    priority: int
    status: str
    scheduled_at: datetime
    interval_seconds: Optional[int]
    retry_count: int
    max_retries: int
    last_error: Optional[str]
    effective_priority: float
    cancellation_requested: bool
    worker_id: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    is_dlq: bool
    created_at: datetime
    updated_at: datetime
    dependencies: Optional[list[UUID]] = None

    model_config = {"from_attributes": True}

class JobListResponse(BaseModel):
    jobs: list[JobResponse]

class JobStatusUpdate(BaseModel):
    status: JobStatus

class JobCancelRequest(BaseModel):
    pass   # No body needed, cancel is triggered by job ID in URL
```

---

## Priority Queue — Heap

**File:** `app/queues/heap_queue.py`

The heap is an in-memory min-heap. The heap lives in Redis as a sorted set for persistence across restarts, but a local `heapq` structure is used in the worker process for O(log n) operations.

### Score Tuple

Each job in the heap is represented as a tuple:

```python
(effective_priority: float, scheduled_at: float, created_at: float, job_id: str)
```

- `effective_priority` — float starting at the raw priority (1.0, 2.0, 3.0), decremented over time by aging. Lower = more urgent. This is the primary sort key.
- `scheduled_at` — Unix timestamp. Earlier time = higher urgency. Breaks ties in effective_priority.
- `created_at` — Unix timestamp. Older job = higher urgency. Breaks ties in scheduled_at.
- `job_id` — UUID string. Ensures uniqueness. Breaks all remaining ties deterministically.

Python's `heapq` compares tuples left-to-right, so `heappop` always gives the job with the smallest tuple — which is the most urgent job.

### Redis Sorted Set

Redis is used as the persistent backing store. The sorted set key is `flint:queue`. The score stored in Redis is `effective_priority` only (a float). The full tuple ordering is enforced in-process when the worker loads jobs from Redis into its local heap.

```python
# Push a job onto the Redis sorted set
await redis.zadd("flint:queue", {job_id: effective_priority})

# Pop the N most urgent job IDs from Redis
job_ids = await redis.zrange("flint:queue", 0, 0)

# Remove a job from Redis after picking it up
await redis.zrem("flint:queue", job_id)
```

### HeapQueue Class

```python
import heapq
import asyncio
from dataclasses import dataclass, field
from typing import Optional
from app.queues.base import BaseQueue

@dataclass(order=True)
class HeapEntry:
    effective_priority: float
    scheduled_at: float
    created_at: float
    job_id: str = field(compare=True)

class HeapQueue(BaseQueue):
    def __init__(self):
        self._heap: list[HeapEntry] = []
        self._lock = asyncio.Lock()
        self._entry_finder: dict[str, HeapEntry] = {}
        self._REMOVED = "__removed__"

    async def push(self, job_id: str, effective_priority: float,
                   scheduled_at: float, created_at: float) -> None:
        async with self._lock:
            if job_id in self._entry_finder:
                await self._mark_removed(job_id)
            entry = HeapEntry(effective_priority, scheduled_at, created_at, job_id)
            self._entry_finder[job_id] = entry
            heapq.heappush(self._heap, entry)

    async def pop(self) -> Optional[str]:
        async with self._lock:
            while self._heap:
                entry = heapq.heappop(self._heap)
                if entry.job_id != self._REMOVED:
                    del self._entry_finder[entry.job_id]
                    return entry.job_id
            return None

    async def remove(self, job_id: str) -> None:
        async with self._lock:
            await self._mark_removed(job_id)

    async def update_priority(self, job_id: str, new_priority: float) -> None:
        """Called by the aging process to re-score a job."""
        if job_id in self._entry_finder:
            old = self._entry_finder[job_id]
            await self.push(job_id, new_priority, old.scheduled_at, old.created_at)

    async def _mark_removed(self, job_id: str) -> None:
        entry = self._entry_finder.pop(job_id)
        entry.job_id = self._REMOVED

    async def size(self) -> int:
        return len(self._entry_finder)
```

**Lazy deletion pattern:** When a job is re-scored or removed, the old entry is marked `__removed__` in-place rather than rebuilding the heap. On `pop`, removed entries are discarded until a valid entry is found. This is the standard Python heapq pattern for mutable priority queues.

---

## Priority Queue — Timing Wheel

**File:** `app/queues/timing_wheel.py`

A timing wheel is a circular array of buckets. Each bucket represents a time slot. A pointer advances every tick and processes the jobs in the current bucket.

### Design

```
Wheel size: 3600 slots (one per second, covering 1 hour)
Tick interval: 1 second
Each slot: a list of job_ids due at that second
Overflow: jobs scheduled beyond the wheel's range are held in an overflow dict
          and inserted into the wheel when they come within range
```

### TimingWheel Class

```python
import asyncio
import time
from collections import defaultdict
from typing import Optional
from app.queues.base import BaseQueue

WHEEL_SIZE = 3600  # 1 hour coverage at 1-second resolution

class TimingWheel(BaseQueue):
    def __init__(self):
        self._wheel: list[list[tuple[float, str]]] = [
            [] for _ in range(WHEEL_SIZE)
        ]
        self._overflow: dict[float, list[tuple[float, str]]] = defaultdict(list)
        self._current_slot: int = 0
        self._start_time: float = time.monotonic()
        self._lock = asyncio.Lock()

    def _get_slot(self, scheduled_at_unix: float) -> Optional[int]:
        """
        Returns the wheel slot index for a given scheduled time.
        Returns None if the job is beyond the wheel's range (goes to overflow).
        """
        now = time.time()
        delta = scheduled_at_unix - now
        if delta < 0:
            delta = 0
        if delta >= WHEEL_SIZE:
            return None
        return (self._current_slot + int(delta)) % WHEEL_SIZE

    async def push(self, job_id: str, effective_priority: float,
                   scheduled_at: float, created_at: float) -> None:
        async with self._lock:
            slot = self._get_slot(scheduled_at)
            entry = (effective_priority, job_id)
            if slot is None:
                self._overflow[scheduled_at].append(entry)
            else:
                self._wheel[slot].append(entry)
                self._wheel[slot].sort(key=lambda x: x[0])  # sort by priority within slot

    async def tick(self) -> list[str]:
        """
        Advance the wheel by one slot. Returns job_ids due in this slot.
        Also drains any overflow jobs that are now within range.
        """
        async with self._lock:
            due_jobs = [job_id for _, job_id in self._wheel[self._current_slot]]
            self._wheel[self._current_slot] = []
            self._current_slot = (self._current_slot + 1) % WHEEL_SIZE

            # Drain overflow
            now = time.time()
            to_insert = [ts for ts in self._overflow if ts <= now + WHEEL_SIZE]
            for ts in to_insert:
                for entry in self._overflow.pop(ts):
                    slot = self._get_slot(ts)
                    if slot is not None:
                        self._wheel[slot].append(entry)
                        self._wheel[slot].sort(key=lambda x: x[0])

            return due_jobs

    async def pop(self) -> Optional[str]:
        due = await self.tick()
        return due[0] if due else None

    async def remove(self, job_id: str) -> None:
        async with self._lock:
            for slot in self._wheel:
                slot[:] = [(p, jid) for p, jid in slot if jid != job_id]

    async def size(self) -> int:
        total = sum(len(slot) for slot in self._wheel)
        total += sum(len(v) for v in self._overflow.values())
        return total
```

### Tradeoffs: Heap vs Timing Wheel

| Property | Heap | Timing Wheel |
|---|---|---|
| Insert | O(log n) | O(1) amortized |
| Pop next | O(log n) | O(1) amortized |
| Re-score (aging) | O(log n) | O(n) worst case |
| Memory | O(n) proportional to jobs | O(W + n) W = wheel size |
| Far-future jobs | Natural, no overhead | Overflow dict, extra complexity |
| Priority ordering | Native, exact | Per-slot sort only |
| Best use case | Priority-heavy workloads | High-throughput time-based scheduling |
| Weakness | Re-scoring is expensive at scale | Poor native priority support |

The heap wins for Flint's workload because priority and re-scoring (aging) are first-class requirements. The timing wheel wins on raw insert/pop throughput at high job volumes when priority is less important.

---

## Worker System

**File:** `worker/worker.py`

Workers are independent processes. They are started separately from the FastAPI app and communicate with it only through the shared PostgreSQL database and Redis.

### Worker Lifecycle

```
Start
  ↓
Register worker_id in Redis (SET flint:worker:<id> "active")
  ↓
Poll loop:
  ↓
  Pop job_id from active queue (heap or timing wheel based on scheduler_strategy setting)
  ↓
  Attempt to claim job (atomic DB update with worker_id + status = processing)
    → If claim fails (another worker got it): skip, continue loop
    → If claim succeeds: process job
  ↓
  Check cancellation_requested flag before processing
    → If True: mark cancelled, log, continue loop
  ↓
  Execute handler
  ↓
  Check cancellation_requested flag at handler checkpoint
    → If True: mark cancelled, log, continue loop
  ↓
  On success: mark completed, log, handle recurrence
  On failure: increment retry_count, schedule retry or send to DLQ
  ↓
  Continue poll loop
```

### Worker Class (`worker/worker.py`)

```python
import asyncio
import os
import signal
import uuid
from app.core.logger import get_logger
from app.db.session import get_db_session
from app.queues.heap_queue import HeapQueue
from app.queues.timing_wheel import TimingWheel
from worker.processor import JobProcessor

logger = get_logger(__name__)

class FlintWorker:
    def __init__(self):
        self.worker_id = os.getenv("WORKER_ID", str(uuid.uuid4())[:8])
        self.running = False
        self.processor = JobProcessor(worker_id=self.worker_id)
        self._shutdown_event = asyncio.Event()

    async def start(self):
        self.running = True
        await self._register()
        logger.info("worker_started", worker_id=self.worker_id)

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, self._handle_shutdown)
        loop.add_signal_handler(signal.SIGINT, self._handle_shutdown)

        await self._poll_loop()

    async def _poll_loop(self):
        while self.running:
            try:
                job_id = await self._pop_next_job()
                if job_id:
                    await self.processor.process(job_id)
                else:
                    await asyncio.sleep(float(os.getenv("WORKER_POLL_INTERVAL", 1.0)))
            except Exception as e:
                logger.error("worker_poll_error", worker_id=self.worker_id, error=str(e))
                await asyncio.sleep(1.0)

    async def _pop_next_job(self):
        # Reads active strategy from settings and pops from the appropriate queue
        ...

    async def _register(self):
        # Sets flint:worker:<worker_id> = "active" in Redis with expiry
        ...

    def _handle_shutdown(self):
        logger.info("worker_shutdown_signal", worker_id=self.worker_id)
        self.running = False
        self._shutdown_event.set()

if __name__ == "__main__":
    worker = FlintWorker()
    asyncio.run(worker.start())
```

### Duplicate Protection

The claim step uses a single atomic SQL UPDATE with a WHERE clause:

```sql
UPDATE jobs
SET
    status = 'processing',
    worker_id = :worker_id,
    started_at = NOW(),
    updated_at = NOW()
WHERE
    id = :job_id
    AND status = 'pending'
    AND worker_id IS NULL
    AND deleted_at IS NULL
RETURNING id;
```

If the UPDATE returns no rows, the job was already claimed by another worker. The current worker discards the job and continues polling. This is safe without additional locking because PostgreSQL UPDATE is atomic at the row level.

---

## Scheduler Process

**File:** `scheduler/scheduler.py`

The scheduler is a separate process responsible for:

1. Watching for `pending` jobs whose `scheduled_at <= NOW()` and pushing them onto the active queue
2. Running the aging loop (starvation prevention)
3. Handling recurring job re-scheduling after completion

```python
import asyncio
from app.core.logger import get_logger
from app.db.session import get_db_session
from worker.aging import AgingProcess

logger = get_logger(__name__)

class FlintScheduler:
    def __init__(self):
        self.running = False

    async def start(self):
        self.running = True
        logger.info("scheduler_started")
        await asyncio.gather(
            self._due_job_loop(),
            self._aging_loop(),
        )

    async def _due_job_loop(self):
        """
        Every SCHEDULER_POLL_INTERVAL seconds:
        - Query for pending jobs where scheduled_at <= NOW() and not in queue yet
        - Push them onto the active heap/timing wheel via Redis
        """
        while self.running:
            try:
                await self._push_due_jobs()
            except Exception as e:
                logger.error("scheduler_due_job_error", error=str(e))
            await asyncio.sleep(float(os.getenv("SCHEDULER_POLL_INTERVAL", 1.0)))

    async def _aging_loop(self):
        """
        Every AGING_INTERVAL seconds:
        - Run the aging process to decrement effective_priority on waiting jobs
        """
        while self.running:
            try:
                await AgingProcess().run()
            except Exception as e:
                logger.error("scheduler_aging_error", error=str(e))
            await asyncio.sleep(float(os.getenv("AGING_INTERVAL", 30.0)))
```

---

## Job Handlers

### Abstract Base Handler (`app/handlers/base.py`)

```python
from abc import ABC, abstractmethod
from typing import Any

class BaseHandler(ABC):
    @abstractmethod
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Execute the job logic.
        Returns a result dict on success.
        Raises an exception on failure — the worker catches this and handles retry logic.
        Must check cancellation flag at meaningful checkpoints.
        """
        ...
```

### Webhook Delivery Handler (`app/handlers/webhook.py`)

This is the primary real-logic handler.

```python
import httpx
from typing import Any
from app.handlers.base import BaseHandler
from app.core.logger import get_logger

logger = get_logger(__name__)

TIMEOUT_SECONDS = 10
MAX_REDIRECTS = 3

class WebhookHandler(BaseHandler):
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Payload shape:
        {
            "url": "https://webhook.site/...",
            "method": "POST",               # default POST
            "headers": {},                  # optional
            "body": {}                      # the data to send
        }

        Real logic:
        - Validates URL is present
        - Makes actual HTTP request using httpx
        - Raises on 4xx/5xx responses (triggers retry)
        - Raises on timeout (triggers retry)
        - Returns response status and body on success
        """
        url = payload.get("url")
        if not url:
            raise ValueError("Webhook payload must include 'url'")

        method = payload.get("method", "POST").upper()
        headers = payload.get("headers", {})
        body = payload.get("body", {})

        logger.info("webhook_attempt", url=url, method=method)

        async with httpx.AsyncClient(
            timeout=TIMEOUT_SECONDS,
            follow_redirects=True,
            max_redirects=MAX_REDIRECTS
        ) as client:
            response = await client.request(
                method=method,
                url=url,
                json=body,
                headers=headers
            )

        if response.status_code >= 400:
            raise Exception(
                f"Webhook failed: HTTP {response.status_code} from {url}. "
                f"Body: {response.text[:500]}"
            )

        logger.info("webhook_success", url=url, status_code=response.status_code)
        return {
            "status_code": response.status_code,
            "response_body": response.text[:1000]
        }
```

### Email Handler (`app/handlers/email.py`)

```python
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any
from app.handlers.base import BaseHandler
from app.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class EmailHandler(BaseHandler):
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Payload shape:
        {
            "to": "user@example.com",
            "subject": "Hello",
            "body": "Email body text",
            "html": "<p>Optional HTML body</p>"
        }

        Real logic:
        - Validates required fields
        - Constructs MIME email (text + optional HTML)
        - Sends via aiosmtplib to Mailhog
        - Raises on SMTP error (triggers retry)
        """
        to = payload.get("to")
        subject = payload.get("subject")
        body = payload.get("body", "")

        if not to or not subject:
            raise ValueError("Email payload must include 'to' and 'subject'")

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = settings.SMTP_FROM
        msg["To"] = to

        msg.attach(MIMEText(body, "plain"))
        if payload.get("html"):
            msg.attach(MIMEText(payload["html"], "html"))

        logger.info("email_attempt", to=to, subject=subject)

        await aiosmtplib.send(
            msg,
            hostname=settings.SMTP_HOST,
            port=settings.SMTP_PORT,
        )

        logger.info("email_success", to=to)
        return {"to": to, "subject": subject, "delivered": True}
```

### Log Processing Handler (`app/handlers/log_processor.py`)

```python
from typing import Any
from collections import Counter
from app.handlers.base import BaseHandler
from app.core.logger import get_logger

logger = get_logger(__name__)

LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

class LogProcessorHandler(BaseHandler):
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Payload shape:
        {
            "lines": [
                "2026-06-09T10:00:00Z ERROR Something failed",
                "2026-06-09T10:00:01Z INFO Request received",
                ...
            ],
            "source": "app-server-01"   # optional label
        }

        Real logic:
        - Parses each line extracting: timestamp, level, message
        - Counts occurrences per log level
        - Identifies the most frequent error messages
        - Returns a structured summary
        - Raises if lines is empty or unparseable (triggers retry)
        """
        lines = payload.get("lines", [])
        source = payload.get("source", "unknown")

        if not lines:
            raise ValueError("Log processing payload must include non-empty 'lines'")

        parsed = []
        parse_errors = []

        for line in lines:
            parts = line.strip().split(" ", 2)
            if len(parts) < 3:
                parse_errors.append(line)
                continue
            timestamp, level, message = parts
            if level not in LOG_LEVELS:
                parse_errors.append(line)
                continue
            parsed.append({"timestamp": timestamp, "level": level, "message": message})

        if not parsed:
            raise ValueError("No parseable log lines found in payload")

        level_counts = Counter(entry["level"] for entry in parsed)
        error_messages = [
            entry["message"] for entry in parsed
            if entry["level"] in ("ERROR", "CRITICAL")
        ]
        top_errors = Counter(error_messages).most_common(5)

        summary = {
            "source": source,
            "total_lines": len(lines),
            "parsed_lines": len(parsed),
            "parse_errors": len(parse_errors),
            "level_counts": dict(level_counts),
            "top_errors": [{"message": msg, "count": cnt} for msg, cnt in top_errors],
            "has_critical": level_counts.get("CRITICAL", 0) > 0,
        }

        logger.info("log_processing_complete", source=source, summary=summary)
        return summary
```

### Handler Registry (`app/handlers/__init__.py`)

```python
from app.models.job import JobType
from app.handlers.webhook import WebhookHandler
from app.handlers.email import EmailHandler
from app.handlers.log_processor import LogProcessorHandler

HANDLER_REGISTRY: dict[str, type] = {
    JobType.WEBHOOK_DELIVERY: WebhookHandler,
    JobType.SEND_EMAIL: EmailHandler,
    JobType.LOG_PROCESSING: LogProcessorHandler,
}

def get_handler(job_type: str):
    handler_class = HANDLER_REGISTRY.get(job_type)
    if not handler_class:
        raise ValueError(f"No handler registered for job type: {job_type}")
    return handler_class()
```

---

## Retry System

**File:** `worker/processor.py`

### Backoff with Jitter

```python
import random
import math

def calculate_next_retry_delay(attempt: int) -> float:
    """
    Exponential backoff with full jitter.
    attempt 1 → base ~1s  → jitter range [0.5, 1.5]
    attempt 2 → base ~5s  → jitter range [2.5, 7.5]
    attempt 3 → base ~25s → jitter range [12.5, 37.5]

    Formula: base = 5^(attempt-1), jitter = base * random(0.5, 1.5)
    """
    base = math.pow(5, attempt - 1)
    jitter = base * random.uniform(0.5, 1.5)
    return round(jitter, 2)
```

### Retry Flow in Processor

```python
async def handle_failure(self, job, error: Exception):
    new_retry_count = job.retry_count + 1

    logger.warning(
        "job_retry_attempted",
        job_id=str(job.id),
        attempt=new_retry_count,
        error=str(error)
    )

    if new_retry_count >= job.max_retries:
        await self._send_to_dlq(job, error)
        return

    delay = calculate_next_retry_delay(new_retry_count)
    next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)

    async with get_db_session() as session:
        await session.execute(
            update(Job)
            .where(Job.id == job.id)
            .values(
                status="pending",
                retry_count=new_retry_count,
                next_retry_at=next_retry_at,
                last_error=str(error),
                worker_id=None,
                updated_at=func.now()
            )
        )
        await session.commit()

    # Schedule the retry: push back onto the queue after delay
    await asyncio.sleep(delay)
    await self.queue.push(
        str(job.id),
        job.effective_priority,
        next_retry_at.timestamp(),
        job.created_at.timestamp()
    )
```

---

## Dead Letter Queue

**File:** `app/services/dlq_service.py`

### Moving a Job to DLQ

```python
async def send_to_dlq(job_id: UUID, error: str, session: AsyncSession):
    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(
            status="failed",
            is_dlq=True,
            last_error=error,
            worker_id=None,
            updated_at=func.now()
        )
    )
    await session.commit()

    await log_job_event(job_id, "job_failed", f"Moved to DLQ after max retries. Error: {error}", session)

    # Check threshold and alert
    dlq_count = await get_dlq_count(session)
    threshold = int(await get_setting("dlq_threshold", session))
    if dlq_count >= threshold:
        await send_dlq_alert(dlq_count, session)
```

### Manual DLQ Retry

When an engineer triggers a manual retry from the DLQ view:

1. Reset `status` to `pending`, `is_dlq` to `False`, `retry_count` to `0`, `last_error` to `None`, `worker_id` to `None`
2. If the job has dependencies — re-evaluate the DAG. If all dependencies are `completed`, push to the queue. If not, leave as `pending` with unresolved dependencies.
3. If the retry fails again → the job goes back to DLQ. There is no limit on manual DLQ retries.
4. Log the manual retry event.

---

## DAG Workflow

**File:** `app/services/dag_service.py`

### Dependency Resolution

When a job completes, the DAG service runs:

```python
async def on_job_completed(job_id: UUID, session: AsyncSession):
    """
    Called after every successful job completion.
    Finds all jobs that depend on this job and checks if they are now unblocked.
    """
    # Find all jobs that list job_id as a dependency
    dependents = await session.execute(
        select(JobDependency.job_id)
        .where(JobDependency.depends_on_id == job_id)
    )
    dependent_job_ids = [row[0] for row in dependents.fetchall()]

    for dependent_id in dependent_job_ids:
        await maybe_unblock_job(dependent_id, session)

async def maybe_unblock_job(job_id: UUID, session: AsyncSession):
    """
    Checks if ALL dependencies of a job are completed.
    If yes, pushes the job onto the active queue.
    """
    deps = await session.execute(
        select(JobDependency.depends_on_id)
        .where(JobDependency.job_id == job_id)
    )
    dep_ids = [row[0] for row in deps.fetchall()]

    if not dep_ids:
        return  # No dependencies, already eligible

    completed_deps = await session.execute(
        select(func.count(Job.id))
        .where(Job.id.in_(dep_ids))
        .where(Job.status == "completed")
    )
    completed_count = completed_deps.scalar()

    if completed_count == len(dep_ids):
        # All dependencies completed, unblock this job
        await session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(status="pending", updated_at=func.now())
        )
        await session.commit()
        job = await session.get(Job, job_id)
        await queue.push(
            str(job.id),
            job.effective_priority,
            job.scheduled_at.timestamp(),
            job.created_at.timestamp()
        )
```

### Cascade Cancellation on Dependency Failure

```python
async def on_job_failed(job_id: UUID, session: AsyncSession):
    """
    When a job fails permanently (goes to DLQ), cancel all downstream dependents.
    """
    await cancel_downstream(job_id, session)

async def cancel_downstream(job_id: UUID, session: AsyncSession):
    dependents = await session.execute(
        select(JobDependency.job_id)
        .where(JobDependency.depends_on_id == job_id)
    )
    dependent_ids = [row[0] for row in dependents.fetchall()]

    for dep_id in dependent_ids:
        await session.execute(
            update(Job)
            .where(Job.id == dep_id)
            .where(Job.status.in_(["pending"]))
            .values(
                status="cancelled",
                last_error=f"Cancelled: dependency job {job_id} failed permanently",
                updated_at=func.now()
            )
        )
        await log_job_event(dep_id, "job_cancelled",
            f"Dependency {job_id} failed. Job cancelled automatically.", session)
        await cancel_downstream(dep_id, session)  # recurse through the graph

    await session.commit()
```

### Cascade Retry on DAG Root Retry

```python
async def on_dag_root_retried(job_id: UUID, session: AsyncSession):
    """
    When a failed DAG root job is manually retried,
    reset all downstream cancelled jobs back to pending.
    """
    await reset_downstream(job_id, session)

async def reset_downstream(job_id: UUID, session: AsyncSession):
    dependents = await session.execute(
        select(JobDependency.job_id)
        .where(JobDependency.depends_on_id == job_id)
    )
    dependent_ids = [row[0] for row in dependents.fetchall()]

    for dep_id in dependent_ids:
        dep_job = await session.get(Job, dep_id)
        if dep_job and dep_job.status == "cancelled" and "dependency" in (dep_job.last_error or ""):
            await session.execute(
                update(Job)
                .where(Job.id == dep_id)
                .values(
                    status="pending",
                    last_error=None,
                    retry_count=0,
                    worker_id=None,
                    updated_at=func.now()
                )
            )
            await log_job_event(dep_id, "job_created",
                f"Reset to pending: upstream dependency {job_id} was retried.", session)
            await reset_downstream(dep_id, session)

    await session.commit()
```

---

## Starvation Prevention

**File:** `worker/aging.py`

### Logic

The aging process runs every `AGING_INTERVAL` seconds (default 30s). It looks at all `pending` jobs that have been waiting longer than their threshold and decrements their `effective_priority`.

```python
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, update
from app.db.session import get_db_session
from app.models.job import Job
from app.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class AgingProcess:
    async def run(self):
        async with get_db_session() as session:
            now = datetime.now(timezone.utc)

            medium_threshold = now - timedelta(seconds=settings.MEDIUM_PRIORITY_AGE_THRESHOLD)
            low_threshold = now - timedelta(seconds=settings.LOW_PRIORITY_AGE_THRESHOLD)

            # Age medium priority jobs waiting > 2 minutes
            await session.execute(
                update(Job)
                .where(Job.status == "pending")
                .where(Job.priority == 2)
                .where(Job.effective_priority > 1.0)
                .where(Job.created_at <= medium_threshold)
                .where(Job.deleted_at.is_(None))
                .values(
                    effective_priority=Job.effective_priority - settings.AGING_DECREMENT,
                    updated_at=func.now()
                )
            )

            # Age low priority jobs waiting > 5 minutes
            await session.execute(
                update(Job)
                .where(Job.status == "pending")
                .where(Job.priority == 3)
                .where(Job.effective_priority > 1.0)
                .where(Job.created_at <= low_threshold)
                .where(Job.deleted_at.is_(None))
                .values(
                    effective_priority=Job.effective_priority - settings.AGING_DECREMENT,
                    updated_at=func.now()
                )
            )

            await session.commit()

            # Update the heap with new effective priorities
            # Re-push affected jobs onto the queue with updated scores
            await self._sync_heap_after_aging(session)

            logger.info("aging_complete", timestamp=now.isoformat())
```

**Documented thresholds:**
- Medium priority (2) jobs waiting longer than **2 minutes** begin aging. Their `effective_priority` decrements by `0.1` every 30 seconds. After 10 aging cycles (~5 minutes), a medium job reaches `effective_priority = 1.0` and competes equally with high priority jobs.
- Low priority (3) jobs waiting longer than **5 minutes** begin aging. They reach `effective_priority = 1.0` after 20 aging cycles (~10 minutes total wait time).
- `effective_priority` is floored at `1.0` and never goes below it.

---

## Cancellation

### Cancellation of Pending Jobs

Straightforward: set `status = 'cancelled'`, `cancellation_requested = True`. The job is never picked up by a worker.

### Cancellation of Processing Jobs

**Documented behaviour:** A cancellation request on a job that is currently `processing` is honoured at the next checkpoint within the handler, not immediately. The job will finish its current atomic step, check the `cancellation_requested` flag, and if `True`, stop execution, perform any available cleanup, and mark itself `cancelled`. This means a processing job may take up to the duration of one handler step to fully cancel.

The worker processor checks the flag at these checkpoints:
1. Before calling `handler.execute()`
2. Inside long-running handlers at natural pause points (e.g. between HTTP retries in webhook handler, between log line batches in log processor)
3. After `handler.execute()` returns but before marking `completed`

```python
async def check_cancellation(self, job_id: UUID, session: AsyncSession) -> bool:
    result = await session.execute(
        select(Job.cancellation_requested)
        .where(Job.id == job_id)
    )
    return result.scalar() or False

# In processor.process():
if await self.check_cancellation(job.id, session):
    await self.mark_cancelled(job.id, session)
    logger.info("job_cancelled", job_id=str(job.id), reason="cancellation_requested")
    return
```

---

## Duplicate Protection

Duplicate protection is enforced at the database level via the atomic claim query described in the Worker section:

```sql
UPDATE jobs
SET status = 'processing', worker_id = :worker_id, started_at = NOW()
WHERE id = :job_id
  AND status = 'pending'
  AND worker_id IS NULL
  AND deleted_at IS NULL
RETURNING id;
```

If two workers attempt to claim the same job simultaneously, PostgreSQL's row-level locking ensures only one UPDATE succeeds. The other gets zero rows back and discards the job. No explicit locks, semaphores, or Redis locking needed — the database atomicity handles it.

---

## Recurring Jobs

When a job with `interval_seconds IS NOT NULL` completes successfully:

```python
async def handle_recurrence(self, job, session: AsyncSession):
    if not job.interval_seconds:
        return

    next_scheduled = datetime.now(timezone.utc) + timedelta(seconds=job.interval_seconds)

    new_job = Job(
        type=job.type,
        payload=job.payload,
        priority=job.priority,
        effective_priority=float(job.priority),
        scheduled_at=next_scheduled,
        interval_seconds=job.interval_seconds,
        status="pending",
    )
    session.add(new_job)
    await session.commit()

    logger.info(
        "recurring_job_scheduled",
        parent_job_id=str(job.id),
        new_job_id=str(new_job.id),
        next_run=next_scheduled.isoformat()
    )
```

The new job is created as a fresh job with `pending` status and the same type, payload, priority, and interval. The scheduler picks it up when its `scheduled_at` is due.

---

## Logging

**File:** `app/core/logger.py`

Flint uses `structlog` for all structured logging. Every log entry is a JSON object written to both stdout and a rotating log file.

```python
import structlog
import logging
import sys
from logging.handlers import RotatingFileHandler
from app.config import settings

def setup_logging():
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.LOG_LEVEL)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
    )

    file_handler = RotatingFileHandler(
        settings.LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    logging.basicConfig(
        handlers=[logging.StreamHandler(sys.stdout), file_handler],
        level=settings.LOG_LEVEL,
    )

def get_logger(name: str):
    return structlog.get_logger(name)
```

### Required Log Events

Every significant event must produce a structured log entry:

| Event | Logger call |
|---|---|
| Job created | `logger.info("job_created", job_id=..., type=..., priority=...)` |
| Job started | `logger.info("job_started", job_id=..., worker_id=...)` |
| Retry attempted | `logger.warning("job_retry_attempted", job_id=..., attempt=..., error=...)` |
| Job failed (DLQ) | `logger.error("job_failed", job_id=..., error=...)` |
| Job cancelled | `logger.info("job_cancelled", job_id=..., reason=...)` |
| Job completed | `logger.info("job_completed", job_id=..., worker_id=..., duration_ms=...)` |
| Worker started | `logger.info("worker_started", worker_id=...)` |
| DLQ threshold hit | `logger.warning("dlq_threshold_reached", count=..., threshold=...)` |
| Aging ran | `logger.info("aging_complete", timestamp=...)` |

---

## SSE — Live Updates

**File:** `app/api/v1/sse.py`

The SSE endpoint streams job status updates to the frontend in real-time.

### Flow

1. When a job's status changes (anywhere in the system), the worker/scheduler publishes a message to a Redis channel: `flint:events`
2. The FastAPI SSE endpoint is subscribed to `flint:events` via Redis pub/sub
3. Connected browser clients receive the event immediately

### Redis Publisher (called from worker after every status change)

```python
import json
import redis.asyncio as aioredis

async def publish_job_event(redis_client, job_id: str, status: str, extra: dict = None):
    event = {
        "job_id": job_id,
        "status": status,
        **(extra or {})
    }
    await redis_client.publish("flint:events", json.dumps(event))
```

### SSE Endpoint

```python
import asyncio
import json
from fastapi import APIRouter, Request, Depends
from fastapi.responses import StreamingResponse
import redis.asyncio as aioredis

router = APIRouter()

async def event_stream(request: Request, redis_client: aioredis.Redis):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("flint:events")
    try:
        async for message in pubsub.listen():
            if await request.is_disconnected():
                break
            if message["type"] == "message":
                data = message["data"].decode()
                yield f"data: {data}\n\n"
    finally:
        await pubsub.unsubscribe("flint:events")
        await pubsub.close()

@router.get("/stream")
async def sse_stream(request: Request):
    return StreamingResponse(
        event_stream(request, get_redis()),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",       # tells Nginx not to buffer this response
            "Connection": "keep-alive",
        }
    )
```

---

## API Endpoints

All endpoints are prefixed with `/api/v1`. All responses follow the standard `FlintResponse` shape.

### Authentication

Every request must include the header:
```
X-API-Key: <your-api-key>
```

Requests without a valid key return:
```json
HTTP 401
{
  "message": "Unauthorized",
  "data": null,
  "errors": [{ "message": "Invalid or missing API key" }],
  "meta": null
}
```

---

### Jobs — `/api/v1/jobs`

#### `POST /api/v1/jobs`
Create a new job.

**Request body:**
```json
{
  "type": "webhook_delivery",
  "payload": {
    "url": "https://webhook.site/abc",
    "body": { "event": "user.created" }
  },
  "priority": 1,
  "scheduled_at": "2026-06-10T10:00:00Z",
  "interval": "1h",
  "dependency_ids": ["uuid-of-other-job"]
}
```

**Response `201`:**
```json
{
  "message": "Job created successfully",
  "data": { ...JobResponse },
  "errors": [],
  "meta": null
}
```

---

#### `GET /api/v1/jobs`
List all jobs (excludes soft-deleted, excludes DLQ).

**Query params:**
- `page` (int, default 1)
- `limit` (int, default 20, max 100)
- `status` (str, optional filter)
- `type` (str, optional filter)
- `priority` (int, optional filter)
- `search` (str, optional — searches job ID and type)

**Response `200`:**
```json
{
  "message": "Jobs retrieved successfully",
  "data": [ ...JobResponse[] ],
  "errors": [],
  "meta": { "page": 1, "limit": 20, "total": 143 }
}
```

---

#### `GET /api/v1/jobs/{job_id}`
Get a single job with its dependency list and logs.

**Response `200`:**
```json
{
  "message": "Job retrieved successfully",
  "data": {
    ...JobResponse,
    "dependencies": [ ...UUID[] ],
    "logs": [ ...JobLogResponse[] ]
  },
  "errors": [],
  "meta": null
}
```

---

#### `PATCH /api/v1/jobs/{job_id}/cancel`
Request cancellation of a job. Works on `pending` and `processing` jobs.

- `pending` → immediately set to `cancelled`
- `processing` → set `cancellation_requested = true`, worker honours at next checkpoint

**Response `200`:**
```json
{
  "message": "Cancellation requested",
  "data": { ...JobResponse },
  "errors": [],
  "meta": null
}
```

---

#### `DELETE /api/v1/jobs/{job_id}`
Soft delete a job. Sets `deleted_at`. Only works on `completed`, `failed`, or `cancelled` jobs.

**Response `200`:**
```json
{
  "message": "Job moved to bin",
  "data": null,
  "errors": [],
  "meta": null
}
```

---

### Bin — `/api/v1/bin`

#### `GET /api/v1/bin`
List soft-deleted jobs.

**Query params:** `page`, `limit`

---

#### `PATCH /api/v1/bin/{job_id}/restore`
Restore a soft-deleted job. Clears `deleted_at`.

---

#### `DELETE /api/v1/bin/{job_id}`
Permanently delete a job. Hard delete — row is removed from the database.

---

### DLQ — `/api/v1/dlq`

#### `GET /api/v1/dlq`
List all DLQ jobs with error details.

**Query params:** `page`, `limit`

**Response `200`:**
```json
{
  "message": "DLQ retrieved successfully",
  "data": [
    {
      "id": "uuid",
      "type": "webhook_delivery",
      "payload": {},
      "last_error": "Webhook failed: HTTP 500 from https://...",
      "retry_count": 3,
      "created_at": "...",
      "updated_at": "..."
    }
  ],
  "errors": [],
  "meta": { "page": 1, "limit": 20, "total": 7 }
}
```

---

#### `POST /api/v1/dlq/{job_id}/retry`
Manually retry a DLQ job. Resets status to `pending`, clears retry count, re-evaluates DAG.

---

#### `DELETE /api/v1/dlq/{job_id}`
Remove a job from DLQ and soft-delete it.

---

### Settings — `/api/v1/settings`

#### `GET /api/v1/settings`
Get all current settings.

**Response `200`:**
```json
{
  "message": "Settings retrieved",
  "data": {
    "dlq_threshold": "5",
    "alert_emails": "[\"admin@example.com\"]",
    "scheduler_strategy": "heap"
  },
  "errors": [],
  "meta": null
}
```

---

#### `PATCH /api/v1/settings`
Update one or more settings.

**Request body:**
```json
{
  "dlq_threshold": "10",
  "alert_emails": "[\"admin@example.com\", \"oncall@example.com\"]",
  "scheduler_strategy": "timing_wheel"
}
```

**Response `200`:**
```json
{
  "message": "Settings updated",
  "data": { ...updated settings },
  "errors": [],
  "meta": null
}
```

---

### Workers — `/api/v1/workers`

#### `GET /api/v1/workers`
List all registered workers and their status.

**Response `200`:**
```json
{
  "message": "Workers retrieved",
  "data": [
    { "worker_id": "w-abc123", "status": "active", "last_seen": "..." }
  ],
  "errors": [],
  "meta": null
}
```

---

#### `POST /api/v1/workers/{worker_id}/stop`
Signal a worker to stop after completing its current job.

---

#### `POST /api/v1/workers/{worker_id}/restart`
Signal a worker to restart.

---

### Logs — `/api/v1/logs`

#### `GET /api/v1/logs`
View structured logs from the log file. Supports filtering by event type and job_id.

**Query params:** `page`, `limit`, `event`, `job_id`

---

### Benchmark — `/api/v1/benchmark`

#### `POST /api/v1/benchmark/run`
Trigger the benchmark script programmatically.

**Request body:**
```json
{
  "n": 10000,
  "algorithm": "both"
}
```

**Response `200`:**
```json
{
  "message": "Benchmark complete",
  "data": {
    "n": 10000,
    "heap": {
      "insert_time_ms": 142.3,
      "pop_time_ms": 138.7,
      "total_time_ms": 281.0
    },
    "timing_wheel": {
      "insert_time_ms": 48.1,
      "pop_time_ms": 44.2,
      "total_time_ms": 92.3
    },
    "winner": "timing_wheel",
    "notes": "Timing wheel wins on raw throughput. Heap wins on priority ordering correctness."
  },
  "errors": [],
  "meta": null
}
```

---

### SSE — `/api/v1/sse`

#### `GET /api/v1/sse/stream`
SSE stream. Returns `text/event-stream`. Emits job status update events.

**Event shape:**
```json
{ "job_id": "uuid", "status": "completed", "worker_id": "w-abc123" }
```

---

## Authentication

**File:** `app/core/security.py`

```python
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from app.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(api_key: str = Security(api_key_header)):
    if not api_key or api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    return api_key
```

Applied globally via FastAPI dependency injection on the router.

---

## Benchmark System

**File:** `benchmark/runner.py`

The benchmark script measures insert and pop performance for both the heap and timing wheel at configurable scale.

```python
import asyncio
import time
import random
import argparse
from app.queues.heap_queue import HeapQueue
from app.queues.timing_wheel import TimingWheel

async def run_benchmark(n: int):
    results = {}

    # Generate n random job entries
    jobs = [
        (
            str(i),                              # job_id
            float(random.randint(1, 3)),         # effective_priority
            time.time() + random.uniform(0, 3600),  # scheduled_at
            time.time() - random.uniform(0, 600),   # created_at
        )
        for i in range(n)
    ]

    # Benchmark Heap
    heap = HeapQueue()
    start = time.perf_counter()
    for job_id, ep, sa, ca in jobs:
        await heap.push(job_id, ep, sa, ca)
    heap_insert_time = time.perf_counter() - start

    start = time.perf_counter()
    while await heap.size() > 0:
        await heap.pop()
    heap_pop_time = time.perf_counter() - start

    results["heap"] = {
        "insert_time_ms": round(heap_insert_time * 1000, 2),
        "pop_time_ms": round(heap_pop_time * 1000, 2),
        "total_time_ms": round((heap_insert_time + heap_pop_time) * 1000, 2),
    }

    # Benchmark Timing Wheel
    wheel = TimingWheel()
    start = time.perf_counter()
    for job_id, ep, sa, ca in jobs:
        await wheel.push(job_id, ep, sa, ca)
    wheel_insert_time = time.perf_counter() - start

    start = time.perf_counter()
    while await wheel.size() > 0:
        await wheel.tick()
    wheel_pop_time = time.perf_counter() - start

    results["timing_wheel"] = {
        "insert_time_ms": round(wheel_insert_time * 1000, 2),
        "pop_time_ms": round(wheel_pop_time * 1000, 2),
        "total_time_ms": round((wheel_insert_time + wheel_pop_time) * 1000, 2),
    }

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=10000)
    args = parser.parse_args()
    results = asyncio.run(run_benchmark(args.n))
    print(results)
```

---

## Email Alerts

**File:** `app/services/alert_service.py`

When the DLQ count crosses the threshold, this service fires an alert email to all configured recipients.

### Jinja Template (`app/templates/emails/dlq_alert.html`)

The email template is a branded Flint HTML email including:
- Flint SVG logo inline
- Alert summary: current DLQ count, threshold, timestamp
- Table of the most recent N DLQ jobs with: job ID, type, error message, retry count, time of failure
- A direct link to the DLQ dashboard page
- Flint brand colors, clean transactional email layout

```python
from jinja2 import Environment, FileSystemLoader
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
from app.config import settings

jinja_env = Environment(loader=FileSystemLoader("app/templates"))

async def send_dlq_alert(dlq_count: int, session):
    alert_emails_raw = await get_setting("alert_emails", session)
    recipients: list[str] = json.loads(alert_emails_raw)

    if not recipients:
        return

    recent_dlq_jobs = await get_recent_dlq_jobs(session, limit=10)

    template = jinja_env.get_template("emails/dlq_alert.html")
    html_body = template.render(
        dlq_count=dlq_count,
        threshold=await get_setting("dlq_threshold", session),
        jobs=recent_dlq_jobs,
        dashboard_url=f"https://app.yourdomain.com/dlq",
        app_name="Flint"
    )

    for recipient in recipients:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[Flint Alert] DLQ threshold reached — {dlq_count} failed jobs"
        msg["From"] = settings.SMTP_FROM
        msg["To"] = recipient
        msg.attach(MIMEText(html_body, "html"))

        await aiosmtplib.send(
            msg,
            hostname=settings.SMTP_HOST,
            port=settings.SMTP_PORT,
        )
```

---

## Testing

### Test Setup (`tests/conftest.py`)

```python
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from app.main import app
from app.db.base import Base
from app.config import settings

TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5433/flint_test"

@pytest_asyncio.fixture(scope="session")
async def engine():
    engine = create_async_engine(TEST_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()

@pytest_asyncio.fixture
async def session(engine):
    async with AsyncSession(engine) as s:
        yield s
        await s.rollback()

@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "test-key"}
    ) as c:
        yield c
```

### Unit Tests

**`tests/unit/test_heap_queue.py`**
- Push N jobs, verify `pop` returns them in correct priority order
- Verify ties broken by `scheduled_at`, then `created_at`
- Verify lazy deletion (remove mid-heap, pop still works correctly)
- Verify `update_priority` re-scores correctly
- Performance: push 10,000 jobs, measure time

**`tests/unit/test_timing_wheel.py`**
- Push jobs at various future times, verify `tick` returns them in the correct slot
- Verify overflow jobs are inserted correctly when they come within range
- Verify slot sorting by priority
- Performance: push 10,000 jobs, measure time

**`tests/unit/test_retry.py`**
- Verify `calculate_next_retry_delay` for attempts 1, 2, 3
- Verify delay is within expected jitter range for each attempt
- Verify retry_count increments on failure
- Verify job goes to DLQ after max_retries

**`tests/unit/test_dag.py`**
- Job with unmet dependency stays `pending`, not pushed to queue
- Job unblocked when all dependencies complete
- Cascade cancellation propagates through a 3-level chain
- Cascade retry resets all downstream cancelled jobs

**`tests/unit/test_starvation.py`**
- Medium priority job waiting > 2 minutes has effective_priority decremented
- Low priority job waiting > 5 minutes has effective_priority decremented
- effective_priority never goes below 1.0

**`tests/unit/test_handlers.py`**
- WebhookHandler raises on 4xx/5xx response
- WebhookHandler raises on timeout
- WebhookHandler returns correct result on 200
- EmailHandler raises on missing `to` / `subject`
- LogProcessorHandler raises on empty lines
- LogProcessorHandler returns correct summary structure

### Integration Tests

**`tests/integration/test_jobs_api.py`**
- `POST /api/v1/jobs` creates a job and returns 201
- `GET /api/v1/jobs` returns paginated list
- `GET /api/v1/jobs/{id}` returns job with logs and dependencies
- `PATCH /api/v1/jobs/{id}/cancel` cancels a pending job immediately
- `DELETE /api/v1/jobs/{id}` soft deletes a completed job
- Invalid priority returns 422 with correct error shape
- Missing API key returns 401

**`tests/integration/test_dlq_api.py`**
- Job lands in DLQ after 3 failed retries
- `GET /api/v1/dlq` returns DLQ jobs with error details
- `POST /api/v1/dlq/{id}/retry` resets job to pending

**`tests/integration/test_settings_api.py`**
- `GET /api/v1/settings` returns all settings
- `PATCH /api/v1/settings` updates dlq_threshold
- `PATCH /api/v1/settings` with invalid strategy returns 422

**`tests/integration/test_worker.py`**
- Worker claims job atomically (two workers cannot claim the same job)
- Worker marks job completed on handler success
- Worker increments retry_count on handler failure
- Worker honours cancellation_requested flag

**`tests/integration/test_sse.py`**
- SSE endpoint returns `text/event-stream` content type
- Status change event is published to Redis and received on stream

---

## Environment Variables

```env
# .env.example

APP_NAME=Flint
APP_ENV=development
DEBUG=false

# Required
API_KEY=your-secret-api-key-here

# Database
DATABASE_URL=postgresql+asyncpg://flint:flint@localhost:5432/flint
REDIS_URL=redis://localhost:6379/0

# SMTP (Mailhog)
SMTP_HOST=mailhog
SMTP_PORT=1025
SMTP_FROM=flint@flint.local

# Worker
WORKER_POLL_INTERVAL=1.0
WORKER_ID=worker-1

# Scheduler
SCHEDULER_POLL_INTERVAL=1.0

# Aging
AGING_INTERVAL=30.0
MEDIUM_PRIORITY_AGE_THRESHOLD=120
LOW_PRIORITY_AGE_THRESHOLD=300
AGING_DECREMENT=0.1

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/flint.log
```

---

## Docker Compose

```yaml
# docker-compose.yml
version: "3.9"

services:

  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: flint
      POSTGRES_PASSWORD: flint
      POSTGRES_DB: flint
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U flint"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  mailhog:
    image: mailhog/mailhog
    ports:
      - "8025:8025"    # Mailhog web UI (dev only, not exposed in prod)

  api:
    build: .
    command: gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000
    env_file: .env
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ./logs:/app/logs

  worker-1:
    build: .
    command: python -m worker.worker
    env_file: .env
    environment:
      WORKER_ID: worker-1
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ./logs:/app/logs

  worker-2:
    build: .
    command: python -m worker.worker
    env_file: .env
    environment:
      WORKER_ID: worker-2
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ./logs:/app/logs

  scheduler:
    build: .
    command: python -m scheduler.scheduler
    env_file: .env
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ./logs:/app/logs

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./nginx/certs:/etc/nginx/certs:ro
    depends_on:
      - api

volumes:
  postgres_data:
  redis_data:
```

---

## Build Order

When implementing Flint, follow this order to avoid circular dependency issues:

1. Database models and migrations
2. Base response schema and core utilities (logger, exceptions, response builder)
3. Settings service and seed data
4. Heap queue and timing wheel (pure logic, no DB dependency)
5. Job schemas and job service (CRUD only, no worker logic)
6. Job handlers (webhook, email, log processor)
7. Retry logic and DLQ service
8. DAG service
9. Worker process
10. Scheduler process and aging process
11. SSE endpoint and Redis pub/sub
12. All API routers
13. Alert service and Jinja email templates
14. Benchmark script and endpoint
15. Tests (unit first, then integration)
16. Docker Compose and Nginx config
