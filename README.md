<div align="center">

# 🔥 Flint

### *Quietly igniting your payload, every job has a spark.*

[![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis&logoColor=white)](https://redis.io)

</div>

Flint is a **standalone, horizontally scalable distributed job scheduling service**.
Clients schedule work over REST; an arbitrary number of worker processes claim and
execute jobs with PostgreSQL-backed durability — leases, retries, DAG workflows,
cooperative cancellation, priority aging, dead letter queue and transactional
recurring jobs.

**Docs:** `[https://flint.muizzyranking.me/docs](https://flint.muizzyranking.me/docs)` · **Architecture:** `[https://flint.muizzyranking.me/docs/architecture](https://flint.muizzyranking.me/docs/architecture)`

---

## How it works

Flint runs as three cooperating parts:

```
                         ┌────────────────────────────────┐
  clients ──HTTP──▶ API │ FastAPI admission + validation │──┐
                         └────────────────────────────────┘  │
                                                             ▼
                                              ┌──────────────────────────┐
                                              │      PostgreSQL 16       │
                                              │  durable queue / truth   │
                                              └──────────────────────────┘
                                                        ▲        │
                          claim `SKIP LOCKED`,          │        │ poll
                          heartbeat, complete,          │        ▼
                          requeue                       │  ┌────────────────────────────┐
                                                        │  │ Worker process (N ×)       │
                                                        │  │ poller → local heap →      │
                                                        │  │ executor → retry/DLQ/next  │
                                                        │  └────────────────────────────┘
                             Redis ── presence, SSE events, stop/restart control (optional)
```

* **API** — the only writer of *new* jobs. It validates the request and the
  payload against the target handler's schema **before inserting**, so no
  invalid job ever enters the queue.
* **PostgreSQL** — the durable queue and source of truth. `available_at` drives
  eligibility and backoff; workers claim rows with
  `SELECT … FOR UPDATE SKIP LOCKED`, heartbeats extend leases, and a maintenance
  loop requeues jobs whose lease expired (worker crash protection).
* **Workers** — stateless and horizontally scalable. Each process polls for
  eligible work, buffers a *bounded, time-aware batch* in a local min-heap,
  claims atomically and executes the registered handler with a per-job timeout.
* **Redis** — best-effort only: worker presence + control channel (`stop`/
  `restart`) and SSE fan-out. Dispatch never depends on Redis being up.

---

## Core features

| Concern | How Flint handles it |
| --- | --- |
| Durability | A job is durable the moment `POST /jobs` returns `201` — it is a PostgreSQL row. |
| Exactly-once per lease | Atomic claim via `FOR UPDATE SKIP LOCKED`; one lease owner per job. |
| Worker crashes | Leases + heartbeats; a maintenance reaper requeues expired leases. |
| Graceful shutdown | Workers stop claiming, wait for in-flight handlers, requeue the rest. |
| Priority | `effective_priority` (lower = more urgent) with **aging** to prevent starvation. |
| Retries | Exponential backoff with full jitter, driven by `available_at`. |
| DLQ | Failed jobs land in the dead letter queue with full error details and manual retry. |
| DAG workflows | Dependencies with cycle detection, cascade-cancel on failure, cascade-reset on retry. |
| Cancellation | Immediate for pending jobs; cooperative flag for processing jobs. |
| Recurring jobs | Interval-based; next run inserted **in the same transaction** as completion, idempotent via `(recurrence_id, recurrence_run)`. |
| Typed handlers | Statically registered at process start; the API can only schedule what it knows. |
| Payload admission | Validated against the handler's Pydantic schema before insert. |
| Observability | Structured job logs, dashboard stats, SSE stream. |
| Errors | One global exception pipeline; every endpoint returns the same envelope. |

---

## Project structure

```
flint/
├── app/
│   ├── api/                  # FastAPI routers (jobs, dlq, bin, settings, workers,
│   │                         # logs, handlers, dashboard, sse)
│   ├── core/                 # config, logger, security, exceptions, global error handlers
│   ├── db/                   # engine/session, Redis helper
│   ├── handlers/             # BaseHandler, registry, utils + handlers/ (email, webhook, http_ping, http_fetch, slack_notification, job_retention, log_processing, echo)
│   ├── models/               # Job, JobDependency, JobLog, Setting + enums
│   ├── queues/
│   │   ├── postgres.py       # durable dispatch: eligibility, claim, heartbeat, reaper
│   │   └── heapq.py          # bounded, time-aware per-worker ready buffer
│   ├── schemas/              # request/response + pagination + filters
│   ├── services/             # job, dag, dlq, settings, alert business logic
│   ├── templates/            # DLQ alert email templates
│   ├── cli.py                # `flint api|worker|migrate|handlers|schedule`
│   └── main.py               # FastAPI app assembly
├── worker/
│   ├── worker.py             # process supervisor, presence, maintenance, shutdown
│   ├── processor.py          # claim → execute → retry/DLQ/recurrence/cancel
│   ├── poller.py             # DB → local heap
│   └── aging.py              # priority aging under a Postgres advisory lock
├── alembic/                  # migrations
├── docker-compose.yml        # postgres, redis, migrate (one-shot), api, worker, mailhog
│                             # + optional frontend/dashboard (profiles: frontend, frontend-dev)
└── pyproject.toml            # uv/pytest/ruff config; `flint` console script
```

---

## Getting started

### Requirements

- Python 3.13 + [uv](https://docs.astral.sh/uv/)
- PostgreSQL 16
- Redis 7 (optional in principle, used for SSE/worker control)
- Docker + Compose (easiest way to run everything)

### 1. Configure

```bash
cp .env.example .env      # then edit; set API_KEY, DATABASE_URL, REDIS_URL
```

### 2. Run with Docker (recommended)

```bash
docker compose up -d --build
docker compose up -d --scale worker=3   # add more workers any time
docker compose --profile frontend up -d # serve the dashboard (defaults to the latest flint-ui artifact)
```

Compose starts: `postgres`, `redis`, a one-shot `migrate` service (runs
`flint migrate` before the others start), `api` (with a healthcheck), `worker`
and Mailhog at http://localhost:8025. The API listens on
`127.0.0.1:8000`; Swagger UI is at http://localhost:8000/api/v1/docs.

### 3. …or run locally

```bash
uv sync --group dev
uv run flint migrate
uv run flint api            # or: uvicorn app.main:app --reload --port 8000
uv run flint worker         # in another terminal; scale by starting more
```

### 4. Schedule a job

Every `/api/v1` route needs `X-API-Key` (dev default: `dev-api-key`).

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -d '{
        "type": "webhook_delivery",
        "payload": {"url": "https://example.com/hooks/flint",
                    "body": {"hello": "from flint"}},
        "priority": 1
      }'
```

A worker picks it up within one poll interval and delivers the webhook. Full
endpoint reference lives at `https://flint.muizzyranking.me/docs/api-reference`.

---

## Handlers

Handlers are **statically registered at process start** — never through HTTP.
This keeps the set of schedulable job types equal to the code that ships in
this repository, so payload validation is trustworthy and there is no
remote-code-execution surface.

Adding one is a single decorated class in `app/handlers/handlers/`:

```python
from pydantic import BaseModel, EmailStr, Field
from app.handlers import BaseHandler, register_handler


class ResizePayload(BaseModel):
    image_url: str


@register_handler("resize_image", description="Resize an image.", tags=("images",))
class ResizeHandler(BaseHandler[ResizePayload]):
    payload_model = ResizePayload

    async def execute(self, payload: ResizePayload) -> dict:
        ...  # payload arrives already validated and typed;
        #     raise on failure so retries/DLQ apply
```

There is deliberately no third-party plugin mechanism: handlers live in the
repository so the schema catalogue, the API and the workers always agree. The
API and every worker process load the registry at startup; workers can further
restrict themselves with `WORKER_HANDLERS=resize_image,send_email`.

`GET /api/v1/handlers` exposes the catalogue, including each payload's JSON
Schema — frontends can render scheduling forms without hard-coding job types.

---

## Scheduling semantics worth knowing

- **Priority / aging / starvation**: medium jobs start aging after 2 minutes, low
  after 5 minutes — `effective_priority` drifts toward `1.0` (HIGH) by 0.1 per
  maintenance pass, floored at `1.0`. Stale-heap compaction keeps the local
  buffer honest.
- **Local heap is not storage**: it is a bounded ordering buffer of at most a
  few hundred entries per worker. If a worker dies, nothing is lost — the rows
  are still in PostgreSQL and any other worker picks them up.
- **Cancellation**: cancelling a *processing* job sets a flag; the worker honours
  it at its next checkpoint. Side effects already performed cannot be rolled
  back.
- **Delivery guarantee**: at-least-once per lease. Handlers should be idempotent
  (a crash mid-handler causes a re-lease and a second attempt).
- **Stop / restart**: `POST /api/v1/workers/{id}/stop|restart` publishes a
  control command; the worker finishes in-flight work and exits cleanly. In
  Compose the worker uses `restart: on-failure`, so clean exits are not
  auto-restarted.

---

## Operations cheat-sheet

```bash
flint --version                                      # build sanity
flint handlers                                       # what is schedulable here
flint api --host 0.0.0.0 --port 8000                 # run API
flint worker                                         # run one worker process
flint migrate                                        # alembic upgrade head
flint schedule --type send_email \
  --payload '{"to": "a@b.c", "subject": "hi"}'       # ops/manual enqueue
```

Health:
`GET /health` (liveness) · `GET /health/ready` (DB + Redis readiness — Redis
degraded still reports ready).

Logs are structured JSON (`structlog`) written to `logs/flint.log`.

---

## Configuration

All settings are env-driven (see `.env.example` and `app/core/config.py`):

| Variable | Default | Purpose |
|---|---|---|
| `API_KEY` / `API_KEYS` | `dev-api-key` | `X-API-Key` auth (prod forbids the dev default). |
| `DATABASE_URL` | `postgresql+asyncpg://…` | PostgreSQL DSN. |
| `REDIS_URL` | `redis://localhost:6379/0` | Presence/SSE/control. |
| `WORKER_CONCURRENCY` | `2` | Executor tasks per worker process. |
| `WORKER_BATCH_SIZE` | `25` | Rows fetched per poll. |
| `WORKER_HANDLERS` | *(empty = all)* | Capability filter, comma separated. |
| `WORKER_POLL_INTERVAL` | `0.5` | Seconds between DB polls. |
| `WORKER_LEASE_SECONDS` | `60` | Lease TTL. |
| `WORKER_HEARTBEAT_INTERVAL` | `15` | Heartbeat cadence. |
| `WORKER_SHUTDOWN_TIMEOUT` | `30` | Drain budget on shutdown. |
| `JOB_TIMEOUT_SECONDS` | `300` | Per-execution handler timeout. |
| `MAINTENANCE_INTERVAL` | `15` | Lease reaper cadence. |
| `*_AGE_THRESHOLD` / `AGING_DECREMENT` | `120,300 / 0.1` | Starvation prevention (aging runs with the maintenance cadence). |
| `MAX_PAYLOAD_BYTES` | `262144` | Admission payload cap. |
| `LOG_LEVEL`, `LOG_FILE` | `INFO`, `logs/flint.log` | Structured logging. |

Runtime-only settings (`dlq_threshold`, `alert_emails`) live in PostgreSQL and
change via `PATCH /api/v1/settings` without restarts.

---

## Tests

```bash
uv run pytest
```

Test development intentionally trails the implementation; the suite
exists (`tests/`) and can be extended as the system stabilises.

---

<div align="center">

*Flint — quietly igniting your payload, every job has a spark.*

</div>
