"""
Worker process supervisor.

A worker is a standalone, horizontally scalable process. It:

* registers presence in Redis (best effort) and its handler capabilities;
* polls PostgreSQL for eligible jobs and orders them in a bounded local heap;
* atomically claims jobs with ``FOR UPDATE SKIP LOCKED``;
* executes handlers with a per-job timeout;
* heartbeats leases for in-flight jobs;
* runs lease recovery + priority aging under a Postgres advisory lock;
* shuts down gracefully, returning unfinished jobs to the queue.
"""

import asyncio
import json
import os
import socket
import uuid
from typing import Any

from sqlalchemy import func, select

from app.core.config import settings
from app.core.logger import get_logger
from app.db.redis import close_redis, get_redis
from app.db.session import AsyncSessionLocal, engine
from app.handlers import list_specs, registered_handler_names
from app.queues.heapq import HeapQueue
from app.queues.postgres import (
    MAINTENANCE_LOCK_KEY,
    JobDispatcher,
    reap_expired_leases,
)
from worker.aging import AgingProcess
from worker.poller import JobPoller
from worker.processor import JobProcessor

logger = get_logger(__name__)

WORKER_KEY_PREFIX = "flint:workers:"
CONTROL_CHANNEL = "flint:worker:control:{worker_id}"
PRESENCE_TTL = 60


class ActiveJobs:
    """Thread-safe-ish tracking of the jobs this process currently owns."""

    def __init__(self) -> None:
        self._ids: set[uuid.UUID] = set()
        self._lock = asyncio.Lock()

    async def add(self, job_id: uuid.UUID) -> None:
        async with self._lock:
            self._ids.add(job_id)

    async def remove(self, job_id: uuid.UUID) -> None:
        async with self._lock:
            self._ids.discard(job_id)

    async def snapshot(self) -> list[uuid.UUID]:
        async with self._lock:
            return list(self._ids)


def generate_worker_id() -> str:
    if settings.WORKER_ID:
        return settings.WORKER_ID
    host = socket.gethostname().split(".")[0][:40]
    return f"{host}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


def resolve_capabilities(requested: list[str] | None) -> list[str]:
    available = registered_handler_names()
    if not available:
        raise RuntimeError(
            "No handlers are registered. Import the handler modules in "
            "app/handlers/handlers/ before starting a worker."
        )
    if requested is None:
        return available

    unknown = sorted(set(requested) - set(available))
    if unknown:
        raise RuntimeError(
            f"Worker requested unknown handler(s): {', '.join(unknown)}. "
            f"Registered handlers: {', '.join(available)}."
        )
    return sorted(set(requested))


async def _heartbeat_loop(
    dispatcher: JobDispatcher,
    active_jobs: ActiveJobs,
    redis_client: Any,
    worker_key: str,
    capabilities: list[str],
    stop_event: asyncio.Event,
) -> None:
    interval = max(1, settings.WORKER_HEARTBEAT_INTERVAL)
    while not stop_event.is_set():
        job_ids = await active_jobs.snapshot()
        presence = json.dumps(
            {
                "status": "busy" if job_ids else "idle",
                "capabilities": capabilities,
                "pid": os.getpid(),
            }
        )

        if redis_client is not None:
            try:
                await redis_client.set(worker_key, presence, ex=PRESENCE_TTL)
            except Exception as exc:
                logger.warning("worker_presence_error", error=str(exc))

        if job_ids:
            try:
                async with AsyncSessionLocal() as session:
                    await dispatcher.heartbeat(session, job_ids)
            except Exception as exc:
                logger.error("worker_lease_heartbeat_error", error=str(exc))

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except TimeoutError:
            pass


async def _control_loop(
    worker_id: str,
    redis_client: Any,
    stop_event: asyncio.Event,
) -> None:
    if redis_client is None:
        return
    channel = CONTROL_CHANNEL.format(worker_id=worker_id)
    pubsub = redis_client.pubsub()
    try:
        await pubsub.subscribe(channel)
        while not stop_event.is_set():
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )
            if message is None:
                continue
            command = message["data"]
            logger.info(
                "worker_control_command",
                worker_id=worker_id,
                command=command,
            )
            if command in {"stop", "restart"}:
                stop_event.set()
                break
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.warning("worker_control_error", error=str(exc))
    finally:
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        except Exception:
            pass


async def _maintenance_loop(
    aging: AgingProcess,
    stop_event: asyncio.Event,
) -> None:
    """Run lease recovery and aging under an advisory lock.

    Only one worker in the fleet performs maintenance at a time. The lock is
    session-level so it survives the commits inside each routine, and is
    released automatically if the connection dies.
    """
    interval = max(1.0, settings.MAINTENANCE_INTERVAL)
    while not stop_event.is_set():
        try:
            async with AsyncSessionLocal() as session:
                locked = await session.scalar(
                    select(func.pg_try_advisory_lock(MAINTENANCE_LOCK_KEY))
                )
                if locked:
                    try:
                        await reap_expired_leases(session)
                        await aging.run(session)
                    finally:
                        await session.scalar(
                            select(func.pg_advisory_unlock(MAINTENANCE_LOCK_KEY))
                        )
                else:
                    await session.rollback()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("maintenance_error", error=str(exc))

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except TimeoutError:
            pass


async def _worker_loop(
    name: str,
    queue: HeapQueue,
    processor: JobProcessor,
    stop_event: asyncio.Event,
) -> None:
    logger.info("worker_task_started", worker=name)
    while not stop_event.is_set():
        try:
            job_id = await queue.pop()
            if job_id is None:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=0.2)
                except TimeoutError:
                    pass
                continue

            logger.info("worker_picked_job", worker=name, job_id=job_id)
            await processor.process(job_id)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(
                "worker_loop_error",
                worker=name,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except TimeoutError:
                pass
    logger.info("worker_task_stopped", worker=name)


async def run_worker_process(
    *,
    worker_id: str | None = None,
    concurrency: int | None = None,
    handlers: list[str] | None = None,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Run a complete worker process until *shutdown_event* is set."""
    worker_id = worker_id or generate_worker_id()
    concurrency = concurrency or settings.WORKER_CONCURRENCY
    capabilities = resolve_capabilities(
        handlers if handlers is not None else settings.worker_handlers
    )

    stop_event = shutdown_event or asyncio.Event()
    queue = HeapQueue(max_size=max(100, settings.WORKER_BATCH_SIZE * 8))
    dispatcher = JobDispatcher(
        worker_id=worker_id,
        capabilities=capabilities,
        lease_seconds=settings.WORKER_LEASE_SECONDS,
    )
    active_jobs = ActiveJobs()
    processor = JobProcessor(
        worker_id=worker_id,
        dispatcher=dispatcher,
        on_start=active_jobs.add,
        on_finish=active_jobs.remove,
    )
    poller = JobPoller(
        queue=queue,
        capabilities=capabilities,
        batch_size=settings.WORKER_BATCH_SIZE,
    )
    aging = AgingProcess()

    redis_client = None
    try:
        redis_client = await get_redis()
    except Exception as exc:
        logger.warning("worker_redis_unavailable", error=str(exc))

    worker_key = f"{WORKER_KEY_PREFIX}{worker_id}"
    initial_presence = json.dumps(
        {"status": "idle", "capabilities": capabilities, "pid": os.getpid()}
    )
    if redis_client is not None:
        try:
            await redis_client.set(worker_key, initial_presence, ex=PRESENCE_TTL)
        except Exception as exc:
            logger.warning("worker_presence_error", error=str(exc))

    logger.info(
        "worker_process_started",
        worker_id=worker_id,
        concurrency=concurrency,
        capabilities=capabilities,
        pid=os.getpid(),
    )

    poller_task = asyncio.create_task(poller.run(stop_event), name="poller")
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(
            dispatcher,
            active_jobs,
            redis_client,
            worker_key,
            capabilities,
            stop_event,
        ),
        name="heartbeat",
    )
    control_task = asyncio.create_task(
        _control_loop(worker_id, redis_client, stop_event),
        name="control",
    )
    maintenance_task = asyncio.create_task(
        _maintenance_loop(aging, stop_event),
        name="maintenance",
    )
    worker_tasks = [
        asyncio.create_task(
            _worker_loop(f"{worker_id}/{i + 1}", queue, processor, stop_event),
            name=f"worker-{i + 1}",
        )
        for i in range(concurrency)
    ]

    try:
        await stop_event.wait()
    finally:
        logger.info("worker_shutdown_started", worker_id=worker_id)
        poller_task.cancel()
        await asyncio.gather(poller_task, return_exceptions=True)

        # Let in-flight handlers finish within the grace period.
        done, pending = await asyncio.wait(
            worker_tasks,
            timeout=settings.WORKER_SHUTDOWN_TIMEOUT,
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        heartbeat_task.cancel()
        control_task.cancel()
        maintenance_task.cancel()
        await asyncio.gather(
            heartbeat_task,
            control_task,
            maintenance_task,
            return_exceptions=True,
        )

        # Any job still held by a cancelled handler is released by the
        # processor. This is a belt-and-braces sweep for jobs whose task died
        # before it could run its cleanup.
        remaining = await active_jobs.snapshot()
        if remaining:
            try:
                async with AsyncSessionLocal() as session:
                    await dispatcher.release(
                        session,
                        remaining,
                        reason="Worker shut down before the handler completed.",
                    )
            except Exception as exc:
                logger.error("worker_release_error", error=str(exc))

        if redis_client is not None:
            try:
                await redis_client.delete(worker_key)
            except Exception:
                pass
            await close_redis()

        await engine.dispose()

        logger.info(
            "worker_process_stopped",
            worker_id=worker_id,
            completed_tasks=len(done),
        )


def describe_handlers() -> list[dict[str, Any]]:
    """Small CLI helper: handler catalogue without payload schemas."""
    return [
        {
            "name": spec.name,
            "description": spec.description,
            "tags": list(spec.tags),
            "version": spec.version,
        }
        for spec in list_specs()
    ]


__all__ = ["run_worker_process", "generate_worker_id", "describe_handlers"]
