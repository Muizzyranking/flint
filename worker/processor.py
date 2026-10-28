import asyncio
import math
import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.db.redis import publish_event
from app.db.session import AsyncSessionLocal
from app.handlers import get_handler
from app.models.enum import JobStatus
from app.models.job import Job
from app.models.job_log import JobLog, LogEvent
from app.queues.postgres import JobDispatcher
from app.services import dag as dag_service
from app.services.dlq import send_to_dlq

logger = get_logger(__name__)


class JobTimeoutError(TimeoutError):
    def __init__(self, job_type: str, seconds: int) -> None:
        super().__init__(f"Job '{job_type}' exceeded its {seconds}s timeout.")


def calculate_next_retry_delay(attempt: int) -> float:
    """Exponential backoff with full jitter."""
    base = math.pow(5, attempt - 1)
    return round(base * random.uniform(0.5, 1.5), 2)


class JobProcessor:
    """Executes one claimed job at a time.

    Completion, retry and DLQ writes are conditional on this worker still
    owning the job's lease. A concurrent cancellation therefore cannot be
    silently overwritten by a late completion.
    """

    def __init__(
        self,
        worker_id: str,
        dispatcher: JobDispatcher,
        on_start: Any | None = None,
        on_finish: Any | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.dispatcher = dispatcher
        self._on_start = on_start
        self._on_finish = on_finish

    async def process(self, job_id: str) -> None:
        try:
            parsed_id = uuid.UUID(job_id)
        except ValueError:
            logger.error("invalid_job_id", job_id=job_id)
            return

        async with AsyncSessionLocal() as session:
            job = await self.dispatcher.claim(session, job_id=parsed_id)

        if job is None:
            # Another worker claimed it first, or it was cancelled/deleted.
            return

        if self._on_start is not None:
            await self._on_start(job.id)
        try:
            await self._execute(job)
        except asyncio.CancelledError:
            # Worker shutdown: nothing must swallow this; the release path in
            # _execute has already requeued the job.
            raise
        except Exception as exc:
            # Containment boundary. Handler failures are already resolved
            # inside _execute (retry or DLQ); anything raising here is an
            # unexpected infrastructure-level error. It must never kill the
            # worker, and the job is left leased so maintenance requeues it
            # on lease expiry.
            logger.error(
                "job_unexpected_failure",
                job_id=str(job.id),
                type=job.type,
                error_type=type(exc).__name__,
                error=str(exc),
            )
        finally:
            if self._on_finish is not None:
                await self._on_finish(job.id)

    async def _execute(self, job: Job) -> None:
        await self._record_started(job)

        if await self._is_cancelled(job.id):
            await self._mark_cancelled(job.id, "cancellation_requested_before_execute")
            return

        logger.info(
            "job_started",
            job_id=str(job.id),
            type=job.type,
            worker_id=self.worker_id,
            retry_count=job.retry_count,
        )

        started = datetime.now(UTC)
        try:
            handler = get_handler(job.type)
            validated = handler.validate_payload(job.payload or {})
            result = await asyncio.wait_for(
                handler.execute(validated),
                timeout=settings.JOB_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            await self._handle_failure(
                job,
                JobTimeoutError(job.type, settings.JOB_TIMEOUT_SECONDS),
            )
            return
        except asyncio.CancelledError:
            # The worker is shutting down. Return the job to the queue instead
            # of leaving it in 'processing' until the lease expires.
            await asyncio.shield(self._release(job.id))
            raise
        except Exception as exc:
            elapsed = self._elapsed_ms(started)
            logger.warning(
                "job_execution_failed",
                job_id=str(job.id),
                type=job.type,
                error_type=type(exc).__name__,
                error=str(exc),
                elapsed_ms=elapsed,
                retry_count=job.retry_count,
            )
            await self._handle_failure(job, exc)
            return

        elapsed = self._elapsed_ms(started)
        await self._complete(job, result, elapsed)

    async def _complete(
        self,
        job: Job,
        result: dict[str, Any],
        elapsed_ms: int,
    ) -> None:
        async with AsyncSessionLocal() as session:
            completed = await session.execute(
                update(Job)
                .where(
                    Job.id == job.id,
                    Job.status == JobStatus.PROCESSING,
                    Job.worker_id == self.worker_id,
                    Job.cancellation_requested.is_(False),
                )
                .values(
                    status=JobStatus.COMPLETED,
                    completed_at=datetime.now(UTC),
                    worker_id=None,
                    lease_expires_at=None,
                    updated_at=datetime.now(UTC),
                )
                .returning(Job.id)
            )
            if completed.scalar_one_or_none() is None:
                # Cancellation won the race, or the lease was recovered.
                if await self._is_cancelled(job.id, session):
                    await self._mark_cancelled(
                        job.id,
                        "cancellation_requested_after_execute",
                        session=session,
                    )
                else:
                    logger.warning(
                        "job_completion_lost",
                        job_id=str(job.id),
                        worker_id=self.worker_id,
                    )
                return

            self._add_log(
                session,
                job.id,
                LogEvent.JOB_COMPLETED,
                f"Job completed by worker {self.worker_id} in {elapsed_ms}ms.",
                {
                    "worker_id": self.worker_id,
                    "duration_ms": elapsed_ms,
                    "result": result,
                },
            )

            # Recurrence is committed in the same transaction as completion.
            if job.interval_seconds:
                await self._schedule_next_run(session, job)

            await session.commit()

        logger.info(
            "job_completed",
            job_id=str(job.id),
            type=job.type,
            worker_id=self.worker_id,
            duration_ms=elapsed_ms,
        )
        await publish_event(
            {
                "job_id": str(job.id),
                "status": JobStatus.COMPLETED,
                "worker_id": self.worker_id,
                "duration_ms": elapsed_ms,
            }
        )

    async def _handle_failure(self, job: Job, error: Exception) -> None:
        new_retry_count = job.retry_count + 1
        error_str = f"{type(error).__name__}: {error}"

        if new_retry_count <= job.max_retries:
            delay = calculate_next_retry_delay(new_retry_count)
            retry_at = datetime.now(UTC) + timedelta(seconds=delay)

            async with AsyncSessionLocal() as session:
                updated = await session.execute(
                    update(Job)
                    .where(
                        Job.id == job.id,
                        Job.status == JobStatus.PROCESSING,
                        Job.worker_id == self.worker_id,
                        Job.cancellation_requested.is_(False),
                    )
                    .values(
                        status=JobStatus.PENDING,
                        retry_count=new_retry_count,
                        next_retry_at=retry_at,
                        available_at=retry_at,
                        last_error=error_str[:1000],
                        worker_id=None,
                        lease_expires_at=None,
                        updated_at=datetime.now(UTC),
                    )
                    .returning(Job.id)
                )
                if updated.scalar_one_or_none() is None:
                    if await self._is_cancelled(job.id, session):
                        await self._mark_cancelled(
                            job.id,
                            "cancellation_requested_after_failure",
                            session=session,
                        )
                    else:
                        logger.warning(
                            "job_retry_lost",
                            job_id=str(job.id),
                            worker_id=self.worker_id,
                        )
                    return

                self._add_log(
                    session,
                    job.id,
                    LogEvent.JOB_RETRY_ATTEMPTED,
                    (
                        f"Attempt {new_retry_count}/{job.max_retries} failed. "
                        f"Retrying in {delay:.1f}s. Error: {error_str[:200]}"
                    ),
                    {
                        "attempt": new_retry_count,
                        "delay_seconds": delay,
                        "error": error_str[:500],
                        "next_retry_at": retry_at.isoformat(),
                    },
                )
                await session.commit()

            logger.warning(
                "job_retry_attempted",
                job_id=str(job.id),
                attempt=new_retry_count,
                max_retries=job.max_retries,
                delay_seconds=delay,
            )
            await publish_event(
                {
                    "job_id": str(job.id),
                    "status": JobStatus.PENDING,
                    "retry_count": new_retry_count,
                    "available_at": retry_at.isoformat(),
                }
            )
            return

        async with AsyncSessionLocal() as session:
            if await self._is_cancelled(job.id, session):
                await self._mark_cancelled(
                    job.id,
                    "cancellation_requested_after_failure",
                    session=session,
                )
                return

            moved = await send_to_dlq(
                job.id,
                error_str[:1000],
                session,
                expected_worker_id=self.worker_id,
            )
            if not moved:
                if await self._is_cancelled(job.id, session):
                    await self._mark_cancelled(
                        job.id,
                        "cancellation_requested_after_failure",
                        session=session,
                    )
                else:
                    logger.warning(
                        "job_dlq_transition_lost",
                        job_id=str(job.id),
                        worker_id=self.worker_id,
                    )
                return

        await publish_event(
            {
                "job_id": str(job.id),
                "status": JobStatus.FAILED,
                "is_dlq": True,
            }
        )

    async def _schedule_next_run(self, session: AsyncSession, job: Job) -> None:
        if not job.interval_seconds:
            return

        next_run = datetime.now(UTC) + timedelta(seconds=job.interval_seconds)
        recurrence_id = job.recurrence_id or job.id
        next_run_number = job.recurrence_run + 1

        new_job_id = uuid.uuid4()
        statement = (
            insert(Job)
            .values(
                id=new_job_id,
                type=job.type,
                payload=job.payload,
                priority=job.priority,
                effective_priority=float(job.priority),
                status=JobStatus.PENDING,
                scheduled_at=next_run,
                available_at=next_run,
                interval_seconds=job.interval_seconds,
                max_retries=job.max_retries,
                retry_count=0,
                recurrence_id=recurrence_id,
                recurrence_run=next_run_number,
            )
            .on_conflict_do_nothing(index_elements=["recurrence_id", "recurrence_run"])
            .returning(Job.id)
        )
        inserted = (await session.execute(statement)).scalar_one_or_none()
        if inserted is None:
            return

        self._add_log(
            session,
            inserted,
            LogEvent.RECURRING_SCHEDULED,
            f"Next run scheduled at {next_run.isoformat()}. Parent: {job.id}.",
            {
                "parent_job_id": str(job.id),
                "interval_seconds": job.interval_seconds,
                "next_run": next_run.isoformat(),
                "recurrence_run": next_run_number,
            },
        )
        logger.info(
            "recurring_job_scheduled",
            parent_job_id=str(job.id),
            new_job_id=str(inserted),
            next_run=next_run.isoformat(),
        )

    async def _release(self, job_id: uuid.UUID) -> None:
        try:
            async with AsyncSessionLocal() as session:
                await self.dispatcher.release(
                    session,
                    [job_id],
                    reason="Worker shut down before the handler completed.",
                )
        except Exception as exc:
            logger.error(
                "job_release_on_shutdown_failed",
                job_id=str(job_id),
                error=str(exc),
            )

    async def _record_started(self, job: Job) -> None:
        async with AsyncSessionLocal() as session:
            self._add_log(
                session,
                job.id,
                LogEvent.JOB_STARTED,
                f"Job started by worker {self.worker_id}.",
                {"worker_id": self.worker_id},
            )
            await session.commit()

        await publish_event(
            {
                "job_id": str(job.id),
                "status": JobStatus.PROCESSING,
                "worker_id": self.worker_id,
            }
        )

    async def _is_cancelled(
        self,
        job_id: uuid.UUID,
        session: AsyncSession | None = None,
    ) -> bool:
        if session is not None:
            result = await session.execute(
                select(Job.cancellation_requested).where(Job.id == job_id)
            )
            return bool(result.scalar_one_or_none())

        async with AsyncSessionLocal() as own_session:
            result = await own_session.execute(
                select(Job.cancellation_requested).where(Job.id == job_id)
            )
            return bool(result.scalar_one_or_none())

    async def _mark_cancelled(
        self,
        job_id: uuid.UUID,
        reason: str,
        *,
        session: AsyncSession | None = None,
    ) -> None:
        async def _apply(db: AsyncSession) -> bool:
            updated = await db.execute(
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status == JobStatus.PROCESSING,
                    Job.worker_id == self.worker_id,
                )
                .values(
                    status=JobStatus.CANCELLED,
                    worker_id=None,
                    lease_expires_at=None,
                    updated_at=datetime.now(UTC),
                )
                .returning(Job.id)
            )
            if updated.scalar_one_or_none() is None:
                return False
            self._add_log(
                db,
                job_id,
                LogEvent.JOB_CANCELLED,
                f"Job cancelled by worker {self.worker_id}. Reason: {reason}.",
                {"worker_id": self.worker_id, "reason": reason},
            )
            return True

        if session is not None:
            applied = await _apply(session)
            if applied:
                await session.commit()
        else:
            async with AsyncSessionLocal() as own_session:
                applied = await _apply(own_session)
                if applied:
                    await own_session.commit()

        if not applied:
            return

        logger.info("job_cancelled", job_id=str(job_id), reason=reason)
        await publish_event(
            {
                "job_id": str(job_id),
                "status": JobStatus.CANCELLED,
                "reason": reason,
            }
        )

        # A cancelled job will never complete, so its pending dependents can
        # never run. Cascade-cancel them, the same treatment failed-to-DLQ
        # jobs already get (see dag.on_job_cancelled). Best effort: the job
        # itself is already cancelled; a cascade hiccup must not cascade
        # into the worker loop.
        try:
            async with AsyncSessionLocal() as cascade_session:
                await dag_service.on_job_cancelled(job_id, cascade_session)
        except Exception as exc:
            logger.error(
                "dependency_cascade_error",
                job_id=str(job_id),
                error=str(exc),
            )

    @staticmethod
    def _add_log(
        session: AsyncSession,
        job_id: uuid.UUID,
        event: str,
        message: str,
        metadata: dict[str, Any],
    ) -> None:
        session.add(
            JobLog(
                job_id=job_id,
                event=event,
                message=message,
                metadata_=metadata,
            )
        )

    @staticmethod
    def _elapsed_ms(start: datetime) -> int:
        return int((datetime.now(UTC) - start).total_seconds() * 1000)
