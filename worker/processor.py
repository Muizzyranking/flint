import asyncio
import math
import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.db.session import AsyncSessionLocal
from app.handlers import get_handler
from app.models.job import Job, JobStatus
from app.models.job_log import JobLog, LogEvent
from app.services import dag as dag_service
from app.services.dlq import send_to_dlq
from app.services.job import (
    _publish_sse_event,
    _sync_job_to_redis,
)

if TYPE_CHECKING:
    from app.queues.base import BaseQueue

logger = get_logger(__name__)


def calculate_next_retry_delay(attempt: int) -> float:
    """
    Exponential backoff with jitter.
    """
    base = math.pow(5, attempt - 1)
    jitter = base * random.uniform(0.5, 1.5)
    return round(jitter, 2)


class JobProcessor:
    def __init__(self, worker_id: str, queue: "BaseQueue") -> None:
        self.worker_id = worker_id
        self.queue = queue

    async def process(self, job_id: str) -> None:
        """
        Full job processing flow for a single job.
        """
        async with AsyncSessionLocal() as db:
            job = await self._load_job(job_id, db)
            if not job:
                return

            claimed = await self._claim_job(job.id, db)
            if not claimed:
                logger.info(
                    "job_claim_failed",
                    job_id=job_id,
                    worker_id=self.worker_id,
                    reason="already_claimed",
                )
                return

            await self._log_event(
                job_id=job.id,
                event=LogEvent.JOB_STARTED,
                message=f"Job started by worker {self.worker_id}.",
                metadata={"worker_id": self.worker_id},
                db=db,
            )
            await _publish_sse_event(
                {
                    "job_id": job_id,
                    "status": JobStatus.PROCESSING,
                    "worker_id": self.worker_id,
                }
            )

            logger.info(
                "job_started",
                job_id=job_id,
                type=job.type,
                worker_id=self.worker_id,
                retry_count=job.retry_count,
            )

            if await self._is_cancellation_requested(job.id, db):
                await self._mark_cancelled(
                    job.id,
                    db,
                    reason="cancellation_requested_before_execute",
                )
                return

            start_time = datetime.now(UTC)
            try:
                handler = get_handler(job.type)
                result = await handler.execute(job.payload)
            except Exception as exc:
                elapsed_ms = self._elapsed_ms(start_time)
                logger.warning(
                    "job_execution_failed",
                    job_id=job_id,
                    type=job.type,
                    error=str(exc),
                    elapsed_ms=elapsed_ms,
                    retry_count=job.retry_count,
                )
                await self._handle_failure(job, exc, db)
                return

            elapsed_ms = self._elapsed_ms(start_time)

            if await self._is_cancellation_requested(job.id, db):
                await self._mark_cancelled(
                    job.id,
                    db,
                    reason="cancellation_requested_after_execute",
                )
                return

            await db.execute(
                update(Job)
                .where(Job.id == job.id)
                .values(
                    status=JobStatus.COMPLETED,
                    completed_at=func.now(),
                    worker_id=None,
                    updated_at=func.now(),
                )
            )

            await self._log_event(
                job_id=job.id,
                event=LogEvent.JOB_COMPLETED,
                message=(
                    f"Job completed successfully by worker {self.worker_id} "
                    f"in {elapsed_ms}ms."
                ),
                metadata={
                    "worker_id": self.worker_id,
                    "duration_ms": elapsed_ms,
                    "result": result,
                },
                db=db,
            )
            await db.commit()

            logger.info(
                "job_completed",
                job_id=job_id,
                type=job.type,
                worker_id=self.worker_id,
                duration_ms=elapsed_ms,
            )

            await _publish_sse_event(
                {
                    "job_id": job_id,
                    "status": JobStatus.COMPLETED,
                    "worker_id": self.worker_id,
                    "duration_ms": elapsed_ms,
                }
            )

            await dag_service.on_job_completed(job.id, db, self.queue)

            if job.interval_seconds:
                await self._handle_recurrence(job, db)

    async def _claim_job(
        self,
        job_id: uuid.UUID,
        db: AsyncSession,
    ) -> bool:
        """
        Atomic claim via single UPDATE with WHERE guard.

        Only succeeds if status='pending' AND worker_id IS NULL.
        If two workers race, exactly one gets the row back.
        PostgreSQL row-level locking guarantees atomicity.

        Returns True if this worker successfully claimed the job.
        """
        result = await db.execute(
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.PENDING,
                Job.worker_id.is_(None),
                Job.deleted_at.is_(None),
            )
            .values(
                status=JobStatus.PROCESSING,
                worker_id=self.worker_id,
                started_at=func.now(),
                updated_at=func.now(),
            )
            .returning(Job.id)
        )
        await db.commit()
        return result.scalar_one_or_none() is not None

    async def _handle_failure(
        self,
        job: Job,
        error: Exception,
        db: AsyncSession,
    ) -> None:
        """
        Handle a job execution failure.
        """
        new_retry_count = job.retry_count + 1
        error_str = str(error)

        if new_retry_count <= job.max_retries:
            delay = calculate_next_retry_delay(new_retry_count)
            next_retry_at = datetime.now(UTC) + timedelta(seconds=delay)

            await db.execute(
                update(Job)
                .where(Job.id == job.id)
                .values(
                    status=JobStatus.PENDING,
                    retry_count=new_retry_count,
                    next_retry_at=next_retry_at,
                    last_error=error_str[:1000],
                    worker_id=None,
                    updated_at=func.now(),
                )
            )

            await self._log_event(
                job_id=job.id,
                event=LogEvent.JOB_RETRY_ATTEMPTED,
                message=(
                    f"Job failed on attempt {new_retry_count}/{job.max_retries}. "
                    f"Retrying in {delay:.1f}s. Error: {error_str[:200]}"
                ),
                metadata={
                    "attempt": new_retry_count,
                    "max_retries": job.max_retries,
                    "delay_seconds": delay,
                    "error": error_str[:500],
                    "next_retry_at": next_retry_at.isoformat(),
                },
                db=db,
            )
            await db.commit()

            logger.warning(
                "job_retry_attempted",
                job_id=str(job.id),
                attempt=new_retry_count,
                max_retries=job.max_retries,
                delay_seconds=delay,
                error=error_str[:200],
            )

            await _publish_sse_event(
                {
                    "job_id": str(job.id),
                    "status": JobStatus.PENDING,
                    "retry_count": new_retry_count,
                }
            )

            asyncio.create_task(
                self._retry_after_delay(
                    job_id=str(job.id),
                    effective_priority=job.effective_priority,
                    scheduled_at=next_retry_at.timestamp(),
                    created_at=job.created_at.timestamp(),
                    delay=delay,
                )
            )

        else:
            await db.execute(
                update(Job)
                .where(Job.id == job.id)
                .values(
                    retry_count=new_retry_count,
                    worker_id=None,
                    updated_at=func.now(),
                )
            )
            await db.flush()
            await send_to_dlq(job.id, error_str[:1000], db)

            await _publish_sse_event(
                {
                    "job_id": str(job.id),
                    "status": JobStatus.FAILED,
                    "is_dlq": True,
                }
            )

    async def _retry_after_delay(
        self,
        job_id: str,
        effective_priority: float,
        scheduled_at: float,
        created_at: float,
        delay: float,
    ) -> None:
        """
        Background task: wait for the backoff delay then push the
        job back onto the queue so it gets picked up again.
        """
        await asyncio.sleep(delay)
        try:
            await self.queue.push(
                job_id=job_id,
                effective_priority=effective_priority,
                scheduled_at=scheduled_at,
                created_at=created_at,
            )
            await _sync_job_to_redis(job_id, effective_priority)
            logger.info("job_retry_queued", job_id=job_id, delay=delay)
        except Exception as exc:
            logger.error(
                "job_retry_queue_error",
                job_id=job_id,
                error=str(exc),
            )

    async def _is_cancellation_requested(
        self,
        job_id: uuid.UUID,
        db: AsyncSession,
    ) -> bool:
        """
        Check the cancellation_requested flag from the DB.
        Called at checkpoints during processing.
        """
        result = await db.execute(
            select(Job.cancellation_requested).where(Job.id == job_id)
        )
        return bool(result.scalar_one_or_none())

    async def _mark_cancelled(
        self,
        job_id: uuid.UUID,
        session: AsyncSession,
        reason: str = "cancellation_requested",
    ) -> None:
        """Mark a job as cancelled and publish SSE event."""
        await session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                status=JobStatus.CANCELLED,
                worker_id=None,
                updated_at=func.now(),
            )
        )

        await self._log_event(
            job_id=job_id,
            event=LogEvent.JOB_CANCELLED,
            message=(f"Job cancelled by worker {self.worker_id}. Reason: {reason}."),
            metadata={"worker_id": self.worker_id, "reason": reason},
            db=session,
        )
        await session.commit()

        logger.info(
            "job_cancelled",
            job_id=str(job_id),
            worker_id=self.worker_id,
            reason=reason,
        )

        await _publish_sse_event(
            {
                "job_id": str(job_id),
                "status": JobStatus.CANCELLED,
                "reason": reason,
            }
        )

    async def _handle_recurrence(
        self,
        job: Job,
        session: AsyncSession,
    ) -> None:
        """
        Schedule the next run of a recurring job.
        """

        if not job.interval_seconds:
            return

        next_run = datetime.now(UTC) + timedelta(seconds=float(job.interval_seconds))

        new_job = Job(
            type=job.type,
            payload=job.payload,
            priority=job.priority,
            effective_priority=float(job.priority),
            status=JobStatus.PENDING,
            scheduled_at=next_run,
            interval_seconds=job.interval_seconds,
            max_retries=job.max_retries,
            retry_count=0,
        )
        session.add(new_job)
        await session.flush()

        await self._log_event(
            job_id=new_job.id,
            event=LogEvent.RECURRING_SCHEDULED,
            message=(
                f"Recurring job scheduled. Next run at {next_run.isoformat()}. "
                f"Parent job: {job.id}."
            ),
            metadata={
                "parent_job_id": str(job.id),
                "interval_seconds": job.interval_seconds,
                "next_run": next_run.isoformat(),
            },
            db=session,
        )
        await session.commit()

        logger.info(
            "recurring_job_scheduled",
            parent_job_id=str(job.id),
            new_job_id=str(new_job.id),
            next_run=next_run.isoformat(),
            interval_seconds=job.interval_seconds,
        )

    async def _load_job(
        self,
        job_id: str,
        db: AsyncSession,
    ) -> Job | None:
        """Load a job by string ID. Returns None if not found."""
        try:
            parsed_id = uuid.UUID(job_id)
        except ValueError:
            logger.error("invalid_job_id", job_id=job_id)
            return None

        result = await db.execute(
            select(Job).where(
                Job.id == parsed_id,
                Job.deleted_at.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def _log_event(
        self,
        job_id: uuid.UUID,
        event: str,
        message: str,
        metadata: dict,
        db: AsyncSession,
    ) -> None:
        """Write a structured log entry to the job_logs table."""
        log_entry = JobLog(
            job_id=job_id,
            event=event,
            message=message,
            metadata_=metadata,
        )
        db.add(log_entry)
        await db.flush()

    @staticmethod
    def _elapsed_ms(start: datetime) -> int:
        """Return elapsed milliseconds since start."""
        delta = datetime.now(UTC) - start
        return int(delta.total_seconds() * 1000)
