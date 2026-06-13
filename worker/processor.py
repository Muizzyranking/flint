import asyncio
import math
import random
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.db.session import AsyncSessionLocal
from app.handlers import get_handler
from app.models.job import Job, JobStatus
from app.models.job_log import JobLog, LogEvent
from app.queues.heapq import HeapQueue
from app.services import dag
from app.services.dlq import send_to_dlq

logger = get_logger(__name__)

_EVENTS_CHANNEL = "flint:events"


def calculate_next_retry_delay(attempt: int) -> float:
    """
    Exponential backoff with full jitter.
    """
    base = math.pow(5, attempt - 1)
    return round(base * random.uniform(0.5, 1.5), 2)


async def _publish_sse(event: dict) -> None:
    """Publish a job status event to Redis pub/sub for SSE streaming."""
    import json

    import redis.asyncio as aioredis

    from app.core.config import settings

    try:
        redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        await redis.publish(_EVENTS_CHANNEL, json.dumps(event))
        await redis.aclose()
    except Exception as exc:
        logger.error("sse_publish_error", error=str(exc))


class JobProcessor:
    def __init__(self, worker_id: str, queue: HeapQueue) -> None:
        self.worker_id = worker_id
        self.queue = queue

    async def process(self, job_id: str) -> None:
        """Full processing lifecycle for a single job."""
        async with AsyncSessionLocal() as session:
            job = await self._load_job(job_id, session)
            if not job:
                return

            claimed = await self._claim_job(job.id, session)
            if not claimed:
                logger.info(
                    "job_claim_failed",
                    job_id=job_id,
                    worker_id=self.worker_id,
                )
                return

            await self._log_event(
                job_id=job.id,
                event=LogEvent.JOB_STARTED,
                message=f"Job started by worker {self.worker_id}.",
                metadata={"worker_id": self.worker_id},
                session=session,
            )
            await _publish_sse(
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

            if await self._is_cancelled(job.id, session):
                await self._mark_cancelled(
                    job.id, session, "cancellation_requested_before_execute"
                )
                return

            start_time = datetime.now(UTC)
            try:
                handler = get_handler(job.type)
                result = await handler.execute(job.payload)
            except Exception as exc:
                elapsed = self._elapsed_ms(start_time)
                logger.warning(
                    "job_execution_failed",
                    job_id=job_id,
                    error=str(exc),
                    elapsed_ms=elapsed,
                    retry_count=job.retry_count,
                )
                await self._handle_failure(job, exc, session)
                return

            elapsed = self._elapsed_ms(start_time)

            if await self._is_cancelled(job.id, session):
                await self._mark_cancelled(
                    job.id, session, "cancellation_requested_after_execute"
                )
                return

            await session.execute(
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
                message=f"Job completed by worker {self.worker_id} in {elapsed}ms.",
                metadata={
                    "worker_id": self.worker_id,
                    "duration_ms": elapsed,
                    "result": result,
                },
                session=session,
            )
            await session.commit()

            logger.info(
                "job_completed",
                job_id=job_id,
                type=job.type,
                worker_id=self.worker_id,
                duration_ms=elapsed,
            )

            await _publish_sse(
                {
                    "job_id": job_id,
                    "status": JobStatus.COMPLETED,
                    "worker_id": self.worker_id,
                    "duration_ms": elapsed,
                }
            )

            await dag.on_job_completed(job.id, session, self.queue)

            if job.interval_seconds:
                await self._handle_recurrence(job, session)

    async def _claim_job(self, job_id: uuid.UUID, session: AsyncSession) -> bool:
        """
        Atomically claim a job.
        Returns True only if this worker successfully set worker_id.
        PostgreSQL row-level atomicity ensures only one winner.
        """
        result = await session.execute(
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
        await session.commit()
        return result.scalar_one_or_none() is not None

    async def _handle_failure(
        self,
        job: Job,
        error: Exception,
        session: AsyncSession,
    ) -> None:
        new_retry_count = job.retry_count + 1
        error_str = str(error)

        if new_retry_count <= job.max_retries:
            delay = calculate_next_retry_delay(new_retry_count)
            next_retry_at = datetime.now(UTC) + timedelta(seconds=delay)

            await session.execute(
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
                    f"Attempt {new_retry_count}/{job.max_retries} failed. "
                    f"Retrying in {delay:.1f}s. Error: {error_str[:200]}"
                ),
                metadata={
                    "attempt": new_retry_count,
                    "delay_seconds": delay,
                    "error": error_str[:500],
                    "next_retry_at": next_retry_at.isoformat(),
                },
                session=session,
            )
            await session.commit()

            logger.warning(
                "job_retry_attempted",
                job_id=str(job.id),
                attempt=new_retry_count,
                max_retries=job.max_retries,
                delay_seconds=delay,
            )

            await _publish_sse(
                {
                    "job_id": str(job.id),
                    "status": JobStatus.PENDING,
                    "retry_count": new_retry_count,
                }
            )

            asyncio.create_task(self._requeue_after_delay(job, next_retry_at, delay))

        else:
            await session.execute(
                update(Job)
                .where(Job.id == job.id)
                .values(
                    retry_count=new_retry_count, worker_id=None, updated_at=func.now()
                )
            )
            await session.flush()
            await send_to_dlq(job.id, error_str[:1000], session)

            await _publish_sse(
                {
                    "job_id": str(job.id),
                    "status": JobStatus.FAILED,
                    "is_dlq": True,
                }
            )

    async def _requeue_after_delay(
        self,
        job: Job,
        next_retry_at: datetime,
        delay: float,
    ) -> None:
        """
        Wait for the backoff delay then push the job back into the heap.
        The scheduler would also pick it up on its next poll — whichever
        happens first, the atomic claim prevents double-processing.
        """
        await asyncio.sleep(delay)
        try:
            await self.queue.push(
                job_id=str(job.id),
                effective_priority=job.effective_priority,
                scheduled_at=next_retry_at.timestamp(),
                created_at=job.created_at.timestamp(),
            )
            logger.info("job_retry_requeued", job_id=str(job.id), delay=delay)
        except Exception as exc:
            logger.error("job_retry_requeue_error", job_id=str(job.id), error=str(exc))

    async def _is_cancelled(self, job_id: uuid.UUID, session: AsyncSession) -> bool:
        result = await session.execute(
            select(Job.cancellation_requested).where(Job.id == job_id)
        )
        return bool(result.scalar_one_or_none())

    async def _mark_cancelled(
        self,
        job_id: uuid.UUID,
        session: AsyncSession,
        reason: str = "cancellation_requested",
    ) -> None:
        await session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(status=JobStatus.CANCELLED, worker_id=None, updated_at=func.now())
        )
        await self._log_event(
            job_id=job_id,
            event=LogEvent.JOB_CANCELLED,
            message=f"Job cancelled by worker {self.worker_id}. Reason: {reason}.",
            metadata={"worker_id": self.worker_id, "reason": reason},
            session=session,
        )
        await session.commit()

        logger.info("job_cancelled", job_id=str(job_id), reason=reason)

        await _publish_sse(
            {
                "job_id": str(job_id),
                "status": JobStatus.CANCELLED,
                "reason": reason,
            }
        )

    async def _handle_recurrence(self, job: Job, session: AsyncSession) -> None:
        if not job.interval_seconds:
            return
        next_run = datetime.now(UTC) + timedelta(seconds=job.interval_seconds)

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
            message=f"Next run scheduled at {next_run.isoformat()}. Parent: {job.id}.",
            metadata={
                "parent_job_id": str(job.id),
                "interval_seconds": job.interval_seconds,
                "next_run": next_run.isoformat(),
            },
            session=session,
        )
        await session.commit()

        now = datetime.now(UTC)
        if next_run <= now:
            await self.queue.push(
                job_id=str(new_job.id),
                effective_priority=float(job.priority),
                scheduled_at=next_run.timestamp(),
                created_at=new_job.created_at.timestamp()
                if new_job.created_at
                else now.timestamp(),
            )

        logger.info(
            "recurring_job_scheduled",
            parent_job_id=str(job.id),
            new_job_id=str(new_job.id),
            next_run=next_run.isoformat(),
        )

    async def _load_job(self, job_id: str, session: AsyncSession) -> Job | None:
        try:
            parsed_id = uuid.UUID(job_id)
        except ValueError:
            logger.error("invalid_job_id", job_id=job_id)
            return None

        result = await session.execute(
            select(Job).where(Job.id == parsed_id, Job.deleted_at.is_(None))
        )
        return result.scalar_one_or_none()

    async def _log_event(
        self,
        job_id: uuid.UUID,
        event: str,
        message: str,
        metadata: dict,
        session: AsyncSession,
    ) -> None:
        log_entry = JobLog(
            job_id=job_id,
            event=event,
            message=message,
            metadata_=metadata,
        )
        session.add(log_entry)
        await session.flush()

    @staticmethod
    def _elapsed_ms(start: datetime) -> int:
        return int((datetime.now(UTC) - start).total_seconds() * 1000)
