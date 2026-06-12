import json
import uuid
from datetime import UTC, datetime

import redis.asyncio as aioredis
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings as app_settings
from app.core.exceptions import (
    JobNotCancellableException,
    JobNotDeletableException,
    JobNotFoundException,
    JobNotInBinException,
)
from app.core.logger import get_logger
from app.models.job import Job, JobStatus
from app.models.job_log import JobLog, LogEvent
from app.schemas.job import JobCreate, JobFilterParams, parse_interval
from app.services import dag

logger = get_logger(__name__)

QUEUE_KEY = "flint:queue"
EVENTS_CHANNEL = "flint:events"


async def _get_redis() -> aioredis.Redis:
    """Get a Redis client. Used internally by this service."""
    return aioredis.from_url(
        app_settings.REDIS_URL,
        decode_responses=True,
        encoding="utf-8",
    )


async def _sync_job_to_redis(job_id: str, effective_priority: float) -> None:
    """
    Add a job to the Redis sorted set (flint:queue).
    Score = effective_priority. Lower score = higher urgency.
    Workers read from this set to know which job to process next.
    """
    redis = await _get_redis()
    try:
        await redis.zadd(QUEUE_KEY, {job_id: effective_priority})
    finally:
        await redis.aclose()


async def _remove_job_from_redis(job_id: str) -> None:
    """Remove a job from the Redis sorted set."""
    redis = await _get_redis()
    try:
        await redis.zrem(QUEUE_KEY, job_id)
    finally:
        await redis.aclose()


async def _publish_sse_event(event: dict) -> None:
    """
    Publish a job status change event to the Redis pub/sub channel.
    The SSE endpoint subscribes to this channel and forwards events
    to connected browser clients.
    """
    redis = await _get_redis()
    try:
        await redis.publish(EVENTS_CHANNEL, json.dumps(event))
    finally:
        await redis.aclose()


async def create_job(
    data: JobCreate,
    db: AsyncSession,
) -> Job:
    """
    Create a new job and optionally push it to the queue.
    """
    # Parse interval
    interval_seconds = None
    if data.interval:
        interval_seconds = parse_interval(data.interval)

    dependency_ids = data.dependency_ids or []
    if dependency_ids:
        await dag.check_cycle(
            new_job_id=uuid.uuid4(),  # placeholder — job not yet in DB
            dependency_ids=dependency_ids,
            db=db,
        )

    scheduled_at = data.scheduled_at or datetime.now(UTC)

    job = Job(
        type=data.type,
        payload=data.payload,
        priority=int(data.priority),
        effective_priority=float(data.priority),
        status=JobStatus.PENDING,
        scheduled_at=scheduled_at,
        interval_seconds=interval_seconds,
        max_retries=data.max_retries,
        retry_count=0,
    )
    db.add(job)
    await db.flush()

    # Now run cycle check with the real job ID
    if dependency_ids:
        await dag.check_cycle(
            new_job_id=job.id,
            dependency_ids=dependency_ids,
            db=db,
        )
        await dag.create_dependencies(job.id, dependency_ids, db)

    log_entry = JobLog(
        job_id=job.id,
        event=LogEvent.JOB_CREATED,
        message=f"Job created with type '{job.type}' and priority {job.priority}.",
        metadata_={
            "type": job.type,
            "priority": job.priority,
            "scheduled_at": scheduled_at.isoformat(),
            "interval_seconds": interval_seconds,
            "dependency_count": len(dependency_ids),
        },
    )
    db.add(log_entry)
    await db.commit()

    logger.info(
        "job_created",
        job_id=str(job.id),
        type=job.type,
        priority=job.priority,
        scheduled_at=scheduled_at.isoformat(),
        has_dependencies=bool(dependency_ids),
    )

    has_unmet = bool(dependency_ids)
    is_due = scheduled_at <= datetime.now(UTC)

    if not has_unmet and is_due:
        await _sync_job_to_redis(str(job.id), job.effective_priority)

    await _publish_sse_event(
        {
            "job_id": str(job.id),
            "status": JobStatus.PENDING,
            "type": job.type,
        }
    )

    return job


async def get_jobs(
    filters: JobFilterParams,
    db: AsyncSession,
) -> tuple[list[Job], int]:
    """
    Return paginated jobs with total count.
    """
    conditions: list = [
        Job.deleted_at.is_(None),
        Job.is_dlq.is_(False),
    ]

    if filters.status:
        conditions.append(Job.status == filters.status)
    if filters.type:
        conditions.append(Job.type == filters.type)
    if filters.priority:
        conditions.append(Job.priority == int(filters.priority))
    if filters.search:
        search_term = f"%{filters.search}%"
        conditions.append(
            or_(
                Job.type.ilike(search_term),
                Job.id.cast(db_string_type()).ilike(search_term),
            )
        )

    total_result = await db.execute(select(func.count(Job.id)).where(*conditions))
    total = total_result.scalar() or 0

    offset = (filters.page - 1) * filters.limit
    jobs_result = await db.execute(
        select(Job)
        .where(*conditions)
        .order_by(Job.created_at.desc())
        .offset(offset)
        .limit(filters.limit)
    )
    jobs = list(jobs_result.scalars().all())

    return jobs, total


def db_string_type():
    """SQLAlchemy string type for UUID casting in search."""
    from sqlalchemy import String

    return String


async def get_job_by_id(
    job_id: uuid.UUID,
    db: AsyncSession,
    include_logs: bool = False,
    include_dependencies: bool = False,
) -> Job:
    """
    Fetch a single job by ID.
    """
    result = await db.execute(
        select(Job).where(
            Job.id == job_id,
            Job.deleted_at.is_(None),
        )
    )
    job = result.scalar_one_or_none()
    if not job:
        raise JobNotFoundException(str(job_id))
    return job


async def get_job_with_details(
    job_id: uuid.UUID,
    db: AsyncSession,
) -> tuple[Job, list[uuid.UUID], list]:
    """
    Fetch a job with its dependency IDs and log entries.
    """
    job = await get_job_by_id(job_id, db)

    dependency_ids = await dag.get_dependency_ids(job_id, db)

    from sqlalchemy import asc

    from app.models.job_log import JobLog

    logs_result = await db.execute(
        select(JobLog).where(JobLog.job_id == job_id).order_by(asc(JobLog.created_at))
    )
    logs = list(logs_result.scalars().all())

    return job, dependency_ids, logs


async def cancel_job(
    job_id: uuid.UUID,
    db: AsyncSession,
) -> Job:
    """
    Request cancellation of a job.
    """
    job = await get_job_by_id(job_id, db)

    if not job.is_cancellable:
        raise JobNotCancellableException(str(job_id), job.status)

    if job.status == JobStatus.PENDING:
        # Immediate cancellation
        await db.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                status=JobStatus.CANCELLED,
                cancellation_requested=True,
                updated_at=func.now(),
            )
        )

        await _remove_job_from_redis(str(job_id))

        # Cascade cancel downstream dependents
        await dag.on_job_failed(job_id, db)

        log_entry = JobLog(
            job_id=job_id,
            event=LogEvent.JOB_CANCELLED,
            message="Job cancelled by user request while pending.",
            metadata_={"reason": "user_requested", "was_status": "pending"},
        )
        db.add(log_entry)

        logger.info("job_cancelled", job_id=str(job_id), was_status="pending")

    elif job.status == JobStatus.PROCESSING:
        await db.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                cancellation_requested=True,
                updated_at=func.now(),
            )
        )

        log_entry = JobLog(
            job_id=job_id,
            event=LogEvent.JOB_CANCELLED,
            message=(
                "Cancellation requested while job is processing. "
                "Worker will honour at next checkpoint."
            ),
            metadata_={"reason": "user_requested", "was_status": "processing"},
        )
        db.add(log_entry)

        logger.info(
            "job_cancellation_requested",
            job_id=str(job_id),
            was_status="processing",
        )

    await db.commit()

    await _publish_sse_event(
        {
            "job_id": str(job_id),
            "status": JobStatus.CANCELLED,
        }
    )

    job = await db.get(Job, job_id)
    assert job is not None, f"Job {job_id} vanished after cancellation"
    return job


async def soft_delete_job(
    job_id: uuid.UUID,
    db: AsyncSession,
) -> None:
    """
    Move a job to the bin (sets deleted_at).
    """
    job = await get_job_by_id(job_id, db)

    if not job.is_terminal:
        raise JobNotDeletableException(str(job_id), job.status)

    await db.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(deleted_at=func.now(), updated_at=func.now())
    )
    await db.commit()

    logger.info("job_soft_deleted", job_id=str(job_id))


async def get_bin_jobs(
    page: int,
    limit: int,
    db: AsyncSession,
) -> tuple[list[Job], int]:
    """
    Return soft-deleted jobs (the bin). Paginated, most recently deleted first.
    """
    conditions = [Job.deleted_at.isnot(None)]

    total_result = await db.execute(select(func.count(Job.id)).where(*conditions))
    total = total_result.scalar() or 0

    offset = (page - 1) * limit
    jobs_result = await db.execute(
        select(Job)
        .where(*conditions)
        .order_by(Job.deleted_at.desc())
        .offset(offset)
        .limit(limit)
    )
    jobs = list(jobs_result.scalars().all())

    return jobs, total


async def restore_job(
    job_id: uuid.UUID,
    db: AsyncSession,
) -> Job:
    """
    Restore a soft-deleted job from the bin.
    Clears deleted_at. Does not re-queue the job.
    Raises JobNotInBinException if the job is not soft-deleted.
    """
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()

    if not job or job.deleted_at is None:
        raise JobNotInBinException(str(job_id))

    await db.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(deleted_at=None, updated_at=func.now())
    )
    await db.commit()

    logger.info("job_restored", job_id=str(job_id))
    job = await db.get(Job, job_id)
    assert job is not None, f"Job {job_id} vanished after cancellation"
    return job


async def hard_delete_job(
    job_id: uuid.UUID,
    db: AsyncSession,
) -> None:
    """
    Permanently delete a job from the database.
    """
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()

    if not job or job.deleted_at is None:
        raise JobNotInBinException(str(job_id))

    await db.delete(job)
    await db.commit()

    logger.info("job_hard_deleted", job_id=str(job_id))


async def get_job_counts_by_status(
    db: AsyncSession,
) -> dict[str, int]:
    """
    Return a dict of job counts per status for the dashboard.
    Excludes soft-deleted jobs. Includes DLQ count separately.
    """

    from app.services.dlq import get_dlq_count

    results = await db.execute(
        select(Job.status, func.count(Job.id))
        .where(
            Job.deleted_at.is_(None),
            Job.is_dlq.is_(False),
        )
        .group_by(Job.status)
    )
    counts = {status: count for status, count in results.fetchall()}

    dlq_count = await get_dlq_count(db)

    return {
        "pending": counts.get(JobStatus.PENDING, 0),
        "processing": counts.get(JobStatus.PROCESSING, 0),
        "completed": counts.get(JobStatus.COMPLETED, 0),
        "failed": counts.get(JobStatus.FAILED, 0),
        "cancelled": counts.get(JobStatus.CANCELLED, 0),
        "dlq": dlq_count,
    }
