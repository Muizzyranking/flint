import json
import uuid
from datetime import UTC, datetime

import redis.asyncio as aioredis
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
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
from app.services import dag as dag_service

logger = get_logger(__name__)

EVENTS_CHANNEL = "flint:events"


async def _publish_sse_event(event: dict) -> None:
    """Publish a job status change to the SSE Redis channel."""
    try:
        redis = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            encoding="utf-8",
        )
        await redis.publish(EVENTS_CHANNEL, json.dumps(event))
        await redis.aclose()
    except Exception as exc:
        logger.error("sse_publish_error", error=str(exc))


async def create_job(data: JobCreate, session: AsyncSession) -> Job:
    """
    Create a new job and write it to PostgreSQL.

    The scheduler (worker/main.py) will pick it up on its next poll
    and push it into the HeapQueue when scheduled_at becomes due
    and all dependencies are met.
    """
    interval_seconds = None
    if data.interval:
        interval_seconds = parse_interval(data.interval)

    dependency_ids = data.dependency_ids or []

    # Validate dependencies and check for cycles
    if dependency_ids:
        temp_id = uuid.uuid4()
        await dag_service.check_cycle(temp_id, dependency_ids, session)

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
    session.add(job)
    await session.flush()

    # Re-run cycle check with the real job ID
    if dependency_ids:
        await dag_service.check_cycle(job.id, dependency_ids, session)
        await dag_service.create_dependencies(job.id, dependency_ids, session)

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
    session.add(log_entry)
    await session.commit()

    logger.info(
        "job_created",
        job_id=str(job.id),
        type=job.type,
        priority=job.priority,
        scheduled_at=scheduled_at.isoformat(),
        has_dependencies=bool(dependency_ids),
    )

    # Notify SSE clients
    await _publish_sse_event(
        {
            "job_id": str(job.id),
            "status": JobStatus.PENDING,
            "type": job.type,
        }
    )

    # Note: the scheduler will push this job into the HeapQueue
    # on its next poll cycle when scheduled_at <= NOW and deps are met.

    return job


# ------------------------------------------------------------------ #
# Read
# ------------------------------------------------------------------ #


async def get_jobs(
    filters: JobFilterParams,
    session: AsyncSession,
) -> tuple[list[Job], int]:
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
        term = f"%{filters.search}%"
        conditions.append(
            or_(
                Job.type.ilike(term),
                Job.id.cast(type_=__import__("sqlalchemy").String).ilike(term),
            )
        )

    total_result = await session.execute(select(func.count(Job.id)).where(*conditions))
    total = total_result.scalar() or 0

    offset = (filters.page - 1) * filters.limit
    jobs_result = await session.execute(
        select(Job)
        .where(*conditions)
        .order_by(Job.created_at.desc())
        .offset(offset)
        .limit(filters.limit)
    )
    return list(jobs_result.scalars().all()), total


async def get_job_by_id(job_id: uuid.UUID, session: AsyncSession) -> Job:
    result = await session.execute(
        select(Job).where(Job.id == job_id, Job.deleted_at.is_(None))
    )
    job = result.scalar_one_or_none()
    if not job:
        raise JobNotFoundException(str(job_id))
    return job


async def get_job_with_details(
    job_id: uuid.UUID,
    session: AsyncSession,
) -> tuple[Job, list[uuid.UUID], list]:
    job = await get_job_by_id(job_id, session)
    dependency_ids = await dag_service.get_dependency_ids(job_id, session)

    from sqlalchemy import asc

    from app.models.job_log import JobLog as JobLogModel

    logs_result = await session.execute(
        select(JobLogModel)
        .where(JobLogModel.job_id == job_id)
        .order_by(asc(JobLogModel.created_at))
    )
    logs = list(logs_result.scalars().all())
    return job, dependency_ids, logs


# ------------------------------------------------------------------ #
# Cancel
# ------------------------------------------------------------------ #


async def cancel_job(job_id: uuid.UUID, session: AsyncSession) -> Job:
    """
    Pending jobs: mark cancelled immediately.
    Processing jobs: set cancellation_requested=True (cooperative).
    Terminal jobs: raise JobNotCancellableException.
    """
    job = await get_job_by_id(job_id, session)

    if not job.is_cancellable:
        raise JobNotCancellableException(str(job_id), job.status)

    if job.status == JobStatus.PENDING:
        await session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                status=JobStatus.CANCELLED,
                cancellation_requested=True,
                updated_at=func.now(),
            )
        )
        await dag_service.on_job_failed(job_id, session)

        session.add(
            JobLog(
                job_id=job_id,
                event=LogEvent.JOB_CANCELLED,
                message="Job cancelled while pending.",
                metadata_={"reason": "user_requested", "was_status": "pending"},
            )
        )
        logger.info("job_cancelled", job_id=str(job_id), was_status="pending")

    elif job.status == JobStatus.PROCESSING:
        await session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(cancellation_requested=True, updated_at=func.now())
        )
        session.add(
            JobLog(
                job_id=job_id,
                event=LogEvent.JOB_CANCELLED,
                message=(
                    "Cancellation requested while processing. "
                    "Worker will honour at next checkpoint."
                ),
                metadata_={"reason": "user_requested", "was_status": "processing"},
            )
        )
        logger.info(
            "job_cancellation_requested",
            job_id=str(job_id),
            was_status="processing",
        )

    await session.commit()
    await _publish_sse_event({"job_id": str(job_id), "status": JobStatus.CANCELLED})
    job = await session.get(Job, job_id)
    assert job is not None, f"Job {job_id} vanished after cancellation"
    return job


# ------------------------------------------------------------------ #
# Soft delete / Bin / Restore / Hard delete
# ------------------------------------------------------------------ #


async def soft_delete_job(job_id: uuid.UUID, session: AsyncSession) -> None:
    job = await get_job_by_id(job_id, session)
    if not job.is_terminal:
        raise JobNotDeletableException(str(job_id), job.status)
    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(deleted_at=func.now(), updated_at=func.now())
    )
    await session.commit()
    logger.info("job_soft_deleted", job_id=str(job_id))


async def get_bin_jobs(
    page: int, limit: int, session: AsyncSession
) -> tuple[list[Job], int]:
    conditions = [Job.deleted_at.isnot(None)]
    total = (
        await session.execute(select(func.count(Job.id)).where(*conditions))
    ).scalar() or 0
    offset = (page - 1) * limit
    jobs = list(
        (
            await session.execute(
                select(Job)
                .where(*conditions)
                .order_by(Job.deleted_at.desc())
                .offset(offset)
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    return jobs, total


async def restore_job(job_id: uuid.UUID, session: AsyncSession) -> Job:
    result = await session.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job or job.deleted_at is None:
        raise JobNotInBinException(str(job_id))
    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(deleted_at=None, updated_at=func.now())
    )
    await session.commit()
    logger.info("job_restored", job_id=str(job_id))
    job = await session.get(Job, job_id)
    assert job is not None, f"Job {job_id} vanished after cancellation"
    return job


async def hard_delete_job(job_id: uuid.UUID, session: AsyncSession) -> None:
    result = await session.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job or job.deleted_at is None:
        raise JobNotInBinException(str(job_id))
    await session.delete(job)
    await session.commit()
    logger.info("job_hard_deleted", job_id=str(job_id))


async def get_job_counts_by_status(session: AsyncSession) -> dict[str, int]:
    from app.services.dlq import get_dlq_count

    results = await session.execute(
        select(Job.status, func.count(Job.id))
        .where(Job.deleted_at.is_(None), Job.is_dlq.is_(False))
        .group_by(Job.status)
    )
    counts = {status: count for status, count in results.fetchall()}
    dlq_count = await get_dlq_count(session)
    return {
        "pending": counts.get(JobStatus.PENDING, 0),
        "processing": counts.get(JobStatus.PROCESSING, 0),
        "completed": counts.get(JobStatus.COMPLETED, 0),
        "failed": counts.get(JobStatus.FAILED, 0),
        "cancelled": counts.get(JobStatus.CANCELLED, 0),
        "dlq": dlq_count,
    }
