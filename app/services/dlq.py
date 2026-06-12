import uuid

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import JobNotInDLQException
from app.core.logger import get_logger
from app.models.job import Job, JobStatus
from app.models.job_log import JobLog, LogEvent
from app.queues.base import BaseQueue
from app.services.settings import get_dlq_threshold

logger = get_logger(__name__)


async def send_to_dlq(
    job_id: uuid.UUID,
    error: str,
    db: AsyncSession,
) -> None:
    """
    Move a job to the dead letter queue after exhausting all retries.
    """
    await db.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(
            status=JobStatus.FAILED,
            is_dlq=True,
            last_error=error,
            worker_id=None,
            updated_at=func.now(),
        )
    )

    log_entry = JobLog(
        job_id=job_id,
        event=LogEvent.JOB_FAILED,
        message=f"Job moved to DLQ after exhausting all retries. Error: {error}",
        metadata_={"error": error, "reason": "max_retries_exhausted"},
    )
    db.add(log_entry)
    await db.flush()

    logger.error(
        "job_failed",
        job_id=str(job_id),
        error=error[:200],
        reason="max_retries_exhausted",
    )

    from app.services import dag

    await dag.on_job_failed(job_id, db)

    await _check_and_alert(db)


async def _check_and_alert(session: AsyncSession) -> None:
    """
    Count current DLQ jobs. If count >= threshold, fire alert email.
    """
    count = await get_dlq_count(session)
    threshold = await get_dlq_threshold(session)

    logger.info("dlq_count_check", count=count, threshold=threshold)

    if count >= threshold:
        logger.warning(
            "dlq_threshold_reached",
            count=count,
            threshold=threshold,
        )
        from app.services import alert

        await alert.send_dlq_alert(count, session)


async def get_dlq_jobs(
    page: int,
    limit: int,
    db: AsyncSession,
) -> tuple[list[Job], int]:
    """
    Return paginated DLQ jobs with total count.
    """
    base_filter = [
        Job.is_dlq.is_(True),
        Job.deleted_at.is_(None),
    ]

    total_result = await db.execute(select(func.count(Job.id)).where(*base_filter))
    total = total_result.scalar() or 0

    offset = (page - 1) * limit
    jobs_result = await db.execute(
        select(Job)
        .where(*base_filter)
        .order_by(Job.updated_at.desc())
        .offset(offset)
        .limit(limit)
    )
    jobs = list(jobs_result.scalars().all())

    return jobs, total


async def get_dlq_count(db: AsyncSession) -> int:
    """Return the current number of jobs in the DLQ."""
    result = await db.execute(
        select(func.count(Job.id)).where(
            Job.is_dlq.is_(True),
            Job.deleted_at.is_(None),
        )
    )
    return result.scalar() or 0


async def get_recent_dlq_jobs(
    db: AsyncSession,
    limit: int = 10,
) -> list[dict]:
    """
    Return the most recent DLQ jobs as plain dicts.
    """
    result = await db.execute(
        select(Job)
        .where(
            Job.is_dlq.is_(True),
            Job.deleted_at.is_(None),
        )
        .order_by(Job.updated_at.desc())
        .limit(limit)
    )
    jobs = result.scalars().all()
    return [
        {
            "id": str(job.id),
            "short_id": str(job.id)[:8],
            "type": job.type,
            "last_error": job.last_error or "Unknown error",
            "retry_count": job.retry_count,
            "max_retries": job.max_retries,
            "updated_at": job.updated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        for job in jobs
    ]


async def retry_dlq_job(
    job_id: uuid.UUID,
    db: AsyncSession,
    queue: BaseQueue,
) -> Job:
    """
    Manually retry a job from the DLQ.
    """
    result = await db.execute(
        select(Job).where(
            Job.id == job_id,
            Job.deleted_at.is_(None),
        )
    )
    job = result.scalar_one_or_none()

    if not job or not job.is_dlq:
        raise JobNotInDLQException(str(job_id))

    # Reset the job
    await db.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(
            status=JobStatus.PENDING,
            is_dlq=False,
            retry_count=0,
            last_error=None,
            worker_id=None,
            effective_priority=float(job.priority),
            updated_at=func.now(),
        )
    )

    log_entry = JobLog(
        job_id=job_id,
        event=LogEvent.JOB_CREATED,
        message="Job manually retried from DLQ by engineer.",
        metadata_={"reason": "manual_dlq_retry"},
    )
    db.add(log_entry)
    await db.flush()

    logger.info("dlq_job_retried", job_id=str(job_id))

    from app.services import dag

    await dag.on_dag_root_retried(job_id, db)

    has_unmet = await dag.has_unmet_dependencies(job_id, db)
    if not has_unmet:
        await queue.push(
            job_id=str(job_id),
            effective_priority=float(job.priority),
            scheduled_at=job.scheduled_at.timestamp(),
            created_at=job.created_at.timestamp(),
        )
        from app.services.job import _sync_job_to_redis

        await _sync_job_to_redis(str(job_id), float(job.priority))

    await db.commit()

    refreshed = await db.get(Job, job_id)
    assert refreshed is not None
    return refreshed


async def remove_from_dlq(
    job_id: uuid.UUID,
    session: AsyncSession,
) -> None:
    """
    Soft-delete a job from the DLQ.
    The job moves to the bin and can be hard-deleted from there.
    """
    result = await session.execute(
        select(Job).where(
            Job.id == job_id,
            Job.deleted_at.is_(None),
        )
    )
    job = result.scalar_one_or_none()

    if not job or not job.is_dlq:
        raise JobNotInDLQException(str(job_id))

    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(deleted_at=func.now(), updated_at=func.now())
    )
    await session.commit()

    logger.info("dlq_job_removed", job_id=str(job_id))
