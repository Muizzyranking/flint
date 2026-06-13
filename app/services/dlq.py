import uuid

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import JobNotInDLQException
from app.core.logger import get_logger
from app.models.job import Job, JobStatus
from app.models.job_log import JobLog, LogEvent
from app.queues.heapq import HeapQueue
from app.services.settings import get_dlq_threshold

logger = get_logger(__name__)


async def send_to_dlq(
    job_id: uuid.UUID,
    error: str,
    session: AsyncSession,
) -> None:
    """
    Move a job to the dead letter queue after exhausting all retries.

    Steps:
      1. Set status='failed', is_dlq=True, last_error, worker_id=None
      2. Write job_failed log entry
      3. Cascade-cancel downstream DAG dependents
      4. Check DLQ count vs threshold — alert if breached
      5. Publish SSE event (handled by caller via redis publish)
    """
    await session.execute(
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
    session.add(log_entry)
    await session.flush()

    logger.error(
        "job_failed",
        job_id=str(job_id),
        error=error[:200],
        reason="max_retries_exhausted",
    )

    # Cascade cancel downstream jobs in the DAG
    from app.services import dag

    await dag.on_job_failed(job_id, session)

    await _check_and_alert(session)


async def _check_and_alert(session: AsyncSession) -> None:
    """
    Count current DLQ jobs. If count >= threshold, fire alert email.
    Imports alert_service lazily to avoid circular imports.
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
        from app.services import alert as alert_service

        await alert_service.send_dlq_alert(count, session)


async def get_dlq_jobs(
    page: int,
    limit: int,
    session: AsyncSession,
) -> tuple[list[Job], int]:
    """
    Return paginated DLQ jobs with total count.
    Excludes soft-deleted jobs.
    Ordered by most recently failed first.
    """
    base_filter = [
        Job.is_dlq.is_(True),
        Job.deleted_at.is_(None),
    ]

    total_result = await session.execute(select(func.count(Job.id)).where(*base_filter))
    total = total_result.scalar() or 0

    offset = (page - 1) * limit
    jobs_result = await session.execute(
        select(Job)
        .where(*base_filter)
        .order_by(Job.updated_at.desc())
        .offset(offset)
        .limit(limit)
    )
    jobs = list(jobs_result.scalars().all())

    return jobs, total


async def get_dlq_count(session: AsyncSession) -> int:
    """Return the current number of jobs in the DLQ."""
    result = await session.execute(
        select(func.count(Job.id)).where(
            Job.is_dlq.is_(True),
            Job.deleted_at.is_(None),
        )
    )
    return result.scalar() or 0


async def get_recent_dlq_jobs(
    session: AsyncSession,
    limit: int = 10,
) -> list[dict]:
    """
    Return the most recent DLQ jobs as plain dicts.
    Used by alert_service to populate the email template.
    """
    result = await session.execute(
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
            "id": str(j.id),
            "short_id": str(j.id)[:8],
            "type": j.type,
            "last_error": j.last_error or "Unknown error",
            "retry_count": j.retry_count,
            "max_retries": j.max_retries,
            "updated_at": j.updated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        for j in jobs
    ]


async def retry_dlq_job(
    job_id: uuid.UUID,
    session: AsyncSession,
    queue: HeapQueue,
) -> Job:
    """
    Manually retry a job from the DLQ.

    Steps:
      1. Verify job exists and is in DLQ
      2. Reset: status='pending', is_dlq=False, retry_count=0,
                last_error=None, worker_id=None
      3. Call dag_service.on_dag_root_retried — reset downstream cancelled jobs
      4. Re-evaluate dependencies: if all met, push to queue
      5. Log and return updated job
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

    # Reset the job
    await session.execute(
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
    session.add(log_entry)
    await session.flush()

    logger.info("dlq_job_retried", job_id=str(job_id))

    from app.services import dag as dag_service

    await dag_service.on_dag_root_retried(job_id, session)

    has_unmet = await dag_service.has_unmet_dependencies(job_id, session)
    if not has_unmet:
        await queue.push(
            job_id=str(job_id),
            effective_priority=float(job.priority),
            scheduled_at=job.scheduled_at.timestamp(),
            created_at=job.created_at.timestamp(),
        )

    await session.commit()

    refreshed = await session.get(Job, job_id)
    return refreshed  # type: ignore


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
