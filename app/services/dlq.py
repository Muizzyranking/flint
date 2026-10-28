import uuid

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import JobNotInDLQException
from app.core.logger import get_logger
from app.models.job import Job, JobStatus
from app.models.job_log import JobLog, LogEvent
from app.services.settings import get_dlq_threshold

logger = get_logger(__name__)


async def send_to_dlq(
    job_id: uuid.UUID,
    error: str,
    session: AsyncSession,
    *,
    expected_worker_id: str | None = None,
) -> bool:
    """
    Move a job to the dead letter queue after exhausting all retries.
    """
    conditions = [Job.id == job_id]
    if expected_worker_id is not None:
        conditions.extend(
            [
                Job.status == JobStatus.PROCESSING,
                Job.worker_id == expected_worker_id,
                Job.cancellation_requested.is_(False),
            ]
        )

    moved = await session.execute(
        update(Job)
        .where(*conditions)
        .values(
            status=JobStatus.FAILED,
            is_dlq=True,
            last_error=error,
            worker_id=None,
            lease_expires_at=None,
            updated_at=func.now(),
        )
        .returning(Job.id)
    )
    if moved.scalar_one_or_none() is None:
        return False

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
    return True


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
) -> Job:
    """
    Manually retry a job from the DLQ.

    Steps:
      1. Verify job exists and is in DLQ
      2. Reset: status='pending', is_dlq=False, retry_count=0,
                last_error=None, worker_id=None, available_at=now
      3. Call dag_service.on_dag_root_retried — reset downstream cancelled jobs
      4. Let the workers' pollers observe the pending row (dependency checks
         happen in the claim query)
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
            available_at=func.now(),
            next_retry_at=None,
            lease_expires_at=None,
            started_at=None,
            completed_at=None,
            cancellation_requested=False,
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

    await session.commit()

    await session.refresh(job)
    return job


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
