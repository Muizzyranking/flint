"""
PostgreSQL-backed durable dispatch primitives.

Postgres is the source of truth and the durable queue. Workers only use the
local heap to order a small batch of eligible jobs; the atomic claim below is
what guarantees that a job is executed exactly once per lease.
"""

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import exists, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.core.logger import get_logger
from app.models.enum import JobStatus
from app.models.job import Job
from app.models.job_depedencies import JobDependency

logger = get_logger(__name__)

#: Key used for the maintenance advisory lock (arbitrary but stable).
MAINTENANCE_LOCK_KEY = 911_001


def _unmet_dependency_exists():
    """Correlated EXISTS: the job has at least one non-completed dependency."""
    dependency_job = aliased(Job)
    return exists(
        select(JobDependency.id)
        .join(dependency_job, dependency_job.id == JobDependency.depends_on_id)
        .where(
            JobDependency.job_id == Job.id,
            dependency_job.status != JobStatus.COMPLETED,
        )
        .correlate(Job)
    )


def eligible_job_filter(capabilities: list[str] | None, now: datetime):
    """SQLAlchemy conditions shared by polling and claiming."""
    conditions = [
        Job.status == JobStatus.PENDING,
        Job.deleted_at.is_(None),
        Job.is_dlq.is_(False),
        Job.available_at <= now,
        ~_unmet_dependency_exists(),
    ]
    if capabilities is not None:
        conditions.append(Job.type.in_(capabilities))
    return conditions


class JobDispatcher:
    """Claims jobs atomically for one worker process."""

    def __init__(
        self,
        worker_id: str,
        capabilities: list[str] | None = None,
        lease_seconds: int = 60,
    ) -> None:
        self.worker_id = worker_id
        self.capabilities = capabilities
        self.lease_seconds = lease_seconds

    async def claim(
        self,
        session: AsyncSession,
        *,
        job_id: uuid.UUID | None = None,
    ) -> Job | None:
        """Claim one eligible job, or the specific *job_id* if still eligible.

        ``FOR UPDATE SKIP LOCKED`` makes concurrent workers skip rows another
        worker is currently claiming instead of blocking or double-processing.
        """
        now = datetime.now(UTC)
        conditions = eligible_job_filter(self.capabilities, now)
        if job_id is not None:
            conditions.append(Job.id == job_id)

        result = await session.execute(
            select(Job)
            .where(*conditions)
            .order_by(
                Job.effective_priority.asc(),
                Job.available_at.asc(),
                Job.created_at.asc(),
                Job.id.asc(),
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        job = result.scalar_one_or_none()
        if job is None:
            return None

        job.status = JobStatus.PROCESSING
        job.worker_id = self.worker_id
        job.started_at = now
        job.lease_expires_at = now + timedelta(seconds=self.lease_seconds)
        job.updated_at = now
        await session.commit()

        logger.info(
            "job_claimed",
            job_id=str(job.id),
            worker_id=self.worker_id,
            type=job.type,
            retry_count=job.retry_count,
        )
        return job

    async def heartbeat(
        self,
        session: AsyncSession,
        job_ids: list[uuid.UUID],
    ) -> int:
        """Extend leases for jobs this worker is still executing."""
        if not job_ids:
            return 0
        now = datetime.now(UTC)
        result = await session.execute(
            update(Job)
            .where(
                Job.id.in_(job_ids),
                Job.worker_id == self.worker_id,
                Job.status == JobStatus.PROCESSING,
            )
            .values(
                lease_expires_at=now + timedelta(seconds=self.lease_seconds),
                updated_at=now,
            )
        )
        await session.commit()
        return result.rowcount or 0

    async def release(
        self,
        session: AsyncSession,
        job_ids: list[uuid.UUID],
        *,
        reason: str,
    ) -> int:
        """Return claimed jobs to the pending queue (graceful shutdown)."""
        if not job_ids:
            return 0
        now = datetime.now(UTC)
        result = await session.execute(
            update(Job)
            .where(
                Job.id.in_(job_ids),
                Job.worker_id == self.worker_id,
                Job.status == JobStatus.PROCESSING,
            )
            .values(
                status=JobStatus.PENDING,
                worker_id=None,
                lease_expires_at=None,
                started_at=None,
                available_at=now,
                last_error=reason[:1000],
                updated_at=now,
            )
        )
        await session.commit()
        return result.rowcount or 0


async def reap_expired_leases(
    session: AsyncSession,
    *,
    limit: int = 100,
) -> list[uuid.UUID]:
    """Requeue jobs whose worker stopped heartbeating.

    The process that held the lease is presumed dead. The job is returned to
    ``pending`` without consuming a user retry attempt; repeated crashes are
    visible through ``last_error`` and worker logs.
    """
    now = datetime.now(UTC)
    result = await session.execute(
        select(Job)
        .where(
            Job.status == JobStatus.PROCESSING,
            Job.lease_expires_at.is_not(None),
            Job.lease_expires_at < now,
        )
        .order_by(Job.lease_expires_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    expired = list(result.scalars().all())
    if not expired:
        return []

    job_ids: list[uuid.UUID] = []
    for job in expired:
        previous_lease = job.lease_expires_at
        job_ids.append(job.id)
        job.status = JobStatus.PENDING
        job.worker_id = None
        job.lease_expires_at = None
        job.started_at = None
        job.available_at = now
        job.last_error = (
            f"Lease expired at {previous_lease or now}; "
            "worker presumed dead and job requeued."
        )
        job.updated_at = now

    await session.commit()
    logger.warning("expired_leases_reaped", count=len(job_ids))
    return job_ids
