from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.models.job import Job, JobPriority, JobStatus

if TYPE_CHECKING:
    from app.queues.base import BaseQueue


logger = get_logger(__name__)

_PRIORITY_FLOOR = 1.0


class AgingProcess:
    """
    Decrements effective_priority on long-waiting pending jobs.
    """

    async def run(
        self,
        session: AsyncSession,
        queue: "BaseQueue",
    ) -> dict:
        """
        Execute one aging cycle.
        """
        now = datetime.now(UTC)

        medium_cutoff = now - timedelta(seconds=settings.MEDIUM_PRIORITY_AGE_THRESHOLD)
        low_cutoff = now - timedelta(seconds=settings.LOW_PRIORITY_AGE_THRESHOLD)

        medium_count = await self._age_priority(
            priority=JobPriority.MEDIUM,
            cutoff=medium_cutoff,
            db=session,
        )

        low_count = await self._age_priority(
            priority=JobPriority.LOW,
            cutoff=low_cutoff,
            db=session,
        )

        await session.commit()

        if medium_count > 0 or low_count > 0:
            await self._sync_queue(session, queue)

        summary = {
            "medium_jobs_aged": medium_count,
            "low_jobs_aged": low_count,
            "total_aged": medium_count + low_count,
            "timestamp": now.isoformat(),
        }

        logger.info(
            "aging_complete",
            **summary,
        )

        return summary

    async def _age_priority(
        self,
        priority: int,
        cutoff: datetime,
        db: AsyncSession,
    ) -> int:
        """
        Decrement effective_priority for all pending jobs of a given
        priority level that have been waiting since before the cutoff.

        Uses GREATEST() to floor at _PRIORITY_FLOOR (1.0).
        Returns the number of rows updated.
        """
        result = await db.execute(
            update(Job)
            .where(
                Job.status == JobStatus.PENDING,
                Job.priority == priority,
                Job.effective_priority > _PRIORITY_FLOOR,
                Job.created_at <= cutoff,
                Job.deleted_at.is_(None),
                Job.is_dlq.is_(False),
            )
            .values(
                effective_priority=func.greatest(
                    _PRIORITY_FLOOR,
                    Job.effective_priority - settings.AGING_DECREMENT,
                ),
                updated_at=func.now(),
            )
            .returning(Job.id)
        )
        updated_ids = result.fetchall()
        count = len(updated_ids)

        if count > 0:
            logger.info(
                "jobs_aged",
                priority=priority,
                count=count,
                decrement=settings.AGING_DECREMENT,
            )

        return count

    async def _sync_queue(
        self,
        db: AsyncSession,
        queue: "BaseQueue",
    ) -> None:
        """
        After aging, fetch all updated pending jobs and re-push them
        to the queue with their new effective_priority scores.
        """
        result = await db.execute(
            select(Job).where(
                Job.status == JobStatus.PENDING,
                Job.deleted_at.is_(None),
                Job.is_dlq.is_(False),
                # Only re-sync jobs that have been aged (below their raw priority)
                Job.effective_priority
                < Job.priority.cast(type_=type(Job.effective_priority.type)),
            )
        )
        aged_jobs = result.scalars().all()

        for job in aged_jobs:
            try:
                await queue.update_priority(
                    job_id=str(job.id),
                    new_priority=job.effective_priority,
                    scheduled_at=job.scheduled_at.timestamp(),
                    created_at=job.created_at.timestamp(),
                )

                # Also update Redis sorted set score
                from app.services.job import _sync_job_to_redis

                await _sync_job_to_redis(
                    str(job.id),
                    job.effective_priority,
                )
            except Exception as exc:
                logger.error(
                    "aging_sync_error",
                    job_id=str(job.id),
                    error=str(exc),
                )
