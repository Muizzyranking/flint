"""
Aging process — starvation prevention.

Decrements effective_priority on long-waiting pending jobs
and updates their score in the HeapQueue directly.

Called every AGING_INTERVAL seconds from worker/main.py.
Operates on the shared HeapQueue instance.

Thresholds:
    Medium priority (2): waiting > 2 minutes → begin aging
    Low priority (3):    waiting > 5 minutes → begin aging

Decrement: 0.1 per cycle. Floor: 1.0 (never below High priority).
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import Float, cast, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.models.job import Job, JobPriority, JobStatus
from app.queues.heapq import HeapQueue

logger = get_logger(__name__)

_PRIORITY_FLOOR = 1.0


class AgingProcess:
    async def run(self, session: AsyncSession, queue: HeapQueue) -> dict:
        """
        One aging cycle:
          1. Decrement effective_priority for qualifying medium + low jobs in DB
          2. For each updated job, call queue.update_priority() on the heap
          3. Log and return summary
        """
        now = datetime.now(timezone.utc)

        medium_cutoff = now - timedelta(seconds=settings.MEDIUM_PRIORITY_AGE_THRESHOLD)
        low_cutoff = now - timedelta(seconds=settings.LOW_PRIORITY_AGE_THRESHOLD)

        medium_count = await self._age(JobPriority.MEDIUM, medium_cutoff, session)
        low_count = await self._age(JobPriority.LOW, low_cutoff, session)

        await session.commit()

        # Sync updated priorities into the heap
        if medium_count > 0 or low_count > 0:
            await self._sync_heap(session, queue)

        summary = {
            "medium_jobs_aged": medium_count,
            "low_jobs_aged": low_count,
            "total_aged": medium_count + low_count,
            "timestamp": now.isoformat(),
        }
        logger.info("aging_complete", **summary)
        return summary

    async def _age(self, priority: int, cutoff: datetime, session: AsyncSession) -> int:
        """
        Decrement effective_priority for pending jobs of a given priority
        that have been waiting since before the cutoff.
        Returns number of rows updated.
        """
        result = await session.execute(
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
        updated = result.fetchall()
        count = len(updated)
        if count > 0:
            logger.info("jobs_aged", priority=priority, count=count)
        return count

    async def _sync_heap(self, session: AsyncSession, queue: HeapQueue) -> None:
        """
        After aging, reload all pending jobs whose effective_priority
        has dropped below their raw priority and update them in the heap.
        """
        result = await session.execute(
            select(Job).where(
                Job.status == JobStatus.PENDING,
                Job.deleted_at.is_(None),
                Job.is_dlq.is_(False),
                Job.effective_priority < cast(Job.priority, Float),
            )
        )
        aged_jobs = result.scalars().all()

        for job in aged_jobs:
            try:
                if await queue.contains(str(job.id)):
                    await queue.update_priority(
                        job_id=str(job.id),
                        new_priority=job.effective_priority,
                        scheduled_at=job.scheduled_at.timestamp(),
                        created_at=job.created_at.timestamp(),
                    )
            except Exception as exc:
                logger.error(
                    "aging_sync_error",
                    job_id=str(job.id),
                    error=str(exc),
                )
