from datetime import UTC, datetime, timedelta

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.models.enum import JobPriority, JobStatus
from app.models.job import Job

logger = get_logger(__name__)

_PRIORITY_FLOOR = 1.0


class AgingProcess:
    """Prevent starvation by lowering the effective priority of waiting jobs.

    Age is measured from ``available_at`` (when the job became eligible), not
    ``created_at``. Otherwise a job scheduled a week in the future would age
    before it could ever run.
    """

    async def run(self, session: AsyncSession) -> dict:
        now = datetime.now(UTC)

        medium_cutoff = now - timedelta(seconds=settings.MEDIUM_PRIORITY_AGE_THRESHOLD)
        low_cutoff = now - timedelta(seconds=settings.LOW_PRIORITY_AGE_THRESHOLD)

        medium_count = await self._age(JobPriority.MEDIUM, medium_cutoff, session)
        low_count = await self._age(JobPriority.LOW, low_cutoff, session)

        await session.commit()

        summary = {
            "medium_jobs_aged": medium_count,
            "low_jobs_aged": low_count,
            "total_aged": medium_count + low_count,
            "timestamp": now.isoformat(),
        }
        if medium_count or low_count:
            logger.info("aging_complete", **summary)
        return summary

    async def _age(
        self,
        priority: int,
        cutoff: datetime,
        session: AsyncSession,
    ) -> int:
        result = await session.execute(
            update(Job)
            .where(
                Job.status == JobStatus.PENDING,
                Job.priority == priority,
                Job.effective_priority > _PRIORITY_FLOOR,
                Job.available_at <= cutoff,
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
        return len(result.fetchall())
