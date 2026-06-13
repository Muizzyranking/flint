import asyncio
from datetime import UTC, datetime

from sqlalchemy import func, select

from app.core.config import settings
from app.core.logger import get_logger
from app.db.session import AsyncSessionLocal
from app.models.job import Job, JobStatus
from app.models.job_depedencies import JobDependency
from app.queues.heapq import HeapQueue

logger = get_logger(__name__)


class FlintScheduler:
    def __init__(self, queue: HeapQueue) -> None:
        self.queue = queue

    async def run(self, shutdown_event: asyncio.Event) -> None:
        """
        Main scheduler coroutine. Runs until shutdown_event is set.
        On startup, loads all currently pending due jobs into the queue.
        """
        logger.info("scheduler_started")

        await self._push_due_jobs()

        while not shutdown_event.is_set():
            try:
                await self._push_due_jobs()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("scheduler_error", error=str(exc))

            try:
                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=settings.SCHEDULER_POLL_INTERVAL,
                )
            except TimeoutError:
                pass

        logger.info("scheduler_stopped")

    async def _push_due_jobs(self) -> None:
        async with AsyncSessionLocal() as session:
            now = datetime.now(UTC)

            result = await session.execute(
                select(Job)
                .where(
                    Job.status == JobStatus.PENDING,
                    Job.scheduled_at <= now,
                    Job.deleted_at.is_(None),
                    Job.is_dlq.is_(False),
                    Job.worker_id.is_(None),
                )
                .order_by(
                    Job.effective_priority.asc(),
                    Job.scheduled_at.asc(),
                    Job.created_at.asc(),
                )
                .limit(200)
            )
            candidates = result.scalars().all()

            if not candidates:
                return

            pushed = 0
            for job in candidates:
                if await self.queue.contains(str(job.id)):
                    continue

                if await self._has_unmet_dependencies(job.id, session):
                    continue

                await self.queue.push(
                    job_id=str(job.id),
                    effective_priority=job.effective_priority,
                    scheduled_at=job.scheduled_at.timestamp(),
                    created_at=job.created_at.timestamp(),
                )
                pushed += 1

            if pushed > 0:
                logger.info(
                    "jobs_pushed_to_queue",
                    count=pushed,
                    queue_size=await self.queue.size(),
                )

    async def _has_unmet_dependencies(self, job_id, session) -> bool:
        """Return True if the job has any dependency not yet completed."""
        result = await session.execute(
            select(func.count(JobDependency.id))
            .join(Job, Job.id == JobDependency.depends_on_id)
            .where(
                JobDependency.job_id == job_id,
                Job.status != JobStatus.COMPLETED,
            )
        )
        return (result.scalar() or 0) > 0
