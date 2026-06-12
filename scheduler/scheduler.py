import asyncio
import signal
from datetime import UTC, datetime

import redis.asyncio as aioredis
from sqlalchemy import func, select

from app.core.config import settings
from app.core.logger import get_logger, setup_logging
from app.db.session import AsyncSessionLocal
from app.models.job import Job, JobStatus
from app.models.job_depedencies import JobDependency
from app.queues.heapq import HeapQueue
from worker.aging import AgingProcess

setup_logging()
logger = get_logger(__name__)

QUEUE_KEY = "flint:queue"
WORKER_KEY_PATTERN = "flint:worker:*"
DEAD_WORKER_CLEANUP_INTERVAL = 60  # seconds


class FlintScheduler:
    def __init__(self) -> None:
        self.running = False
        self._redis: aioredis.Redis
        self._queue = HeapQueue()

    async def start(self) -> None:
        """Start all scheduler loops concurrently."""
        self.running = True

        self._redis = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            encoding="utf-8",
        )

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown)

        logger.info("scheduler_started")

        await asyncio.gather(
            self._due_job_loop(),
            self._aging_loop(),
            self._dead_worker_cleanup_loop(),
        )

    async def _due_job_loop(self) -> None:
        """
        Every SCHEDULER_POLL_INTERVAL seconds:
        Find all pending jobs that are due and eligible, push to Redis queue.
        """
        while self.running:
            try:
                await self._push_due_jobs()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("scheduler_due_job_error", error=str(exc))
            await asyncio.sleep(settings.SCHEDULER_POLL_INTERVAL)

    async def _push_due_jobs(self) -> None:
        """
        Query for eligible jobs and push them to the Redis sorted set.
        """
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
                .limit(100)
            )
            candidate_jobs = result.scalars().all()

            if not candidate_jobs:
                return

            pushed = 0
            for job in candidate_jobs:
                score = await self._redis.zscore(QUEUE_KEY, str(job.id))
                if score is not None:
                    continue

                unmet = await self._has_unmet_dependencies(job.id, session)
                if unmet:
                    continue

                await self._redis.zadd(
                    QUEUE_KEY,
                    {str(job.id): job.effective_priority},
                )
                pushed += 1

            if pushed > 0:
                logger.info(
                    "jobs_pushed_to_queue",
                    count=pushed,
                    timestamp=now.isoformat(),
                )

    async def _has_unmet_dependencies(
        self,
        job_id,
        session,
    ) -> bool:
        """Return True if the job has any dependency that is not completed."""
        result = await session.execute(
            select(func.count(JobDependency.id))
            .join(Job, Job.id == JobDependency.depends_on_id)
            .where(
                JobDependency.job_id == job_id,
                Job.status != JobStatus.COMPLETED,
            )
        )
        return (result.scalar() or 0) > 0

    async def _aging_loop(self) -> None:
        """
        Every AGING_INTERVAL seconds: run the starvation prevention
        aging process to decrement effective_priority on waiting jobs.
        """
        while self.running:
            try:
                async with AsyncSessionLocal() as session:
                    aging = AgingProcess()
                    await aging.run(session, self._queue)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("scheduler_aging_error", error=str(exc))
            await asyncio.sleep(settings.AGING_INTERVAL)

    async def _dead_worker_cleanup_loop(self) -> None:
        """
        Every 60 seconds: find jobs stuck in 'processing' state whose
        worker heartbeat has expired in Redis. Reset them to 'pending'
        so they get picked up again.
        """
        while self.running:
            try:
                await self._cleanup_dead_worker_jobs()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("scheduler_cleanup_error", error=str(exc))
            await asyncio.sleep(DEAD_WORKER_CLEANUP_INTERVAL)

    async def _cleanup_dead_worker_jobs(self) -> None:
        """
        Find active worker IDs from Redis. Any job with a worker_id
        not in that set has been abandoned by a dead worker and should
        be reset to pending.
        """
        worker_keys = await self._redis.keys(WORKER_KEY_PATTERN)
        active_worker_ids = {key.replace("flint:worker:", "") for key in worker_keys}  # type: ignore

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Job).where(
                    Job.status == JobStatus.PROCESSING,
                    Job.deleted_at.is_(None),
                    Job.worker_id.isnot(None),
                )
            )
            processing_jobs = result.scalars().all()

            reset_count = 0
            for job in processing_jobs:
                if job.worker_id not in active_worker_ids:
                    from sqlalchemy import update

                    await session.execute(
                        update(Job)
                        .where(Job.id == job.id)
                        .values(
                            status=JobStatus.PENDING,
                            worker_id=None,
                            updated_at=func.now(),
                        )
                    )

                    from app.models.job_log import JobLog, LogEvent

                    log = JobLog(
                        job_id=job.id,
                        event=LogEvent.JOB_CREATED,
                        message=(
                            f"Job reset to pending: worker '{job.worker_id}' "
                            f"is no longer active (heartbeat expired)."
                        ),
                        metadata_={
                            "dead_worker_id": job.worker_id,
                            "reason": "dead_worker_recovery",
                        },
                    )
                    session.add(log)
                    reset_count += 1

                    logger.warning(
                        "dead_worker_job_reset",
                        job_id=str(job.id),
                        dead_worker_id=job.worker_id,
                    )

            if reset_count > 0:
                await session.commit()
                logger.info(
                    "dead_worker_cleanup_complete",
                    reset_count=reset_count,
                )

    def _handle_shutdown(self) -> None:
        logger.info("scheduler_shutdown_signal")
        self.running = False


if __name__ == "__main__":
    scheduler = FlintScheduler()
    asyncio.run(scheduler.start())
