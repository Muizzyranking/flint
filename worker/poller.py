"""
Per-worker poller.

Each worker process maintains a small, bounded local heap. The poller reads
eligible jobs from PostgreSQL (the durable queue) and mirrors their ordering
into that heap. A job is only executed after an atomic claim, so every worker
may safely observe the same candidate rows.
"""

import asyncio
from datetime import UTC, datetime

from sqlalchemy import select

from app.core.config import settings
from app.core.logger import get_logger
from app.db.session import AsyncSessionLocal
from app.models.job import Job
from app.queues.heapq import HeapQueue
from app.queues.postgres import eligible_job_filter

logger = get_logger(__name__)


class JobPoller:
    def __init__(
        self,
        queue: HeapQueue,
        capabilities: list[str] | None = None,
        batch_size: int | None = None,
        poll_interval: float | None = None,
    ) -> None:
        self.queue = queue
        self.capabilities = capabilities
        self.batch_size = batch_size or settings.WORKER_BATCH_SIZE
        self.poll_interval = (
            settings.WORKER_POLL_INTERVAL if poll_interval is None else poll_interval
        )
        self._empty_polls = 0

    async def run(self, shutdown_event: asyncio.Event) -> None:
        logger.info(
            "poller_started",
            batch_size=self.batch_size,
            poll_interval=self.poll_interval,
        )
        while not shutdown_event.is_set():
            try:
                pushed = await self.poll_once()
                self._empty_polls = 0 if pushed else self._empty_polls + 1
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("poller_error", error=str(exc))
                self._empty_polls += 1

            # Small adaptive backoff: idle workers poll less aggressively but
            # wake quickly when work appears.
            delay = self.poll_interval
            if self._empty_polls > 10:
                delay = min(self.poll_interval * 4, 5.0)

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
            except TimeoutError:
                pass

        logger.info("poller_stopped")

    async def poll_once(self) -> int:
        now = datetime.now(UTC)
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(
                    Job.id,
                    Job.effective_priority,
                    Job.available_at,
                    Job.created_at,
                )
                .where(*eligible_job_filter(self.capabilities, now))
                .order_by(
                    Job.effective_priority.asc(),
                    Job.available_at.asc(),
                    Job.created_at.asc(),
                    Job.id.asc(),
                )
                .limit(self.batch_size)
            )
            rows = result.all()

        pushed = 0
        for row in rows:
            job_id = str(row.id)
            available_at = row.available_at.timestamp()
            current_priority = await self.queue.get_priority(job_id)
            current_available_at = await self.queue.get_available_at(job_id)
            if current_priority is None or current_available_at is None:
                accepted = await self.queue.push(
                    job_id=job_id,
                    effective_priority=row.effective_priority,
                    available_at=available_at,
                    created_at=row.created_at.timestamp(),
                )
            elif (
                current_priority != row.effective_priority
                or current_available_at != available_at
            ):
                accepted = await self.queue.update_priority(
                    job_id=job_id,
                    new_priority=row.effective_priority,
                    available_at=available_at,
                    created_at=row.created_at.timestamp(),
                )
            else:
                continue

            if not accepted:
                logger.warning("worker_buffer_full", capacity=self.queue.capacity)
                break
            pushed += 1

        if pushed:
            logger.debug(
                "poller_pushed",
                count=pushed,
                queue_size=await self.queue.size(),
            )
        return pushed
