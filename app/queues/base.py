from abc import ABC, abstractmethod


class BaseQueue(ABC):
    """Local, process-owned ready buffer.

    This is *not* the durable queue. PostgreSQL is the source of truth for job
    state; ``BaseQueue`` only orders jobs that one worker process has already
    observed as eligible.
    """

    @abstractmethod
    async def push(
        self,
        job_id: str,
        effective_priority: float,
        available_at: float,
        created_at: float,
    ) -> bool:
        """Add or replace a job. Returns False when the buffer is full."""
        ...

    @abstractmethod
    async def pop(self, now: float | None = None) -> str | None:
        """Return the most urgent job that is due, or None."""
        ...

    @abstractmethod
    async def remove(self, job_id: str) -> None:
        """Remove a job if present. No-op otherwise."""
        ...

    @abstractmethod
    async def size(self) -> int:
        """Return the number of valid jobs in the buffer."""
        ...

    @abstractmethod
    async def contains(self, job_id: str) -> bool:
        """Return whether a job is currently buffered."""
        ...

    async def update_priority(
        self,
        job_id: str,
        new_priority: float,
        available_at: float,
        created_at: float,
    ) -> bool:
        """Re-score a job, dropping and re-inserting it."""
        await self.remove(job_id)
        return await self.push(job_id, new_priority, available_at, created_at)
