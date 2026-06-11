from abc import ABC, abstractmethod


class BaseQueue(ABC):
    @abstractmethod
    async def push(
        self,
        job_id: str,
        effective_priority: float,
        scheduled_at: float,
        created_at: float,
    ) -> None:
        """
        Add a job to the queue.
        """
        ...

    @abstractmethod
    async def pop(self) -> str | None:
        """
        Remove and return the most urgent job_id from the queue.
        Returns None if the queue is empty.
        """
        ...

    @abstractmethod
    async def remove(self, job_id: str) -> None:
        """
        Remove a specific job from the queue without processing it.
        Used when a job is cancelled while still pending in the queue.
        No-op if the job is not in the queue.
        """
        ...

    @abstractmethod
    async def size(self) -> int:
        """
        Return the number of valid (non-removed) jobs in the queue.
        """
        ...

    async def update_priority(
        self,
        job_id: str,
        new_priority: float,
        scheduled_at: float,
        created_at: float,
    ) -> None:
        """
        Re-score a job with a new effective_priority.
        Called by the aging process when it decrements a job's priority.

        Default implementation: remove then re-push.
        HeapQueue overrides this with lazy deletion for efficiency.
        """
        await self.remove(job_id)
        await self.push(job_id, new_priority, scheduled_at, created_at)
