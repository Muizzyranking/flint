import asyncio
import heapq
from dataclasses import dataclass, field

from app.queues.base import BaseQueue

_REMOVED = "__removed__"


@dataclass(order=True)
class HeapEntry:
    """
    Sort order (left-to-right tuple comparison):
      1. effective_priority  — lower value = more urgent
      2. scheduled_at        — earlier time = more urgent
      3. created_at          — older job = more urgent
      4. job_id              — deterministic tiebreaker (UUID string sort)
    """

    effective_priority: float
    scheduled_at: float
    created_at: float
    job_id: str = field(compare=True)


class HeapQueue(BaseQueue):
    """
    In-memory min-heap priority queue.

    The heap is local to each worker process. On worker startup it is
    populated from the Redis sorted set (flint:queue) which acts as the
    persistent backing store.
    """

    def __init__(self) -> None:
        self._heap: list[HeapEntry] = []
        self._entry_finder: dict[str, HeapEntry] = {}
        self._lock = asyncio.Lock()

    async def push(
        self,
        job_id: str,
        effective_priority: float,
        scheduled_at: float,
        created_at: float,
    ) -> None:
        """
        Push a job onto the heap.

        If the job_id already exists (e.g. being re-scored by aging),
        the old entry is lazily marked REMOVED and a new one is pushed.
        This avoids an O(n) heap rebuild.
        """
        async with self._lock:
            if job_id in self._entry_finder:
                self._mark_removed(job_id)

            entry = HeapEntry(
                effective_priority=effective_priority,
                scheduled_at=scheduled_at,
                created_at=created_at,
                job_id=job_id,
            )
            self._entry_finder[job_id] = entry
            heapq.heappush(self._heap, entry)

    async def pop(self) -> str | None:
        """
        Pop and return the most urgent job_id.

        Skips entries that have been marked REMOVED (lazy deletion).
        Returns None if no valid entries remain.
        """
        async with self._lock:
            while self._heap:
                entry = heapq.heappop(self._heap)
                if entry.job_id != _REMOVED:
                    self._entry_finder.pop(entry.job_id, None)
                    return entry.job_id
            return None

    async def remove(self, job_id: str) -> None:
        """
        Mark a job as removed. It will be silently skipped on the next pop().
        No-op if the job is not in the queue.
        """
        async with self._lock:
            if job_id in self._entry_finder:
                self._mark_removed(job_id)

    async def update_priority(
        self,
        job_id: str,
        new_priority: float,
        scheduled_at: float,
        created_at: float,
    ) -> None:
        """
        Re-score an existing job with a new effective_priority.

        Called by the aging process every AGING_INTERVAL seconds.
        Uses lazy deletion: marks the old entry removed, pushes a new one.
        O(log n) — no heap rebuild required.
        """
        async with self._lock:
            if job_id in self._entry_finder:
                self._mark_removed(job_id)

            entry = HeapEntry(
                effective_priority=new_priority,
                scheduled_at=scheduled_at,
                created_at=created_at,
                job_id=job_id,
            )
            self._entry_finder[job_id] = entry
            heapq.heappush(self._heap, entry)

    async def size(self) -> int:
        """
        Returns the number of valid (non-removed) jobs in the queue.
        O(1) — reads from _entry_finder, not the heap list.
        """
        async with self._lock:
            return len(self._entry_finder)

    async def peek(self) -> str | None:
        """
        Return the most urgent job_id without removing it.
        Skips REMOVED entries but does not pop them from the heap.
        """
        async with self._lock:
            for entry in self._heap:
                if entry.job_id != _REMOVED:
                    return entry.job_id
            return None

    async def contains(self, job_id: str) -> bool:
        """Check if a job_id is currently in the queue."""
        async with self._lock:
            return job_id in self._entry_finder

    def _mark_removed(self, job_id: str) -> None:
        """
        Mark an entry as removed in-place.
        The entry remains in the heap list but will be skipped on pop().
        """
        entry = self._entry_finder.pop(job_id)
        entry.job_id = _REMOVED
