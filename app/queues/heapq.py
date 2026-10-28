import asyncio
import heapq
import itertools
import time
from dataclasses import dataclass, field

from app.queues.base import BaseQueue


@dataclass(order=True)
class HeapEntry:
    """A buffered job ordered by urgency.

    Sort order:
      1. effective_priority — lower = more urgent
      2. available_at       — earlier = more urgent
      3. created_at         — older = more urgent
      4. job_id             — deterministic tiebreaker
    """

    effective_priority: float
    available_at: float
    created_at: float
    job_id: str = field(compare=True)
    removed: bool = field(default=False, compare=False)


class HeapQueue(BaseQueue):
    """Bounded, time-aware local priority buffer.

    Design notes
    ------------
    * ``_ready`` holds due jobs; ``_delayed`` holds jobs whose
      ``available_at`` is in the future. Delayed entries are promoted lazily
      when ``pop()`` is called, so a future job can never be executed early.
    * Re-scoring uses *lazy deletion*: the old entry is flagged removed and a
      replacement is inserted. Stale entries are discarded on pop.
    * The old implementation mutated the comparison key of an entry already
      inside the heap, which breaks heap invariants. The ``removed`` flag is
      excluded from ordering for that reason.
    * The buffer is bounded so a slow worker cannot grow memory without limit.
      When full, a more urgent arrival evicts the least urgent buffered entry.
    """

    def __init__(self, max_size: int = 10_000) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._max_size = max_size
        self._ready: list[HeapEntry] = []
        self._delayed: list[tuple[float, int, HeapEntry]] = []
        self._finder: dict[str, HeapEntry] = {}
        self._sequence = itertools.count()
        self._lock = asyncio.Lock()

    async def push(
        self,
        job_id: str,
        effective_priority: float,
        available_at: float,
        created_at: float,
    ) -> bool:
        async with self._lock:
            existing = self._finder.get(job_id)
            if existing is None and len(self._finder) >= self._max_size:
                # A full buffer should drop the least urgent buffered job, not
                # the new arrival. Otherwise a burst of low-priority work could
                # hide a newly arrived high-priority job until the next batch.
                candidate = HeapEntry(
                    effective_priority=effective_priority,
                    available_at=available_at,
                    created_at=created_at,
                    job_id=job_id,
                )
                worst = max(self._finder.values())
                if candidate >= worst:
                    return False
                worst.removed = True
                del self._finder[worst.job_id]

            if existing is not None:
                existing.removed = True

            entry = HeapEntry(
                effective_priority=effective_priority,
                available_at=available_at,
                created_at=created_at,
                job_id=job_id,
            )
            self._finder[job_id] = entry

            if available_at <= time.time():
                heapq.heappush(self._ready, entry)
            else:
                heapq.heappush(
                    self._delayed,
                    (available_at, next(self._sequence), entry),
                )
            return True

    async def pop(self, now: float | None = None) -> str | None:
        async with self._lock:
            current = time.time() if now is None else now
            self._compact_if_needed(current)
            self._promote(current)

            while self._ready:
                entry = heapq.heappop(self._ready)
                if entry.removed or self._finder.get(entry.job_id) is not entry:
                    continue
                del self._finder[entry.job_id]
                return entry.job_id
            return None

    async def remove(self, job_id: str) -> None:
        async with self._lock:
            entry = self._finder.pop(job_id, None)
            if entry is not None:
                entry.removed = True

    async def update_priority(
        self,
        job_id: str,
        new_priority: float,
        available_at: float,
        created_at: float,
    ) -> bool:
        async with self._lock:
            existing = self._finder.get(job_id)
            if existing is None and len(self._finder) >= self._max_size:
                return False
            if existing is not None:
                existing.removed = True

            entry = HeapEntry(
                effective_priority=new_priority,
                available_at=available_at,
                created_at=created_at,
                job_id=job_id,
            )
            self._finder[job_id] = entry
            if available_at <= time.time():
                heapq.heappush(self._ready, entry)
            else:
                heapq.heappush(
                    self._delayed,
                    (available_at, next(self._sequence), entry),
                )
            return True

    async def size(self) -> int:
        async with self._lock:
            return len(self._finder)

    @property
    def capacity(self) -> int:
        return self._max_size

    async def contains(self, job_id: str) -> bool:
        async with self._lock:
            return job_id in self._finder

    async def get_priority(self, job_id: str) -> float | None:
        """Return the buffered effective priority, or None if absent."""
        async with self._lock:
            entry = self._finder.get(job_id)
            return None if entry is None else entry.effective_priority

    async def get_available_at(self, job_id: str) -> float | None:
        """Return the buffered availability timestamp, or None if absent."""
        async with self._lock:
            entry = self._finder.get(job_id)
            return None if entry is None else entry.available_at

    async def peek(self, now: float | None = None) -> str | None:
        async with self._lock:
            current = time.time() if now is None else now
            self._compact_if_needed(current)
            self._promote(current)
            for entry in self._ready:
                if not entry.removed and self._finder.get(entry.job_id) is entry:
                    return entry.job_id
            return None

    async def clear(self) -> None:
        async with self._lock:
            self._ready.clear()
            self._delayed.clear()
            self._finder.clear()

    def _promote(self, now: float) -> None:
        while self._delayed and self._delayed[0][0] <= now:
            _, _, entry = heapq.heappop(self._delayed)
            if entry.removed or self._finder.get(entry.job_id) is not entry:
                continue
            heapq.heappush(self._ready, entry)

    def _compact_if_needed(self, now: float) -> None:
        """Rebuild both heaps when lazy-deletion tombstones dominate.

        Re-scoring is intentionally O(log n) via tombstones, but a continuously
        aged buffer would otherwise accumulate dead entries. The live job count
        stays bounded by ``max_size``; the arrays are compacted opportunistically.
        """
        if len(self._ready) + len(self._delayed) <= self._max_size * 4:
            return

        self._ready.clear()
        self._delayed.clear()
        for entry in self._finder.values():
            if entry.available_at <= now:
                heapq.heappush(self._ready, entry)
            else:
                heapq.heappush(
                    self._delayed,
                    (entry.available_at, next(self._sequence), entry),
                )
