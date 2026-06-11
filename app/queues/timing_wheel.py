import asyncio
import time
from collections import defaultdict

from app.queues.base import BaseQueue

WHEEL_SIZE = 3600


class TimingWheel(BaseQueue):
    def __init__(self) -> None:
        self._wheel: list[list[tuple[float, str]]] = [[] for _ in range(WHEEL_SIZE)]
        self._overflow: dict[float, list[tuple[float, str]]] = defaultdict(list)
        self._current_slot: int = 0
        self._start_time: float = time.monotonic()
        self._lock = asyncio.Lock()

    def _seconds_until(self, scheduled_at_unix: float) -> float:
        """
        Return how many seconds until scheduled_at_unix from now.
        Negative or zero means the job is due immediately (slot 0 offset).
        """
        delta = scheduled_at_unix - time.time()
        return max(0.0, delta)

    def _slot_for(self, seconds_from_now: float) -> int | None:
        """
        Map a delay in seconds to a wheel slot index.
        Returns None if the delay exceeds the wheel's range (goes to overflow).
        """
        if seconds_from_now >= WHEEL_SIZE:
            return None
        offset = int(seconds_from_now)
        return (self._current_slot + offset) % WHEEL_SIZE

    def _insert_into_slot(
        self,
        slot: int,
        priority: float,
        job_id: str,
    ) -> None:
        """
        Insert a job into a wheel slot, keeping the slot sorted by priority.
        """
        self._wheel[slot].append((priority, job_id))
        # Keep ascending sort so index 0 = highest priority (lowest score)
        self._wheel[slot].sort(key=lambda x: x[0])

    def _drain_overflow(self) -> None:
        """
        Move overflow jobs that are now within the wheel's range into slots.
        """
        now = time.time()
        to_insert = [ts for ts in self._overflow if ts <= now + WHEEL_SIZE]
        for ts in to_insert:
            entries = self._overflow.pop(ts)
            seconds = max(0.0, ts - now)
            slot = self._slot_for(seconds)
            if slot is not None:
                for priority, job_id in entries:
                    self._insert_into_slot(slot, priority, job_id)

    async def push(
        self,
        job_id: str,
        effective_priority: float,
        scheduled_at: float,
        created_at: float,
    ) -> None:
        """
        Place a job in the appropriate wheel slot or overflow.
        created_at is accepted for interface compatibility but not used —
        the timing wheel sorts within a slot by priority only.
        """
        async with self._lock:
            seconds = self._seconds_until(scheduled_at)
            slot = self._slot_for(seconds)
            if slot is None:
                self._overflow[scheduled_at].append((effective_priority, job_id))
            else:
                self._insert_into_slot(slot, effective_priority, job_id)

    async def pop(self) -> str | None:
        """
        Advance one tick and return the highest-priority job due in this slot.
        Also drains overflow jobs that have come within the wheel's range.
        """
        due = await self.tick()
        return due[0] if due else None

    async def tick(self) -> list[str]:
        """
        Advance the wheel pointer by one slot.

        Returns the list of job_ids due in the current slot (ordered by
        effective_priority within the slot, most urgent first).

        Also drains overflow jobs that are now within wheel range.
        """
        async with self._lock:
            due_entries = self._wheel[self._current_slot]
            due_job_ids = [job_id for _, job_id in due_entries]
            self._wheel[self._current_slot] = []

            self._current_slot = (self._current_slot + 1) % WHEEL_SIZE

            self._drain_overflow()

            return due_job_ids

    async def remove(self, job_id: str) -> None:
        """
        Remove a job from the wheel by scanning all slots and overflow.
        """
        async with self._lock:
            for slot in self._wheel:
                slot[:] = [(p, jid) for p, jid in slot if jid != job_id]
            for ts in list(self._overflow.keys()):
                self._overflow[ts] = [
                    (p, jid) for p, jid in self._overflow[ts] if jid != job_id
                ]
                if not self._overflow[ts]:
                    del self._overflow[ts]

    async def size(self) -> int:
        """
        Return total number of jobs across all wheel slots and overflow.
        """
        async with self._lock:
            wheel_count = sum(len(slot) for slot in self._wheel)
            overflow_count = sum(len(v) for v in self._overflow.values())
            return wheel_count + overflow_count

    async def contains(self, job_id: str) -> bool:
        """Check whether a job_id exists anywhere in the wheel or overflow."""
        async with self._lock:
            for slot in self._wheel:
                if any(jid == job_id for _, jid in slot):
                    return True
            for entries in self._overflow.values():
                if any(jid == job_id for _, jid in entries):
                    return True
            return False
