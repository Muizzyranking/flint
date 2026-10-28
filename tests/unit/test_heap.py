import asyncio
import time

from app.queues.heapq import HeapQueue


def run(coro):
    return asyncio.run(coro)


def test_pop_orders_by_priority_then_available_time():
    queue = HeapQueue()
    now = time.time()

    async def scenario():
        await queue.push("low", 3.0, now, now)
        await queue.push("high", 1.0, now, now)
        await queue.push("medium", 2.0, now, now)
        return [await queue.pop(now) for _ in range(3)]

    assert run(scenario()) == ["high", "medium", "low"]


def test_pop_never_returns_a_future_job():
    queue = HeapQueue()
    now = time.time()

    async def scenario():
        await queue.push("future-high", 1.0, now + 60, now)
        await queue.push("due-low", 3.0, now, now)
        assert await queue.pop(now) == "due-low"
        assert await queue.pop(now) is None
        return await queue.pop(now + 61)

    assert run(scenario()) == "future-high"


def test_rescoring_uses_lazy_deletion_and_latest_priority():
    queue = HeapQueue()
    now = time.time()

    async def scenario():
        await queue.push("a", 3.0, now, now)
        await queue.push("b", 2.0, now, now)
        await queue.update_priority("a", 1.0, now, now)
        assert await queue.size() == 2
        return [await queue.pop(now) for _ in range(2)]

    assert run(scenario()) == ["a", "b"]


def test_removed_jobs_are_skipped():
    queue = HeapQueue()
    now = time.time()

    async def scenario():
        await queue.push("a", 1.0, now, now)
        await queue.push("b", 2.0, now, now)
        await queue.remove("a")
        assert await queue.pop(now) == "b"
        assert await queue.pop(now) is None
        assert await queue.size() == 0

    run(scenario())


def test_buffer_is_bounded():
    queue = HeapQueue(max_size=2)
    now = time.time()

    async def scenario():
        assert await queue.push("a", 1.0, now, now) is True
        assert await queue.push("b", 1.0, now, now) is True
        assert await queue.push("c", 1.0, now, now) is False
        assert await queue.size() == 2

    run(scenario())


def test_contains_and_priority_lookup():
    queue = HeapQueue()
    now = time.time()

    async def scenario():
        await queue.push("a", 2.0, now, now)
        assert await queue.contains("a")
        assert await queue.get_priority("a") == 2.0
        await queue.remove("a")
        assert not await queue.contains("a")
        assert await queue.get_priority("a") is None

    run(scenario())
