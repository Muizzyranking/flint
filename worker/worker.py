import asyncio

import redis.asyncio as aioredis

from app.core.config import settings
from app.core.logger import get_logger
from app.queues.heapq import HeapQueue
from worker.processor import JobProcessor

logger = get_logger(__name__)

WORKER_TTL = 60
HEARTBEAT_INTERVAL = 15


async def run_worker(
    worker_id: str,
    queue: HeapQueue,
    shutdown_event: asyncio.Event,
) -> None:

    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    processor = JobProcessor(worker_id=worker_id, queue=queue)
    worker_key = f"flint:workers:{worker_id}"
    control_channel = f"flint:worker:control:{worker_id}"

    # Register worker
    await redis.set(worker_key, "idle", ex=WORKER_TTL)
    logger.info("worker_started", worker_id=worker_id)
    local_stop = asyncio.Event()

    async def _heartbeat_loop():
        while not local_stop.is_set() and not shutdown_event.is_set():
            await redis.set(worker_key, "active", ex=WORKER_TTL)
            try:
                await asyncio.wait_for(
                    asyncio.shield(local_stop.wait()), timeout=HEARTBEAT_INTERVAL
                )
            except TimeoutError:
                pass

    async def _control_loop():
        pubsub = redis.pubsub()
        await pubsub.subscribe(control_channel)
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                command = message["data"]
                logger.info(
                    "worker_control_command", worker_id=worker_id, command=command
                )
                if command == "stop":
                    local_stop.set()
                    break
                elif command == "restart":
                    local_stop.set()
                    break
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(control_channel)
            await pubsub.aclose()

    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(), name=f"{worker_id}-heartbeat"
    )
    control_task = asyncio.create_task(_control_loop(), name=f"{worker_id}-control")

    try:
        while not local_stop.is_set() and not shutdown_event.is_set():
            try:
                job_id = await queue.pop()
                if job_id is None:
                    await asyncio.sleep(settings.WORKER_POLL_INTERVAL)
                    continue

                await redis.set(worker_key, "busy", ex=WORKER_TTL)
                logger.info("worker_picked_job", worker_id=worker_id, job_id=job_id)
                await processor.process(job_id)
                await redis.set(worker_key, "idle", ex=WORKER_TTL)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("worker_loop_error", worker_id=worker_id, error=str(exc))
                await redis.set(worker_key, "idle", ex=WORKER_TTL)
                await asyncio.sleep(1.0)
    finally:
        heartbeat_task.cancel()
        control_task.cancel()
        await asyncio.gather(heartbeat_task, control_task, return_exceptions=True)
        await redis.delete(worker_key)
        await redis.aclose()
        logger.info("worker_stopped", worker_id=worker_id)
