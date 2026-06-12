import asyncio
import os
import signal
import sys
import uuid

import redis.asyncio as aioredis

from app.core.config import settings
from app.core.logger import get_logger, setup_logging
from app.queues.heapq import HeapQueue
from app.queues.timing_wheel import TimingWheel
from worker.processor import JobProcessor

setup_logging()
logger = get_logger(__name__)

WORKER_REGISTRY_KEY = "flint:workers:{worker_id}"
WORKER_CONTROL_CHANNEL = "flint:worker:control:{worker_id}"
QUEUE_KEY = "flint:queue"
HEARTBEAT_TTL = 60  # seconds — key expires if worker dies without deregistering
HEARTBEAT_INTERVAL = 30  # seconds between heartbeat refreshes


class FlintWorker:
    def __init__(self) -> None:
        self.worker_id = settings.WORKER_ID or f"worker-{str(uuid.uuid4())[:8]}"
        self.running = False
        self._redis: aioredis.Redis
        self._queue: HeapQueue | TimingWheel | None = None
        self._processor: JobProcessor
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """
        Main entry point. Initialises resources and starts all loops.
        """
        self.running = True

        self._redis = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            encoding="utf-8",
        )
        self._redis_pubsub = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            encoding="utf-8",
            socket_timeout=None,
            socket_connect_timeout=5,
        )

        self._queue = await self._build_queue()

        await self._load_queue_from_redis()

        self._processor = JobProcessor(
            worker_id=self.worker_id,
            queue=self._queue,
        )

        await self._register()

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown_signal)

        logger.info(
            "worker_started",
            worker_id=self.worker_id,
            queue_strategy=self._queue.__class__.__name__,
        )
        try:
            await asyncio.gather(
                self._poll_loop(),
                self._heartbeat_loop(),
                self._control_channel_loop(),
            )
        finally:
            await self._redis_pubsub.aclose()
            await self._redis.aclose()

    async def _poll_loop(self) -> None:
        """
        Core poll loop. Pops job IDs from the queue and processes them.
        Sleeps for WORKER_POLL_INTERVAL when the queue is empty.
        """
        while self.running:
            try:
                new_queue = await self._maybe_switch_queue()
                if new_queue:
                    self._queue = new_queue
                    self._processor.queue = new_queue
                    await self._load_queue_from_redis()

                job_id = await self._pop_next()
                if job_id:
                    await self._processor.process(job_id)
                else:
                    await asyncio.sleep(settings.WORKER_POLL_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "worker_poll_error",
                    worker_id=self.worker_id,
                    error=str(exc),
                )
                await asyncio.sleep(1.0)

        await self._deregister()
        logger.info("worker_stopped", worker_id=self.worker_id)

    async def _pop_next(self) -> str | None:
        """
        Pop the next job_id from the Redis sorted set.
        Uses ZPOPMIN to atomically remove and return the lowest-score member.
        """
        items = await self._redis.zpopmin(QUEUE_KEY, 1)
        if not items:
            return None

        job_id, score = (
            items[0],
            float(items[1]) if len(items) > 1 else float(items[0][1]),
        )

        # Handle both tuple and flat list responses
        if isinstance(items[0], (list, tuple)):
            job_id = items[0][0]
            score = float(items[0][1])
        else:
            job_id = items[0]
            score = float(items[1])

        return str(job_id)

    async def _build_queue(self) -> HeapQueue | TimingWheel:
        """Build the correct queue based on scheduler_strategy setting."""
        strategy = await self._get_strategy()
        if strategy == "timing_wheel":
            logger.info("queue_strategy", strategy="timing_wheel")
            return TimingWheel()
        logger.info("queue_strategy", strategy="heap")
        return HeapQueue()

    async def _maybe_switch_queue(self) -> HeapQueue | TimingWheel | None:
        """
        Check if the scheduler_strategy setting has changed.
        If it has, return a new queue instance. Otherwise return None.
        """
        current_strategy = (
            self._queue.__class__.__name__.lower()
            .replace("heapqueue", "heap")
            .replace("timingwheel", "timing_wheel")
        )

        new_strategy = await self._get_strategy()

        if new_strategy != current_strategy:
            logger.info(
                "queue_strategy_switched",
                from_strategy=current_strategy,
                to_strategy=new_strategy,
                worker_id=self.worker_id,
            )
            if new_strategy == "timing_wheel":
                return TimingWheel()
            return HeapQueue()
        return None

    async def _get_strategy(self) -> str:
        """Read scheduler_strategy from Redis cache or fall back to 'heap'."""
        try:
            from app.db.session import AsyncSessionLocal
            from app.services.settings import get_scheduler_strategy

            async with AsyncSessionLocal() as session:
                return await get_scheduler_strategy(session)
        except Exception:
            return "heap"

    async def _load_queue_from_redis(self) -> None:
        """
        On startup or strategy switch, load all pending job IDs from
        the Redis sorted set into the local in-memory queue.
        This populates the heap so the worker has the full queue state.
        """
        import time

        try:
            # ZRANGE with WITHSCORES returns [member, score, member, score, ...]
            items = await self._redis.zrange(QUEUE_KEY, 0, -1, withscores=True)
            count = 0
            for i in range(0, len(items), 2):
                job_id = items[i]
                score = float(items[i + 1])
                now = time.time()
                await self._queue.push(
                    job_id=job_id,
                    effective_priority=score,
                    scheduled_at=now,
                    created_at=now,
                )
                count += 1
            logger.info(
                "queue_loaded_from_redis",
                worker_id=self.worker_id,
                job_count=count,
            )
        except Exception as exc:
            logger.error(
                "queue_load_error",
                worker_id=self.worker_id,
                error=str(exc),
            )

    async def _heartbeat_loop(self) -> None:
        """
        Refresh the worker's Redis registration key every HEARTBEAT_INTERVAL.
        If the worker dies, the key expires after HEARTBEAT_TTL seconds
        and the API reflects the worker as gone.
        """
        while self.running:
            try:
                await self._register()
            except Exception as exc:
                logger.error(
                    "heartbeat_error",
                    worker_id=self.worker_id,
                    error=str(exc),
                )
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _register(self) -> None:
        """Write/refresh the worker registration key in Redis."""
        key = WORKER_REGISTRY_KEY.format(worker_id=self.worker_id)
        await self._redis.set(key, "active", ex=HEARTBEAT_TTL)

    async def _deregister(self) -> None:
        """Remove the worker registration key on clean shutdown."""
        key = WORKER_REGISTRY_KEY.format(worker_id=self.worker_id)
        await self._redis.delete(key)
        logger.info("worker_deregistered", worker_id=self.worker_id)

    async def _control_channel_loop(self) -> None:
        """
        Subscribe to the worker's Redis control channel.
        The API publishes 'stop' or 'restart' signals here.
        This allows workers to be controlled from the dashboard.
        """
        channel = WORKER_CONTROL_CHANNEL.format(worker_id=self.worker_id)

        while self.running:
            pubsub = self._redis_pubsub.pubsub()
            await pubsub.subscribe(channel)
            try:
                async for message in pubsub.listen():
                    if not self.running:
                        return
                    if message["type"] != "message":
                        continue
                    command = message.get("data", "").strip().lower()
                    logger.info(
                        "worker_control_command",
                        worker_id=self.worker_id,
                        command=command,
                    )
                    if command == "stop":
                        self._handle_shutdown_signal()
                        return
                    elif command == "restart":
                        logger.info("worker_restarting", worker_id=self.worker_id)
                        self._handle_shutdown_signal()
                        os.execv(sys.executable, [sys.executable] + sys.argv)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning(
                    "control_channel_reconnecting",
                    worker_id=self.worker_id,
                    error=str(exc),
                )
                await asyncio.sleep(2)
            finally:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()

    def _handle_shutdown_signal(self) -> None:
        """
        Handle SIGTERM / SIGINT.
        """
        if self.running:
            logger.info(
                "worker_shutdown_signal",
                worker_id=self.worker_id,
            )
            self.running = False
            self._shutdown_event.set()


if __name__ == "__main__":
    worker = FlintWorker()
    asyncio.run(worker.start())
