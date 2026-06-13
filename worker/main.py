import asyncio
import os
import signal

from app.core.logger import get_logger, setup_logging
from app.queues.heapq import HeapQueue
from scheduler.scheduler import FlintScheduler
from worker.aging import AgingProcess
from worker.worker import run_worker

setup_logging()
logger = get_logger(__name__)


async def main() -> None:
    worker_count = int(os.getenv("WORKER_COUNT", "2"))
    queue = HeapQueue()

    logger.info(
        "flint_starting",
        worker_count=worker_count,
        queue_type="HeapQueue",
    )

    scheduler = FlintScheduler(queue=queue)
    aging = AgingProcess()

    shutdown_event = asyncio.Event()

    def _handle_signal():
        logger.info("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    tasks = [
        asyncio.create_task(scheduler.run(shutdown_event), name="scheduler"),
        asyncio.create_task(
            _aging_loop(aging, queue, shutdown_event),
            name="aging",
        ),
    ]

    for i in range(worker_count):
        tasks.append(
            asyncio.create_task(
                run_worker(
                    worker_id=f"worker-{i + 1}",
                    queue=queue,
                    shutdown_event=shutdown_event,
                ),
                name=f"worker-{i + 1}",
            )
        )

    logger.info("all_tasks_started", task_count=len(tasks))

    await shutdown_event.wait()

    logger.info("shutting_down")

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("flint_stopped")


async def _aging_loop(
    aging: AgingProcess,
    queue: HeapQueue,
    shutdown_event: asyncio.Event,
) -> None:
    from app.core.config import settings
    from app.db.session import AsyncSessionLocal

    while not shutdown_event.is_set():
        try:
            async with AsyncSessionLocal() as session:
                await aging.run(session, queue)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("aging_loop_error", error=str(exc))

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.AGING_INTERVAL,
            )
        except TimeoutError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
