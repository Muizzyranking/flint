import asyncio
import signal

from app.core.logger import get_logger, setup_logging
from app.handlers import load_handlers
from worker.worker import run_worker_process

setup_logging()
logger = get_logger(__name__)


async def main() -> None:
    load_handlers()

    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    await run_worker_process(shutdown_event=shutdown_event)


if __name__ == "__main__":
    asyncio.run(main())
