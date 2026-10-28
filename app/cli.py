import argparse
import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

PACKAGE_ROOT = Path(__file__).resolve().parent.parent


def _find_project_root() -> Path:
    """
    Locate the directory containing alembic.ini.

    The console script is installed into the virtualenv, so its module path is
    *not* the repository checkout. The working directory is used first (the
    Docker image and the usual local invocation both run from the project
    root), then parents, then the installed package as a fallback.
    """
    candidates = [Path.cwd(), *Path.cwd().parents, PACKAGE_ROOT]
    for candidate in candidates:
        if (candidate / "alembic.ini").is_file() and (candidate / "alembic").is_dir():
            return candidate
    return PACKAGE_ROOT


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flint",
        description="Distributed job scheduling service.",
    )
    parser.add_argument("--version", action="version", version="flint 0.2.0")
    subparsers = parser.add_subparsers(dest="command", required=True)

    api = subparsers.add_parser("api", help="Run the HTTP API.")
    api.add_argument("--host", default="0.0.0.0")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument("--reload", action="store_true", help="Reload on code changes.")
    api.add_argument(
        "--http-workers",
        type=int,
        default=1,
        help="Number of API worker processes (ignored with --reload).",
    )

    worker = subparsers.add_parser("worker", help="Run a job worker.")
    worker.add_argument("--worker-id", default=None)
    worker.add_argument("--concurrency", type=int, default=None)
    worker.add_argument(
        "--handlers",
        default=None,
        help="Comma-separated capability filter, e.g. send_email,webhook_delivery.",
    )

    subparsers.add_parser("migrate", help="Apply database migrations.")

    handlers = subparsers.add_parser("handlers", help="List registered handlers.")
    handlers.add_argument("--json", action="store_true", help="Emit JSON.")

    schedule = subparsers.add_parser("schedule", help="Enqueue a job via the DB.")
    schedule.add_argument("--type", required=True, dest="job_type")
    schedule.add_argument("--payload", default="{}", help="JSON object payload.")
    schedule.add_argument("--priority", type=int, choices=[1, 2, 3], default=2)
    schedule.add_argument("--at", default=None, help="ISO 8601 scheduled time.")
    schedule.add_argument("--interval", default=None, help="e.g. 5m, 1h.")
    schedule.add_argument("--max-retries", type=int, default=3)

    return parser


def _run_api(args: argparse.Namespace) -> int:
    import uvicorn

    if args.reload:
        uvicorn.run(
            "app.main:app",
            host=args.host,
            port=args.port,
            reload=True,
            log_config=None,
        )
    else:
        uvicorn.run(
            "app.main:app",
            host=args.host,
            port=args.port,
            workers=max(1, args.http_workers),
            log_config=None,
        )
    return 0


def _run_worker(args: argparse.Namespace) -> int:
    from app.core.logger import setup_logging
    from app.handlers import load_handlers
    from worker.worker import run_worker_process

    setup_logging()
    load_handlers()

    handlers = (
        [item.strip() for item in args.handlers.split(",") if item.strip()]
        if args.handlers
        else None
    )

    async def _main() -> None:
        shutdown = asyncio.Event()

        def _stop() -> None:
            shutdown.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _stop)

        await run_worker_process(
            worker_id=args.worker_id,
            concurrency=args.concurrency,
            handlers=handlers,
            shutdown_event=shutdown,
        )

    asyncio.run(_main())
    return 0


def _run_migrate() -> int:
    from alembic.config import Config

    from alembic import command

    root = _find_project_root()
    previous = Path.cwd()
    try:
        os.chdir(root)
        config = Config(str(root / "alembic.ini"))
        config.set_main_option("script_location", str(root / "alembic"))
        command.upgrade(config, "head")
    except Exception as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1
    finally:
        os.chdir(previous)
    return 0


def _run_handlers(as_json: bool) -> int:
    from app.handlers import list_specs, load_handlers

    load_handlers()
    specs = list_specs()
    if as_json:
        print(json.dumps([spec.to_dict() for spec in specs], indent=2))
        return 0

    if not specs:
        print("No handlers registered.")
        return 1
    for spec in specs:
        tags = f" [{', '.join(spec.tags)}]" if spec.tags else ""
        print(f"{spec.name}{tags} — {spec.description}")
    return 0


def _run_schedule(args: argparse.Namespace) -> int:
    from datetime import datetime

    from app.core.exceptions import FlintException
    from app.core.logger import setup_logging
    from app.db.session import AsyncSessionLocal, engine
    from app.handlers import load_handlers
    from app.schemas.job import JobCreate
    from app.services.job import create_job

    setup_logging()
    load_handlers()

    try:
        payload: dict[str, Any] = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON payload: {exc}", file=sys.stderr)
        return 2

    scheduled_at = None
    if args.at:
        try:
            scheduled_at = datetime.fromisoformat(args.at.replace("Z", "+00:00"))
        except ValueError as exc:
            print(f"Invalid --at value: {exc}", file=sys.stderr)
            return 2

    try:
        body = JobCreate(
            type=args.job_type,
            payload=payload,
            priority=args.priority,
            scheduled_at=scheduled_at,
            interval=args.interval,
            max_retries=args.max_retries,
        )
    except Exception as exc:
        print(f"Invalid job: {exc}", file=sys.stderr)
        return 2

    async def _create() -> str:
        try:
            async with AsyncSessionLocal() as session:
                job = await create_job(body, session)
                return str(job.id)
        finally:
            await engine.dispose()

    try:
        job_id = asyncio.run(_create())
    except FlintException as exc:
        print(f"Invalid job: {exc.message}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Could not create job: {exc}", file=sys.stderr)
        return 1
    print(job_id)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    from app.core.logger import setup_logging

    setup_logging()

    if args.command == "api":
        return _run_api(args)
    if args.command == "worker":
        return _run_worker(args)
    if args.command == "migrate":
        return _run_migrate()
    if args.command == "handlers":
        return _run_handlers(args.json)
    if args.command == "schedule":
        return _run_schedule(args)

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
