from typing import Annotated

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.core.exceptions import WorkerNotFoundException
from app.db.redis import get_redis
from app.schemas.response import ApiResponse

router = APIRouter()

WORKER_KEY_PATTERN = "flint:workers:*"
WORKER_CONTROL_CHANNEL = "flint:worker:control:{worker_id}"


@router.get(
    "",
    summary="List active workers",
    response_model=ApiResponse[list[dict]],
)
async def list_workers(redis: Annotated[Redis, Depends(get_redis)]):
    """
    Returns all workers currently registered in Redis.
    Workers that have died without deregistering will disappear
    automatically once their TTL (60s) expires.
    """
    keys = await redis.keys(WORKER_KEY_PATTERN)

    workers = []
    for key in keys:
        worker_id = key.replace("flint:workers:", "")  # type: ignore
        status = await redis.get(key) or "unknown"
        ttl = await redis.ttl(key)
        workers.append(
            {
                "worker_id": worker_id,
                "status": status,
                "ttl_seconds": ttl,
            }
        )
    return ApiResponse[list[dict]](
        message="Workers retrieved successfully.",
        data=workers,
    )


@router.post(
    "/{worker_id}/stop",
    summary="Signal a worker to stop",
    description=(
        "Publishes a 'stop' command to the worker's control channel. "
        "The worker finishes its current job then exits cleanly."
    ),
)
async def stop_worker(worker_id: str, redis: Annotated[Redis, Depends(get_redis)]):
    await _assert_worker_exists(worker_id, redis)
    channel = WORKER_CONTROL_CHANNEL.format(worker_id=worker_id)
    await redis.publish(channel, "stop")
    return ApiResponse[dict](
        message=f"Stop signal sent to worker '{worker_id}'.",
        data={"worker_id": worker_id, "command": "stop"},
    )


@router.post(
    "/{worker_id}/restart",
    summary="Signal a worker to restart",
    description=(
        "Publishes a 'restart' command to the worker's control channel. "
        "The worker will re-exec itself after finishing its current job."
    ),
)
async def restart_worker(worker_id: str, redis: Annotated[Redis, Depends(get_redis)]):
    await _assert_worker_exists(worker_id, redis)
    channel = WORKER_CONTROL_CHANNEL.format(worker_id=worker_id)
    await redis.publish(channel, "restart")
    return ApiResponse[dict](
        message=f"Restart signal sent to worker '{worker_id}'.",
        data={"worker_id": worker_id, "command": "restart"},
    )


async def _assert_worker_exists(worker_id: str, redis: Redis) -> None:
    """Raise WorkerNotFoundException if the worker is not in Redis."""
    key = f"flint:workers:{worker_id}"
    exists = await redis.exists(key)
    if not exists:
        raise WorkerNotFoundException(worker_id)
