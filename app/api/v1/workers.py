import json
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.core.exceptions import WorkerNotFoundException
from app.db.redis import get_redis
from app.schemas.response import ApiResponse

router = APIRouter()

WORKER_KEY_PREFIX = "flint:workers:"
WORKER_KEY_PATTERN = f"{WORKER_KEY_PREFIX}*"
WORKER_CONTROL_CHANNEL = "flint:worker:control:{worker_id}"


@router.get(
    "",
    summary="List active workers",
    response_model=ApiResponse[list[dict]],
)
async def list_workers(redis: Annotated[Redis, Depends(get_redis)]):
    """Return workers currently registered in Redis.

    Presence is best-effort and expires automatically if a worker dies.
    Worker IDs include a host/pid/random suffix, so multiple containers can
    share a Redis instance without colliding.
    """
    keys = await redis.keys(WORKER_KEY_PATTERN)

    workers: list[dict[str, Any]] = []
    for key in keys:
        # redis-py is configured with decode_responses=True, but its typing
        # allows bytes keys, so decode defensively before stripping.
        key_text = key.decode() if isinstance(key, bytes) else str(key)
        worker_id = key_text.removeprefix(WORKER_KEY_PREFIX)
        raw = await redis.get(key)
        ttl = await redis.ttl(key)
        status: Any = raw or "unknown"
        capabilities: list[str] = []
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    status = parsed.get("status", "unknown")
                    capabilities = list(parsed.get("capabilities", []))
            except (json.JSONDecodeError, TypeError):
                pass
        workers.append(
            {
                "worker_id": worker_id,
                "status": status,
                "capabilities": capabilities,
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
        "The worker stops accepting work and exits so the orchestrator can "
        "replace it."
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
    key = f"{WORKER_KEY_PREFIX}{worker_id}"
    if not await redis.exists(key):
        raise WorkerNotFoundException(worker_id)
