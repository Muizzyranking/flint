import json
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio import Redis

from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

EVENTS_CHANNEL = "flint:events"

_redis_client: Redis | None = None


async def get_redis() -> Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            encoding="utf-8",
        )
    return _redis_client


async def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None


async def publish_event(event: dict[str, Any]) -> None:
    try:
        client = await get_redis()
        await client.publish(EVENTS_CHANNEL, json.dumps(event, default=str))
    except Exception as exc:
        logger.error("sse_publish_error", error=str(exc))
