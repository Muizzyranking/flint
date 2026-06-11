import redis.asyncio as aioredis
from redis.asyncio import Redis

from app.core.config import settings

_redis_client: Redis | None = None


async def get_redis() -> Redis:
    """
    Returns the shared async Redis client.
    Creates the client on first call using REDIS_URL from settings.
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            encoding="utf-8",
        )
    return _redis_client


async def close_redis() -> None:
    """
    Cleanly closes the Redis connection pool.
    """
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
