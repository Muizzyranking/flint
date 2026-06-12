import asyncio

import redis.asyncio as aioredis
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

router = APIRouter()

EVENTS_CHANNEL = "flint:events"


async def _event_generator(request: Request):
    """
    Async generator that yields SSE-formatted strings.
    """
    redis_client = aioredis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_timeout=None,
        socket_connect_timeout=5,
    )
    pubsub = redis_client.pubsub()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(EVENTS_CHANNEL)

    logger.info("sse_client_connected", path=str(request.url))

    try:
        async for message in pubsub.listen():
            if await request.is_disconnected():
                break

            if message["type"] != "message":
                continue

            data = message.get("data", "")
            if isinstance(data, bytes):
                data = data.decode("utf-8")

            yield f"data: {data}\n\n"

    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.error("sse_stream_error", error=str(exc))
    finally:
        await pubsub.unsubscribe(EVENTS_CHANNEL)
        await pubsub.aclose()
        logger.info("sse_client_disconnected")


@router.get(
    "/stream",
    summary="Live job event stream",
    description=(
        "Server-Sent Events stream. Connect with EventSource to receive "
        "real-time job status updates. No API key required (EventSource "
        "cannot set custom headers)."
    ),
    response_class=StreamingResponse,
    tags=["SSE"],
)
async def sse_stream(request: Request):
    return StreamingResponse(
        _event_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        },
    )
