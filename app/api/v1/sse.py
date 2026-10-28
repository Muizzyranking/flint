import asyncio

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from app.core.logger import get_logger
from app.core.security import verify_sse_api_key
from app.db.redis import EVENTS_CHANNEL, get_redis

logger = get_logger(__name__)

router = APIRouter()
KEEPALIVE_SECONDS = 15.0


async def _event_generator(request: Request):
    redis_client = await get_redis()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(EVENTS_CHANNEL)

    logger.info("sse_client_connected", path=str(request.url.path))
    try:
        while True:
            if await request.is_disconnected():
                break

            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=KEEPALIVE_SECONDS,
            )
            if message is None:
                # Comment frame keeps proxies and browsers from closing idle
                # connections while also giving us a chance to notice
                # disconnects.
                yield ": keepalive\n\n"
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
        try:
            await pubsub.unsubscribe(EVENTS_CHANNEL)
            await pubsub.aclose()
        except Exception:
            pass
        logger.info("sse_client_disconnected")


@router.get(
    "/stream",
    summary="Live job event stream",
    description=(
        "Server-Sent Events stream of job status changes. EventSource cannot "
        "set custom headers, so pass `?api_key=...` when using it from the "
        "browser."
    ),
    response_class=StreamingResponse,
    dependencies=[Depends(verify_sse_api_key)],
)
async def sse_stream(request: Request):
    return StreamingResponse(
        _event_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
