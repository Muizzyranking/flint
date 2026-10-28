import time
from typing import Any

import httpx
from pydantic import BaseModel, Field, field_validator

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.registry import register_handler
from app.handlers.utils import assert_safe_outbound_url, validate_outbound_url_syntax

logger = get_logger(__name__)

PING_TIMEOUT_SECONDS = 10


class HttpPingPayload(BaseModel):
    url: str = Field(description="Public HTTP(S) endpoint to request (GET).")
    expected_status: int = Field(
        default=200,
        ge=100,
        le=599,
        description="Status code the response must match, otherwise the job fails.",
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        value = value.strip()
        validate_outbound_url_syntax(value)
        return value


@register_handler(
    "http_ping",
    description=(
        "Request a public HTTP(S) endpoint and verify its status code. "
        "Pair it with a recurring interval for cheap uptime monitoring."
    ),
    tags=("http", "monitoring"),
)
class HttpPingHandler(BaseHandler[HttpPingPayload]):
    payload_model = HttpPingPayload

    async def execute(self, payload: HttpPingPayload) -> dict[str, Any]:
        url = payload.url

        await assert_safe_outbound_url(url)

        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(PING_TIMEOUT_SECONDS),
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.get(url)
        except httpx.TimeoutException as exc:
            raise RuntimeError(
                f"Ping to {url} timed out after {PING_TIMEOUT_SECONDS}s. Detail: {exc}"
            ) from exc
        except httpx.RequestError as exc:
            raise RuntimeError(f"Ping connection error to {url}: {exc}") from exc

        elapsed_ms = int((time.perf_counter() - started) * 1000)

        if 300 <= response.status_code < 400:
            raise RuntimeError(
                f"Ping to {url} returned a redirect ({response.status_code}); "
                "redirects are not followed for security reasons."
            )
        if response.status_code != payload.expected_status:
            raise RuntimeError(
                f"Ping to {url} returned HTTP {response.status_code}, "
                f"expected {payload.expected_status}."
            )

        logger.info(
            "http_ping_complete",
            url=url,
            status_code=response.status_code,
            elapsed_ms=elapsed_ms,
        )

        return {
            "url": url,
            "status_code": response.status_code,
            "expected_status": payload.expected_status,
            "elapsed_ms": elapsed_ms,
        }
