import time
from typing import Any

import httpx
from pydantic import BaseModel, Field, field_validator

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.registry import register_handler
from app.handlers.utils import assert_safe_outbound_url, validate_outbound_url_syntax

logger = get_logger(__name__)

FETCH_TIMEOUT_SECONDS = 30
MAX_RESULT_BODY_BYTES = 64 * 1024


class HttpFetchPayload(BaseModel):
    url: str = Field(description="Public HTTP(S) URL to fetch (GET).")
    expected_status: int = Field(
        default=200,
        ge=100,
        le=599,
        description="Status code the response must match, otherwise the job fails.",
    )
    include_body: bool = Field(
        default=True,
        description="Include the (capped) response body in the result metadata.",
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        value = value.strip()
        validate_outbound_url_syntax(value)
        return value


@register_handler(
    "http_fetch",
    description=(
        "Fetch a public HTTP(S) endpoint and (optionally) carry its JSON body "
        "into the result metadata, for use by downstream jobs or inspection."
    ),
    tags=("http", "data"),
)
class HttpFetchHandler(BaseHandler[HttpFetchPayload]):
    payload_model = HttpFetchPayload

    async def execute(self, payload: HttpFetchPayload) -> dict[str, Any]:
        url = payload.url

        await assert_safe_outbound_url(url)

        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(FETCH_TIMEOUT_SECONDS),
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.get(url)
        except httpx.TimeoutException as exc:
            raise RuntimeError(
                f"Fetch of {url} timed out after {FETCH_TIMEOUT_SECONDS}s. "
                f"Detail: {exc}"
            ) from exc
        except httpx.RequestError as exc:
            raise RuntimeError(f"Fetch connection error to {url}: {exc}") from exc

        elapsed_ms = int((time.perf_counter() - started) * 1000)

        if 300 <= response.status_code < 400:
            raise RuntimeError(
                f"Fetch of {url} returned a redirect ({response.status_code}); "
                "redirects are not followed for security reasons."
            )
        if response.status_code != payload.expected_status:
            raise RuntimeError(
                f"Fetch of {url} returned HTTP {response.status_code}, "
                f"expected {payload.expected_status}."
            )

        result: dict[str, Any] = {
            "url": url,
            "status_code": response.status_code,
            "elapsed_ms": elapsed_ms,
            "content_type": response.headers.get("content-type"),
            "body_bytes": len(response.content),
            "truncated": False,
            "body": None,
        }

        if payload.include_body and response.content:
            content = response.content
            if len(content) > MAX_RESULT_BODY_BYTES:
                content = content[:MAX_RESULT_BODY_BYTES]
                result["truncated"] = True

            try:
                result["body"] = response.json()
            except ValueError:
                result["body"] = content.decode("utf-8", errors="replace")

        logger.info(
            "http_fetch_complete",
            url=url,
            status_code=response.status_code,
            elapsed_ms=elapsed_ms,
            body_bytes=result["body_bytes"],
        )

        return result
