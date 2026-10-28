import json
from typing import Any

import httpx
from pydantic import BaseModel, Field, field_validator

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.registry import register_handler
from app.handlers.utils import assert_safe_outbound_url, validate_outbound_url_syntax

logger = get_logger(__name__)

TIMEOUT_SECONDS = 10
MAX_RESPONSE_BYTES = 256 * 1024
MAX_REQUEST_BYTES = 256 * 1024
ALLOWED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}
ALLOWED_HEADERS = {
    "accept",
    "content-type",
    "user-agent",
    "x-request-id",
    "x-correlation-id",
}


class WebhookPayload(BaseModel):
    url: str = Field(description="Public HTTP(S) URL to call.")
    method: str | None = Field(default="POST", description="HTTP method.")
    headers: dict[str, str] = Field(
        default_factory=dict, description="Optional request headers."
    )
    body: dict[str, Any] = Field(
        default_factory=dict, description="Optional JSON request body."
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        value = value.strip()
        validate_outbound_url_syntax(value)
        return value

    @field_validator("method")
    @classmethod
    def validate_method(cls, value: str | None) -> str:
        value = (value or "POST").upper()
        if value not in ALLOWED_METHODS:
            raise ValueError(
                f"Invalid HTTP method '{value}'. "
                f"Must be one of: {', '.join(sorted(ALLOWED_METHODS))}."
            )
        return value

    @field_validator("headers")
    @classmethod
    def validate_headers(cls, value: dict[str, str]) -> dict[str, str]:
        if len(value) > 20:
            raise ValueError("At most 20 custom headers are allowed.")
        for key in value:
            if key.lower() not in ALLOWED_HEADERS:
                raise ValueError(
                    f"Header '{key}' is not allowed. "
                    f"Allowed headers: {', '.join(sorted(ALLOWED_HEADERS))}."
                )
        return value


@register_handler(
    "webhook_delivery",
    description="Deliver a JSON payload to a public HTTP(S) endpoint.",
    tags=("http", "integration"),
)
class WebhookHandler(BaseHandler[WebhookPayload]):
    payload_model = WebhookPayload

    async def execute(self, payload: WebhookPayload) -> dict[str, Any]:
        url = payload.url
        method = payload.method or "POST"
        headers = payload.headers or {}
        body = payload.body or {}

        await assert_safe_outbound_url(url)

        body_bytes = len(
            json.dumps(body, separators=(",", ":"), default=str).encode("utf-8")
        )
        if body_bytes > MAX_REQUEST_BYTES:
            raise ValueError(f"Webhook body exceeds {MAX_REQUEST_BYTES} bytes.")

        logger.info(
            "webhook_attempt",
            url=url,
            method=method,
            body_keys=list(body.keys()),
        )

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(TIMEOUT_SECONDS),
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.request(
                    method=method,
                    url=url,
                    json=body if body else None,
                    headers=headers,
                )
        except httpx.TimeoutException as exc:
            raise RuntimeError(
                f"Webhook timed out after {TIMEOUT_SECONDS}s: {url}. Detail: {exc}"
            ) from exc
        except httpx.RequestError as exc:
            raise RuntimeError(f"Webhook connection error to {url}: {exc}") from exc

        if 300 <= response.status_code < 400:
            raise RuntimeError(
                f"Webhook returned a redirect ({response.status_code}) from {url}; "
                "redirects are not followed for security reasons."
            )

        if response.status_code >= 400:
            raise RuntimeError(
                f"Webhook failed: HTTP {response.status_code} from {url}. "
                f"Body: {response.text[:500]}"
            )

        content = response.content
        if len(content) > MAX_RESPONSE_BYTES:
            content = content[:MAX_RESPONSE_BYTES]

        result = {
            "status_code": response.status_code,
            "response_body": content.decode("utf-8", errors="replace"),
            "url": url,
            "method": method,
        }

        logger.info(
            "webhook_success",
            url=url,
            status_code=response.status_code,
        )

        return result
