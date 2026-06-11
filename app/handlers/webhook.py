"""
Payload shape:
    {
        "url":     "https://webhook.site/abc",   # required
        "method":  "POST",                        # optional, default POST
        "headers": {"X-Custom": "value"},         # optional
        "body":    {"event": "user.created"}      # optional
    }

Result shape (on success):
    {
        "status_code": 200,
        "response_body": "...",
        "url": "https://...",
        "method": "POST"
    }
"""

from typing import Any

import httpx
from pydantic import BaseModel, field_validator

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.utils import parse_payload

logger = get_logger(__name__)

TIMEOUT_SECONDS = 10
MAX_REDIRECTS = 3
ALLOWED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


class WebhookPayload(BaseModel):
    url: str
    method: str | None = "POST"
    headers: dict[str, str] = {}
    body: dict[str, Any] = {}

    @field_validator("method")
    @classmethod
    def validate_method(cls, v):
        if v is None:
            return "POST"

        if v.upper() not in ALLOWED_METHODS:
            raise ValueError(
                f"Invalid HTTP method '{v}'. "
                f"Must be one of: {', '.join(sorted(ALLOWED_METHODS))}."
            )
        return v.upper()


class WebhookHandler(BaseHandler):
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        p = parse_payload(payload, WebhookPayload)
        url = p.url
        method = p.method or "POST"
        headers = p.headers or {}
        body = p.body or {}

        logger.info(
            "webhook_attempt",
            url=url,
            method=method,
            body_keys=list(body.keys()),
        )

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(TIMEOUT_SECONDS),
                follow_redirects=True,
                max_redirects=MAX_REDIRECTS,
            ) as client:
                response = await client.request(
                    method=method,
                    url=url,
                    json=body if body else None,
                    headers=headers,
                )
        except httpx.TimeoutException as exc:
            raise Exception(
                f"Webhook timed out after {TIMEOUT_SECONDS}s: {url}. Detail: {str(exc)}"
            ) from exc
        except httpx.TooManyRedirects as exc:
            raise Exception(
                f"Webhook exceeded max redirects ({MAX_REDIRECTS}): {url}."
            ) from exc
        except httpx.RequestError as exc:
            raise Exception(f"Webhook connection error to {url}: {str(exc)}") from exc

        if response.status_code >= 400:
            raise Exception(
                f"Webhook failed: HTTP {response.status_code} from {url}. "
                f"Body: {response.text[:500]}"
            )

        result = {
            "status_code": response.status_code,
            "response_body": response.text[:1000],
            "url": url,
            "method": method,
        }

        logger.info(
            "webhook_success",
            url=url,
            status_code=response.status_code,
        )

        return result
