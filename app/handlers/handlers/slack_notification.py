from typing import Any

import httpx
from pydantic import BaseModel, Field, field_validator

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.registry import register_handler
from app.handlers.utils import assert_safe_outbound_url, validate_outbound_url_syntax

logger = get_logger(__name__)

MAX_TEXT_LENGTH = 4000
TIMEOUT_SECONDS = 10


class SlackNotificationPayload(BaseModel):
    webhook_url: str = Field(
        description="Slack-compatible incoming webhook URL (e.g. "
        "https://hooks.slack.com/services/...)."
    )
    text: str = Field(description="Message text to post.")

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: str) -> str:
        value = value.strip()
        validate_outbound_url_syntax(value)
        return value

    @field_validator("text")
    @classmethod
    def validate_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("'text' must not be empty.")
        if len(value) > MAX_TEXT_LENGTH:
            raise ValueError(f"'text' must be at most {MAX_TEXT_LENGTH} characters.")
        return value


@register_handler(
    "slack_notification",
    description=(
        "Post a message to a Slack-compatible incoming webhook. Reliable "
        "operator notifications, DLQ alerts, deploy pings."
    ),
    tags=("notification", "chat", "http"),
)
class SlackNotificationHandler(BaseHandler[SlackNotificationPayload]):
    payload_model = SlackNotificationPayload

    async def execute(self, payload: SlackNotificationPayload) -> dict[str, Any]:
        url = payload.webhook_url

        await assert_safe_outbound_url(url)

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(TIMEOUT_SECONDS),
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.post(url, json={"text": payload.text})
        except httpx.TimeoutException as exc:
            raise RuntimeError(
                f"Slack webhook timed out after {TIMEOUT_SECONDS}s: {url}. "
                f"Detail: {exc}"
            ) from exc
        except httpx.RequestError as exc:
            raise RuntimeError(
                f"Slack webhook connection error to {url}: {exc}"
            ) from exc

        if response.status_code != 200:
            raise RuntimeError(
                f"Slack webhook returned HTTP {response.status_code}: "
                f"{response.text[:200]}"
            )

        logger.info(
            "slack_notification_sent",
            text_length=len(payload.text),
            status_code=response.status_code,
        )

        return {"delivered": True, "text_length": len(payload.text)}
