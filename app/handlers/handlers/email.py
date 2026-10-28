from typing import Any

from pydantic import BaseModel, EmailStr, Field, field_validator

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.registry import register_handler

logger = get_logger(__name__)

MAX_SUBJECT_LENGTH = 200
MAX_BODY_BYTES = 512 * 1024


class EmailPayload(BaseModel):
    to: EmailStr = Field(description="Recipient email address.", max_length=320)
    subject: str = Field(description="Email subject.")
    body: str | None = Field(default="", description="Plain text body.")
    html: str | None = Field(default=None, description="Optional HTML body.")

    @field_validator("subject")
    @classmethod
    def validate_subject(cls, value: str) -> str:
        value = value.strip()
        if "\r" in value or "\n" in value:
            raise ValueError("'subject' must not contain line breaks.")
        if not value:
            raise ValueError("'subject' must not be empty.")
        if len(value) > MAX_SUBJECT_LENGTH:
            raise ValueError(f"'subject' must be at most {MAX_SUBJECT_LENGTH} chars.")
        return value

    @field_validator("body", "html")
    @classmethod
    def validate_body_size(cls, value: str | None) -> str | None:
        if value is not None and len(value.encode("utf-8")) > MAX_BODY_BYTES:
            raise ValueError(f"email body exceeds {MAX_BODY_BYTES} bytes.")
        return value


@register_handler(
    "send_email",
    description="Send a transactional email.",
    tags=("email", "notifications"),
)
class EmailHandler(BaseHandler[EmailPayload]):
    payload_model = EmailPayload

    async def execute(self, payload: EmailPayload) -> dict[str, Any]:
        from app.core.email import EmailMessage, send_email

        to = str(payload.to)
        subject = payload.subject

        logger.info(
            "email_attempt",
            to=to,
            subject=subject,
            has_html=bool(payload.html),
        )

        result = await send_email(
            EmailMessage(
                to=to,
                subject=subject,
                body=payload.body or "",
                html=payload.html,
            )
        )

        logger.info("email_success", to=to, subject=subject)
        return result
