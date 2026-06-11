"""
Payload shape:
    {
        "to":      "user@example.com",    # required
        "subject": "Welcome to Flint",    # required
        "body":    "Plain text body",     # optional
        "html":    "<p>HTML body</p>"     # optional
    }

Result shape (on success):
    {
        "to": "user@example.com",
        "subject": "Welcome to Flint",
        "delivered": true
    }
"""

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import aiosmtplib
from pydantic import BaseModel

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.utils import parse_payload

logger = get_logger(__name__)


class EmailPayload(BaseModel):
    to: str
    subject: str
    body: str | None = ""
    html: str | None = None


class EmailHandler(BaseHandler):
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        from app.core.config import settings

        p = parse_payload(payload, EmailPayload)

        to = p.to
        subject = p.subject

        body = p.body or ""
        html = p.html

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = settings.SMTP_FROM
        msg["To"] = to

        msg.attach(MIMEText(body, "plain", "utf-8"))

        if html:
            msg.attach(MIMEText(html, "html", "utf-8"))

        logger.info(
            "email_attempt",
            to=to,
            subject=subject,
            has_html=bool(html),
            smtp_host=settings.SMTP_HOST,
            smtp_port=settings.SMTP_PORT,
        )

        try:
            await aiosmtplib.send(
                msg,
                hostname=settings.SMTP_HOST,
                port=settings.SMTP_PORT,
                use_tls=False,
                start_tls=False,
            )
        except aiosmtplib.SMTPException as exc:
            raise Exception(f"SMTP delivery failed to '{to}': {str(exc)}") from exc
        except OSError as exc:
            raise Exception(
                f"Cannot connect to SMTP server "
                f"{settings.SMTP_HOST}:{settings.SMTP_PORT} — {str(exc)}"
            ) from exc

        result = {
            "to": to,
            "subject": subject,
            "delivered": True,
        }

        logger.info("email_success", to=to, subject=subject)

        return result
