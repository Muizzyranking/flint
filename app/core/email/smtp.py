"""SMTP transport — the default EmailSender backend."""

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import aiosmtplib

from app.core.config import settings
from app.core.email.base import EmailMessage
from app.core.logger import get_logger

logger = get_logger(__name__)


class SmtpEmailSender:
    """
    Deliver mail over the SMTP relay configured in settings.

    Development: MailHog in Compose (`smtp` host, port 1025).
    Production: point `SMTP_HOST`/`SMTP_PORT` (and `SMTP_FROM`) at a real relay.
    """

    async def send(self, message: EmailMessage) -> dict[str, Any]:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = message.subject
        msg["From"] = settings.SMTP_FROM
        msg["To"] = message.to

        msg.attach(MIMEText(message.body, "plain", "utf-8"))
        if message.html:
            msg.attach(MIMEText(message.html, "html", "utf-8"))

        try:
            await aiosmtplib.send(
                msg,
                hostname=settings.SMTP_HOST,
                port=settings.SMTP_PORT,
                use_tls=False,
                start_tls=False,
            )
        except aiosmtplib.SMTPException as exc:
            raise RuntimeError(
                f"SMTP delivery failed to '{message.to}': {exc}"
            ) from exc
        except OSError as exc:
            raise RuntimeError(
                f"Cannot connect to SMTP server "
                f"{settings.SMTP_HOST}:{settings.SMTP_PORT} — {exc}"
            ) from exc

        return {
            "to": message.to,
            "subject": message.subject,
            "delivered": True,
        }
