from typing import Any

from app.core.email.base import EmailMessage, EmailSender
from app.core.email.smtp import SmtpEmailSender

__all__ = [
    "EmailMessage",
    "EmailSender",
    "SmtpEmailSender",
    "get_email_sender",
    "send_email",
]


def get_email_sender() -> EmailSender:
    """Single edit point for the email transport."""
    return SmtpEmailSender()


async def send_email(message: EmailMessage) -> dict[str, Any]:
    """Send one email through the configured transport."""
    return await get_email_sender().send(message)
