"""Core email abstractions shared by every sender in Flint."""

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class EmailMessage:
    """A single outgoing email, transport-agnostic."""

    to: str
    subject: str
    body: str = ""
    html: str | None = None


class EmailSender(Protocol):
    """
    Transport contract.

    Implementations must attempt delivery when called and return a result
    dict (``to``/``subject``/``delivered``). Failures must **raise** — the
    `send_email` handler relies on that to trigger retries/DLQ, while the
    DLQ alert path catches exceptions per recipient.
    """

    async def send(self, message: EmailMessage) -> dict[str, Any]: ...
