from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.registry import register_handler

logger = get_logger(__name__)


class EchoPayload(BaseModel):
    """Anything JSON-serialisable; extra keys are preserved and echoed."""

    model_config = ConfigDict(extra="allow")

    note: str | None = Field(default=None, description="Optional note echoed back.")


@register_handler(
    "echo",
    description=(
        "Echo the job payload back into the result metadata. Useful for "
        "smoke tests, demos and pipeline heartbeats."
    ),
    tags=("dev", "demo"),
)
class EchoHandler(BaseHandler[EchoPayload]):
    payload_model = EchoPayload

    async def execute(self, payload: EchoPayload) -> dict[str, Any]:
        echoed = payload.model_dump()

        logger.info("echo_complete", key_count=len(echoed), note=payload.note)

        return {"echoed": True, "payload": echoed}
