from typing import Any

from pydantic import BaseModel, Field


class HandlerInfo(BaseModel):
    name: str
    description: str
    tags: list[str]
    version: str
    payload_schema: dict[str, Any] = Field(
        description="JSON Schema describing the accepted payload."
    )


class HandlerListResponse(BaseModel):
    handlers: list[HandlerInfo]
