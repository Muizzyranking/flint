from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class LogEntryResponse(BaseModel):
    id: UUID
    job_id: UUID
    event: str
    message: str
    metadata_: dict[str, Any] | None = Field(None, serialization_alias="metadata")
    created_at: datetime

    model_config = {
        "from_attributes": True,
        "populate_by_name": True,
    }


class LogFilterParams(BaseModel):
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=20, ge=1, le=100)
    event: str | None = Field(
        default=None,
        description="Filter by event name e.g. 'job_completed'.",
    )
    job_id: UUID | None = Field(
        default=None,
        description="Filter logs for a specific job.",
    )
