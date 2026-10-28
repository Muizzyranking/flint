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
