import re
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.models.enum import JobType
from app.models.job import JobPriority, JobStatus

INTERVAL_MULTIPLIERS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "mo": 2592000,
    "y": 31536000,
}

INTERVAL_PATTERN = re.compile(r"^(\d+)(s|mo|m|h|d|y)$")


def parse_interval(interval_str: str) -> int:
    """
    Parse a flexible interval string into total seconds.

    Accepted formats:
        "30s"  → 30
        "5m"   → 300
        "2h"   → 7200
        "1d"   → 86400
        "1mo"  → 2592000
        "1y"   → 31536000
    """
    match = INTERVAL_PATTERN.match(interval_str.strip().lower())
    if not match:
        raise ValueError(
            f"Invalid interval '{interval_str}'. "
            "Use <number><unit> where unit is one of: s, m, h, d, mo, y. "
            "Examples: '30s', '5m', '2h', '1d', '1mo', '1y'."
        )
    value = int(match.group(1))
    unit = match.group(2)
    if value <= 0:
        raise ValueError("Interval value must be greater than 0.")
    return value * INTERVAL_MULTIPLIERS[unit]


def format_interval_seconds(seconds: int) -> str:
    """
    Convert interval_seconds back to a human-readable string.
    """
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    if seconds < 2592000:
        return f"{seconds // 86400}d"
    if seconds < 31536000:
        return f"{seconds // 2592000}mo"
    return f"{seconds // 31536000}y"


class JobCreate(BaseModel):
    type: JobType
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: JobPriority = JobPriority.MEDIUM
    scheduled_at: datetime | None = Field(
        default=None,
        description="ISO 8601 datetime. Defaults to now if not provided.",
    )
    interval: str | None = Field(
        default=None,
        description="Recurring interval e.g. '5m', '1h'. Null means non-recurring.",
    )
    dependency_ids: list[UUID] | None = Field(
        default=None,
        description="List of job IDs that must complete before this job runs.",
    )
    max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum retry attempts before the job moves to DLQ.",
    )

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: str | None) -> str | None:
        if v is not None:
            parse_interval(v)
        return v

    @field_validator("dependency_ids")
    @classmethod
    def validate_dependency_ids(cls, v: list[UUID] | None) -> list[UUID] | None:
        if v is not None and len(v) != len(set(v)):
            raise ValueError("dependency_ids must not contain duplicates.")
        return v

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "type": "webhook_delivery",
                    "payload": {
                        "url": "https://webhook.site/abc",
                        "body": {"event": "user.created"},
                    },
                    "priority": 1,
                    "scheduled_at": "2026-06-10T10:00:00Z",
                    "interval": "1h",
                    "dependency_ids": [],
                    "max_retries": 3,
                }
            ]
        }
    }


class JobLogResponse(BaseModel):
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


class JobResponse(BaseModel):
    id: UUID
    type: str
    payload: dict[str, Any]
    priority: int
    status: str
    scheduled_at: datetime
    interval_seconds: int | None
    retry_count: int
    max_retries: int
    last_error: str | None
    effective_priority: float
    cancellation_requested: bool
    worker_id: str | None
    started_at: datetime | None
    completed_at: datetime | None
    is_dlq: bool
    created_at: datetime
    updated_at: datetime

    # Populated on detail endpoint only
    dependencies: list[UUID] | None = None
    logs: list[JobLogResponse] | None = None

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    jobs: list[JobResponse]


class JobFilterParams(BaseModel):
    page: int = Field(default=1, ge=1, description="Page number (1-indexed).")
    limit: int = Field(default=20, ge=1, le=100, description="Items per page.")
    status: JobStatus | None = Field(default=None, description="Filter by status.")
    type: JobType | None = Field(default=None, description="Filter by job type.")
    priority: JobPriority | None = Field(
        default=None, description="Filter by priority."
    )
    search: str | None = Field(
        default=None,
        description="Search across job ID (partial) and type.",
    )
