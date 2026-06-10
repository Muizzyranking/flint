import uuid

from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import BaseModel


class LogEvent(str):
    """
    Structured event names. Not an Enum so new events
    can be added without a migration.
    """

    JOB_CREATED = "job_created"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    JOB_CANCELLED = "job_cancelled"
    JOB_RETRY_ATTEMPTED = "job_retry_attempted"
    DLQ_THRESHOLD_REACHED = "dlq_threshold_reached"
    AGING_COMPLETE = "aging_complete"
    RECURRING_SCHEDULED = "recurring_job_scheduled"


class JobLog(BaseModel):
    __tablename__ = "job_logs"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Event name — one of the LogEvent constants
    event: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
    )

    # Human-readable message describing what happened
    message: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    # Optional structured metadata (worker_id, duration_ms, error details, etc.)
    metadata_: Mapped[dict | None] = mapped_column(
        "metadata",
        JSONB,
        nullable=True,
    )

    def __repr__(self) -> str:
        return f"<JobLog job={str(self.job_id)[:8]} event={self.event}>"
