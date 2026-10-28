from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Index,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import BaseModel
from app.models.enum import JobPriority, JobStatus


class Job(BaseModel):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint("priority IN (1, 2, 3)", name="ck_jobs_priority"),
        CheckConstraint(
            "status IN ('pending','processing','completed','failed','cancelled')",
            name="ck_jobs_status",
        ),
        UniqueConstraint(
            "recurrence_id",
            "recurrence_run",
            name="uq_jobs_recurrence_run",
        ),
        Index(
            "idx_jobs_dispatch",
            "status",
            "available_at",
            "effective_priority",
            "created_at",
            postgresql_where=text("deleted_at IS NULL AND is_dlq = false"),
        ),
        Index("idx_jobs_lease_expires_at", "lease_expires_at"),
        Index("idx_jobs_recurrence_id", "recurrence_id"),
    )

    # Core fields
    type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
    )
    payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
    )
    priority: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
        default=JobPriority.MEDIUM,
        index=True,
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=JobStatus.PENDING,
        index=True,
    )

    # Scheduling
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )
    #: The instant the job becomes eligible for claiming. For a new job this
    #: equals ``scheduled_at``; for a retry it is ``now + backoff``. Dispatch
    #: must only use ``available_at`` so retries cannot bypass their backoff.
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    interval_seconds: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )

    # Retry tracking
    retry_count: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
        default=0,
    )
    max_retries: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
        default=3,
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    # Priority queue scoring (used by the DB dispatcher + local heap)
    effective_priority: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=float(JobPriority.MEDIUM),
        index=True,
    )

    # Cooperative cancellation flag
    cancellation_requested: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )

    # Worker tracking / leases
    worker_id: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Recurring chain identity. Multiple workers may race to enqueue the next
    # run; the unique constraint makes the operation idempotent.
    recurrence_id: Mapped[UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )
    recurrence_run: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
        default=0,
        server_default="0",
    )

    # Dead letter queue flag
    is_dlq: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        index=True,
    )

    # Soft delete
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )

    def __repr__(self) -> str:
        return (
            f"<Job id={str(self.id)[:8]} type={self.type} "
            f"status={self.status} priority={self.priority}>"
        )

    @property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        )

    @property
    def is_cancellable(self) -> bool:
        return self.status in (
            JobStatus.PENDING,
            JobStatus.PROCESSING,
        )
