import uuid

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import BaseModel


class JobDependency(BaseModel):
    __tablename__ = "job_dependencies"
    __table_args__ = (
        UniqueConstraint(
            "job_id",
            "depends_on_id",
            name="uq_job_dependency",
        ),
    )

    # The job that is waiting
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # The job that must complete first
    depends_on_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    def __repr__(self) -> str:
        return (
            f"<JobDependency job={str(self.job_id)[:8]} "
            f"depends_on={str(self.depends_on_id)[:8]}>"
        )
