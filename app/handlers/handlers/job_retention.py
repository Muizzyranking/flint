from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import delete

from app.core.logger import get_logger
from app.db.session import AsyncSessionLocal
from app.handlers.base import BaseHandler
from app.handlers.registry import register_handler
from app.models.enum import JobStatus
from app.models.job import Job

logger = get_logger(__name__)


class JobRetentionPayload(BaseModel):
    max_age_days: int = Field(
        description=(
            "Purge terminal jobs whose last update is older than this many days."
        ),
        gt=0,
        le=3650,
    )
    include_bin: bool = Field(
        default=True,
        description="Also hard-delete soft-deleted (bin) jobs older than the cutoff.",
    )


@register_handler(
    "job_retention",
    description=(
        "Housekeeping for this deployment's own database: permanently remove "
        "completed/cancelled jobs (and optionally bin items) older than "
        "max_age_days. Schedule it recurring — daily or weekly is typical."
    ),
    tags=("ops", "maintenance"),
)
class JobRetentionHandler(BaseHandler[JobRetentionPayload]):
    payload_model = JobRetentionPayload

    async def execute(self, payload: JobRetentionPayload) -> dict[str, Any]:
        cutoff = datetime.now(UTC) - timedelta(days=payload.max_age_days)

        purged_terminal = 0
        purged_bin = 0

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                delete(Job).where(
                    Job.status.in_([JobStatus.COMPLETED, JobStatus.CANCELLED]),
                    Job.updated_at < cutoff,
                )
            )
            purged_terminal = result.rowcount or 0  # type: ignore

            if payload.include_bin:
                result = await session.execute(
                    delete(Job).where(
                        Job.deleted_at.is_not(None),
                        Job.deleted_at < cutoff,
                    )
                )
                purged_bin = result.rowcount or 0  # type: ignore

            await session.commit()

        summary = {
            "cutoff": cutoff.isoformat(),
            "purged_terminal_jobs": purged_terminal,
            "purged_bin_jobs": purged_bin,
        }

        logger.info("job_retention_complete", **summary)
        return summary
