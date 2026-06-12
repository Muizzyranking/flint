from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from app.dependencies import DBSession, PaginationParams
from app.models.job_log import JobLog
from app.schemas.log import LogEntryResponse
from app.schemas.response import ApiResponse, Meta

router = APIRouter()


@router.get(
    "",
    summary="List job event logs",
    description=(
        "Returns structured log entries from the job_logs table. "
        "Filter by event type or job_id. Ordered newest first."
    ),
)
async def list_logs(
    page_params: PaginationParams,
    db: DBSession,
    event: Annotated[str | None, Query()] = None,
    job_id: Annotated[UUID | None, Query()] = None,
):
    page = page_params.page
    limit = page_params.limit
    conditions = []
    if event:
        conditions.append(JobLog.event == event)
    if job_id:
        conditions.append(JobLog.job_id == job_id)

    total_result = await db.execute(
        select(func.count(JobLog.id)).where(*conditions)
        if conditions
        else select(func.count(JobLog.id))
    )
    total = total_result.scalar() or 0

    offset = (page - 1) * limit

    query = (
        select(JobLog).order_by(JobLog.created_at.desc()).offset(offset).limit(limit)
    )
    if conditions:
        query = query.where(*conditions)

    result = await db.execute(query)
    logs = result.scalars().all()
    return ApiResponse[list[LogEntryResponse]](
        message="Logs retrieved successfully.",
        data=[LogEntryResponse.model_validate(log) for log in logs],
        meta=Meta(
            page=page,
            limit=limit,
            total=total,
        ),
    )
