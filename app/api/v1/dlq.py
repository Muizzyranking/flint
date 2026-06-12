from uuid import UUID

from fastapi import APIRouter

from app.core.exceptions import FlintException
from app.dependencies import DBSession, PaginationParams
from app.queues.heapq import HeapQueue
from app.schemas.job import JobResponse
from app.schemas.response import ApiResponse, Meta, error_response
from app.services import dlq as dlq_service

router = APIRouter()

_queue = HeapQueue()


@router.get("", summary="List DLQ jobs", response_model=ApiResponse[list[JobResponse]])
async def list_dlq(
    page_params: PaginationParams,
    db: DBSession,
):
    page = page_params.page
    limit = page_params.limit
    jobs, total = await dlq_service.get_dlq_jobs(page, limit, db)
    return ApiResponse[list[JobResponse]](
        message="DLQ retrieved successfully.",
        data=[JobResponse.model_validate(j) for j in jobs],
        meta=Meta(page=page, limit=limit, total=total),
    )


@router.post(
    "/{job_id}/retry",
    summary="Retry a DLQ job",
)
async def retry_dlq_job(job_id: UUID, db: DBSession):
    try:
        job = await dlq_service.retry_dlq_job(job_id, db, _queue)
        return ApiResponse[JobResponse](
            message="Job re-queued from DLQ successfully.",
            data=JobResponse.model_validate(job),
        )
    except FlintException as exc:
        return error_response(
            message=exc.message,
            errors=[{"message": exc.message}],
            status_code=exc.status_code,
        )


@router.delete(
    "/{job_id}",
    summary="Remove a job from DLQ (soft-delete)",
    response_model=ApiResponse[None],
)
async def remove_from_dlq(
    job_id: UUID,
    db: DBSession,
):
    try:
        await dlq_service.remove_from_dlq(job_id, db)
        return ApiResponse[None](message="Job removed from DLQ and moved to bin.")
    except FlintException as exc:
        return error_response(
            message=exc.message,
            errors=[{"message": exc.message}],
            status_code=exc.status_code,
        )
