from uuid import UUID

from fastapi import APIRouter, Depends

from app.core.exceptions import FlintException
from app.core.security import verify_api_key
from app.dependencies import DBSession, PaginationParams
from app.schemas.job import JobResponse
from app.schemas.response import ApiResponse, Meta, error_response
from app.services import job

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.get(
    "",
    summary="List bin (soft-deleted jobs)",
    response_model=ApiResponse[list[JobResponse]],
)
async def list_bin(db: DBSession, page_params: PaginationParams):
    page = page_params.page
    limit = page_params.limit
    jobs, total = await job.get_bin_jobs(page, limit, db)
    return ApiResponse[list[JobResponse]](
        message="Bin retrieved successfully.",
        data=[JobResponse.model_validate(j) for j in jobs],
        meta=Meta(page=page, limit=limit, total=total),
    )


@router.patch(
    "/{job_id}/restore",
    summary="Restore a job from the bin",
    response_model=ApiResponse[JobResponse],
)
async def restore_job(job_id: UUID, db: DBSession):
    try:
        job_result = await job.restore_job(job_id, db)
        return ApiResponse[JobResponse](
            message="Job restored successfully.",
            data=JobResponse.model_validate(job_result),
        )
    except FlintException as exc:
        return error_response(
            message=exc.message,
            errors=[{"message": exc.message}],
            status_code=exc.status_code,
        )


@router.delete(
    "/{job_id}",
    summary="Permanently delete a job",
    description=("Hard-deletes a job from the database."),
    response_model=ApiResponse[None],
)
async def hard_delete_job(job_id: UUID, db: DBSession):
    try:
        await job.hard_delete_job(job_id, db)
        return ApiResponse[None](message="Job permanently deleted.")
    except FlintException as exc:
        return error_response(
            message=exc.message,
            errors=[{"message": exc.message}],
            status_code=exc.status_code,
        )
