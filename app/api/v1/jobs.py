from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends

from app.dependencies import DBSession
from app.schemas.job import (
    JobCreate,
    JobFilterParams,
    JobLogResponse,
    JobResponse,
)
from app.schemas.response import ApiResponse, Meta
from app.services import job as job_service

router = APIRouter()


@router.post(
    "",
    summary="Create a job",
    response_model=ApiResponse[JobResponse],
    status_code=201,
)
async def create_job(
    body: JobCreate,
    db: DBSession,
):
    job = await job_service.create_job(body, db)
    return ApiResponse[JobResponse](
        message="Job created successfully.",
        data=JobResponse.model_validate(job),
    )


@router.get(
    "",
    summary="List jobs",
    response_model=ApiResponse[list[JobResponse]],
)
async def list_jobs(
    db: DBSession,
    filters: Annotated[JobFilterParams, Depends(JobFilterParams)],
):
    jobs, total = await job_service.get_jobs(filters, db)
    return ApiResponse[list[JobResponse]](
        message="Jobs retrieved successfully.",
        data=[JobResponse.model_validate(j) for j in jobs],
        meta=Meta(
            page=filters.page,
            limit=filters.limit,
            total=total,
        ),
    )


@router.get(
    "/{job_id}",
    summary="Get job detail",
    response_model=ApiResponse[JobResponse],
)
async def get_job(
    job_id: UUID,
    db: DBSession,
):
    job, dependency_ids, logs = await job_service.get_job_with_details(job_id, db)
    data = JobResponse.model_validate(job)
    data.dependencies = dependency_ids
    data.logs = [JobLogResponse.model_validate(log) for log in logs]
    return ApiResponse[JobResponse](
        message="Job retrieved successfully.",
        data=data,
    )


@router.patch(
    "/{job_id}/cancel",
    summary="Cancel a job",
    response_model=ApiResponse[JobResponse],
)
async def cancel_job(
    job_id: UUID,
    db: DBSession,
):
    job = await job_service.cancel_job(job_id, db)
    return ApiResponse[JobResponse](
        message="Cancellation requested successfully.",
        data=JobResponse.model_validate(job),
    )


@router.delete(
    "/{job_id}",
    summary="Soft-delete a job (move to bin)",
    response_model=ApiResponse[None],
)
async def soft_delete_job(job_id: UUID, db: DBSession):
    await job_service.soft_delete_job(job_id, db)
    return ApiResponse[None](message="Job moved to bin.")
