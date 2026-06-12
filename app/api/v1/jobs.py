from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends

from app.core.exceptions import FlintException
from app.dependencies import DBSession
from app.schemas.job import (
    JobCreate,
    JobFilterParams,
    JobLogResponse,
    JobResponse,
)
from app.schemas.response import ApiResponse, Meta, error_response
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
    try:
        job = await job_service.create_job(body, db)
        return ApiResponse[JobResponse](
            message="Job created successfully.",
            data=JobResponse.model_validate(job),
        )
    except FlintException as exc:
        return error_response(
            message=exc.message,
            errors=[{"message": exc.message}],
            status_code=exc.status_code,
        )
    except ValueError as exc:
        return error_response(
            message="Validation error.",
            errors=[{"message": str(exc)}],
            status_code=422,
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
    try:
        job, dependency_ids, logs = await job_service.get_job_with_details(job_id, db)
        data = JobResponse.model_validate(job)
        data.dependencies = dependency_ids
        data.logs = [JobLogResponse.model_validate(log) for log in logs]
        return ApiResponse[JobResponse](
            message="Job retrieved successfully.",
            data=data,
        )
    except FlintException as exc:
        return error_response(
            message=exc.message,
            errors=[{"message": exc.message}],
            status_code=exc.status_code,
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
    try:
        job = await job_service.cancel_job(job_id, db)
        return ApiResponse[JobResponse](
            message="Cancellation requested successfully.",
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
    summary="Soft-delete a job (move to bin)",
    response_model=ApiResponse[None],
)
async def soft_delete_job(job_id: UUID, db: DBSession):
    try:
        await job_service.soft_delete_job(job_id, db)
        return ApiResponse[None](message="Job moved to bin.")
    except FlintException as exc:
        return error_response(
            message=exc.message,
            errors=[{"message": exc.message}],
            status_code=exc.status_code,
        )
