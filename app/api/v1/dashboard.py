from fastapi import APIRouter

from app.dependencies import DBSession
from app.schemas.response import ApiResponse
from app.services.job import get_job_counts_by_status

router = APIRouter()


@router.get(
    "/stats",
    summary="Get dashboard job counts",
    description="Returns job counts grouped by status for the dashboard.",
    response_model=ApiResponse[dict],
)
async def get_dashboard_stats(db: DBSession):
    counts = await get_job_counts_by_status(db)
    return ApiResponse[dict](
        message="Dashboard stats retrieved successfully.", data=counts
    )
