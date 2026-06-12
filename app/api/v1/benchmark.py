from fastapi import APIRouter, Depends

from app.core.security import verify_api_key
from app.schemas.benchmark import BenchmarkRequest, BenchmarkResult
from app.schemas.response import ApiResponse

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.post(
    "/run",
    summary="Run queue algorithm benchmark",
    response_model=ApiResponse[BenchmarkResult],
)
async def run_benchmark(body: BenchmarkRequest):
    try:
        from benchmark.runner import run_benchmark as _run

        result = await _run(n=body.n, algorithm=body.algorithm)
        return ApiResponse[BenchmarkResult](
            message="Benchmark completed successfully.",
            data=BenchmarkResult(**result),
        )

    except Exception as exc:
        return ApiResponse[None](
            message="Benchmark failed.",
            errors=[{"message": str(exc)}],
        ), 500
