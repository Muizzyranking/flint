from fastapi import APIRouter, Depends

from app.api.v1 import (
    benchmark,
    bin,
    dashboard,
    dlq,
    jobs,
    logs,
    settings,
    sse,
    workers,
)
from app.core.security import verify_api_key

router = APIRouter()
# NOTE: this is to require api key heeader. will add later
api_router = APIRouter(dependencies=[Depends(verify_api_key)])

router.include_router(jobs.router, prefix="/jobs", tags=["Jobs"])
router.include_router(dlq.router, prefix="/dlq", tags=["DLQ"])
router.include_router(bin.router, prefix="/bin", tags=["Bin"])
router.include_router(settings.router, prefix="/settings", tags=["Settings"])
router.include_router(workers.router, prefix="/workers", tags=["Workers"])
router.include_router(logs.router, prefix="/logs", tags=["Logs"])
router.include_router(benchmark.router, prefix="/benchmark", tags=["Benchmark"])
router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
router.include_router(sse.router, prefix="/sse", tags=["SSE"])
