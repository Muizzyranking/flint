from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.router import router, sse_router
from app.core.config import settings
from app.core.exception_handlers import register_exception_handlers
from app.core.logger import get_logger, setup_logging
from app.db.redis import close_redis, get_redis
from app.db.session import AsyncSessionLocal, engine
from app.handlers import load_handlers

setup_logging()

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_handlers()
    logger.info(
        "api_starting",
        env=settings.APP_ENV,
        cors_origins=settings.cors_origins,
    )
    yield
    await close_redis()
    await engine.dispose()
    logger.info("api_stopped")


app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "**Quietly igniting your payload, every job has a spark.**\n\n"
        "Distributed job scheduling service. The API accepts jobs, validates "
        "their payload against the registered handler schema, and stores them "
        "durably in PostgreSQL. Independent workers claim and execute jobs."
    ),
    version="0.2.0",
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
    openapi_url="/api/v1/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials="*" not in settings.cors_origins,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-API-Key"],
)

register_exception_handlers(app)
app.include_router(router, prefix=settings.API_V1_PREFIX)
app.include_router(sse_router, prefix=settings.API_V1_PREFIX)


@app.get("/", include_in_schema=False)
def home():
    return {
        "service": settings.APP_NAME,
        "version": app.version,
        "docs": "/api/v1/docs",
    }


@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "ok"}


@app.get("/health/live", tags=["Health"])
def liveness():
    return {"status": "alive"}


@app.get("/health/ready", tags=["Health"])
async def readiness():
    """Readiness probe: dependencies needed to serve traffic are reachable."""
    components: dict[str, str] = {}

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        components["database"] = "ok"
    except Exception as exc:
        logger.error("readiness_database_error", error=str(exc))
        components["database"] = "unavailable"
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "components": components},
        )

    try:
        redis = await get_redis()
        await redis.ping()
        components["redis"] = "ok"
    except Exception as exc:
        logger.warning("readiness_redis_error", error=str(exc))
        components["redis"] = "degraded"

    return {"status": "ready", "components": components}
