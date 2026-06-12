from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import router
from app.core.config import settings
from app.core.exception_handlers import register_exception_handlers
from app.core.logger import get_logger, setup_logging
from app.db.redis import close_redis

setup_logging()

logging = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting up the application...")
    yield
    await close_redis()
    logging.info("Shutting down the application...")


app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "**Quietly igniting your payload, every job has a spark.**\n\n"
        "Background job scheduler with priority queuing, DAG workflows, "
        "retry logic, dead letter queue, and real-time status updates."
    ),
    version="0.1.0",
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
    openapi_url="/api/v1/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_exception_handlers(app)
app.include_router(router, prefix=settings.API_V1_PREFIX)


@app.get("/")
def home():
    return {"message": "fastapi is running!"}


@app.get("/health")
def health_check():
    return {"status": "ok"}
