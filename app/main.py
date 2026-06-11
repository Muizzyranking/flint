from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
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
    logging.info("Shutting down the application...")
    await close_redis()


app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    lifespan=lifespan,
)

register_exception_handlers(app)
app.include_router(api_router, prefix=settings.API_V1_PREFIX)


@app.get("/")
def home():
    return {"message": "fastapi is running!"}


@app.get("/health")
def health_check():
    return {"status": "ok"}
