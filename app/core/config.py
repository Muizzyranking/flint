from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # App
    # ------------------------------------------------------------------
    APP_NAME: str = "Flint"
    APP_ENV: str = "development"
    DEBUG: bool = False
    API_V1_PREFIX: str = "/api/v1"
    FRONTEND_URL: str = "http://localhost:3000"
    TRUSTED_PROXIES: str = ""
    COOKIE_SECURE: bool = True

    # ------------------------------------------------------------------
    # API KEY
    # ------------------------------------------------------------------
    API_KEY: str = "dev-api-key"

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    DATABASE_URL: str = "postgresql+asyncpg://flint:flint@localhost:5432/flint"
    REDIS_URL: str = "redis://localhost:6379/0"

    # ------------------------------------------------------------------
    # SMTP (Mailhog)
    # ------------------------------------------------------------------
    SMTP_HOST: str = "mailhog"
    SMTP_PORT: int = 1025
    SMTP_FROM: str = "flint@flint.local"

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------
    WORKER_POLL_INTERVAL: float = 1.0
    WORKER_ID: str = "worker-default"

    # ------------------------------------------------------------------
    # Scheduler
    # ------------------------------------------------------------------
    SCHEDULER_POLL_INTERVAL: float = 1.0

    # ------------------------------------------------------------------
    # Aging / Starvation Prevention
    # ------------------------------------------------------------------
    AGING_INTERVAL: float = 30.0
    MEDIUM_PRIORITY_AGE_THRESHOLD: int = 120  # 2 minutes
    LOW_PRIORITY_AGE_THRESHOLD: int = 300  # 5 minutes
    AGING_DECREMENT: float = 0.1

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/flint.log"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
