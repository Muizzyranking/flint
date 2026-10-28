from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_DEV_API_KEY = "dev-api-key"


class Settings(BaseSettings):
    """
    Process configuration shared by the API, workers and CLI.
    """

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
    CORS_ORIGINS: str = ""

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------
    API_KEY: str = DEFAULT_DEV_API_KEY
    API_KEYS: str = ""

    # ------------------------------------------------------------------
    # Database / Redis
    # ------------------------------------------------------------------
    DATABASE_URL: str = "postgresql+asyncpg://flint:flint@localhost:5432/flint"
    REDIS_URL: str = "redis://localhost:6379/0"

    # ------------------------------------------------------------------
    # SMTP (Mailhog in development)
    # ------------------------------------------------------------------
    SMTP_HOST: str = "mailhog"
    SMTP_PORT: int = 1025
    SMTP_FROM: str = "flint@flint.local"

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------
    WORKER_ID: str = ""
    WORKER_CONCURRENCY: int = 2
    WORKER_BATCH_SIZE: int = 25
    # Empty means "every handler registered in this process".
    WORKER_HANDLERS: str = ""
    WORKER_POLL_INTERVAL: float = 0.5
    WORKER_LEASE_SECONDS: int = 60
    WORKER_HEARTBEAT_INTERVAL: int = 15
    WORKER_SHUTDOWN_TIMEOUT: int = 30
    JOB_TIMEOUT_SECONDS: int = 300

    # ------------------------------------------------------------------
    # Maintenance / aging
    # ------------------------------------------------------------------
    MAINTENANCE_INTERVAL: float = 15.0
    MEDIUM_PRIORITY_AGE_THRESHOLD: int = 120
    LOW_PRIORITY_AGE_THRESHOLD: int = 300
    AGING_DECREMENT: float = 0.1

    # ------------------------------------------------------------------
    # Limits / logging
    # ------------------------------------------------------------------
    MAX_PAYLOAD_BYTES: int = 256 * 1024
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/flint.log"

    @property
    def cors_origins(self) -> list[str]:
        raw = self.CORS_ORIGINS.strip()
        if not raw:
            return [self.FRONTEND_URL.rstrip("/")]
        return [
            origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()
        ]

    @property
    def worker_handlers(self) -> list[str] | None:
        raw = self.WORKER_HANDLERS.strip()
        if not raw:
            return None
        return [name.strip() for name in raw.split(",") if name.strip()]

    @property
    def api_keys(self) -> list[str]:
        keys = [self.API_KEY]
        keys.extend(key.strip() for key in self.API_KEYS.split(",") if key.strip())
        return [key for key in keys if key]

    @model_validator(mode="after")
    def validate_production_config(self) -> "Settings":
        if self.APP_ENV.lower() in {"production", "prod"}:
            if self.API_KEY == DEFAULT_DEV_API_KEY and not self.API_KEYS.strip():
                raise ValueError(
                    "API_KEY must be configured in production; the development "
                    "default is not allowed."
                )
            if self.DEBUG:
                raise ValueError("DEBUG must be disabled in production.")
            if "*" in self.cors_origins:
                raise ValueError("Wildcard CORS origins are not allowed in production.")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
