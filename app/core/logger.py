import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import structlog


def setup_logging() -> None:
    """
    Configure structlog for structured JSON logging.
    Output goes to both stdout and a rotating log file.
    Called once at application startup in app/main.py.
    """
    from app.core.config import settings

    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    log_dir = os.path.dirname(settings.LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        _scrub_sensitive_fields,
    ]

    structlog.configure(
        processors=shared_processors
        + [
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # File handler — rotating at 10MB, keeping 5 backups
    file_handler = RotatingFileHandler(
        settings.LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter("%(message)s"))

    # Stdout handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))

    # Root logger — captures structlog output and SQLAlchemy warnings
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Avoid adding duplicate handlers on reload
    if not root_logger.handlers:
        root_logger.addHandler(stdout_handler)
        root_logger.addHandler(file_handler)

    # Quieten noisy third-party loggers
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aiosmtplib").setLevel(logging.WARNING)


def _scrub_sensitive_fields(
    logger: object,
    method: str,
    event_dict: dict,
) -> dict:
    """
    Processor that removes sensitive keys from log entries
    before they are written to stdout or file.
    """
    _SENSITIVE_KEYS = frozenset(
        {
            "api_key",
            "password",
            "secret",
            "token",
            "smtp_password",
            "authorization",
            "x-api-key",
        }
    )
    for key in list(event_dict.keys()):
        if key.lower() in _SENSITIVE_KEYS:
            event_dict[key] = "***REDACTED***"
    return event_dict


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Returns a structlog bound logger for the given module name.
    Usage:
        from app.core.logger import get_logger
        logger = get_logger(__name__)
        logger.info("job_created", job_id=str(job.id), type=job.type)
    """
    return structlog.get_logger(name)
