import logging
import os
import sys
from collections.abc import MutableMapping
from logging.handlers import RotatingFileHandler
from typing import Any

import structlog

_SENSITIVE_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "password",
        "secret",
        "token",
        "smtp_password",
        "authorization",
        "x-api-key",
        "cookie",
        "set-cookie",
    }
)


def _scrub_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "***REDACTED***"
            if str(key).lower() in _SENSITIVE_KEYS
            else _scrub_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_scrub_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_scrub_value(item) for item in value)
    return value


def _scrub_sensitive_fields(
    logger: Any, method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Recursively redact secrets before they reach any sink."""
    return _scrub_value(event_dict)


def setup_logging() -> None:
    """
    Configure structured JSON logs for stdout and a rotating file.

    structlog is wired through the stdlib logging framework so the same
    formatted record reaches both sinks.
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
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )

    file_handler = RotatingFileHandler(
        settings.LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    file_handler._flint_handler = True  # type: ignore

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(formatter)
    stdout_handler._flint_handler = True  # type: ignore

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in list(root_logger.handlers):
        if getattr(handler, "_flint_handler", False):
            root_logger.removeHandler(handler)

    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(file_handler)

    for noisy in (
        "sqlalchemy.engine",
        "sqlalchemy.pool",
        "uvicorn.access",
        "httpx",
        "aiosmtplib",
        "redis",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
