from app.handlers.base import BaseHandler
from app.handlers.email import EmailHandler
from app.handlers.log_processor import LogProcessorHandler
from app.handlers.webhook import WebhookHandler
from app.models.enum import JobType

HANDLER_REGISTRY: dict[str, type[BaseHandler]] = {
    JobType.WEBHOOK_DELIVERY: WebhookHandler,
    JobType.SEND_EMAIL: EmailHandler,
    JobType.LOG_PROCESSING: LogProcessorHandler,
}


def get_handler(job_type: str) -> BaseHandler:
    handler_class = HANDLER_REGISTRY.get(job_type)
    if not handler_class:
        from app.core.exceptions import HandlerNotFoundException

        raise HandlerNotFoundException(job_type)
    return handler_class()


__all__ = [
    "BaseHandler",
    "WebhookHandler",
    "EmailHandler",
    "LogProcessorHandler",
    "HANDLER_REGISTRY",
    "get_handler",
]
