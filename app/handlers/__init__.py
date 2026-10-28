from app.handlers.base import BaseHandler
from app.handlers.registry import (
    HandlerSpec,
    get_handler,
    get_spec,
    is_registered,
    list_specs,
    register_handler,
    registered_handler_names,
    validate_payload,
)

__all__ = [
    "BaseHandler",
    "HandlerSpec",
    "get_handler",
    "get_spec",
    "is_registered",
    "list_specs",
    "load_handlers",
    "register_handler",
    "registered_handler_names",
    "validate_payload",
]


def load_handlers() -> None:
    """Register every built-in handler by importing its module.

    Idempotent: handler modules are cached after their first import, so the
    ``@register_handler`` decorators run at most once per process. Called by
    each long-running process (API, worker, CLI) before touching the registry.
    """
    from app.handlers import handlers  # noqa: F401
