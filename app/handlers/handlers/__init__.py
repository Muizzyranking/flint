"""Job handlers.

Importing this package imports every handler module, and each module's
``@register_handler`` decorator registers its handler class.
"""

from app.handlers.handlers import (  # noqa: F401
    echo,
    email,
    http_fetch,
    http_ping,
    job_retention,
    log_processor,
    slack_notification,
    webhook,
)
