from enum import IntEnum, StrEnum


class JobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(StrEnum):
    SEND_EMAIL = "send_email"
    WEBHOOK_DELIVERY = "webhook_delivery"
    LOG_PROCESSING = "log_processing"


class JobPriority(IntEnum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3
