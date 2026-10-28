from enum import IntEnum, StrEnum


class JobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobPriority(IntEnum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3
