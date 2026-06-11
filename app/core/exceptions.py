class FlintException(Exception):
    """
    Base exception for all Flint domain errors.
    """

    def __init__(self, message: str, status_code: int = 400) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class JobNotFoundException(FlintException):
    """
    Raised when a requested job does not exist or has been hard-deleted.
    """

    def __init__(self, job_id: str) -> None:
        super().__init__(
            message=f"Job '{job_id}' not found.",
            status_code=404,
        )


class JobNotCancellableException(FlintException):
    """
    Raised when a cancel request is made on a job in a terminal state
    (completed, failed, cancelled).
    """

    def __init__(self, job_id: str, status: str) -> None:
        super().__init__(
            message=(
                f"Job '{job_id}' cannot be cancelled because it is already '{status}'. "
                "Only pending and processing jobs can be cancelled."
            ),
            status_code=409,
        )


class JobNotDeletableException(FlintException):
    """
    Raised when a soft-delete is attempted on a job that is still
    pending or processing.
    """

    def __init__(self, job_id: str, status: str) -> None:
        super().__init__(
            message=(
                f"Job '{job_id}' cannot be deleted while it is '{status}'. "
                "Cancel the job first, or wait for it to reach a terminal state."
            ),
            status_code=409,
        )


class JobAlreadyProcessingException(FlintException):
    """
    Raised when two workers attempt to claim the same job.
    """

    def __init__(self, job_id: str) -> None:
        super().__init__(
            message=f"Job '{job_id}' is already being processed by another worker.",
            status_code=409,
        )


class JobNotInBinException(FlintException):
    """
    Raised when a restore or hard-delete is attempted on a non-deleted job.
    """

    def __init__(self, job_id: str) -> None:
        super().__init__(
            message=f"Job '{job_id}' is not in the bin.",
            status_code=404,
        )


class JobNotInDLQException(FlintException):
    """
    Raised when a DLQ retry is attempted on a job that is not in the DLQ.
    """

    def __init__(self, job_id: str) -> None:
        super().__init__(
            message=f"Job '{job_id}' is not in the dead letter queue.",
            status_code=404,
        )


class DependencyCycleException(FlintException):
    """
    Raised when adding a dependency would create a cycle in the
    job dependency graph.
    """

    def __init__(self, job_id: str, dependency_id: str) -> None:
        super().__init__(
            message=(
                f"Adding dependency '{dependency_id}' to job '{job_id}' "
                "would create a cycle in the dependency graph. "
                "Circular dependencies are not allowed."
            ),
            status_code=422,
        )


class DependencyNotFoundException(FlintException):
    """Raised when a specified dependency job ID does not exist."""

    def __init__(self, dependency_id: str) -> None:
        super().__init__(
            message=f"Dependency job '{dependency_id}' not found.",
            status_code=404,
        )


class HandlerNotFoundException(FlintException):
    """
    Raised when no handler is registered for a given job type.
    """

    def __init__(self, job_type: str) -> None:
        super().__init__(
            message=(
                f"No handler registered for job type '{job_type}'. "
                "Supported types: send_email, webhook_delivery, log_processing."
            ),
            status_code=422,
        )


class InvalidIntervalException(FlintException):
    """Raised when an interval string cannot be parsed."""

    def __init__(self, interval: str) -> None:
        super().__init__(
            message=(
                f"Invalid interval format: '{interval}'. "
                "Use <number><unit> where unit is one of: s, m, h, d, mo, y. "
                "Examples: '30s', '5m', '2h', '1d', '1mo', '1y'."
            ),
            status_code=422,
        )


class SettingNotFoundException(FlintException):
    """
    Raised when a requested settings key does not exist.
    """

    def __init__(self, key: str) -> None:
        super().__init__(
            message=f"Setting '{key}' not found.",
            status_code=404,
        )


class WorkerNotFoundException(FlintException):
    """Raised when a control action targets an unknown worker ID."""

    def __init__(self, worker_id: str) -> None:
        super().__init__(
            message=f"Worker '{worker_id}' not found or is no longer active.",
            status_code=404,
        )
