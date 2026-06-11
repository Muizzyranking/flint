HANDLER_REGISTRY = {}


def get_handler(job_type: str):
    handler_class = HANDLER_REGISTRY.get(job_type)
    if not handler_class:
        raise ValueError(f"No handler registered for job type: {job_type}")
    return handler_class()
