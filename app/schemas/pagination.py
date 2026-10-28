from fastapi import Query


class PageParams:
    """Shared pagination dependency: ``?page=1&limit=20``."""

    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number, 1-indexed."),
        limit: int = Query(20, ge=1, le=100, description="Items per page."),
    ):
        self.page = page
        self.limit = limit

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.limit
