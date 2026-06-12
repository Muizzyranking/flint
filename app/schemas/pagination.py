from fastapi import Query


class PageParams:
    """Inject this as a dependency in any endpoint that needs pagination."""

    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number, 1-indexed"),
        size: int = Query(20, ge=1, le=100, description="Items per page"),
    ):
        self.page = page
        self.size = size

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.size

    @property
    def limit(self) -> int:
        return self.size
