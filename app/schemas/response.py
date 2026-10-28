from typing import Any

from pydantic import BaseModel, Field


class Meta(BaseModel):
    page: int | None = None
    limit: int | None = None
    total: int | None = None


class ErrorDetail(BaseModel):
    field: str | None = None
    message: str
    type: str | None = None


class ApiResponse[T](BaseModel):
    message: str
    data: T | None = None
    errors: list[ErrorDetail | dict[str, Any]] = Field(default_factory=list)
    meta: Meta | None = None
