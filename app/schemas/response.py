from typing import Any

from pydantic import BaseModel, Field
from starlette.responses import JSONResponse


class Meta(BaseModel):
    page: int | None = None
    limit: int | None = None
    total: int | None = None


class ErrorDetail(BaseModel):
    field: str | None = None
    message: str


class ApiResponse[T](BaseModel):
    message: str
    data: T | None = None
    errors: list[ErrorDetail | dict] = Field(default_factory=list)
    meta: Meta | None = None


def error_response(
    message: str,
    errors: list[dict[str, Any]],
    status_code: int = 400,
) -> JSONResponse:
    """
    Build an error API response.

    Args:
        message: Human-readable summary of the error.
        errors: List of error detail dicts. Each may have 'field' and 'message'.
        status_code: HTTP status code. Default 400.
    """
    body = {
        "message": message,
        "data": None,
        "errors": [ErrorDetail(**e).model_dump() for e in errors],
        "meta": None,
    }
    return JSONResponse(status_code=status_code, content=body)
