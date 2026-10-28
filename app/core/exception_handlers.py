from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.exceptions import FlintException
from app.core.logger import get_logger
from app.schemas.response import ApiResponse, ErrorDetail

logger = get_logger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(FlintException, flint_exception_handler)  # type: ignore
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # type: ignore
    app.add_exception_handler(RequestValidationError, validation_exception_handler)  # type: ignore
    app.add_exception_handler(ValueError, value_error_handler)  # type: ignore
    app.add_exception_handler(Exception, fallback_exception_handler)


def _error_response(
    *,
    status_code: int,
    message: str,
    errors: list[ErrorDetail],
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ApiResponse[None](
            message=message,
            data=None,
            errors=errors,
            meta=None,
        ).model_dump(mode="json"),
        headers=headers,
    )


def _normalise_errors(
    raw_errors: Any,
    fallback_message: str,
) -> list[ErrorDetail]:
    if not raw_errors:
        return [ErrorDetail(message=fallback_message)]

    if not isinstance(raw_errors, list):
        return [ErrorDetail(message=str(raw_errors))]

    normalised: list[ErrorDetail] = []
    for error in raw_errors:
        if isinstance(error, ErrorDetail):
            normalised.append(error)
        elif isinstance(error, dict):
            normalised.append(
                ErrorDetail(
                    field=error.get("field"),
                    message=str(error.get("message") or fallback_message),
                    type=error.get("type"),
                )
            )
        else:
            normalised.append(ErrorDetail(message=str(error)))
    return normalised or [ErrorDetail(message=fallback_message)]


async def flint_exception_handler(
    request: Request,
    exc: FlintException,
) -> JSONResponse:
    logger.warning(
        "domain_exception",
        path=request.url.path,
        status_code=exc.status_code,
        error=exc.message,
    )
    return _error_response(
        status_code=exc.status_code,
        message=exc.message,
        errors=_normalise_errors(
            getattr(exc, "errors", None),
            exc.message,
        ),
    )


async def http_exception_handler(
    request: Request,
    exc: StarletteHTTPException,
) -> JSONResponse:
    detail = exc.detail
    if isinstance(detail, str):
        message = detail
    elif detail:
        message = str(detail)
    else:
        message = "HTTP error"
    return _error_response(
        status_code=exc.status_code,
        message=message,
        errors=[ErrorDetail(message=message)],
        headers=exc.headers,  # type: ignore
    )


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    errors: list[ErrorDetail] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", ()))
        context_error = (error.get("ctx") or {}).get("error")
        nested_errors = getattr(context_error, "errors", None)

        if isinstance(nested_errors, list):
            for nested in nested_errors:
                field = nested.get("field")
                if field in {None, "", "(root)"}:
                    field = f"{location}.payload" if location else "payload"
                else:
                    field = f"{location}.payload.{field}" if location else field
                errors.append(
                    ErrorDetail(
                        field=field,
                        message=str(nested.get("message", "Invalid value")),
                        type=nested.get("type"),
                    )
                )
            continue

        errors.append(
            ErrorDetail(
                field=location or None,
                message=str(error.get("msg", "Invalid value")),
                type=error.get("type"),
            )
        )

    return _error_response(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        message="Validation failed.",
        errors=errors,
    )


async def value_error_handler(
    request: Request,
    exc: ValueError,
) -> JSONResponse:
    logger.warning(
        "value_error",
        path=request.url.path,
        error=str(exc),
    )
    return _error_response(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        message=str(exc) or "Invalid request.",
        errors=[ErrorDetail(message=str(exc) or "Invalid request.")],
    )


async def fallback_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    logger.exception(
        "unhandled_exception",
        path=request.url.path,
        error=str(exc),
    )
    return _error_response(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        message="Internal server error.",
        errors=[ErrorDetail(message="An unexpected error occurred.")],
    )
