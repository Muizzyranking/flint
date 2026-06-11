from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.core.exceptions import FlintException
from app.core.logger import get_logger
from app.schemas.response import ApiResponse, ErrorDetail

logger = get_logger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(FlintException, flint_exception_handler)  # type: ignore
    app.add_exception_handler(HTTPException, http_exception_handler)  # type: ignore
    app.add_exception_handler(HTTPException, http_exception_handler)  # type: ignore
    app.add_exception_handler(Exception, fallback_exception_handler)


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
    return JSONResponse(
        status_code=exc.status_code,
        content=ApiResponse[None](
            message=exc.message,
            errors=[ErrorDetail(message=exc.message)],
        ).model_dump(mode="json"),
    )


async def http_exception_handler(
    request: Request,
    exc: HTTPException,
) -> JSONResponse:
    message = str(exc.detail) if exc.detail else "HTTP error"
    return JSONResponse(
        status_code=exc.status_code,
        content=ApiResponse[None](
            message=message,
            errors=[ErrorDetail(message=message)],
        ).model_dump(mode="json"),
        headers=exc.headers,
    )


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    errors = [
        ErrorDetail(
            field=".".join(str(part) for part in error["loc"]),
            message=str(error["msg"]),
        )
        for error in exc.errors()
    ]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=ApiResponse[None](
            message="Validation failed",
            errors=errors,
        ).model_dump(mode="json"),
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
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ApiResponse[None](
            message="Internal server error",
            errors=[ErrorDetail(message="An unexpected error occurred.")],
        ).model_dump(mode="json"),
    )
