from fastapi import APIRouter

from app.core.exceptions import HandlerNotFoundException
from app.handlers import get_spec, list_specs, registered_handler_names
from app.schemas.handler import HandlerInfo, HandlerListResponse
from app.schemas.response import ApiResponse

router = APIRouter()


@router.get(
    "",
    summary="List schedulable handler types",
    description=(
        "Returns every handler registered in the API process, including the "
        "JSON Schema for its payload. Frontends can use this to render forms "
        "and to discover what can be scheduled without hard-coding enums."
    ),
    response_model=ApiResponse[HandlerListResponse],
)
async def list_handlers():
    handlers = [HandlerInfo(**spec.to_dict()) for spec in list_specs()]
    return ApiResponse[HandlerListResponse](
        message="Handlers retrieved successfully.",
        data=HandlerListResponse(handlers=handlers),
    )


@router.get(
    "/{handler_name}",
    summary="Get one handler definition",
    response_model=ApiResponse[HandlerInfo],
)
async def get_handler_definition(handler_name: str):
    spec = get_spec(handler_name)
    if spec is None:
        raise HandlerNotFoundException(
            handler_name,
            supported=registered_handler_names(),
        )
    return ApiResponse[HandlerInfo](
        message="Handler retrieved successfully.",
        data=HandlerInfo(**spec.to_dict()),
    )
