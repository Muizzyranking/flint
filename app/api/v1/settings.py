from fastapi import APIRouter

from app.core.exceptions import FlintException
from app.dependencies import DBSession
from app.schemas.response import ApiResponse, error_response
from app.schemas.settings import SettingsUpdate
from app.services import settings

router = APIRouter()


@router.get("", summary="Get all settings", response_model=ApiResponse[dict | None])
async def get_settings(db: DBSession):
    data = await settings.get_all_settings(db)
    return ApiResponse[dict | None](
        message="Settings retrieved successfully.",
        data=data,
    )


@router.patch(
    "",
    summary="Update settings",
    description=(
        "Update one or more settings. All fields are optional — "
        "only provided keys are updated. Changes take effect immediately."
    ),
    response_model=ApiResponse[dict],
)
async def update_settings(body: SettingsUpdate, db: DBSession):
    try:
        updates = {k: v for k, v in body.model_dump().items() if v is not None}

        if not updates:
            return error_response(
                message="No settings provided to update.",
                errors=[{"message": "Request body must include at least one setting."}],
                status_code=422,
            )

        updated = await settings.update_settings(updates, db)
        return ApiResponse[dict](
            message="Settings updated successfully.",
            data=updated,
        )
    except FlintException as exc:
        return error_response(
            message=exc.message,
            errors=[{"message": exc.message}],
            status_code=exc.status_code,
        )
