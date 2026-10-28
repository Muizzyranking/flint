import secrets

from fastapi import HTTPException, Query, Request, Security, status
from fastapi.security import APIKeyHeader

from app.core.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _key_is_valid(candidate: str) -> bool:
    """Constant-time comparison against every configured key."""
    valid = False
    for key in settings.api_keys:
        if secrets.compare_digest(candidate, key):
            valid = True
    return valid


async def verify_api_key(api_key: str | None = Security(api_key_header)) -> str:
    if not api_key or not _key_is_valid(api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "X-API-Key"},
        )
    return api_key


async def verify_sse_api_key(
    request: Request,
    api_key: str | None = Query(default=None),
) -> str:
    candidate = request.headers.get("X-API-Key") or api_key
    if not candidate or not _key_is_valid(candidate):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "X-API-Key"},
        )
    return candidate
