from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio.session import AsyncSession

from app.db.session import get_db
from app.schemas.pagination import PageParams

DBSession = Annotated[AsyncSession, Depends(get_db)]
PaginationParams = Annotated[PageParams, Depends(PageParams)]
