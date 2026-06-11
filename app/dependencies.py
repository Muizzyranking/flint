from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio.session import AsyncSession

from app.db.session import get_db

DBSession = Annotated[AsyncSession, Depends(get_db)]
