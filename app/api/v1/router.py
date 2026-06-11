from fastapi import APIRouter, Depends

from app.core.security import verify_api_key

api_router = APIRouter(dependencies=[Depends(verify_api_key)])
