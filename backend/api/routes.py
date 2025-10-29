from fastapi import APIRouter

from backend.api import events

router = APIRouter(prefix="/api", tags=["core"])


@router.get("/ping")
def ping():
    return {"message": "pong"}


router.include_router(events.router)
