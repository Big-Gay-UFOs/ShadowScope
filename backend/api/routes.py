from fastapi import APIRouter

router = APIRouter(prefix="/api", tags=["core"])


@router.get("/ping")
def ping():
    return {"message": "pong"}
