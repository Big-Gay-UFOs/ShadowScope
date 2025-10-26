"""Placeholder API routes for future phases."""
from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api", tags=["core"])


@router.get("/ping", summary="Simple liveliness probe")
def ping() -> dict[str, str]:
    return {"message": "pong"}
