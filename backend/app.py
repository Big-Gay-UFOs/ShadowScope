"""FastAPI application entrypoint for ShadowScope."""
from __future__ import annotations

import logging
import os

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from backend.api.routes import router
from backend.db.models import configure_session, ensure_schema, session_scope

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ShadowScope API", version="0.1.0")


def get_db_session():
    database_url = os.getenv("DATABASE_URL")
    with session_scope(database_url) as session:
        yield session


@app.on_event("startup")
def startup_event() -> None:
    database_url = os.getenv("DATABASE_URL")
    configure_session(database_url)
    try:
        ensure_schema(database_url)
        logger.info("Database schema ensured")
    except RuntimeError as exc:
        logger.warning("Database initialization skipped: %s", exc)


@app.get("/health", tags=["system"])
def health_check(db: Session = Depends(get_db_session)) -> dict[str, str]:
    db.execute("SELECT 1")
    return {"status": "ok"}


app.include_router(router)
