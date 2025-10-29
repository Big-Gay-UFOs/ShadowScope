import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.api.deps import get_db_session
from backend.api.routes import router
from backend.db.models import configure_session
from backend.db.ops import sync_database
from backend.logging_config import configure_logging
from backend.runtime import ensure_runtime_directories

@asynccontextmanager
async def lifespan(app: FastAPI):
    database_url = os.getenv("DATABASE_URL")
    configure_logging()
    ensure_runtime_directories()
    configure_session(database_url)
    sync_database(database_url)
    yield


app = FastAPI(title="ShadowScope API", version="0.1.0", lifespan=lifespan)


@app.get("/health", tags=["system"])
def health(db: Session = Depends(get_db_session)):
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


app.include_router(router)
