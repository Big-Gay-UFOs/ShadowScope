from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import os

from backend.api.routes import router
from backend.db.models import configure_session, ensure_schema, session_scope

app = FastAPI(title="ShadowScope API", version="0.1.0")


def get_db_session():
    database_url = os.getenv("DATABASE_URL")
    with session_scope(database_url) as s:
        yield s


@app.on_event("startup")
def startup():
    database_url = os.getenv("DATABASE_URL")
    configure_session(database_url)
    ensure_schema(database_url)


@app.get("/health", tags=["system"])
def health(db: Session = Depends(get_db_session)):
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


app.include_router(router)
