"""FastAPI dependency helpers."""
from __future__ import annotations

import os
from typing import Iterator

from sqlalchemy.orm import Session

from backend.db.models import session_scope


def get_db_session() -> Iterator[Session]:
    database_url = os.getenv("DATABASE_URL")
    with session_scope(database_url) as session:
        yield session


__all__ = ["get_db_session"]
