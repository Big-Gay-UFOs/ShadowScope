"""Tests for the USAspending connector."""
from __future__ import annotations

import os
from datetime import datetime
from typing import List

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from backend.connectors.usaspending import normalize_awards
from backend.db.models import Base, Event

SAMPLE_RESULTS: List[dict] = [
    {
        "generated_unique_award_id": f"AWD{i:05d}",
        "Action Date": "2023-01-{0:02d}".format((i % 28) + 1),
        "Place of Performance": "Los Alamos, NM",
        "Description": "Test procurement event #{i}".format(i=i),
        "piid": f"PIID{i:05d}",
    }
    for i in range(1, 13)
]


@pytest.fixture(scope="module")
def db_session() -> Session:
    database_url = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope",
    )
    engine = create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        pytest.skip(
            "Postgres database not available. Set TEST_DATABASE_URL and ensure the service is running."
        )
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        yield session
        session.rollback()


def test_normalize_awards_returns_expected_shape():
    events = normalize_awards(SAMPLE_RESULTS)
    assert len(events) == len(SAMPLE_RESULTS)
    first = events[0]
    assert first["category"] == "procurement"
    assert first["source"] == "USAspending"
    assert first["hash"]
    if first["occurred_at"]:
        assert isinstance(first["occurred_at"], datetime)


def test_can_insert_normalized_awards_into_postgres(db_session: Session):
    events = normalize_awards(SAMPLE_RESULTS)
    db_session.bulk_insert_mappings(Event, events[:10])
    db_session.commit()
    stored = db_session.query(Event).count()
    assert stored == 10
