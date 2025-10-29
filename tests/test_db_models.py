from __future__ import annotations

from pathlib import Path

import pytest

from backend.db import models


@pytest.fixture(autouse=True)
def reset_model_caches():
    models._engines.clear()
    models._session_factories.clear()
    yield
    models._engines.clear()
    models._session_factories.clear()


def test_get_engine_caches_per_url(tmp_path: Path):
    url_one = f"sqlite:///{tmp_path / 'one.db'}"
    url_two = f"sqlite:///{tmp_path / 'two.db'}"

    engine_one_first = models.get_engine(url_one)
    engine_one_second = models.get_engine(url_one)
    engine_two = models.get_engine(url_two)

    assert engine_one_first is engine_one_second
    assert engine_one_first is not engine_two


def test_created_at_defaults_are_populated(tmp_path: Path):
    database_url = f"sqlite:///{tmp_path / 'defaults.db'}"
    models.ensure_schema(database_url)

    with models.session_scope(database_url) as session:
        entity = models.Entity(name="Test Entity")
        session.add(entity)
        session.flush()
        session.refresh(entity)
        assert entity.created_at is not None

        event = models.Event(
            category="procurement",
            source="test",
            raw_json={},
            hash="abc",
        )
        session.add(event)
        session.flush()
        session.refresh(event)
        assert event.created_at is not None

        correlation = models.Correlation(
            score="low",
            window_days=7,
            radius_km=10.0,
            lanes_hit=["procurement"],
        )
        session.add(correlation)
        session.flush()
        session.refresh(correlation)
        assert correlation.created_at is not None
