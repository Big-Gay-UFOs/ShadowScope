from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.correlate import correlate
from backend.db.models import Correlation, CorrelationLink, Entity, Event, ensure_schema, get_session_factory


def test_rebuild_entity_correlations_creates_expected_rows(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'corr.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    db = SessionFactory()

    now = datetime.now(timezone.utc)

    ent = Entity(name="ACME INC", uei="UEI123")
    db.add(ent)
    db.flush()

    # two events inside the window, one outside
    db.add_all(
        [
            Event(category="procurement", source="USAspending", hash="h1", entity_id=ent.id, created_at=now - timedelta(days=1)),
            Event(category="procurement", source="USAspending", hash="h2", entity_id=ent.id, created_at=now - timedelta(days=2)),
            Event(category="procurement", source="USAspending", hash="h3", entity_id=ent.id, created_at=now - timedelta(days=60)),
            Event(category="procurement", source="USAspending", hash="h4", entity_id=None, created_at=now - timedelta(days=1)),
        ]
    )
    db.commit()
    db.close()

    res = correlate.rebuild_entity_correlations(window_days=30, source="USAspending", min_events=2, dry_run=False, database_url=db_url)
    assert res["correlations_created"] == 1
    assert res["links_created"] == 2

    db2 = SessionFactory()
    assert db2.query(Correlation).count() == 1
    assert db2.query(CorrelationLink).count() == 2

    corr = db2.query(Correlation).one()
    assert corr.window_days == 30
    assert isinstance(corr.lanes_hit, dict)
    assert corr.lanes_hit.get("lane") == "same_entity"
    assert corr.lanes_hit.get("uei") == "UEI123"
    db2.close()

    # running again is stable (rebuild clears + recreates)
    res2 = correlate.rebuild_entity_correlations(window_days=30, source="USAspending", min_events=2, dry_run=False, database_url=db_url)
    assert res2["correlations_created"] == 1
    assert res2["links_created"] == 2


def test_rebuild_entity_correlations_dry_run_writes_nothing(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'corr_dry.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    db = SessionFactory()

    now = datetime.now(timezone.utc)
    ent = Entity(name="ACME INC", uei="UEI123")
    db.add(ent)
    db.flush()

    db.add_all(
        [
            Event(category="procurement", source="USAspending", hash="h1", entity_id=ent.id, created_at=now - timedelta(days=1)),
            Event(category="procurement", source="USAspending", hash="h2", entity_id=ent.id, created_at=now - timedelta(days=2)),
        ]
    )
    db.commit()
    db.close()

    res = correlate.rebuild_entity_correlations(window_days=30, source="USAspending", min_events=2, dry_run=True, database_url=db_url)
    assert res["correlations_created"] == 1
    assert res["links_created"] == 2

    db2 = SessionFactory()
    assert db2.query(Correlation).count() == 0
    assert db2.query(CorrelationLink).count() == 0
    db2.close()