from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.correlate import correlate
from backend.db.models import Correlation, CorrelationLink, Entity, Event, ensure_schema, get_session_factory


def test_rebuild_entity_correlations_is_idempotent(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'corr.db'}"
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

    res1 = correlate.rebuild_entity_correlations(window_days=30, source="USAspending", min_events=2, dry_run=False, database_url=db_url)
    assert res1["correlations_created"] == 1
    assert res1["links_created"] == 2

    db2 = SessionFactory()
    c1 = db2.query(Correlation).one()
    cid1 = c1.id
    assert db2.query(CorrelationLink).count() == 2
    db2.close()

    # second run should update same correlation row (stable ID) and keep 2 links
    res2 = correlate.rebuild_entity_correlations(window_days=30, source="USAspending", min_events=2, dry_run=False, database_url=db_url)
    assert res2["correlations_created"] == 0
    assert res2["correlations_updated"] == 1

    db3 = SessionFactory()
    c2 = db3.query(Correlation).one()
    assert c2.id == cid1
    assert db3.query(CorrelationLink).count() == 2
    db3.close()