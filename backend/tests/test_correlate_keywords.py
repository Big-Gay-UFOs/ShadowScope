from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.correlate import correlate
from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory


def test_rebuild_keyword_correlations_is_idempotent(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'kw.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    db = SessionFactory()

    now = datetime.now(timezone.utc)

    db.add_all(
        [
            Event(category="procurement", source="USAspending", hash="k1", keywords=["pack:alpha", "pack:beta"], created_at=now - timedelta(days=1)),
            Event(category="procurement", source="USAspending", hash="k2", keywords=["pack:alpha"], created_at=now - timedelta(days=2)),
            Event(category="procurement", source="USAspending", hash="k3", keywords=["pack:gamma"], created_at=now - timedelta(days=3)),
        ]
    )
    db.commit()
    db.close()

    res1 = correlate.rebuild_keyword_correlations(window_days=30, source="USAspending", min_events=2, max_events=200, dry_run=False, database_url=db_url)
    assert res1["correlations_created"] == 1
    assert res1["links_created"] == 2

    db2 = SessionFactory()
    c1 = db2.query(Correlation).one()
    cid1 = c1.id
    assert c1.correlation_key == "same_keyword|USAspending|30|kw:pack:alpha"
    assert db2.query(CorrelationLink).count() == 2
    db2.close()

    res2 = correlate.rebuild_keyword_correlations(window_days=30, source="USAspending", min_events=2, max_events=200, dry_run=False, database_url=db_url)
    assert res2["correlations_created"] == 0
    assert res2["correlations_updated"] == 1

    db3 = SessionFactory()
    c2 = db3.query(Correlation).one()
    assert c2.id == cid1
    assert db3.query(CorrelationLink).count() == 2
    db3.close()