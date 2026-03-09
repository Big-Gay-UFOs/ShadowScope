from datetime import datetime, timedelta, timezone

from backend.correlate import correlate
from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory


def test_rebuild_sam_naics_correlations_is_idempotent(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'sam_naics_corr.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    db = SessionFactory()

    now = datetime.now(timezone.utc)
    db.add_all(
        [
            Event(
                category="procurement",
                source="SAM.gov",
                hash="sam-naics-1",
                raw_json={"sam_naics_code": "541330", "sam_naics_description": "Engineering Services"},
                created_at=now - timedelta(days=1),
                keywords=[],
                clauses=[],
            ),
            Event(
                category="procurement",
                source="SAM.gov",
                hash="sam-naics-2",
                raw_json={"sam_naics_code": "541330", "sam_naics_description": "Engineering Services"},
                created_at=now - timedelta(days=2),
                keywords=[],
                clauses=[],
            ),
            Event(
                category="procurement",
                source="SAM.gov",
                hash="sam-naics-3",
                raw_json={"sam_naics_code": "541512", "sam_naics_description": "Computer Systems Design Services"},
                created_at=now - timedelta(days=3),
                keywords=[],
                clauses=[],
            ),
            Event(
                category="procurement",
                source="USAspending",
                hash="usa-naics-ignore",
                raw_json={"sam_naics_code": "541330"},
                created_at=now - timedelta(days=1),
                keywords=[],
                clauses=[],
            ),
        ]
    )
    db.commit()
    db.close()

    res1 = correlate.rebuild_sam_naics_correlations(
        window_days=30,
        source="SAM.gov",
        min_events=2,
        max_events=200,
        dry_run=False,
        database_url=db_url,
    )
    assert res1["correlations_created"] == 1
    assert res1["eligible_naics"] == 1
    assert res1["links_created"] == 2

    db2 = SessionFactory()
    c1 = db2.query(Correlation).one()
    cid1 = c1.id
    assert c1.correlation_key == "same_sam_naics|SAM.gov|30|naics:541330"
    assert db2.query(CorrelationLink).count() == 2
    db2.close()

    res2 = correlate.rebuild_sam_naics_correlations(
        window_days=30,
        source="SAM.gov",
        min_events=2,
        max_events=200,
        dry_run=False,
        database_url=db_url,
    )
    assert res2["correlations_created"] == 0
    assert res2["correlations_updated"] == 1

    db3 = SessionFactory()
    c2 = db3.query(Correlation).one()
    assert c2.id == cid1
    assert db3.query(CorrelationLink).count() == 2
    db3.close()
