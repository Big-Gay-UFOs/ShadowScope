from datetime import datetime, timezone
from pathlib import Path

from backend.db import models
from backend.services.deltas import compute_lead_deltas


def test_lead_delta_sqlite(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'delta.db'}"
    models.ensure_schema(db_url)

    SessionFactory = models.get_session_factory(db_url)
    s = SessionFactory()
    try:
        e1 = models.Event(
            category="procurement",
            source="USAspending",
            occurred_at=datetime.now(timezone.utc),
            snippet="e1",
            hash="h1",
            keywords=[],
            clauses=[],
        )
        e2 = models.Event(
            category="procurement",
            source="USAspending",
            occurred_at=datetime.now(timezone.utc),
            snippet="e2",
            hash="h2",
            keywords=[],
            clauses=[],
        )
        e3 = models.Event(
            category="procurement",
            source="USAspending",
            occurred_at=datetime.now(timezone.utc),
            snippet="e3",
            hash="h3",
            keywords=[],
            clauses=[],
        )
        s.add_all([e1, e2, e3])
        s.commit()
        s.refresh(e1); s.refresh(e2); s.refresh(e3)

        snap1 = models.LeadSnapshot(source="USAspending", min_score=1, limit=200, scoring_version="v1", notes="a")
        s.add(snap1); s.commit(); s.refresh(snap1)

        s.add_all([
            models.LeadSnapshotItem(snapshot_id=snap1.id, event_id=e1.id, event_hash=e1.hash, rank=1, score=10, score_details={"clause_score": 10}),
            models.LeadSnapshotItem(snapshot_id=snap1.id, event_id=e2.id, event_hash=e2.hash, rank=2, score=5, score_details={"clause_score": 5}),
        ])
        s.commit()

        snap2 = models.LeadSnapshot(source="USAspending", min_score=1, limit=200, scoring_version="v1", notes="b")
        s.add(snap2); s.commit(); s.refresh(snap2)

        s.add_all([
            # h2 changed
            models.LeadSnapshotItem(snapshot_id=snap2.id, event_id=e2.id, event_hash=e2.hash, rank=1, score=20, score_details={"clause_score": 20}),
            # h3 new
            models.LeadSnapshotItem(snapshot_id=snap2.id, event_id=e3.id, event_hash=e3.hash, rank=2, score=7, score_details={"clause_score": 7}),
        ])
        s.commit()

        from_id = snap1.id
        to_id = snap2.id
    finally:
        s.close()

    d = compute_lead_deltas(from_snapshot_id=from_id, to_snapshot_id=to_id, database_url=db_url)
    assert d["counts"]["new"] == 1
    assert d["counts"]["removed"] == 1
    assert d["counts"]["changed"] == 1