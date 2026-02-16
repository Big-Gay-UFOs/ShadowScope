from datetime import datetime, timezone
from pathlib import Path

from backend.db import models
from backend.services.leads import create_lead_snapshot


def test_create_lead_snapshot_sqlite(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'leadsnap.db'}"
    models.ensure_schema(db_url)

    SessionFactory = models.get_session_factory(db_url)
    s = SessionFactory()
    try:
        e1 = models.Event(
            category="procurement",
            source="USAspending",
            occurred_at=datetime.now(timezone.utc),
            snippet="Hot cell radiation work",
            hash="h1",
            keywords=[],
            clauses=[{"pack": "p", "rule": "r1", "weight": 12, "field": "snippet", "match": "hot cell"}],
        )
        e2 = models.Event(
            category="procurement",
            source="USAspending",
            occurred_at=datetime.now(timezone.utc),
            snippet="Dosimetry support",
            hash="h2",
            keywords=[],
            clauses=[{"pack": "p", "rule": "r2", "weight": 5, "field": "snippet", "match": "dosimetry"}],
        )
        s.add_all([e1, e2])
        s.commit()
    finally:
        s.close()

    res = create_lead_snapshot(
        analysis_run_id=None,
        source="USAspending",
        min_score=1,
        limit=10,
        scan_limit=100,
        scoring_version="v1",
        notes="test",
        database_url=db_url,
    )
    assert res["status"] == "ok"
    assert res["items"] == 2

    s = SessionFactory()
    try:
        snap = s.query(models.LeadSnapshot).filter_by(id=res["snapshot_id"]).one()
        items = (
            s.query(models.LeadSnapshotItem)
            .filter_by(snapshot_id=snap.id)
            .order_by(models.LeadSnapshotItem.rank.asc())
            .all()
        )
        assert len(items) == 2
        assert items[0].score >= items[1].score
        assert items[0].event_hash in ("h1", "h2")
        assert isinstance(items[0].score_details, dict)
        assert "clause_score" in items[0].score_details
    finally:
        s.close()