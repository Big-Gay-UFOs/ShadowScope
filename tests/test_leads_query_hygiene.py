from datetime import datetime, timedelta, timezone

from backend.db import models
from backend.services.leads import compute_leads, create_lead_snapshot


def _seed_mixed_source_events(db, now: datetime) -> None:
    db.add_all(
        [
            models.Event(
                category="opportunity",
                source="SAM.gov",
                hash="sam_recent",
                occurred_at=now - timedelta(days=1),
                created_at=now - timedelta(days=10),
                snippet="Recent SAM event by occurred_at",
                raw_json={},
                keywords=[],
                clauses=[],
            ),
            models.Event(
                category="opportunity",
                source="SAM.gov",
                hash="sam_stale",
                occurred_at=now - timedelta(days=5),
                created_at=now - timedelta(hours=1),
                snippet="Stale SAM event with newer created_at fallback",
                raw_json={},
                keywords=[],
                clauses=[],
            ),
        ]
    )
    db.commit()

    db.add_all(
        [
            models.Event(
                category="award",
                source="USAspending",
                hash=f"usa_latest_{idx}",
                occurred_at=now - timedelta(minutes=idx),
                created_at=now - timedelta(minutes=idx),
                snippet=f"Recent USAspending event {idx}",
                raw_json={},
                keywords=[],
                clauses=[],
            )
            for idx in range(1, 6)
        ]
    )
    db.commit()


def _lead_hashes(ranked):
    return [event.hash for _score, event, _details in ranked]


def test_compute_leads_filters_source_before_scan_limit_with_mixed_sources(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_source_hygiene.db').as_posix()}"
    models.ensure_schema(db_url)
    SessionFactory = models.get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_mixed_source_events(db, now)

        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=0,
            limit=10,
            scan_limit=2,
            scoring_version="v1",
        )

    assert scanned == 2
    assert _lead_hashes(ranked) == ["sam_recent", "sam_stale"]


def test_compute_leads_filters_exclude_source_before_scan_limit_with_mixed_sources(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_exclude_source_hygiene.db').as_posix()}"
    models.ensure_schema(db_url)
    SessionFactory = models.get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_mixed_source_events(db, now)

        ranked, scanned = compute_leads(
            db,
            exclude_source="USAspending",
            min_score=0,
            limit=10,
            scan_limit=2,
            scoring_version="v1",
        )

    assert scanned == 2
    assert _lead_hashes(ranked) == ["sam_recent", "sam_stale"]


def test_create_lead_snapshot_uses_source_filtered_candidate_pool_before_scan_limit(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'lead_snapshot_source_hygiene.db').as_posix()}"
    models.ensure_schema(db_url)
    SessionFactory = models.get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_mixed_source_events(db, now)

    result = create_lead_snapshot(
        source="SAM.gov",
        min_score=0,
        limit=10,
        scan_limit=2,
        scoring_version="v1",
        database_url=db_url,
    )

    assert result["scanned"] == 2
    assert result["items"] == 2

    with SessionFactory() as db:
        snapshot_items = (
            db.query(models.LeadSnapshotItem)
            .filter(models.LeadSnapshotItem.snapshot_id == int(result["snapshot_id"]))
            .order_by(models.LeadSnapshotItem.rank.asc())
            .all()
        )

    assert [item.event_hash for item in snapshot_items] == ["sam_recent", "sam_stale"]
