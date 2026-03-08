from datetime import datetime, timezone

from backend.db.models import Entity, Event, LeadSnapshot, ensure_schema, get_session_factory
from backend.services.doctor import doctor_status


def test_doctor_status_uses_source_specific_hints(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'doctor_hints.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add(
            Event(
                category="procurement",
                source="SAM.gov",
                occurred_at=now,
                created_at=now,
                snippet="Generators",
                raw_json={},
                hash="doctor_sam_1",
                keywords=[],
                clauses=[],
            )
        )
        # This snapshot is intentionally from a DIFFERENT source.
        # It should not suppress SAM.gov-specific doctor hints.
        db.add(
            LeadSnapshot(
                source="USAspending",
                min_score=1,
                limit=10,
                scoring_version="v2",
            )
        )
        db.commit()

    res = doctor_status(
        database_url=db_url,
        days=30,
        source="SAM.gov",
        scan_limit=100,
        max_keywords_per_event=10,
    )
    joined = "\n".join(res["hints"])

    assert 'ss ontology apply --path ontology.json --days 30 --source "SAM.gov"' in joined
    assert 'ss entities link --source "SAM.gov" --days 30' in joined
    assert 'ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200' in joined


def test_doctor_status_reports_entity_coverage_diagnostics(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'doctor_entity_cov.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        ent = Entity(name="Known Recipient")
        db.add(ent)
        db.flush()

        db.add_all(
            [
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="Linked event",
                    raw_json={"recipient_name": "Known Recipient", "recipient_id": "RID-100"},
                    hash="doctor_cov_1",
                    entity_id=ent.id,
                    keywords=["solicitation"],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="Unlinked identity event",
                    raw_json={"recipient_name": "Other Recipient", "recipient_id": "RID-200"},
                    hash="doctor_cov_2",
                    keywords=["solicitation"],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="No identity signal",
                    raw_json={},
                    hash="doctor_cov_3",
                    keywords=["solicitation"],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = doctor_status(
        database_url=db_url,
        days=30,
        source="SAM.gov",
        scan_limit=100,
        max_keywords_per_event=10,
    )

    entities = res["entities"]
    assert entities["window_linked_coverage_pct"] == 33.3
    assert entities["sample_scanned_events"] == 3
    assert entities["sample_events_with_identity_signal"] == 2
    assert entities["sample_events_with_identity_signal_linked"] == 1
    assert entities["sample_identity_signal_coverage_pct"] == 50.0
    assert entities["sample_events_with_name"] == 2
    assert entities["sample_events_with_name_linked"] == 1
    assert entities["sample_name_coverage_pct"] == 50.0
