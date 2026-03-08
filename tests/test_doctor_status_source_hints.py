from datetime import datetime, timezone

from backend.db.models import Event, LeadSnapshot, ensure_schema, get_session_factory
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
    joined = "\\n".join(res["hints"])

    assert 'ss ontology apply --path ontology.json --days 30 --source "SAM.gov"' in joined
    assert 'ss entities link --source "SAM.gov" --days 30' in joined
    assert 'ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200' in joined
