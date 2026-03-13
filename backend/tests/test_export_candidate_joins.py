import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.correlate.candidate_joins import rebuild_sam_usaspending_candidate_joins
from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.export_correlations import export_candidate_joins, summarize_candidate_joins


def test_export_and_summarize_candidate_joins(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'candidate_join_export.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="procurement",
                    source="SAM.gov",
                    hash="sam-export",
                    created_at=now,
                    solicitation_number="EXP-001",
                    recipient_name="Acme Systems LLC",
                    recipient_uei="UEI-EXP",
                    awarding_agency_code="DOE",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="USAspending",
                    hash="usa-export",
                    created_at=now - timedelta(days=20),
                    piid="EXP-001",
                    recipient_name="ACME SYSTEMS",
                    recipient_uei="UEI-EXP",
                    awarding_agency_code="DOE",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    rebuild_sam_usaspending_candidate_joins(database_url=db_url)

    out_dir = tmp_path / "exports"
    exported = export_candidate_joins(database_url=db_url, output=out_dir, limit=20)
    summary = summarize_candidate_joins(database_url=db_url, limit=20)

    assert exported["count"] == 1
    assert summary["count"] == 1
    assert summary["likely_incumbent_count"] == 1

    payload = json.loads(Path(exported["json"]).read_text(encoding="utf-8"))
    row = payload["items"][0]
    assert row["sam_event"]["hash"] == "sam-export"
    assert row["usaspending_event"]["hash"] == "usa-export"
    assert "identifier_exact" in row["evidence_types"]
