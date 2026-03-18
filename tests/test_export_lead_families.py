import json
from datetime import datetime, timedelta, timezone

from backend.correlate.candidate_joins import rebuild_sam_usaspending_candidate_joins
from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.export_leads import export_lead_snapshot
from backend.services.leads import create_lead_snapshot


def test_export_lead_snapshot_includes_lead_family_and_candidate_corroboration(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'export_lead_family.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        sam_event = Event(
            category="notice",
            source="SAM.gov",
            hash="sam-export-lineage",
            snippet="classified follow-on task order continuity support",
            doc_id="sam-export-doc",
            solicitation_number="EXPORT-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="Acme Mission Support LLC",
            recipient_uei="UEI-EXPORT",
            source_url="http://example.com/sam/export",
            raw_json={},
            keywords=[
                "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context"
            ],
            clauses=[
                {
                    "pack": "sam_proxy_procurement_continuity_classified_followon",
                    "rule": "sole_source_follow_on_classified_context",
                    "weight": 2,
                    "field": "snippet",
                    "match": "classified follow-on",
                }
            ],
            created_at=now,
        )
        usa_event = Event(
            category="award",
            source="USAspending",
            hash="usa-export-lineage",
            snippet="prior award",
            doc_id="usa-export-doc",
            piid="EXPORT-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="ACME MISSION SUPPORT",
            recipient_uei="UEI-EXPORT",
            source_url="http://example.com/usa/export",
            raw_json={},
            keywords=[],
            clauses=[],
            created_at=now - timedelta(days=30),
        )
        db.add_all([sam_event, usa_event])
        db.commit()

    rebuild_sam_usaspending_candidate_joins(database_url=db_url)
    snapshot = create_lead_snapshot(
        source="SAM.gov",
        min_score=0,
        limit=10,
        scan_limit=50,
        scoring_version="v2",
        database_url=db_url,
    )

    out_dir = tmp_path / "out"
    result = export_lead_snapshot(snapshot_id=int(snapshot["snapshot_id"]), database_url=db_url, output=out_dir)
    payload = json.loads(result["json"].read_text(encoding="utf-8"))

    assert payload["family_groups"][0]["lead_family"] == "vendor_network_contract_lineage"
    assert payload["count"] == 1

    item = payload["items"][0]
    assert item["lead_family"] == "vendor_network_contract_lineage"
    assert json.loads(item["correlation_types_hit_json"]) == ["sam_usaspending_candidate_join"]

    candidate_join_evidence = json.loads(item["candidate_join_evidence_json"])
    assert candidate_join_evidence[0]["status"] == "candidate"
    assert "identifier_exact" in candidate_join_evidence[0]["evidence_types"]
    assert candidate_join_evidence[0]["linked_records"][0]["source"] == "USAspending"

    linked_source_summary = json.loads(item["linked_source_summary_json"])
    assert linked_source_summary[0]["source"] == "USAspending"

    assignments = json.loads(item["lead_family_assignments_json"])
    assert assignments[0]["family"] == "vendor_network_contract_lineage"
