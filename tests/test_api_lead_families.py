import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1]))

from backend.app import app
from backend.correlate.candidate_joins import rebuild_sam_usaspending_candidate_joins
from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.leads import create_lead_snapshot


def test_api_leads_and_snapshot_items_support_lead_family_grouping(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'api_lead_family.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        sam_event = Event(
            category="notice",
            source="SAM.gov",
            hash="sam-lineage",
            snippet="classified follow-on task order continuity support",
            doc_id="sam-lineage-doc",
            solicitation_number="LINEAGE-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="Acme Mission Support LLC",
            recipient_uei="UEI-LINEAGE",
            source_url="http://example.com/sam/lineage",
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
            hash="usa-lineage",
            snippet="prior award",
            doc_id="usa-lineage-doc",
            piid="LINEAGE-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="ACME MISSION SUPPORT",
            recipient_uei="UEI-LINEAGE",
            source_url="http://example.com/usa/lineage",
            raw_json={},
            keywords=[],
            clauses=[],
            created_at=now - timedelta(days=45),
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

    os.environ["DATABASE_URL"] = db_url
    with TestClient(app) as client:
        response = client.get(
            "/api/leads",
            params={
                "limit": 10,
                "source": "SAM.gov",
                "lead_family": "vendor_network_contract_lineage",
                "group_by_family": True,
                "min_score": 0,
            },
        )
        assert response.status_code == 200
        payload = response.json()

        assert len(payload["items"]) == 1
        item = payload["items"][0]
        assert item["lead_family"] == "vendor_network_contract_lineage"
        assert item["corroboration_summary"]["correlation_types_hit"] == ["sam_usaspending_candidate_join"]
        candidate = item["corroboration_summary"]["candidate_join_evidence"][0]
        assert candidate["status"] == "candidate"
        assert "identifier_exact" in candidate["evidence_types"]
        assert candidate["linked_records"][0]["source"] == "USAspending"
        assert payload["family_groups"][0]["lead_family"] == "vendor_network_contract_lineage"

        snapshot_response = client.get(
            f"/api/lead-snapshots/{snapshot['snapshot_id']}/items",
            params={
                "lead_family": "vendor_network_contract_lineage",
                "group_by_family": True,
            },
        )
        assert snapshot_response.status_code == 200
        snapshot_payload = snapshot_response.json()

    assert len(snapshot_payload["items"]) == 1
    snapshot_item = snapshot_payload["items"][0]
    assert snapshot_item["lead_family"] == "vendor_network_contract_lineage"
    assert snapshot_item["corroboration_summary"]["linked_source_summary"][0]["source"] == "USAspending"
    assert snapshot_payload["family_groups"][0]["lead_family"] == "vendor_network_contract_lineage"
