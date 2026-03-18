import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1]))

from backend.app import app
from backend.correlate.candidate_joins import rebuild_sam_usaspending_candidate_joins
from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory
from backend.services.leads import create_lead_snapshot


def _make_family_event(*, event_hash: str, pack: str, rule: str, created_at: datetime) -> Event:
    return Event(
        category="notice",
        source="SAM.gov",
        hash=event_hash,
        snippet=f"{pack} {rule}",
        doc_id=f"{event_hash}-doc",
        source_url=f"http://example.com/{event_hash}",
        raw_json={},
        keywords=[f"{pack}:{rule}"],
        clauses=[
            {
                "pack": pack,
                "rule": rule,
                "weight": 2,
                "field": "snippet",
                "match": f"{pack}:{rule}",
            }
        ],
        created_at=created_at,
    )


def _attach_lane(db, *, event_id: int, lane: str, suffix: str) -> None:
    correlation = Correlation(
        correlation_key=f"{lane}|SAM.gov|30|{suffix}",
        score="5",
        window_days=30,
        radius_km=0.0,
        lanes_hit={"lane": lane, "event_count": 2},
    )
    db.add(correlation)
    db.commit()
    db.refresh(correlation)
    db.add(CorrelationLink(correlation_id=int(correlation.id), event_id=int(event_id)))
    db.commit()


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


def test_api_family_groups_follow_secondary_family_filters(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'api_lead_family_secondary.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        sam_event = Event(
            category="notice",
            source="SAM.gov",
            hash="sam-multi-family",
            snippet="classified follow-on continuity support with DD254 handling",
            doc_id="sam-multi-family-doc",
            solicitation_number="MULTI-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="Acme Mission Support LLC",
            recipient_uei="UEI-MULTI",
            source_url="http://example.com/sam/multi-family",
            raw_json={},
            keywords=[
                "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context",
                "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
            ],
            clauses=[
                {
                    "pack": "sam_proxy_procurement_continuity_classified_followon",
                    "rule": "sole_source_follow_on_classified_context",
                    "weight": 2,
                    "field": "snippet",
                    "match": "classified follow-on",
                },
                {
                    "pack": "sam_proxy_classified_contract_security_admin",
                    "rule": "dd254_classification_guide_contract_context",
                    "weight": 2,
                    "field": "snippet",
                    "match": "DD254",
                },
            ],
            created_at=now,
        )
        usa_event = Event(
            category="award",
            source="USAspending",
            hash="usa-multi-family",
            snippet="prior award",
            doc_id="usa-multi-family-doc",
            piid="MULTI-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="ACME MISSION SUPPORT",
            recipient_uei="UEI-MULTI",
            source_url="http://example.com/usa/multi-family",
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

    os.environ["DATABASE_URL"] = db_url
    with TestClient(app) as client:
        response = client.get(
            "/api/leads",
            params={
                "limit": 10,
                "source": "SAM.gov",
                "lead_family": "compartmented_support_intel",
                "group_by_family": True,
                "min_score": 0,
            },
        )
        assert response.status_code == 200
        payload = response.json()

        assert len(payload["items"]) == 1
        item = payload["items"][0]
        assert "compartmented_support_intel" in item["secondary_lead_families"]
        assert payload["family_groups"][0]["lead_family"] == "compartmented_support_intel"

        snapshot_response = client.get(
            f"/api/lead-snapshots/{snapshot['snapshot_id']}/items",
            params={
                "lead_family": "compartmented_support_intel",
                "group_by_family": True,
            },
        )
        assert snapshot_response.status_code == 200
        snapshot_payload = snapshot_response.json()

    assert len(snapshot_payload["items"]) == 1
    snapshot_item = snapshot_payload["items"][0]
    assert "compartmented_support_intel" in snapshot_item["secondary_lead_families"]
    assert snapshot_payload["family_groups"][0]["lead_family"] == "compartmented_support_intel"


def test_snapshot_family_groups_summarize_full_filtered_snapshot_before_limit(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'api_lead_family_limit.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        vendor_event = _make_family_event(
            event_hash="vendor-family-event",
            pack="sam_proxy_procurement_continuity_classified_followon",
            rule="sole_source_follow_on_classified_context",
            created_at=now,
        )
        facility_event = _make_family_event(
            event_hash="facility-family-event",
            pack="sam_proxy_secure_compartmented_facility_engineering",
            rule="icd705_scif_sapf_facility_upgrade_context",
            created_at=now - timedelta(minutes=1),
        )
        db.add_all([vendor_event, facility_event])
        db.commit()
        db.refresh(vendor_event)
        db.refresh(facility_event)
        _attach_lane(db, event_id=int(vendor_event.id), lane="same_contract_id", suffix="vendor-family")
        _attach_lane(db, event_id=int(facility_event.id), lane="same_agency", suffix="facility-family")

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
            f"/api/lead-snapshots/{snapshot['snapshot_id']}/items",
            params={
                "limit": 1,
                "group_by_family": True,
            },
        )
        assert response.status_code == 200
        payload = response.json()

    assert len(payload["items"]) == 1
    assert {group["lead_family"] for group in payload["family_groups"]} == {
        "facility_security_hardening",
        "vendor_network_contract_lineage",
    }
