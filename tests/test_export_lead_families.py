import json
from datetime import datetime, timedelta, timezone

from backend.correlate.candidate_joins import rebuild_sam_usaspending_candidate_joins
from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory
from backend.services.export_leads import export_lead_snapshot
from backend.services.leads import create_lead_snapshot


def _attach_manual_correlation(
    db,
    *,
    event_ids: list[int],
    lane: str,
    suffix: str,
    lanes_hit: dict[str, object],
    score: str = "5",
    summary: str | None = None,
    rationale: str | None = None,
) -> int:
    correlation = Correlation(
        correlation_key=f"{lane}|SAM.gov|30|{suffix}",
        score=score,
        window_days=30,
        radius_km=0.0,
        lanes_hit={"lane": lane, **lanes_hit},
        summary=summary,
        rationale=rationale,
    )
    db.add(correlation)
    db.commit()
    db.refresh(correlation)
    for event_id in event_ids:
        db.add(CorrelationLink(correlation_id=int(correlation.id), event_id=int(event_id)))
    db.commit()
    return int(correlation.id)


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


def test_export_lead_snapshot_groups_filtered_secondary_family_under_requested_bucket(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'export_lead_family_secondary.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        sam_event = Event(
            category="notice",
            source="SAM.gov",
            hash="sam-export-multi-family",
            snippet="classified follow-on continuity support with DD254 handling",
            doc_id="sam-export-multi-family-doc",
            solicitation_number="EXPORT-MULTI-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="Acme Mission Support LLC",
            recipient_uei="UEI-EXPORT-MULTI",
            source_url="http://example.com/sam/export-multi-family",
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
            hash="usa-export-multi-family",
            snippet="prior award",
            doc_id="usa-export-multi-family-doc",
            piid="EXPORT-MULTI-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="ACME MISSION SUPPORT",
            recipient_uei="UEI-EXPORT-MULTI",
            source_url="http://example.com/usa/export-multi-family",
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

    out_dir = tmp_path / "out_secondary"
    result = export_lead_snapshot(
        snapshot_id=int(snapshot["snapshot_id"]),
        database_url=db_url,
        output=out_dir,
        lead_family="compartmented_support_intel",
    )
    payload = json.loads(result["json"].read_text(encoding="utf-8"))

    assert payload["lead_family_filter"] == "compartmented_support_intel"
    assert payload["count"] == 1
    assert payload["family_groups"][0]["lead_family"] == "compartmented_support_intel"

    item = payload["items"][0]
    assert "compartmented_support_intel" in json.loads(item["secondary_lead_families_json"])


def test_export_lead_snapshot_serializes_primary_secondary_selection_and_distribution(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'export_lead_family_selection.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        sam_event = Event(
            category="notice",
            source="SAM.gov",
            hash="sam-export-ambiguous-family",
            snippet="SCIF upgrade with DD254 handling and continuity follow-on support",
            doc_id="sam-export-ambiguous-family-doc",
            document_id="DOC-AMB-001",
            solicitation_number="EXPORT-AMB-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="Acme Mission Support LLC",
            recipient_uei="UEI-EXPORT-AMB",
            psc_code="R499",
            naics_code="541330",
            place_of_performance_state="CA",
            place_of_performance_country="USA",
            source_url="http://example.com/sam/export-ambiguous-family",
            raw_json={},
            keywords=[
                "sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context",
                "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
                "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context",
            ],
            clauses=[
                {
                    "pack": "sam_proxy_secure_compartmented_facility_engineering",
                    "rule": "icd705_scif_sapf_facility_upgrade_context",
                    "weight": 2,
                    "field": "snippet",
                    "match": "SCIF upgrade",
                },
                {
                    "pack": "sam_proxy_classified_contract_security_admin",
                    "rule": "dd254_classification_guide_contract_context",
                    "weight": 2,
                    "field": "snippet",
                    "match": "DD254",
                },
                {
                    "pack": "sam_proxy_procurement_continuity_classified_followon",
                    "rule": "sole_source_follow_on_classified_context",
                    "weight": 2,
                    "field": "snippet",
                    "match": "follow-on",
                },
            ],
            created_at=now,
        )
        usa_event = Event(
            category="award",
            source="USAspending",
            hash="usa-export-ambiguous-family",
            snippet="linked support award",
            doc_id="usa-export-ambiguous-family-doc",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            recipient_name="ACME MISSION SUPPORT",
            recipient_uei="UEI-EXPORT-AMB",
            naics_code="541330",
            place_of_performance_state="CA",
            place_of_performance_country="USA",
            source_url="http://example.com/usa/export-ambiguous-family",
            raw_json={},
            keywords=[],
            clauses=[],
            created_at=now - timedelta(days=20),
        )
        db.add_all([sam_event, usa_event])
        db.commit()
        db.refresh(sam_event)
        db.refresh(usa_event)

        _attach_manual_correlation(
            db,
            event_ids=[int(sam_event.id)],
            lane="same_doc_id",
            suffix="ambiguous-doc",
            lanes_hit={"event_count": 2},
            summary="same document family",
            rationale="shared document handle",
        )
        _attach_manual_correlation(
            db,
            event_ids=[int(sam_event.id), int(usa_event.id)],
            lane="sam_usaspending_candidate_join",
            suffix="ambiguous-generic-join",
            score="30",
            lanes_hit={
                "confidence_score": 30,
                "likely_incumbent": False,
                "time_delta_days": 20,
                "evidence_types": ["awarding_agency", "naics", "place_region"],
                "matched_values": {},
                "evidence": [
                    {"type": "awarding_agency", "weight": 12, "description": "Awarding agency aligns."},
                    {"type": "naics", "weight": 10, "description": "NAICS aligns."},
                    {"type": "place_region", "weight": 8, "description": "Place aligns."},
                ],
            },
            summary="generic cross-source context",
            rationale="generic cross-source context only",
        )

    snapshot = create_lead_snapshot(
        source="SAM.gov",
        min_score=0,
        limit=10,
        scan_limit=50,
        scoring_version="v2",
        database_url=db_url,
    )

    out_dir = tmp_path / "out_selection"
    result = export_lead_snapshot(snapshot_id=int(snapshot["snapshot_id"]), database_url=db_url, output=out_dir)
    payload = json.loads(result["json"].read_text(encoding="utf-8"))
    review_summary = json.loads(result["review_summary_json"].read_text(encoding="utf-8"))

    primary_distribution = {
        row["lead_family"]: row["count"] for row in payload["family_distribution"]["primary"]
    }
    secondary_distribution = {
        row["lead_family"]: row["count"] for row in payload["family_distribution"]["secondary"]
    }
    any_distribution = {
        row["lead_family"]: row["count"] for row in payload["family_distribution"]["any_assignment"]
    }
    assert primary_distribution == {"facility_security_hardening": 1}
    assert secondary_distribution == {
        "compartmented_support_intel": 1,
        "vendor_network_contract_lineage": 1,
    }
    assert any_distribution == {
        "facility_security_hardening": 1,
        "compartmented_support_intel": 1,
        "vendor_network_contract_lineage": 1,
    }
    assert review_summary["family_distribution"]["ambiguous_items"] == 1

    item = payload["items"][0]
    assert item["lead_family"] == "facility_security_hardening"
    secondaries = json.loads(item["secondary_lead_families_json"])
    assert "compartmented_support_intel" in secondaries
    assert "vendor_network_contract_lineage" in secondaries

    assignments = json.loads(item["lead_family_assignments_json"])
    assert assignments[0]["family"] == "facility_security_hardening"
    assert assignments[0]["score_breakdown"]["specific_corroboration_score"] >= 1
    assert assignments[0]["selection"]["selection_score"] >= assignments[0]["score"]

    selection = json.loads(item["lead_family_selection_json"])
    assert selection["primary_family"] == "facility_security_hardening"
    assert selection["runner_up_family"] in {
        "compartmented_support_intel",
        "vendor_network_contract_lineage",
    }
