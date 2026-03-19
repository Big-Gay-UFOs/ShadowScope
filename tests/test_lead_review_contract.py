import csv
import json
import os
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from backend.app import app
from backend.db.models import (
    Correlation,
    CorrelationLink,
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
    ensure_schema,
    get_session_factory,
)
from backend.services.export_leads import export_lead_snapshot
from backend.services.review_contract import (
    CANONICAL_RANKED_LEAD_REVIEW_FIELDS,
    RANKED_LEAD_REVIEW_CONTRACT_VERSION,
    review_effective_window,
)


def _seed_review_contract_snapshot(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'lead_review_contract.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        notice_event = Event(
            category="notice",
            source="SAM.gov",
            hash="review-notice",
            doc_id="notice-doc-001",
            source_url="http://example.com/review/notice",
            snippet="classified follow-on continuity support for secure program operations",
            raw_json={},
            keywords=[
                "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context"
            ],
            clauses=[
                {
                    "pack": "sam_proxy_procurement_continuity_classified_followon",
                    "rule": "sole_source_follow_on_classified_context",
                    "weight": 3,
                    "field": "snippet",
                    "match": "classified follow-on continuity support",
                }
            ],
            occurred_at=now - timedelta(days=2),
            created_at=now - timedelta(days=1),
            place_text="Northern Virginia",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            solicitation_number="NOTICE-001",
            notice_id="NOTICE-ID-001",
            document_id="DOC-N-001",
            source_record_id="SRC-N-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            contracting_office_code="DOE-42",
            contracting_office_name="DOE Procurement Office",
            psc_code="R425",
            psc_description="Engineering and Technical Services",
            naics_code="541330",
            naics_description="Engineering Services",
        )
        award_event = Event(
            category="award",
            source="USAspending",
            hash="review-award",
            doc_id="award-doc-001",
            source_url="http://example.com/review/award",
            snippet="prior award for continuity support",
            raw_json={},
            keywords=[],
            clauses=[],
            occurred_at=now - timedelta(days=20),
            created_at=now - timedelta(days=20),
            place_text="Albuquerque",
            place_of_performance_state="NM",
            place_of_performance_country="USA",
            award_id="AWARD-001",
            piid="PIID-001",
            generated_unique_award_id="GUA-001",
            source_record_id="SRC-A-001",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            funding_agency_code="NNSA",
            funding_agency_name="National Nuclear Security Administration",
            contracting_office_code="NNSA-7",
            contracting_office_name="NNSA Field Office",
            recipient_name="Acme Mission Support LLC",
            recipient_uei="UEI-REVIEW-001",
            recipient_parent_uei="PARENT-UEI-001",
            recipient_duns="DUNS-001",
            recipient_cage_code="CAGE-001",
            psc_code="R425",
            psc_description="Engineering and Technical Services",
            naics_code="541330",
            naics_description="Engineering Services",
        )
        db.add_all([notice_event, award_event])
        db.commit()
        db.refresh(notice_event)
        db.refresh(award_event)

        candidate_join = Correlation(
            correlation_key="sam_usaspending_candidate_join|SAM.gov|365|notice-001",
            score="72",
            window_days=365,
            radius_km=0.0,
            lanes_hit={
                "lane": "sam_usaspending_candidate_join",
                "event_count": 2,
                "score_signal": 72,
                "likely_incumbent": True,
                "evidence_types": ["identifier_exact", "contract_family"],
                "matched_values": {"solicitation_number": "NOTICE-001", "piid": "PIID-001"},
                "evidence": [
                    {"kind": "identifier_exact", "value": "NOTICE-001"},
                    {"kind": "contract_family", "value": "continuity_support"},
                ],
            },
        )
        db.add(candidate_join)
        db.commit()
        db.refresh(candidate_join)

        db.add_all(
            [
                CorrelationLink(correlation_id=int(candidate_join.id), event_id=int(notice_event.id)),
                CorrelationLink(correlation_id=int(candidate_join.id), event_id=int(award_event.id)),
            ]
        )
        db.commit()

        snapshot = LeadSnapshot(source=None, min_score=0, scoring_version="v3")
        db.add(snapshot)
        db.commit()
        db.refresh(snapshot)

        db.add_all(
            [
                LeadSnapshotItem(
                    snapshot_id=int(snapshot.id),
                    event_id=int(notice_event.id),
                    event_hash=notice_event.hash,
                    rank=1,
                    score=22,
                    score_details={
                        "scoring_version": "v3",
                        "proxy_relevance_score": 9,
                        "investigability_score": 4,
                        "corroboration_score": 7,
                        "structural_context_score": 3,
                        "noise_penalty": 1,
                        "total_score": 22,
                        "top_positive_signals": [
                            {
                                "label": "classified follow-on continuity",
                                "bucket": "proxy_relevance",
                                "signal_type": "clause",
                                "contribution": 6,
                            }
                        ],
                        "top_suppressors": [
                            {
                                "label": "routine admin noise",
                                "signal_type": "keyword",
                                "penalty": 1,
                            }
                        ],
                    },
                ),
                LeadSnapshotItem(
                    snapshot_id=int(snapshot.id),
                    event_id=int(award_event.id),
                    event_hash=award_event.hash,
                    rank=2,
                    score=14,
                    score_details={
                        "scoring_version": "v3",
                        "proxy_relevance_score": 4,
                        "investigability_score": 4,
                        "corroboration_score": 4,
                        "structural_context_score": 2,
                        "noise_penalty": 0,
                        "total_score": 14,
                    },
                ),
            ]
        )
        db.commit()

    return db_url, int(snapshot.id)


def test_export_lead_snapshot_uses_canonical_review_contract(tmp_path):
    db_url, snapshot_id = _seed_review_contract_snapshot(tmp_path)

    result = export_lead_snapshot(
        snapshot_id=snapshot_id,
        database_url=db_url,
        output=tmp_path / "review_export",
    )
    payload = json.loads(result["json"].read_text(encoding="utf-8"))

    assert payload["review_contract_version"] == RANKED_LEAD_REVIEW_CONTRACT_VERSION
    assert payload["canonical_review_fields"] == list(CANONICAL_RANKED_LEAD_REVIEW_FIELDS)
    assert payload["count"] == 2

    items = payload["items"]
    notice_item = next(item for item in items if item["category"] == "notice")
    award_item = next(item for item in items if item["category"] == "award")

    assert set(CANONICAL_RANKED_LEAD_REVIEW_FIELDS).issubset(notice_item.keys())
    assert set(CANONICAL_RANKED_LEAD_REVIEW_FIELDS).issubset(award_item.keys())

    assert notice_item["lead_family"] == "vendor_network_contract_lineage"
    assert notice_item["scoring_version"] == "v3"
    assert notice_item["score"] == 22
    assert notice_item["why_summary"]
    assert notice_item["score_details"]["scoring_version"] == "v3"
    assert notice_item["top_positive_signals"][0]["contribution"] == 6
    assert notice_item["top_suppressors"][0]["penalty"] == 1
    assert notice_item["candidate_join_evidence"][0]["status"] == "candidate"
    assert notice_item["linked_source_summary"][0]["source"] == "USAspending"
    assert notice_item["award_id"] is None
    assert notice_item["piid"] is None
    assert notice_item["generated_unique_award_id"] is None
    assert notice_item["recipient_name"] is None
    assert notice_item["has_core_identifiers"] is True
    assert notice_item["has_agency_target"] is True
    assert notice_item["has_vendor_context"] is False
    assert notice_item["has_classification_context"] is True
    assert notice_item["has_foia_handles"] is True
    assert "has_vendor_context" in notice_item["completeness_summary"]["missing_context_categories"]

    assert award_item["solicitation_number"] is None
    assert award_item["notice_id"] is None
    assert award_item["recipient_name"] == "Acme Mission Support LLC"
    assert award_item["vendor_name"] == "Acme Mission Support LLC"
    assert award_item["vendor_uei"] == "UEI-REVIEW-001"
    assert award_item["award_id"] == "AWARD-001"
    assert award_item["piid"] == "PIID-001"
    assert award_item["generated_unique_award_id"] == "GUA-001"
    assert award_item["has_vendor_context"] is True

    with result["csv"].open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert "score_details" in rows[0]
    assert "top_positive_signals" in rows[0]
    assert "candidate_join_evidence" in rows[0]
    assert "linked_source_summary" in rows[0]
    assert "corroboration_summary" in rows[0]
    assert "completeness_summary" in rows[0]
    assert "has_core_identifiers" in rows[0]

    notice_csv = next(row for row in rows if row["category"] == "notice")
    assert notice_csv["award_id"] == ""
    assert notice_csv["has_vendor_context"] == "False"
    assert json.loads(notice_csv["candidate_join_evidence"])[0]["status"] == "candidate"
    assert json.loads(notice_csv["score_details"])["scoring_version"] == "v3"


def test_export_lead_snapshot_writes_review_summary(tmp_path):
    db_url, snapshot_id = _seed_review_contract_snapshot(tmp_path)

    result = export_lead_snapshot(
        snapshot_id=snapshot_id,
        database_url=db_url,
        output=tmp_path / "review_summary_export",
    )
    summary_payload = json.loads(result["review_summary_json"].read_text(encoding="utf-8"))

    assert summary_payload["review_contract_version"] == RANKED_LEAD_REVIEW_CONTRACT_VERSION
    assert summary_payload["snapshot_id"] == snapshot_id
    assert summary_payload["scoring_version"] == "v3"
    assert summary_payload["effective_window"]["earliest"] is not None
    assert summary_payload["review_artifact_filenames"]["lead_snapshot_csv"] == result["csv"].name
    assert summary_payload["review_artifact_filenames"]["lead_snapshot_json"] == result["json"].name
    assert summary_payload["review_artifact_filenames"]["review_summary_json"] == result["review_summary_json"].name
    assert summary_payload["evidence_package_availability"]["available"] is True
    assert summary_payload["evidence_package_availability"]["snapshot_id"] == snapshot_id
    assert summary_payload["completeness_counts"]["has_vendor_context"] == 1


def test_review_effective_window_normalizes_naive_and_aware_timestamps():
    summary = review_effective_window(
        [
            {"occurred_at": "2026-03-01T00:00:00"},
            {"created_at": "2026-03-02T00:00:00+00:00"},
            {"occurred_at": "2026-03-03T00:00:00Z"},
            {"occurred_at": "not-a-date"},
        ]
    )

    assert summary["earliest"] == "2026-03-01T00:00:00+00:00"
    assert summary["latest"] == "2026-03-03T00:00:00+00:00"
    assert summary["span_days"] == 2


def test_export_lead_snapshot_preserves_snapshot_event_hash(tmp_path):
    db_url, snapshot_id = _seed_review_contract_snapshot(tmp_path)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        notice_event = db.query(Event).filter(Event.hash == "review-notice").one()
        notice_item = (
            db.query(LeadSnapshotItem)
            .filter(LeadSnapshotItem.snapshot_id == snapshot_id)
            .filter(LeadSnapshotItem.event_id == int(notice_event.id))
            .one()
        )
        notice_event.hash = "review-notice-renormalized"
        db.commit()
        db.refresh(notice_item)

    result = export_lead_snapshot(
        snapshot_id=snapshot_id,
        database_url=db_url,
        output=tmp_path / "review_hash_export",
    )
    payload = json.loads(result["json"].read_text(encoding="utf-8"))
    notice_item = next(item for item in payload["items"] if item["category"] == "notice")

    assert notice_item["event_hash"] == "review-notice"


def test_api_ranked_lead_routes_share_review_contract_fields(tmp_path):
    db_url, snapshot_id = _seed_review_contract_snapshot(tmp_path)
    os.environ["DATABASE_URL"] = db_url

    with TestClient(app) as client:
        snapshot_response = client.get(f"/api/lead-snapshots/{snapshot_id}/items")
        assert snapshot_response.status_code == 200
        snapshot_items = snapshot_response.json()

        leads_response = client.get("/api/leads", params={"min_score": 0, "limit": 10, "scoring_version": "v3"})
        assert leads_response.status_code == 200
        lead_items = leads_response.json()

    snapshot_notice = next(item for item in snapshot_items if item["category"] == "notice")
    assert set(CANONICAL_RANKED_LEAD_REVIEW_FIELDS).issubset(snapshot_notice.keys())
    assert snapshot_notice["candidate_join_evidence"][0]["status"] == "candidate"
    assert snapshot_notice["linked_source_summary"][0]["source"] == "USAspending"
    assert snapshot_notice["has_vendor_context"] is False
    assert snapshot_notice["score_details"]["scoring_version"] == "v3"

    lead_notice = next(item for item in lead_items if item["category"] == "notice")
    assert set(CANONICAL_RANKED_LEAD_REVIEW_FIELDS).issubset(lead_notice.keys())
    assert lead_notice["rank"] == 1
    assert lead_notice["scoring_version"] == "v3"
    assert "score_details" in lead_notice
