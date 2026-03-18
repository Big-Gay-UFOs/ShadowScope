import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.append(str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from backend.app import app
from backend.db.models import Correlation, CorrelationLink, Event, LeadSnapshotItem, ensure_schema, get_session_factory
from backend.services.leads import create_lead_snapshot


def test_api_leads_defaults_to_v2_and_supports_v1_and_v3(tmp_path):
    db_path = tmp_path / "api_leads.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        event = Event(
            category="award",
            source="USAspending",
            hash="api_test_hash_1",
            snippet="x",
            place_text="",
            doc_id="d1",
            source_url="http://example.com",
            raw_json={},
            keywords=["k1"],
            clauses=[],
        )
        db.add(event)
        db.commit()

    os.environ["DATABASE_URL"] = db_url

    with TestClient(app) as client:
        response = client.get("/api/leads?limit=10")
        assert response.status_code == 200
        payload = response.json()
        assert payload and payload[0]["score_details"]["scoring_version"] == "v2"
        assert payload[0]["scoring_version"] == "v2"

        legacy = client.get("/api/leads?limit=10&scoring_version=v1")
        assert legacy.status_code == 200
        payload_v1 = legacy.json()
        assert payload_v1 and payload_v1[0]["score_details"]["scoring_version"] == "v1"
        assert payload_v1[0]["scoring_version"] == "v1"

        v3 = client.get("/api/leads?limit=10&scoring_version=v3")
        assert v3.status_code == 200
        payload_v3 = v3.json()
        assert payload_v3 and payload_v3[0]["score_details"]["scoring_version"] == "v3"
        assert payload_v3[0]["scoring_version"] == "v3"


def test_api_leads_and_snapshot_scoring_agree_on_identical_input(tmp_path):
    db_path = tmp_path / "api_snapshot_agreement.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        e1 = Event(
            category="award",
            source="USAspending",
            hash="api_snapshot_hash_1",
            snippet="alpha beta procurement",
            place_text="",
            doc_id="d1",
            source_url="http://example.com/1",
            raw_json={},
            keywords=["alpha", "beta"],
            clauses=[{"pack": "investigation", "rule": "alpha_pair", "weight": 7, "field": "snippet", "match": "alpha beta"}],
        )
        e2 = Event(
            category="award",
            source="USAspending",
            hash="api_snapshot_hash_2",
            snippet="gamma only",
            place_text="",
            doc_id="d2",
            source_url="http://example.com/2",
            raw_json={},
            keywords=["gamma"],
            clauses=[],
        )
        db.add_all([e1, e2])
        db.commit()
        db.refresh(e1)
        db.refresh(e2)

        correlation = Correlation(
            correlation_key="kw_pair|USAspending|30|pair:aaaaaaaaaaaaaaaa",
            score="3",
            window_days=30,
            radius_km=0.0,
            lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": 3},
        )
        db.add(correlation)
        db.commit()
        db.refresh(correlation)

        db.add(CorrelationLink(correlation_id=int(correlation.id), event_id=int(e1.id)))
        db.commit()

    snapshot = create_lead_snapshot(
        source="USAspending",
        min_score=0,
        limit=10,
        scan_limit=50,
        scoring_version="v2",
        database_url=db_url,
    )

    with SessionFactory() as db:
        snapshot_items = (
            db.query(LeadSnapshotItem)
            .filter(LeadSnapshotItem.snapshot_id == int(snapshot["snapshot_id"]))
            .order_by(LeadSnapshotItem.rank.asc())
            .all()
        )
        expected = [
            {
                "event_id": int(item.event_id),
                "event_hash": item.event_hash,
                "score": int(item.score),
                "score_details": item.score_details,
            }
            for item in snapshot_items
        ]

    os.environ["DATABASE_URL"] = db_url
    with TestClient(app) as client:
        response = client.get("/api/leads?limit=10&scan_limit=50&min_score=0&source=USAspending")
        assert response.status_code == 200
        payload = response.json()

    assert [item["id"] for item in payload] == [row["event_id"] for row in expected]
    assert [item["score"] for item in payload] == [row["score"] for row in expected]
    assert [item["score_details"] for item in payload] == [row["score_details"] for row in expected]
    assert all(item.get("scoring_version") == "v2" for item in payload)
    assert payload[0]["score_details"]["pair_bonus_applied"] == payload[0]["score_details"]["pair_bonus"]
    assert "kw_pair" in payload[0]["score_details"]["contributing_lanes"]
    assert payload[0]["score_details"]["matched_ontology_rules"] == ["investigation:alpha_pair"]


def test_api_leads_supports_investigator_filters_and_sorting(tmp_path):
    db_path = tmp_path / "api_leads_filters.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        e1 = Event(
            category="notice",
            source="SAM.gov",
            hash="lead_filter_1",
            snippet="alpha beta DOE item",
            place_text="Northern Virginia",
            doc_id="sam-1",
            award_id="AWARD-LEAD-1",
            recipient_uei="UEI-LEAD-1",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            psc_code="R425",
            psc_description="Engineering and Technical Services",
            naics_code="541330",
            naics_description="Engineering Services",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            source_url="http://example.com/sam/1",
            raw_json={},
            keywords=["alpha", "beta"],
            clauses=[{"pack": "investigation", "rule": "alpha_pair", "weight": 5, "field": "snippet", "match": "alpha beta"}],
            created_at=now - timedelta(hours=3),
        )
        e2 = Event(
            category="notice",
            source="SAM.gov",
            hash="lead_filter_2",
            snippet="alpha only item",
            place_text="Maryland",
            doc_id="sam-2",
            recipient_uei="UEI-LEAD-2",
            awarding_agency_code="NASA",
            awarding_agency_name="National Aeronautics and Space Administration",
            psc_code="R499",
            naics_code="541512",
            place_of_performance_state="MD",
            place_of_performance_country="USA",
            source_url="http://example.com/sam/2",
            raw_json={},
            keywords=["alpha"],
            clauses=[],
            created_at=now - timedelta(hours=1),
        )
        db.add_all([e1, e2])
        db.commit()
        db.refresh(e1)

        corr = Correlation(
            correlation_key="kw_pair|SAM.gov|30|pair:testpair",
            score="7",
            window_days=30,
            radius_km=0.0,
            lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": 4, "score_signal": 7},
        )
        db.add(corr)
        db.commit()
        db.refresh(corr)
        db.add(CorrelationLink(correlation_id=int(corr.id), event_id=int(e1.id)))
        db.commit()

    os.environ["DATABASE_URL"] = db_url

    with TestClient(app) as client:
        response = client.get(
            "/api/leads",
            params={
                "limit": 10,
                "min_score": 0,
                "source": "SAM.gov",
                "agency": "Department of Energy",
                "award_id": "AWARD-LEAD-1",
                "recipient_uei": "uei-lead-1",
                "place_region": "VA,USA",
                "lane": "kw_pair",
                "min_event_count": 4,
                "min_score_signal": 7,
                "sort_by": "created_at",
                "sort_dir": "asc",
            },
        )
        assert response.status_code == 200
        payload = response.json()

    assert [item["id"] for item in payload] == [1]
    assert payload[0]["place_region"] == "VA, USA"
    assert payload[0]["score_details"]["contributing_correlations"][0]["score_signal"] == 7
