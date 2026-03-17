import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from backend.app import app
from backend.db.models import Correlation, CorrelationLink, Entity, Event, ensure_schema, get_session_factory


def test_api_kw_pair_clusters_are_investigator_facing(tmp_path):
    db_path = tmp_path / "api_kw_pair_clusters.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        entity = Entity(name="Acme Labs", uei="UEI-123")
        db.add(entity)
        db.flush()

        ev1 = Event(
            category="award",
            source="USAspending",
            hash="corr_ev_1",
            snippet="alpha beta support",
            place_text="Northern Virginia",
            doc_id="d1",
            source_url="http://x/1",
            raw_json={},
            entity_id=entity.id,
            recipient_name="Acme Labs",
            recipient_uei="UEI-123",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            psc_code="R425",
            psc_description="Engineering and Technical Services",
            naics_code="541330",
            naics_description="Engineering Services",
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 5}],
        )
        ev2 = Event(
            category="award",
            source="USAspending",
            hash="corr_ev_2",
            snippet="alpha beta sustainment",
            place_text="Northern Virginia",
            doc_id="d2",
            source_url="http://x/2",
            raw_json={},
            recipient_name="Acme Labs",
            recipient_uei="UEI-123",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            psc_code="R425",
            psc_description="Engineering and Technical Services",
            naics_code="541330",
            naics_description="Engineering Services",
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 6}],
        )
        db.add_all([ev1, ev2])
        db.flush()

        corr = Correlation(
            correlation_key="kw_pair|USAspending|30|pair:aaaaaaaaaaaaaaaa",
            score="4",
            window_days=30,
            radius_km=0.0,
            lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": 2, "score_signal": 4},
        )
        db.add(corr)
        db.flush()
        db.add_all([
            CorrelationLink(correlation_id=int(corr.id), event_id=int(ev1.id)),
            CorrelationLink(correlation_id=int(corr.id), event_id=int(ev2.id)),
        ])
        db.commit()

    os.environ["DATABASE_URL"] = db_url
    with TestClient(app) as client:
        response = client.get(
            "/api/correlations/",
            params={
                "lane": "kw_pair",
                "source": "USAspending",
                "keyword": "alpha",
                "recipient_uei": "uei-123",
                "place_region": "VA,USA",
                "min_score_signal": 4,
                "sort_by": "event_count",
                "limit": 10,
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["total"] == 1
        item = payload["items"][0]
        assert item["score_signal"] == 4
        assert item["event_count"] == 2
        assert item["matched_event_count"] == 2
        assert item["total_event_count"] == 2
        assert item["pair_label"] == "alpha + beta"
        assert item["top_entities"][0]["label"] == "Acme Labs"
        assert item["top_agencies"][0]["label"] == "Department of Energy (DOE)"
        assert item["top_psc"][0]["psc_code"] == "R425"
        assert item["top_naics"][0]["naics_code"] == "541330"
        assert item["matched_ontology_rules"] == ["focus:alpha_beta"]

        detail = client.get(f"/api/correlations/{item['correlation_id']}")
        assert detail.status_code == 200
        detail_payload = detail.json()
        assert detail_payload["member_event_hashes"] == ["corr_ev_1", "corr_ev_2"]
        assert len(detail_payload["member_events"]) == 2
