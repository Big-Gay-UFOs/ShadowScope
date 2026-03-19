import json

from backend.db.models import Correlation, CorrelationLink, Entity, Event, ensure_schema, get_session_factory
from backend.services.export_correlations import export_kw_pairs


def test_export_kw_pairs_writes_investigator_facing_files(tmp_path):
    db_path = tmp_path / "kwpairs.db"
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
            hash="kw_ev_1",
            snippet="alpha beta support",
            place_text="",
            doc_id="d1",
            source_url="http://x/1",
            raw_json={},
            entity_id=entity.id,
            recipient_name="Acme Labs",
            recipient_uei="UEI-123",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            psc_code="R425",
            psc_description="Engineering and Technical Services",
            naics_code="541330",
            naics_description="Engineering Services",
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 5, "field": "snippet", "match": "alpha beta"}],
        )
        ev2 = Event(
            category="award",
            source="USAspending",
            hash="kw_ev_2",
            snippet="alpha beta sustainment",
            place_text="",
            doc_id="d2",
            source_url="http://x/2",
            raw_json={},
            recipient_name="Acme Labs",
            recipient_uei="UEI-123",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            psc_code="R425",
            psc_description="Engineering and Technical Services",
            naics_code="541330",
            naics_description="Engineering Services",
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 6, "field": "snippet", "match": "alpha beta"}],
        )
        db.add_all([ev1, ev2])
        db.flush()

        corr = Correlation(
            correlation_key="kw_pair|USAspending|30|pair:aaaaaaaaaaaaaaaa",
            score="3",
            window_days=30,
            radius_km=0.0,
            lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": 2},
        )
        db.add(corr)
        db.flush()
        db.add_all([
            CorrelationLink(correlation_id=int(corr.id), event_id=int(ev1.id)),
            CorrelationLink(correlation_id=int(corr.id), event_id=int(ev2.id)),
        ])
        db.commit()

    out_dir = tmp_path / "out"
    result = export_kw_pairs(database_url=db_url, output=out_dir, limit=50, min_event_count=2)

    assert result["csv"].exists()
    assert result["json"].exists()

    payload = json.loads(result["json"].read_text(encoding="utf-8"))
    assert payload["count"] == 1
    item = payload["items"][0]
    assert item["score_signal"] == 3
    assert item["event_count"] == 2
    assert item["pair_label"] == "alpha + beta"
    assert item["member_event_hashes"] == ["kw_ev_1", "kw_ev_2"]
    assert item["top_entities"][0]["label"] == "Acme Labs"
    assert item["top_agencies"][0]["label"] == "Department of Energy (DOE)"
    assert item["top_psc"][0]["psc_code"] == "R425"
    assert item["top_naics"][0]["naics_code"] == "541330"
    assert item["matched_ontology_rules"] == ["focus:alpha_beta"]

    csv_text = result["csv"].read_text(encoding="utf-8")
    assert "score_signal" in csv_text
    assert "member_events_json" in csv_text.splitlines()[0]
