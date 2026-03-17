import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.db.models import (
    Correlation,
    CorrelationLink,
    Entity,
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
    ensure_schema,
    get_session_factory,
)
from backend.services.evidence_package import export_evidence_package


def test_export_lead_evidence_package_collects_reviewable_sources(tmp_path: Path):
    db_path = tmp_path / "lead_pkg.db"
    db_url = f"sqlite:///{db_path.as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        entity = Entity(name="Acme Labs", uei="UEI123")
        db.add(entity)
        db.flush()

        e1 = Event(
            category="notice",
            source="SAM.gov",
            hash="lead_pkg_1",
            snippet="alpha beta notice",
            place_text="Northern Virginia",
            doc_id="sam-1",
            source_url="http://example.com/sam/1",
            award_id="AWARD-1",
            recipient_name="Acme Labs",
            recipient_uei="UEI123",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            psc_code="R425",
            naics_code="541330",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            raw_json={},
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 6, "field": "snippet", "match": "alpha beta"}],
            entity_id=entity.id,
            created_at=now - timedelta(days=2),
        )
        e2 = Event(
            category="award",
            source="USAspending",
            hash="lead_pkg_2",
            snippet="alpha beta award",
            place_text="Northern Virginia",
            doc_id="usa-1",
            source_url="http://example.com/usa/1",
            award_id="AWARD-1",
            recipient_name="Acme Labs",
            recipient_uei="UEI123",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            psc_code="R425",
            naics_code="541330",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            raw_json={},
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 5, "field": "snippet", "match": "alpha beta"}],
            entity_id=entity.id,
            created_at=now - timedelta(days=1),
        )
        db.add_all([e1, e2])
        db.flush()

        corr = Correlation(
            correlation_key="kw_pair|SAM.gov|30|pair:testpkg",
            score="5",
            window_days=30,
            radius_km=0.0,
            lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": 2, "score_signal": 5},
            summary="alpha beta cluster",
            rationale="shared keyword pair",
        )
        db.add(corr)
        db.flush()
        db.add_all([
            CorrelationLink(correlation_id=int(corr.id), event_id=int(e1.id)),
            CorrelationLink(correlation_id=int(corr.id), event_id=int(e2.id)),
        ])

        snapshot = LeadSnapshot(source="SAM.gov", min_score=1, limit=10, scoring_version="v2")
        db.add(snapshot)
        db.flush()
        db.add(
            LeadSnapshotItem(
                snapshot_id=int(snapshot.id),
                event_id=int(e1.id),
                event_hash=e1.hash,
                rank=1,
                score=11,
                score_details={"scoring_version": "v2", "pair_bonus": 6},
            )
        )
        db.commit()

    result = export_evidence_package(
        snapshot_id=int(snapshot.id),
        lead_event_id=int(e1.id),
        database_url=db_url,
        output=tmp_path / "lead_package.json",
    )
    payload = json.loads(Path(result["json"]).read_text(encoding="utf-8"))

    assert payload["package_type"] == "lead_evidence_package"
    assert payload["review_guardrails"] == {
        "evidence_only": True,
        "claims_inferred": False,
        "foia_letter_generated": False,
    }
    assert payload["lead"]["event_id"] == int(e1.id)
    assert set(payload["source_record_ids"]) == {int(e1.id), int(e2.id)}
    assert payload["matched_identifiers"]["recipient_ueis"] == ["UEI123"]
    assert payload["matched_ontology_rules"] == ["focus:alpha_beta"]
    assert payload["supporting_correlations"][0]["correlation_id"] == int(corr.id)
    assert len(payload["time_window_summary"]["timeline"]) == 2


def test_export_correlation_evidence_package_stays_descriptive(tmp_path: Path):
    db_path = tmp_path / "corr_pkg.db"
    db_url = f"sqlite:///{db_path.as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        e1 = Event(
            category="notice",
            source="SAM.gov",
            hash="corr_pkg_1",
            snippet="alpha beta notice",
            place_text="Northern Virginia",
            doc_id="sam-1",
            source_url="http://example.com/sam/1",
            recipient_name="Acme Labs",
            recipient_uei="UEI123",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            raw_json={},
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 4}],
            created_at=now - timedelta(days=2),
        )
        e2 = Event(
            category="award",
            source="USAspending",
            hash="corr_pkg_2",
            snippet="alpha beta award",
            place_text="Northern Virginia",
            doc_id="usa-1",
            source_url="http://example.com/usa/1",
            recipient_name="Acme Labs",
            recipient_uei="UEI123",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            raw_json={},
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 5}],
            created_at=now - timedelta(days=1),
        )
        db.add_all([e1, e2])
        db.flush()

        corr = Correlation(
            correlation_key="kw_pair|SAM.gov|30|pair:testpkg",
            score="5",
            window_days=30,
            radius_km=0.0,
            lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": 2, "score_signal": 5},
            summary="alpha beta cluster",
            rationale="shared keyword pair",
        )
        db.add(corr)
        db.flush()
        db.add_all([
            CorrelationLink(correlation_id=int(corr.id), event_id=int(e1.id)),
            CorrelationLink(correlation_id=int(corr.id), event_id=int(e2.id)),
        ])
        db.commit()

    result = export_evidence_package(
        correlation_id=int(corr.id),
        database_url=db_url,
        output=tmp_path / "corr_package.json",
    )
    payload = json.loads(Path(result["json"]).read_text(encoding="utf-8"))

    assert payload["package_type"] == "correlation_evidence_package"
    assert payload["review_guardrails"]["foia_letter_generated"] is False
    assert payload["correlation"]["lane"] == "kw_pair"
    assert payload["correlation"]["score_signal"] == 5
    assert payload["matched_identifiers"]["recipient_ueis"] == ["UEI123"]
    assert len(payload["source_records"]) == 2
    assert payload["source_urls"] == ["http://example.com/sam/1", "http://example.com/usa/1"]
