from datetime import datetime, timezone

import pytest

from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory
from backend.services.leads import compute_leads


def _make_event(*, event_hash: str, pack: str, rule: str, created_at: datetime) -> Event:
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


@pytest.mark.parametrize(
    ("pack", "rule", "lane", "expected_family"),
    [
        (
            "sam_proxy_secure_compartmented_facility_engineering",
            "icd705_scif_sapf_facility_upgrade_context",
            "same_agency",
            "facility_security_hardening",
        ),
        (
            "sam_proxy_materials_exploitation_forensics",
            "materials_forensic_lab_context",
            "kw_pair",
            "exploitation_materials_handling",
        ),
        (
            "sam_proxy_maritime_remote_recovery_systems",
            "rov_lars_heavy_lift_context",
            "same_place_region",
            "undersea_recovery_salvage",
        ),
        (
            "sam_proxy_classified_contract_security_admin",
            "dd254_classification_guide_contract_context",
            "same_doc_id",
            "compartmented_support_intel",
        ),
        (
            "sam_dod_flight_test_range_instrumentation",
            "edwards_412th_plant42_range_context",
            "same_place_region",
            "range_test_infrastructure",
        ),
        (
            "sam_proxy_procurement_continuity_classified_followon",
            "sole_source_follow_on_classified_context",
            "same_contract_id",
            "vendor_network_contract_lineage",
        ),
    ],
)
def test_compute_leads_assigns_expected_primary_family(tmp_path, pack: str, rule: str, lane: str, expected_family: str):
    db_url = f"sqlite:///{(tmp_path / f'{expected_family}.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        event = _make_event(
            event_hash=f"{expected_family}-event",
            pack=pack,
            rule=rule,
            created_at=now,
        )
        db.add(event)
        db.commit()
        db.refresh(event)
        _attach_lane(db, event_id=int(event.id), lane=lane, suffix=f"{expected_family}-lane")

    with SessionFactory() as db:
        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=0,
            limit=10,
            scan_limit=50,
            scoring_version="v2",
        )

    assert scanned == 1
    assert len(ranked) == 1

    score, event, details = ranked[0]
    assert score >= 0
    assert event.hash == f"{expected_family}-event"
    assert details["lead_family"] == expected_family
    assert details["lead_family_assignments"][0]["family"] == expected_family
    assert details["lead_family_assignments"][0]["ontology_matches"]
    assert any(match.get("kind") == "lane" and match.get("lane") == lane for match in details["lead_family_assignments"][0]["corroboration_matches"])
    assert lane in details["corroboration_summary"]["correlation_types_hit"]
