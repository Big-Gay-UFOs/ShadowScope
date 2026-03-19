from datetime import datetime, timezone

from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory
from backend.services.leads import compute_leads


def _seed_kw_pair_link(db, event_id: int, key_suffix: str = "abcd", score: str = "4") -> None:
    corr = Correlation(
        correlation_key=f"kw_pair|SAM.gov|30|pair:{key_suffix}",
        score=score,
        window_days=30,
        radius_km=0.0,
        lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": int(score)},
    )
    db.add(corr)
    db.commit()
    db.refresh(corr)

    db.add(CorrelationLink(correlation_id=int(corr.id), event_id=int(event_id)))
    db.commit()


def test_compute_leads_adds_foia_matrix_metadata_and_bonus(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_foia_matrix.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        ev = Event(
            category="opportunity",
            source="SAM.gov",
            hash="lead_foia_1",
            created_at=now,
            snippet="DoD contextual lead",
            raw_json={},
            keywords=[
                "sam_dod_program_protection_sap:afosi_program_security_context",
                "sam_dod_flight_test_range_instrumentation:edwards_412th_plant42_range_context",
                "sam_dod_intel_recovery_undersea_support:intel_org_support_context",
                "sam_dod_advanced_aerospace_support:low_observable_rcs_support_context",
            ],
            clauses=[],
        )
        db.add(ev)
        db.commit()
        db.refresh(ev)

        _seed_kw_pair_link(db, int(ev.id), key_suffix="foia1111", score="4")

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

    score, _event, details = ranked[0]
    assert details["dod_lane_count"] == 4
    assert details["dod_keyword_hit_count"] == 4
    assert details["pair_count"] == 1
    assert details["foia_matrix_bonus"] == 3
    assert details["foia_potential_tier"] == "high"
    assert score == 18


def test_compute_leads_keeps_noise_penalty_while_exposing_foia_metadata(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_foia_noise.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        ev = Event(
            category="opportunity",
            source="SAM.gov",
            hash="lead_foia_noise_1",
            created_at=now,
            snippet="DoD contextual lead with commodity noise",
            raw_json={},
            keywords=[
                "sam_dod_program_protection_sap:afosi_program_security_context",
                "sam_dod_flight_test_range_instrumentation:edwards_412th_plant42_range_context",
                "sam_dod_intel_recovery_undersea_support:intel_org_support_context",
                "sam_dod_advanced_aerospace_support:low_observable_rcs_support_context",
                "operational_noise_terms:nsn_line_item_commodity_noise",
            ],
            clauses=[],
        )
        db.add(ev)
        db.commit()
        db.refresh(ev)

        _seed_kw_pair_link(db, int(ev.id), key_suffix="foia2222", score="4")

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

    score, _event, details = ranked[0]
    assert details["has_noise"] is True
    assert details["noise_penalty"] == 8
    assert details["pair_bonus"] == 2
    assert details["dod_lane_count"] == 4
    assert details["dod_keyword_hit_count"] == 4
    assert details["foia_matrix_bonus"] == 1
    assert details["foia_potential_tier"] == "high"
    assert score == 10


def test_compute_leads_v3_exposes_structural_context_and_lead_family(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_v3.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        rich = Event(
            category="opportunity",
            source="SAM.gov",
            hash="lead_v3_rich",
            created_at=now,
            snippet="Structurally rich SAM lead",
            raw_json={
                "noticeType": "Sources Sought",
                "solicitationNumber": "DOE-V3-001",
                "naicsCode": "541330",
                "typeOfSetAside": "SBA",
                "responseDeadLine": "2026-03-20",
                "fullParentPathCode": "DOE.HQ",
                "Recipient Name": "Acme Federal",
            },
            keywords=["sam_dod_program_protection_sap:afosi_program_security_context"],
            clauses=[],
        )
        thin = Event(
            category="opportunity",
            source="SAM.gov",
            hash="lead_v3_thin",
            created_at=now,
            snippet="Thin SAM lead",
            raw_json={},
            keywords=["sam_dod_program_protection_sap:afosi_program_security_context"],
            clauses=[],
        )
        db.add_all([rich, thin])
        db.commit()

        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=0,
            limit=10,
            scan_limit=50,
            scoring_version="v3",
        )

    assert scanned == 2
    assert len(ranked) == 2

    top_score, top_event, top_details = ranked[0]
    assert top_event.hash == "lead_v3_rich"
    assert top_details["scoring_version"] == "v3"
    assert top_details["structural_context_score"] > 0
    assert top_details["structural_core_score"] > 0
    assert top_details["lead_family"] in {
        "foia_contextual",
        "high_context_pair_supported",
        "high_context_structural",
    }
    assert top_score > ranked[1][0]
