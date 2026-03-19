from datetime import datetime, timezone

from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory
from backend.services.leads import compute_leads



def _seed_kw_pair_link(
    db,
    event_id: int,
    key_suffix: str = "abcd",
    *,
    event_count: int = 4,
    score_signal: float = 0.5,
    score_secondary: float = 1.0,
) -> None:
    corr = Correlation(
        correlation_key=f"kw_pair|SAM.gov|30|pair:{key_suffix}",
        score=f"{float(score_signal):.6f}",
        window_days=30,
        radius_km=0.0,
        lanes_hit={
            "lane": "kw_pair",
            "keyword_1": "alpha",
            "keyword_2": "beta",
            "event_count": int(event_count),
            "c12": int(event_count),
            "c1": int(event_count),
            "c2": int(event_count),
            "keyword_1_df": int(event_count),
            "keyword_2_df": int(event_count),
            "total_events": 10,
            "score_signal": float(score_signal),
            "score_kind": "npmi",
            "score_secondary": float(score_secondary),
            "score_secondary_kind": "log_odds",
        },
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

        _seed_kw_pair_link(db, int(ev.id), key_suffix="foia1111", event_count=4, score_signal=0.5)

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
    assert details["pair_count_total"] == 1
    assert details["pair_strength"] == 0.5
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

        _seed_kw_pair_link(db, int(ev.id), key_suffix="foia2222", event_count=4, score_signal=0.5)

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

def test_compute_leads_pair_bonus_requires_signal_and_event_count_thresholds(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_pair_signal_thresholds.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        ev = Event(
            category="opportunity",
            source="SAM.gov",
            hash="lead_signal_thresholds_1",
            created_at=now,
            snippet="Thresholded pair bonus lead",
            raw_json={},
            keywords=[
                "sam_dod_program_protection_sap:afosi_program_security_context",
                "sam_dod_flight_test_range_instrumentation:edwards_412th_plant42_range_context",
            ],
            clauses=[],
        )
        db.add(ev)
        db.commit()
        db.refresh(ev)

        _seed_kw_pair_link(db, int(ev.id), key_suffix="sig11111", event_count=3, score_signal=0.7)
        _seed_kw_pair_link(db, int(ev.id), key_suffix="sig22222", event_count=5, score_signal=0.05)
        _seed_kw_pair_link(db, int(ev.id), key_suffix="sig33333", event_count=1, score_signal=0.9)

        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=0,
            limit=10,
            scan_limit=50,
            scoring_version="v2",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    assert scanned == 1
    assert len(ranked) == 1

    score, _event, details = ranked[0]
    assert details["pair_count"] == 1
    assert details["pair_count_total"] == 3
    assert details["pair_strength"] == 0.7
    assert details["pair_bonus"] == 4
    assert details["pair_signal_threshold"] == 0.15
    assert details["pair_event_count_threshold"] == 2
    assert score == 13
def test_compute_leads_treats_proxy_noise_pack_as_noise(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_proxy_noise.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        ev = Event(
            category="opportunity",
            source="SAM.gov",
            hash="lead_proxy_noise_1",
            created_at=now,
            snippet="Proxy noise-only lead",
            raw_json={},
            keywords=["sam_proxy_noise_expansion:generic_lab_supply_noise"],
            clauses=[],
        )
        db.add(ev)
        db.commit()

        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-10,
            limit=10,
            scan_limit=50,
            scoring_version="v2",
        )

    assert scanned == 1
    assert len(ranked) == 1

    score, _event, details = ranked[0]
    assert details["has_noise"] is True
    assert details["noise_penalty"] == 8
    assert score == -5


def test_compute_leads_v3_exposes_structural_context_and_lead_family(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_v3_structural.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        rich = Event(
            category="opportunity",
            source="SAM.gov",
            hash="lead_v3_rich",
            created_at=now,
            doc_id="RICH-1",
            source_url="https://sam.gov/opp/rich-1",
            snippet="Structurally rich SAM notice",
            raw_json={
                "noticeType": "Sources Sought",
                "solicitationNumber": "RICH-1",
                "naicsCode": "541330",
                "typeOfSetAside": "SBA",
                "responseDeadLine": "2026-03-20",
                "fullParentPathCode": "DOE.HQ",
                "Recipient Name": "Acme Federal",
            },
            keywords=["sam_procurement_starter:notice_type_sources_sought"],
            clauses=[],
        )
        thin = Event(
            category="opportunity",
            source="SAM.gov",
            hash="lead_v3_thin",
            created_at=now,
            doc_id="THIN-1",
            source_url="https://sam.gov/opp/thin-1",
            snippet="Thin SAM notice",
            raw_json={},
            keywords=["sam_procurement_starter:notice_type_sources_sought"],
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

    by_doc = {event.doc_id: details for _score, event, details in ranked}
    rich_details = by_doc["RICH-1"]
    thin_details = by_doc["THIN-1"]
    assert rich_details["scoring_version"] == "v3"
    assert thin_details["scoring_version"] == "v3"
    assert "structural_context_score" in rich_details
    assert "noise_penalty" in rich_details
    assert (rich_details.get("subscore_math") or {}).get("formula")
