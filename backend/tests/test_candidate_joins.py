from datetime import datetime, timedelta, timezone

from backend.correlate.candidate_joins import (
    CANDIDATE_JOIN_LANE,
    rebuild_sam_usaspending_candidate_joins,
)
from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory


def _make_event(**kwargs):
    base = {
        "category": "procurement",
        "raw_json": {},
        "keywords": [],
        "clauses": [],
    }
    base.update(kwargs)
    return Event(**base)


def test_candidate_joins_strong_exact_match(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'candidate_joins_exact.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                _make_event(
                    source="SAM.gov",
                    hash="sam-strong",
                    created_at=now,
                    solicitation_number="ABC-RFP-001",
                    recipient_name="Acme Federal, Inc.",
                    recipient_uei="UEI123",
                    awarding_agency_code="DOE",
                    psc_code="R425",
                    naics_code="541330",
                    place_of_performance_country="USA",
                    place_of_performance_state="VA",
                ),
                _make_event(
                    source="USAspending",
                    hash="usa-strong",
                    created_at=now - timedelta(days=45),
                    piid="ABC-RFP-001",
                    recipient_name="ACME FEDERAL",
                    recipient_uei="UEI123",
                    awarding_agency_code="DOE",
                    psc_code="R425",
                    naics_code="541330",
                    place_of_performance_country="USA",
                    place_of_performance_state="VA",
                ),
            ]
        )
        db.commit()

    res = rebuild_sam_usaspending_candidate_joins(
        window_days=30,
        history_days=365,
        min_score=45,
        max_matches_per_key=25,
        max_candidates_per_sam=10,
        database_url=db_url,
    )

    assert res["candidate_pairs_above_threshold"] == 1
    assert res["likely_incumbent_count"] == 1

    with SessionFactory() as db:
        corr = db.query(Correlation).one()
        links = db.query(CorrelationLink).filter(CorrelationLink.correlation_id == int(corr.id)).all()

    assert corr.correlation_key.startswith(f"{CANDIDATE_JOIN_LANE}|SAM.gov__USAspending|30|hist365|pair:")
    assert corr.score == "100"
    assert len(links) == 2
    assert corr.lanes_hit["likely_incumbent"] is True
    assert set(corr.lanes_hit["evidence_types"]) >= {
        "identifier_exact",
        "recipient_uei",
        "recipient_name",
        "awarding_agency",
        "psc",
        "naics",
        "place_region",
        "time_window",
    }


def test_candidate_joins_partial_multi_signal_match(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'candidate_joins_partial.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                _make_event(
                    source="SAM.gov",
                    hash="sam-partial",
                    created_at=now,
                    recipient_name="Blue Rocket Systems LLC",
                    awarding_agency_name="Department of Energy",
                    naics_code="541512",
                    place_of_performance_country="USA",
                    place_of_performance_state="CO",
                ),
                _make_event(
                    source="USAspending",
                    hash="usa-partial",
                    created_at=now - timedelta(days=120),
                    recipient_name="BLUE ROCKET SYSTEMS",
                    awarding_agency_name="DEPARTMENT OF ENERGY",
                    naics_code="541512",
                    place_of_performance_country="USA",
                    place_of_performance_state="CO",
                ),
            ]
        )
        db.commit()

    res = rebuild_sam_usaspending_candidate_joins(
        window_days=30,
        history_days=365,
        min_score=45,
        max_matches_per_key=25,
        max_candidates_per_sam=10,
        database_url=db_url,
    )

    assert res["candidate_pairs_above_threshold"] == 1

    with SessionFactory() as db:
        corr = db.query(Correlation).one()

    assert corr.score == "57"
    assert corr.lanes_hit["likely_incumbent"] is True
    assert corr.lanes_hit["evidence_types"] == [
        "recipient_name",
        "awarding_agency",
        "naics",
        "place_region",
        "time_window",
    ]


def test_candidate_joins_non_match_produces_no_correlations(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'candidate_joins_none.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                _make_event(
                    source="SAM.gov",
                    hash="sam-none",
                    created_at=now,
                    recipient_name="Northwind Research",
                    awarding_agency_code="NASA",
                    naics_code="541715",
                ),
                _make_event(
                    source="USAspending",
                    hash="usa-none",
                    created_at=now - timedelta(days=30),
                    recipient_name="Southridge Logistics",
                    awarding_agency_code="DOE",
                    naics_code="484110",
                ),
            ]
        )
        db.commit()

    res = rebuild_sam_usaspending_candidate_joins(
        window_days=30,
        history_days=365,
        min_score=45,
        max_matches_per_key=25,
        max_candidates_per_sam=10,
        database_url=db_url,
    )

    assert res["candidate_pairs_considered"] == 0
    assert res["candidate_pairs_above_threshold"] == 0

    with SessionFactory() as db:
        assert db.query(Correlation).count() == 0


def test_candidate_joins_reject_overly_common_keys(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'candidate_joins_common.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add(
            _make_event(
                source="SAM.gov",
                hash="sam-common",
                created_at=now,
                awarding_agency_code="DOE",
                naics_code="541330",
            )
        )
        for idx in range(12):
            db.add(
                _make_event(
                    source="USAspending",
                    hash=f"usa-common-{idx}",
                    created_at=now - timedelta(days=idx + 1),
                    awarding_agency_code="DOE",
                    naics_code="541330",
                    recipient_name=f"Vendor {idx}",
                )
            )
        db.commit()

    res = rebuild_sam_usaspending_candidate_joins(
        window_days=30,
        history_days=365,
        min_score=45,
        max_matches_per_key=5,
        max_candidates_per_sam=10,
        database_url=db_url,
    )

    assert res["blocked_key_counts"]["awarding_agency"] >= 1
    assert res["blocked_key_counts"]["naics"] >= 1
    assert res["rejected_common_keys"]["awarding_agency"] >= 1
    assert res["rejected_common_keys"]["naics"] >= 1
    assert res["candidate_pairs_considered"] == 0

    with SessionFactory() as db:
        assert db.query(Correlation).count() == 0
