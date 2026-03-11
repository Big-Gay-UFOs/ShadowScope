from datetime import datetime, timedelta, timezone

from backend.correlate import correlate
from backend.db.models import Correlation, Event, ensure_schema, get_session_factory


def test_rebuild_normalized_same_field_lanes_create_expected_clusters(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'norm_lanes.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="procurement",
                    source="USAspending",
                    hash="n1",
                    award_id="AW-001",
                    piid="PI-100",
                    document_id="DOC-900",
                    awarding_agency_code="DOE",
                    psc_code="R425",
                    naics_code="541330",
                    place_of_performance_country="USA",
                    place_of_performance_state="VA",
                    created_at=now - timedelta(days=1),
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="USAspending",
                    hash="n2",
                    award_id="AW-001",
                    piid="PI-100",
                    document_id="DOC-900",
                    awarding_agency_code="DOE",
                    psc_code="R425",
                    naics_code="541330",
                    place_of_performance_country="USA",
                    place_of_performance_state="VA",
                    created_at=now - timedelta(days=2),
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="USAspending",
                    hash="n3",
                    award_id="AW-001",
                    piid="PI-101",
                    document_id="DOC-901",
                    awarding_agency_code="DOE",
                    psc_code="R425",
                    naics_code="541330",
                    place_of_performance_country="USA",
                    place_of_performance_state="VA",
                    created_at=now - timedelta(days=3),
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="USAspending",
                    hash="n4",
                    award_id="AW-002",
                    piid="PI-200",
                    document_id="DOC-999",
                    awarding_agency_code="NASA",
                    psc_code="J099",
                    naics_code="541512",
                    place_of_performance_country="USA",
                    place_of_performance_state="MD",
                    created_at=now - timedelta(days=2),
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res_award = correlate.rebuild_award_id_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        max_events=50,
        dry_run=False,
        database_url=db_url,
    )
    assert res_award["eligible_keys"] == 1
    assert res_award["keys_capped"] == 0

    res_contract = correlate.rebuild_contract_id_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        max_events=50,
        dry_run=False,
        database_url=db_url,
    )
    assert res_contract["eligible_keys"] == 1

    res_doc = correlate.rebuild_doc_id_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        max_events=50,
        dry_run=False,
        database_url=db_url,
    )
    assert res_doc["eligible_keys"] == 1

    res_agency = correlate.rebuild_agency_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        max_events=50,
        dry_run=False,
        database_url=db_url,
    )
    assert res_agency["eligible_keys"] == 1

    res_psc = correlate.rebuild_psc_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        max_events=50,
        dry_run=False,
        database_url=db_url,
    )
    assert res_psc["eligible_keys"] == 1

    res_naics = correlate.rebuild_naics_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        max_events=50,
        dry_run=False,
        database_url=db_url,
    )
    assert res_naics["eligible_keys"] == 1

    res_region = correlate.rebuild_place_region_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        max_events=50,
        dry_run=False,
        database_url=db_url,
    )
    assert res_region["eligible_keys"] == 1

    with SessionFactory() as db:
        corr_rows = db.query(Correlation).all()

    corr_keys = {c.correlation_key for c in corr_rows}
    assert any(k.startswith("same_award_id|USAspending|30|award:") for k in corr_keys)
    assert any(k.startswith("same_contract_id|USAspending|30|contract:") for k in corr_keys)
    assert any(k.startswith("same_doc_id|USAspending|30|doc:") for k in corr_keys)
    assert any(k.startswith("same_agency|USAspending|30|agency:") for k in corr_keys)
    assert any(k.startswith("same_psc|USAspending|30|psc:") for k in corr_keys)
    assert any(k.startswith("same_naics|USAspending|30|naics:") for k in corr_keys)
    assert any(k.startswith("same_place_region|USAspending|30|region:") for k in corr_keys)

    # Explainability payload carries lane key counts.
    lane_payloads = [c.lanes_hit for c in corr_rows if isinstance(c.lanes_hit, dict)]
    assert any(lp.get("key_count") == lp.get("event_count") for lp in lane_payloads)


def test_rebuild_naics_lane_caps_extremely_common_keys(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'naics_cap.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        for idx in range(6):
            db.add(
                Event(
                    category="procurement",
                    source="USAspending",
                    hash=f"cap-{idx}",
                    naics_code="541330",
                    created_at=now - timedelta(days=1),
                    raw_json={},
                    keywords=[],
                    clauses=[],
                )
            )
        db.commit()

    res = correlate.rebuild_naics_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        max_events=3,
        dry_run=False,
        database_url=db_url,
    )

    assert res["keys_seen"] == 1
    assert res["keys_capped"] == 1
    assert res["eligible_keys"] == 0

    with SessionFactory() as db:
        count = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like("same_naics|USAspending|30|naics:%"))
            .count()
        )
    assert count == 0
