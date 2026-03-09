from datetime import datetime, timezone

from backend.db.models import Entity, Event, LeadSnapshot, ensure_schema, get_session_factory
from backend.services.doctor import doctor_status


def test_doctor_status_uses_source_specific_hints(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'doctor_hints.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add(
            Event(
                category="procurement",
                source="SAM.gov",
                occurred_at=now,
                created_at=now,
                snippet="Generators",
                raw_json={},
                hash="doctor_sam_1",
                keywords=[],
                clauses=[],
            )
        )
        # This snapshot is intentionally from a DIFFERENT source.
        # It should not suppress SAM.gov-specific doctor hints.
        db.add(
            LeadSnapshot(
                source="USAspending",
                min_score=1,
                limit=10,
                scoring_version="v2",
            )
        )
        db.commit()

    res = doctor_status(
        database_url=db_url,
        days=30,
        source="SAM.gov",
        scan_limit=100,
        max_keywords_per_event=10,
    )
    joined = "\n".join(res["hints"])

    assert 'ss ontology apply --path ontology.json --days 30 --source "SAM.gov"' in joined
    assert 'ss entities link --source "SAM.gov" --days 30' in joined
    assert 'ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200' in joined


def test_doctor_status_reports_entity_coverage_diagnostics(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'doctor_entity_cov.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        ent = Entity(name="Known Recipient")
        db.add(ent)
        db.flush()

        db.add_all(
            [
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="Linked event",
                    raw_json={"recipient_name": "Known Recipient", "recipient_id": "RID-100"},
                    hash="doctor_cov_1",
                    entity_id=ent.id,
                    keywords=["solicitation"],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="Unlinked identity event",
                    raw_json={"recipient_name": "Other Recipient", "recipient_id": "RID-200"},
                    hash="doctor_cov_2",
                    keywords=["solicitation"],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="No identity signal",
                    raw_json={},
                    hash="doctor_cov_3",
                    keywords=["solicitation"],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = doctor_status(
        database_url=db_url,
        days=30,
        source="SAM.gov",
        scan_limit=100,
        max_keywords_per_event=10,
    )

    entities = res["entities"]
    assert entities["window_linked_coverage_pct"] == 33.3
    assert entities["sample_scanned_events"] == 3
    assert entities["sample_events_with_identity_signal"] == 2
    assert entities["sample_events_with_identity_signal_linked"] == 1
    assert entities["sample_identity_signal_coverage_pct"] == 50.0
    assert entities["sample_events_with_name"] == 2
    assert entities["sample_events_with_name_linked"] == 1
    assert entities["sample_name_coverage_pct"] == 50.0


def test_doctor_status_kw_pair_hint_recommends_min_events_two(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'doctor_kw_pair_hint.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="award",
                    source="USAspending",
                    occurred_at=now,
                    created_at=now,
                    snippet="software maintenance support services",
                    raw_json={"Recipient Name": "Example 1"},
                    hash="doctor_kw_hint_1",
                    keywords=["sustainment_it_ops:software_support_maintenance_bundle", "sustainment_it_ops:support_services"],
                    clauses=[],
                ),
                Event(
                    category="award",
                    source="USAspending",
                    occurred_at=now,
                    created_at=now,
                    snippet="cloud hosting support services",
                    raw_json={"Recipient Name": "Example 2"},
                    hash="doctor_kw_hint_2",
                    keywords=["sustainment_it_ops:cloud_ops_support_services", "sustainment_it_ops:support_services"],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = doctor_status(
        database_url=db_url,
        days=30,
        source="USAspending",
        scan_limit=100,
        max_keywords_per_event=10,
    )

    joined = "\n".join(res["hints"])
    assert 'ss correlate rebuild-keyword-pairs --window-days 30 --source "USAspending" --min-events 2' in joined

def test_doctor_status_reports_sam_context_depth_metrics(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'doctor_sam_context.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="Sources Sought engineering support",
                    raw_json={
                        "noticeType": "Sources Sought",
                        "solicitationNumber": "DOE-RFP-100",
                        "naicsCode": "541330",
                        "typeOfSetAside": "SBA",
                        "fullParentPathCode": "DOE.HQ",
                        "responseDeadLine": "2026-03-15",
                    },
                    hash="doctor_ctx_1",
                    keywords=["sam_procurement_starter:request_for_proposal"],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="Sources Sought engineering sustainment",
                    raw_json={
                        "noticeType": "Sources Sought",
                        "solicitationNumber": "DOE-RFP-101",
                        "naicsCode": "541330",
                        "typeOfSetAside": "SBA",
                        "fullParentPathCode": "DOE.HQ",
                        "responseDeadLine": "2026-03-16",
                    },
                    hash="doctor_ctx_2",
                    keywords=["sam_procurement_starter:request_for_proposal"],
                    clauses=[],
                ),
                Event(
                    category="procurement",
                    source="SAM.gov",
                    occurred_at=now,
                    created_at=now,
                    snippet="Generic procurement update",
                    raw_json={
                        "fullParentPathCode": "DOE.FIELD",
                    },
                    hash="doctor_ctx_3",
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = doctor_status(
        database_url=db_url,
        days=30,
        source="SAM.gov",
        scan_limit=100,
        max_keywords_per_event=10,
    )

    sam_ctx = res["sam_context"]
    assert sam_ctx["scanned_events"] == 3
    assert sam_ctx["events_with_research_context"] == 2
    assert sam_ctx["events_with_core_procurement_context"] == 2
    assert sam_ctx["research_context_coverage_pct"] == 66.7
    assert sam_ctx["core_procurement_context_coverage_pct"] == 66.7
    assert sam_ctx["coverage_by_field_pct"]["sam_naics_code"] == 66.7

    top_naics = sam_ctx["top_naics_codes"]
    assert top_naics
    assert top_naics[0]["naics_code"] == "541330"
    assert top_naics[0]["count"] == 2
