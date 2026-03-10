from datetime import datetime, timezone
from pathlib import Path

from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.tagging import apply_ontology_to_events


STARTER_ONTOLOGY_PATH = Path("examples/ontology_sam_procurement_starter.json")
DOD_COMPANION_ONTOLOGY_PATH = Path("examples/ontology_sam_dod_foia_companion.json")
STARTER_PLUS_DOD_ONTOLOGY_PATH = Path("examples/ontology_sam_procurement_plus_dod_foia.json")


def test_sam_starter_ontology_tags_contextual_procurement_signals(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_ontology_patterns.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_ont_pat_1",
                    created_at=now,
                    snippet="Sources Sought RFP for engineering support",
                    raw_json={
                        "noticeType": "Sources Sought",
                        "solicitationNumber": "DOE-RFP-100",
                        "naicsCode": "541330",
                        "typeOfSetAside": "SBA",
                        "fullParentPathCode": "DOE.HQ",
                        "responseDeadLine": "2026-03-15",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_ont_pat_2",
                    created_at=now,
                    snippet="Request for Proposal for delivery order engineering sustainment",
                    raw_json={
                        "noticeType": "Solicitation",
                        "solicitationNumber": "DOE-RFP-101",
                        "naicsCode": "541330",
                        "typeOfSetAside": "SBA",
                        "fullParentPathCode": "DOE.HQ",
                        "responseDeadLine": "2026-03-16",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_ont_pat_3",
                    created_at=now,
                    snippet="Combined Synopsis Solicitation for cybersecurity services",
                    raw_json={
                        "noticeType": "Combined Synopsis/Solicitation",
                        "solicitationNumber": "DOE-RFP-102",
                        "naicsCode": "541512",
                        "fullParentPathCode": "DOE.FIELD",
                    },
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = apply_ontology_to_events(
        ontology_path=STARTER_ONTOLOGY_PATH,
        days=30,
        source="SAM.gov",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    assert res["scanned"] == 3
    assert res["updated"] == 3

    with SessionFactory() as db:
        rows = db.query(Event).filter(Event.source == "SAM.gov").all()

    all_keywords = sorted({kw for ev in rows for kw in (ev.keywords or [])})
    assert "sam_procurement_starter:notice_type_sources_sought" in all_keywords
    assert "sam_procurement_starter:request_for_proposal" in all_keywords
    assert "sam_procurement_starter:naics_context" in all_keywords
    assert "sam_procurement_starter:solicitation_number_present" in all_keywords


def test_sam_starter_ontology_guardrail_avoids_generic_term_overtagging(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_ontology_guardrail.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_ont_guard_hit",
                    created_at=now,
                    snippet="Sources Sought RFP for engineering support",
                    raw_json={
                        "noticeType": "Sources Sought",
                        "solicitationNumber": "DOE-RFP-200",
                        "naicsCode": "541330",
                        "typeOfSetAside": "SBA",
                        "fullParentPathCode": "DOE.HQ",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_ont_guard_noise",
                    created_at=now,
                    snippet="Weekly bulletin discussed construction generator water valve maintenance for campus facilities.",
                    raw_json={"note": "internal weekly bulletin"},
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    apply_ontology_to_events(
        ontology_path=STARTER_ONTOLOGY_PATH,
        days=30,
        source="SAM.gov",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    with SessionFactory() as db:
        hit = db.query(Event).filter(Event.hash == "sam_ont_guard_hit").one()
        noise = db.query(Event).filter(Event.hash == "sam_ont_guard_noise").one()

    assert len(hit.keywords or []) > 0
    assert noise.keywords == []


def test_sam_procurement_plus_dod_foia_tags_starter_and_dod_context(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_ontology_plus_dod.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add(
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="sam_plus_dod_1",
                created_at=now,
                snippet="Sources Sought RFP for Edwards AFB 412th telemetry range instrumentation integration support",
                raw_json={
                    "noticeType": "Sources Sought",
                    "solicitationNumber": "AFRL-TEST-100",
                    "naicsCode": "541715",
                    "title": "Edwards AFB range instrumentation telemetry integration support",
                },
                keywords=[],
                clauses=[],
            )
        )
        db.commit()

    res = apply_ontology_to_events(
        ontology_path=STARTER_PLUS_DOD_ONTOLOGY_PATH,
        days=30,
        source="SAM.gov",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    assert res["scanned"] == 1
    assert res["updated"] == 1

    with SessionFactory() as db:
        row = db.query(Event).filter(Event.hash == "sam_plus_dod_1").one()

    keywords = set(row.keywords or [])
    assert "sam_procurement_starter:notice_type_sources_sought" in keywords
    assert any(k.startswith("sam_dod_flight_test_range_instrumentation:") for k in keywords)


def test_sam_dod_companion_noise_guardrails_for_commodity_medical_context(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_ontology_noise_guard.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_dod_real_1",
                    created_at=now,
                    snippet=(
                        "Low observable radar cross section analysis and mission systems integration engineering support "
                        "for advanced aerospace platform upgrades"
                    ),
                    raw_json={
                        "title": "Advanced aerospace low observable integration support",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_dod_noise_1",
                    created_at=now,
                    snippet=(
                        "NSN 4720-01-123-4567 hydraulic hose quantity 12 for hospital clinic supplies and "
                        "conference catering event support"
                    ),
                    raw_json={
                        "lineItem": "NSN 4720-01-123-4567",
                        "description": "Medical center commodity order and event support",
                    },
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    apply_ontology_to_events(
        ontology_path=DOD_COMPANION_ONTOLOGY_PATH,
        days=30,
        source="SAM.gov",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    with SessionFactory() as db:
        dod = db.query(Event).filter(Event.hash == "sam_dod_real_1").one()
        noise = db.query(Event).filter(Event.hash == "sam_dod_noise_1").one()

    dod_keywords = set(dod.keywords or [])
    noise_keywords = set(noise.keywords or [])

    assert any(k.startswith("sam_dod_advanced_aerospace_support:") for k in dod_keywords)
    assert any(k.startswith("operational_noise_terms:") for k in noise_keywords)
    assert not any(k.startswith("sam_dod_") for k in noise_keywords)
