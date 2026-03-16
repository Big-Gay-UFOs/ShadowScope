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


def test_sam_dod_companion_tags_site_operator_and_hardened_pairs(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_ontology_new_pairs.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add(
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="sam_dod_pairs_1",
                created_at=now,
                snippet=(
                    "NTESS and Battelle Memorial Institute at Dugway Proving Ground provide range instrumentation "
                    "and tunnel boring machine access shaft modernization engineering support for hardened portal "
                    "door ventilation systems."
                ),
                raw_json={
                    "title": "Aerial Operations Facility NVH1 infrastructure sustainment support",
                    "description": "R2508 tunnel lining and protected communications upgrade",
                },
                keywords=[],
                clauses=[],
            )
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
        row = db.query(Event).filter(Event.hash == "sam_dod_pairs_1").one()

    keywords = set(row.keywords or [])
    assert any(k.startswith("sam_dod_flight_test_range_instrumentation:") for k in keywords)
    assert any(k.startswith("sam_dod_hardened_subsurface_infrastructure:") for k in keywords)


def test_sam_dod_companion_guardrail_blocks_entity_singletons_and_routes_lore_to_noise(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_ontology_new_guardrails.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_dod_guard_entity_only",
                    created_at=now,
                    snippet="Battelle Memorial Institute published a public annual report and internship schedule.",
                    raw_json={
                        "title": "Corporate update",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_dod_guard_generic_only",
                    created_at=now,
                    snippet="Program research materials technology underground tunnels base facility classified discussion.",
                    raw_json={
                        "note": "generic words without procurement anchors",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_dod_guard_lore_noise",
                    created_at=now,
                    snippet="Open-source forum about UAP UFO crash retrieval reverse engineering and biologics claims.",
                    raw_json={
                        "note": "lore-only discussion",
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
        entity_only = db.query(Event).filter(Event.hash == "sam_dod_guard_entity_only").one()
        generic_only = db.query(Event).filter(Event.hash == "sam_dod_guard_generic_only").one()
        lore_noise = db.query(Event).filter(Event.hash == "sam_dod_guard_lore_noise").one()

    assert entity_only.keywords == []
    assert generic_only.keywords == []

    lore_keywords = set(lore_noise.keywords or [])
    assert any(k.startswith("operational_noise_terms:") for k in lore_keywords)
    assert not any(k.startswith("sam_dod_") for k in lore_keywords)


def test_sam_dod_companion_guardrail_avoids_rd_road_false_positive(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_ontology_rd_false_positive.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add(
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="sam_dod_guard_rd_road",
                created_at=now,
                snippet=(
                    "Road resurfacing on Main Rd with access control gate updates and transport routing support "
                    "for municipal traffic operations."
                ),
                raw_json={
                    "title": "Road and gate maintenance services",
                },
                keywords=[],
                clauses=[],
            )
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
        row = db.query(Event).filter(Event.hash == "sam_dod_guard_rd_road").one()

    keywords = set(row.keywords or [])
    assert not any(k.startswith("sam_dod_program_protection_sap:") for k in keywords)

PROXY_PRECISION_ONTOLOGY_PATH = Path("examples/ontology_sam_hidden_program_proxy_companion.json")
PROXY_EXPLORATORY_ONTOLOGY_PATH = Path("examples/ontology_sam_hidden_program_proxy_exploratory.json")
STARTER_PLUS_DOD_PROXY_ONTOLOGY_PATH = Path("examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json")
STARTER_PLUS_DOD_PROXY_EXPLORATORY_ONTOLOGY_PATH = Path(
    "examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json"
)


def test_sam_hidden_program_proxy_precision_matches_snippet_and_raw_json(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_proxy_precision.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_proxy_precision_snippet",
                    created_at=now,
                    snippet="ICD 705 SCIF modernization design upgrade for accredited area hardening",
                    raw_json={"title": "secure facility modernization"},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_proxy_precision_raw_json",
                    created_at=now,
                    snippet="",
                    raw_json={
                        "details": "TEMPEST shielded room testing certification and integration support",
                    },
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = apply_ontology_to_events(
        ontology_path=PROXY_PRECISION_ONTOLOGY_PATH,
        days=30,
        source="SAM.gov",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    assert res["scanned"] == 2
    assert res["updated"] == 2

    with SessionFactory() as db:
        snippet_row = db.query(Event).filter(Event.hash == "sam_proxy_precision_snippet").one()
        raw_json_row = db.query(Event).filter(Event.hash == "sam_proxy_precision_raw_json").one()

    assert "sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context" in set(
        snippet_row.keywords or []
    )
    assert any(
        clause.get("rule") == "icd705_scif_sapf_facility_upgrade_context" and clause.get("field") == "snippet"
        for clause in (snippet_row.clauses or [])
    )
    assert "sam_proxy_secure_compartmented_facility_engineering:tempest_emanations_shielding_context" in set(
        raw_json_row.keywords or []
    )
    assert any(
        clause.get("rule") == "tempest_emanations_shielding_context" and clause.get("field") == "raw_json"
        for clause in (raw_json_row.clauses or [])
    )


def test_sam_hidden_program_proxy_precision_noise_guardrails_cover_lab_supply_and_training(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_proxy_noise.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_proxy_noise_lab",
                    created_at=now,
                    snippet="reagent consumables pipette centrifuge nitrile gloves for routine lab supply support",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_proxy_noise_training",
                    created_at=now,
                    snippet="ICD 705 SCIF COMSEC training course awareness seminar for staff",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    apply_ontology_to_events(
        ontology_path=PROXY_PRECISION_ONTOLOGY_PATH,
        days=30,
        source="SAM.gov",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    with SessionFactory() as db:
        lab = db.query(Event).filter(Event.hash == "sam_proxy_noise_lab").one()
        training = db.query(Event).filter(Event.hash == "sam_proxy_noise_training").one()

    assert set(lab.keywords or []) == {"sam_proxy_noise_expansion:generic_lab_supply_noise"}
    assert set(training.keywords or []) == {"sam_proxy_noise_expansion:security_training_noise"}


def test_sam_hidden_program_proxy_composite_routes_lore_to_existing_noise_only(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_proxy_lore_noise.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add(
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="sam_proxy_lore_only",
                created_at=now,
                snippet="Open-source UAP UFO crash retrieval reverse engineering biologics black budget claims forum thread.",
                raw_json={"note": "lore only"},
                keywords=[],
                clauses=[],
            )
        )
        db.commit()

    apply_ontology_to_events(
        ontology_path=STARTER_PLUS_DOD_PROXY_ONTOLOGY_PATH,
        days=30,
        source="SAM.gov",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    with SessionFactory() as db:
        row = db.query(Event).filter(Event.hash == "sam_proxy_lore_only").one()

    keywords = set(row.keywords or [])
    assert keywords
    assert all(item.startswith("operational_noise_terms:") for item in keywords)
    assert not any(item.startswith("sam_proxy_") for item in keywords)
    assert not any(item.startswith("sam_dod_") for item in keywords)


def test_sam_hidden_program_proxy_exploratory_requires_context_for_broad_terms(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'sam_proxy_exploratory.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_proxy_exploratory_hit",
                    created_at=now,
                    snippet="Hall thruster test instrumentation and facility calibration support for electric propulsion diagnostics",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_proxy_exploratory_singleton",
                    created_at=now,
                    snippet="Hall thruster overview brochure for outreach display",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    apply_ontology_to_events(
        ontology_path=PROXY_EXPLORATORY_ONTOLOGY_PATH,
        days=30,
        source="SAM.gov",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    with SessionFactory() as db:
        hit = db.query(Event).filter(Event.hash == "sam_proxy_exploratory_hit").one()
        singleton = db.query(Event).filter(Event.hash == "sam_proxy_exploratory_singleton").one()

    assert "sam_proxy_plasma_propulsion_diagnostics:electric_propulsion_test_context" in set(hit.keywords or [])
    assert singleton.keywords == []
