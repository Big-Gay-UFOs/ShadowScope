from datetime import datetime, timezone
from pathlib import Path

from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.tagging import apply_ontology_to_events


ONTOLOGY_PATH = Path("examples/ontology_usaspending_starter.json")


def test_usaspending_starter_ontology_tags_targeted_sustainment_patterns(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'usa_ontology_patterns.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="award",
                    source="USAspending",
                    hash="usa_ont_pat_1",
                    created_at=now,
                    snippet="SOFTWARE LICENSE RENEWAL SUPPORT SERVICES FOR CLOUD HOSTING OPERATIONS",
                    raw_json={
                        "Description": "SOFTWARE LICENSE RENEWAL SUPPORT SERVICES FOR CLOUD HOSTING OPERATIONS",
                        "Recipient Name": "Example Integrator",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="award",
                    source="USAspending",
                    hash="usa_ont_pat_2",
                    created_at=now,
                    snippet="CYBERSECURITY OPERATIONS SUPPORT SERVICES WITH SIEM MONITORING",
                    raw_json={
                        "Description": "CYBERSECURITY OPERATIONS SUPPORT SERVICES WITH SIEM MONITORING",
                        "Recipient Name": "Example Integrator",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="award",
                    source="USAspending",
                    hash="usa_ont_pat_3",
                    created_at=now,
                    snippet="INSTRUCTOR TRAINING SERVICES SUPPORT FOR SOFTWARE PLATFORM ADMINISTRATORS",
                    raw_json={
                        "Description": "INSTRUCTOR TRAINING SERVICES SUPPORT FOR SOFTWARE PLATFORM ADMINISTRATORS",
                        "Recipient Name": "Example Integrator",
                    },
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = apply_ontology_to_events(
        ontology_path=ONTOLOGY_PATH,
        days=30,
        source="USAspending",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    assert res["scanned"] == 3
    assert res["updated"] == 3

    with SessionFactory() as db:
        rows = db.query(Event).filter(Event.source == "USAspending").all()

    all_keywords = sorted({kw for ev in rows for kw in (ev.keywords or [])})
    assert "sustainment_it_ops:software_license_renewal_support" in all_keywords
    assert "sustainment_it_ops:cloud_ops_support_services" in all_keywords
    assert "sustainment_it_ops:cybersecurity_operations_support" in all_keywords
    assert "sustainment_it_ops:it_training_services" in all_keywords


def test_usaspending_starter_ontology_guardrail_avoids_generic_term_overtagging(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'usa_ontology_guardrail.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="award",
                    source="USAspending",
                    hash="usa_ont_guard_hit",
                    created_at=now,
                    snippet="SOFTWARE LICENSE RENEWAL SUPPORT SERVICES FOR CLOUD OPERATIONS",
                    raw_json={"Description": "SOFTWARE LICENSE RENEWAL SUPPORT SERVICES FOR CLOUD OPERATIONS"},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="award",
                    source="USAspending",
                    hash="usa_ont_guard_noise",
                    created_at=now,
                    snippet="Weekly bulletin discussed software trends, cloud adoption, and security training metrics for employees.",
                    raw_json={
                        "Description": "Weekly bulletin discussed software trends, cloud adoption, and security training metrics for employees."
                    },
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    apply_ontology_to_events(
        ontology_path=ONTOLOGY_PATH,
        days=30,
        source="USAspending",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    with SessionFactory() as db:
        hit = db.query(Event).filter(Event.hash == "usa_ont_guard_hit").one()
        noise = db.query(Event).filter(Event.hash == "usa_ont_guard_noise").one()

    assert len(hit.keywords or []) > 0
    assert noise.keywords == []
