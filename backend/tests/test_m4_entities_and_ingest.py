from datetime import datetime, timezone
from pathlib import Path

from backend.db.models import Entity, Event, ensure_schema, get_session_factory
from backend.services.entities import link_entities_from_events
from backend.services.ingest import _upsert_events


def test_upsert_backfills_raw_json_without_wiping_tags(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 't.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    db = SessionFactory()

    now = datetime.now(timezone.utc)
    db.add(
        Event(
            category="procurement",
            source="USAspending",
            hash="h1",
            raw_json={},
            keywords=["keepme"],
            clauses=[{"k": "v"}],
            created_at=now,
        )
    )
    db.commit()

    incoming = [
        {
            "hash": "h1",
            "raw_json": {
                "Award ID": "A1",
                "Recipient Name": "ACME INC",
                "Recipient UEI": "UEI123",
                "Recipient DUNS Number": "123456789",
                "recipient_id": "RID-XYZ",
            },
            "doc_id": "A1",
            "source_url": "https://example.com/award/A1",
            "snippet": "desc",
            "place_text": "place",
            "occurred_at": now,
        }
    ]

    inserted = _upsert_events(db, incoming)
    db.commit()

    assert inserted == 0
    row = db.query(Event).filter_by(hash="h1").one()
    assert isinstance(row.raw_json, dict)
    assert row.raw_json.get("Recipient UEI") == "UEI123"
    assert row.keywords == ["keepme"]
    assert row.clauses == [{"k": "v"}]


def test_entity_linking_uei_first_is_idempotent(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'e.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    db = SessionFactory()

    now = datetime.now(timezone.utc)
    db.add_all(
        [
            Event(
                category="procurement",
                source="USAspending",
                hash="h1",
                raw_json={
                    "Recipient Name": "ACME INC",
                    "Recipient UEI": "uei123",
                    "Recipient DUNS Number": "123456789",
                    "recipient_id": "RID-XYZ",
                },
                created_at=now,
            ),
            Event(
                category="procurement",
                source="USAspending",
                hash="h2",
                raw_json={
                    "Recipient Name": "Acme Inc",
                    "Recipient UEI": "UEI123",
                    "Recipient DUNS Number": "123456789",
                    "recipient_id": "RID-XYZ",
                },
                created_at=now,
            ),
        ]
    )
    db.commit()
    db.close()

    res1 = link_entities_from_events(source="USAspending", days=3650, batch=100, dry_run=False, database_url=db_url)
    assert res1["linked"] == 2
    assert res1["entities_created"] == 1
    assert res1["skipped_no_name"] == 0

    # second run: idempotent
    res2 = link_entities_from_events(source="USAspending", days=3650, batch=100, dry_run=False, database_url=db_url)
    assert res2["linked"] == 0
    assert res2["entities_created"] == 0

    db2 = SessionFactory()
    ents = db2.query(Entity).all()
    assert len(ents) == 1
    assert ents[0].uei == "UEI123"

    evs = db2.query(Event).order_by(Event.id.asc()).all()
    assert evs[0].entity_id == evs[1].entity_id
    db2.close()