from datetime import datetime, timezone
from pathlib import Path

from backend.db import models
from backend.services.entities import link_entities_from_events


def test_link_entities_idempotent_sqlite(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'entities.db'}"
    models.ensure_schema(db_url)

    SessionFactory = models.get_session_factory(db_url)
    s = SessionFactory()
    try:
        ev = models.Event(
            category="procurement",
            source="USAspending",
            occurred_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
            snippet="test",
            raw_json={"Recipient Name": "ACME SYSTEMS INC"},
            hash="h_entity_1",
            keywords=[],
            clauses=[],
        )
        s.add(ev)
        s.commit()
        s.refresh(ev)
        assert ev.entity_id is None
    finally:
        s.close()

    r1 = link_entities_from_events(source="USAspending", days=3650, batch=50, dry_run=False, database_url=db_url)
    assert r1["linked"] == 1
    assert r1["entities_created"] == 1

    r2 = link_entities_from_events(source="USAspending", days=3650, batch=50, dry_run=False, database_url=db_url)
    assert r2["linked"] == 0
    assert r2["entities_created"] == 0

    s = SessionFactory()
    try:
        ev = s.query(models.Event).filter_by(hash="h_entity_1").one()
        assert ev.entity_id is not None
        ent = s.query(models.Entity).filter_by(id=ev.entity_id).one()
        assert ent.name == "ACME SYSTEMS INC"
    finally:
        s.close()