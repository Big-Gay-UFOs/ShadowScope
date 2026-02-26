from backend.db.models import Entity, Event, ensure_schema, get_session_factory
from backend.services.entities import link_entities_from_events


def test_entity_linking_prefers_cage_match(tmp_path):
    db_path = tmp_path / "ent_link.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        # Pre-existing entity keyed by CAGE
        ent = Entity(name="Seed Entity", cage="1A2B3")
        db.add(ent)
        db.commit()
        db.refresh(ent)

        ev = Event(
            category="award",
            source="USAspending",
            hash="ev_cage_1",
            snippet="x",
            place_text="",
            doc_id="d1",
            source_url="http://x/1",
            raw_json={
                "Recipient Name": "Different Name LLC",
                "Recipient CAGE Code": "1a2b3",
                "Recipient UEI": "ZZZZZ1234567",
            },
            keywords=[],
            clauses=[],
        )
        db.add(ev)
        db.commit()
        db.refresh(ev)

    # Link
    res = link_entities_from_events(source="USAspending", days=30, batch=100, dry_run=False, database_url=db_url)
    assert res["linked"] == 1

    with SessionFactory() as db:
        ev2 = db.query(Event).filter(Event.hash == "ev_cage_1").one()
        assert ev2.entity_id is not None
        assert int(ev2.entity_id) == int(ent.id)

        ent2 = db.query(Entity).filter(Entity.id == ent.id).one()
        # Should backfill UEI onto existing entity
        assert ent2.uei == "ZZZZZ1234567"
        assert ent2.cage == "1A2B3"
