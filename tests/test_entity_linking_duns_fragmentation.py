from backend.db.models import Entity, Event, ensure_schema, get_session_factory
from backend.services.entities import link_entities_from_events


def test_entity_linking_duns_then_cage_does_not_fragment(tmp_path):
    db_path = tmp_path / "ent_link_duns.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    # Event A: DUNS only (no name) => creates DUNS entity via synthetic display_name
    with SessionFactory() as db:
        ev_a = Event(
            category="award",
            source="USAspending",
            hash="ev_duns_only",
            snippet="x",
            place_text="",
            doc_id="d1",
            source_url="http://x/1",
            raw_json={"Recipient DUNS Number": "123456789"},
            keywords=[],
            clauses=[],
        )
        db.add(ev_a)
        db.commit()

    res = link_entities_from_events(source="USAspending", days=30, batch=100, dry_run=False, database_url=db_url)
    assert res["linked"] == 1

    # Event B: same DUNS + CAGE but still no name => should match existing entity via DUNS, not create new
    with SessionFactory() as db:
        ev_b = Event(
            category="award",
            source="USAspending",
            hash="ev_duns_plus_cage",
            snippet="x",
            place_text="",
            doc_id="d2",
            source_url="http://x/2",
            raw_json={"Recipient DUNS Number": "123456789", "Recipient CAGE Code": "1A2B3"},
            keywords=[],
            clauses=[],
        )
        db.add(ev_b)
        db.commit()

    res2 = link_entities_from_events(source="USAspending", days=30, batch=100, dry_run=False, database_url=db_url)
    assert res2["linked"] == 1
    assert res2["entities_created"] == 0

    with SessionFactory() as db:
        ents = db.query(Entity).all()
        assert len(ents) == 1
