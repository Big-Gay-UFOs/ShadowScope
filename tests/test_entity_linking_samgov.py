from datetime import datetime, timezone

from backend.db.models import Entity, Event, ensure_schema, get_session_factory
from backend.services.entities import link_entities_from_events


def test_entity_linking_samgov_creates_agency_entity(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'sam_entities.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        ev = Event(
            category="procurement",
            source="SAM.gov",
            occurred_at=now,
            created_at=now,
            snippet="Generators",
            raw_json={
                "fullParentPathName": "DEPT OF DEFENSE.DEPT OF THE ARMY.MICC",
                "fullParentPathCode": "021.2100.MICC",
                "organizationType": "OFFICE",
            },
            hash="sam_ev_1",
            keywords=[],
            clauses=[],
        )
        db.add(ev)
        db.commit()

    res = link_entities_from_events(source="SAM.gov", days=30, batch=100, dry_run=False, database_url=db_url)
    assert res["linked"] == 1
    assert res["entities_created"] == 1

    with SessionFactory() as db:
        ev2 = db.query(Event).filter(Event.hash == "sam_ev_1").one()
        assert ev2.entity_id is not None
        ent = db.query(Entity).filter(Entity.id == ev2.entity_id).one()
        assert ent.name == "DEPT OF DEFENSE.DEPT OF THE ARMY.MICC"
        assert ent.type == "OFFICE"
        assert ent.sites_json["sam_parent_path_code"] == "021.2100.MICC"


def test_entity_linking_samgov_matches_existing_by_parent_path_code(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'sam_entities_match.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        ent = Entity(
            name="Older Agency Name",
            type="OFFICE",
            sites_json={"sam_parent_path_code": "070.7008.70Z030"},
        )
        db.add(ent)
        db.commit()
        db.refresh(ent)
        ent_id = ent.id

        ev = Event(
            category="procurement",
            source="SAM.gov",
            occurred_at=now,
            created_at=now,
            snippet="Front gate replacement",
            raw_json={
                "fullParentPathName": "HOMELAND SECURITY, DEPARTMENT OF.US COAST GUARD.BASE CLEVELAND(00030)",
                "fullParentPathCode": "070.7008.70Z030",
                "organizationType": "OFFICE",
            },
            hash="sam_ev_match_1",
            keywords=[],
            clauses=[],
        )
        db.add(ev)
        db.commit()

    res = link_entities_from_events(source="SAM.gov", days=30, batch=100, dry_run=False, database_url=db_url)
    assert res["linked"] == 1
    assert res["entities_created"] == 0

    with SessionFactory() as db:
        ev2 = db.query(Event).filter(Event.hash == "sam_ev_match_1").one()
        assert ev2.entity_id == ent_id
