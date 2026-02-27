import json

from backend.db.models import Entity, Event, ensure_schema, get_session_factory
from backend.services.export_entities import export_entities_bundle


def test_export_entities_bundle_writes_files(tmp_path):
    db_path = tmp_path / "entities.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        ent = Entity(name="Acme Corp", uei="UEI123", cage="CAGE1")
        db.add(ent)
        db.commit()
        db.refresh(ent)

        ev = Event(category="award", source="USAspending", hash="ev_ent_1", entity_id=int(ent.id), raw_json={"Recipient Name": "Acme Corp", "UEI": "UEI123", "DUNS": "DUNS9"})
        db.add(ev)
        db.commit()

    out_dir = tmp_path / "out"
    res = export_entities_bundle(database_url=db_url, output=out_dir)

    assert res["entities_csv"].exists()
    assert res["entities_json"].exists()
    assert res["event_entities_csv"].exists()
    assert res["event_entities_json"].exists()

    payload = json.loads(res["event_entities_json"].read_text(encoding="utf-8"))
    assert payload["count"] == 1
    row = payload["items"][0]
    assert row["entity_name"] == "Acme Corp"
    assert row["recipient_uei"] == "UEI123"
