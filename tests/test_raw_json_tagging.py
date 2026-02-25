import json
from backend.db.models import Event, get_session_factory, ensure_schema
from backend.services.tagging import apply_ontology_to_events


def test_raw_json_field_is_tagged(tmp_path):
    db_path = tmp_path / "test_raw_json.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    # Create schema directly (reliable for sqlite temp DB)
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    try:
        ev = Event(
            category="award",
            source="USAspending",
            hash="testhash_raw_json_1",
            snippet="",
            place_text="",
            doc_id="doc1",
            source_url="http://example.com",
            raw_json={"note": ("x" * 70000) + "ultra_secret_widget"},
            keywords=[],
            clauses=[],
        )
        db.add(ev)
        db.commit()
    finally:
        db.close()

    ontology = {
        "version": "test",
        "defaults": {"fields": ["raw_json"]},
        "packs": [
            {
                "id": "t",
                "name": "t",
                "enabled": True,
                "rules": [
                    {"id": "x", "type": "phrase", "pattern": "ultra_secret_widget", "weight": 5}
                ],
            }
        ],
    }

    onto_path = tmp_path / "onto.json"
    onto_path.write_text(json.dumps(ontology), encoding="utf-8")

    apply_ontology_to_events(
        onto_path,
        days=30,
        source="USAspending",
        batch=100,
        dry_run=False,
        database_url=db_url,
    )

    db = SessionFactory()
    try:
        ev2 = db.query(Event).filter(Event.hash == "testhash_raw_json_1").one()
        assert "t:x" in (ev2.keywords or [])
        assert any(c.get("field") == "raw_json" for c in (ev2.clauses or []))
    finally:
        db.close()
