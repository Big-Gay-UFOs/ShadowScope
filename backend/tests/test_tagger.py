from datetime import datetime, timezone
from pathlib import Path

from backend.analysis.ontology import load_ontology, validate_ontology
from backend.analysis.tagger import compile_for_tagging, tag_fields
from backend.services.tagging import apply_ontology_to_events
from backend.db import models


def test_tagger_phrase_and_regex():
    # minimal ontology
    ont = {
        "version": "x",
        "defaults": {"case_insensitive": True, "fields": ["snippet"]},
        "packs": [
            {
                "id": "p",
                "name": "P",
                "enabled": True,
                "rules": [
                    {"id": "r1", "type": "phrase", "pattern": "radiation", "weight": 5},
                    {"id": "r2", "type": "regex", "pattern": r"\\bhot\\s*cell\\b", "weight": 7},
                ],
            }
        ],
    }
    meta, rules = compile_for_tagging(ont)
    res = tag_fields(meta, rules, {"snippet": "Hot cell radiation safety support"})
    assert "p:r1" in res["keywords"]
    assert "p:r2" in res["keywords"]
    assert any(c["rule"] == "r2" for c in res["clauses"])


def test_apply_idempotent_sqlite(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'tagger_test.db'}"
    models.ensure_schema(db_url)

    SessionFactory = models.get_session_factory(db_url)
    s = SessionFactory()
    try:
        ev = models.Event(
            category="procurement",
            source="USAspending",
            occurred_at=datetime.now(timezone.utc),
            snippet="Radiation safety support",
            hash="testhash123",
            keywords=[],
            clauses=[],
        )
        s.add(ev)
        s.commit()
    finally:
        s.close()

    root = Path(__file__).resolve().parents[2]
    ont_path = root / "ontology.json"
    obj = load_ontology(ont_path)
    assert validate_ontology(obj) == []

    r1 = apply_ontology_to_events(ont_path, days=3650, source="USAspending", batch=50, dry_run=False, database_url=db_url)
    assert r1["updated"] == 1
    r2 = apply_ontology_to_events(ont_path, days=3650, source="USAspending", batch=50, dry_run=False, database_url=db_url)
    assert r2["updated"] == 0