import json
from datetime import datetime, timezone
from pathlib import Path

from backend.analysis.ontology import load_ontology, validate_ontology
from backend.analysis.tagger import compile_for_tagging, safe_json_text, tag_fields
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
                    {"id": "r2", "type": "regex", "pattern": r"\bhot\s*cell\b", "weight": 7},
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

def test_compile_uses_default_fields_and_default_weight_fallback():
    ont = {
        "version": "x",
        "defaults": {"fields": ["raw_json"], "default_weight": 4},
        "packs": [
            {
                "id": "p",
                "name": "P",
                "enabled": True,
                "rules": [
                    {"id": "r1", "type": "phrase", "pattern": "priority_signal"},
                ],
            }
        ],
    }

    meta, rules = compile_for_tagging(ont)
    assert meta["default_fields"] == ["raw_json"]
    assert len(rules) == 1
    assert rules[0].fields == ["raw_json"]
    assert rules[0].weight == 4
    assert rules[0].weight_source == "defaults.default_weight"

    res = tag_fields(meta, rules, {"raw_json": '{"signal":"priority_signal"}'})
    assert "p:r1" in res["keywords"]
    clause = res["clauses"][0]
    assert clause["field"] == "raw_json"
    assert clause["weight"] == 4
    assert clause["weight_applied"] == 4
    assert clause["weight_source"] == "defaults.default_weight"



def test_safe_json_text_is_deterministic_and_bounded():
    obj1 = {
        "b": list(range(0, 500)),
        "a": ("x" * 5000) + "tail_marker",
    }
    obj2 = {
        "a": ("x" * 5000) + "tail_marker",
        "b": list(range(0, 500)),
    }

    t1 = safe_json_text(obj1, max_len=512)
    t2 = safe_json_text(obj2, max_len=512)

    assert t1 == t2
    assert len(t1) <= 512
    assert "truncated" in t1.lower()
    assert "__truncated_items__" in t1


def test_safe_json_text_preserves_empty_sequences_as_arrays():
    payload = {
        "items": [],
        "nested": {
            "values": tuple(),
            "set_values": set(),
        },
    }

    text = safe_json_text(payload, max_len=4096)
    parsed = json.loads(text)

    assert parsed["items"] == []
    assert parsed["nested"]["values"] == []
    assert parsed["nested"]["set_values"] == []

