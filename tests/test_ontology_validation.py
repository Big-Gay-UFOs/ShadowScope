from backend.analysis.ontology import lint_ontology, validate_ontology


def test_validate_ontology_allows_raw_json_in_defaults_fields():
    onto = {
        "version": "test",
        "defaults": {"fields": ["snippet", "raw_json"]},
        "packs": [],
    }
    errors = validate_ontology(onto)
    assert errors == []


def test_validate_ontology_rejects_unknown_default_field():
    onto = {
        "version": "test",
        "defaults": {"fields": ["snippet", "not_a_field"]},
        "packs": [],
    }
    errors = validate_ontology(onto)
    assert errors != []



def test_lint_ontology_flags_unsupplied_and_regex_risk_fields():
    onto = {
        "version": "test",
        "defaults": {"fields": ["raw_json"]},
        "packs": [
            {
                "id": "p",
                "name": "P",
                "enabled": True,
                "default_weight": 1,
                "rules": [
                    {"id": "r1", "type": "regex", "pattern": "(a+)+", "fields": ["raw_json"]},
                ],
            }
        ],
    }

    report = lint_ontology(onto, supplied_fields=["snippet"])
    issues = report["issues"]
    assert any(i.get("type") == "field_not_supplied" for i in issues)
    assert any(i.get("type") == "field_map_mismatch" for i in issues)
    assert any(i.get("type") == "regex_risk" for i in issues)


def test_lint_ontology_flags_unknown_field_reference():
    onto = {
        "version": "test",
        "defaults": {"fields": ["snippet", "not_a_field"]},
        "packs": [
            {
                "id": "p",
                "name": "P",
                "enabled": True,
                "default_weight": 1,
                "rules": [
                    {"id": "r1", "type": "phrase", "pattern": "x", "fields": ["not_a_field"]},
                ],
            }
        ],
    }

    report = lint_ontology(onto, supplied_fields=["snippet"])
    issues = report["issues"]
    assert any(i.get("type") == "unknown_field_reference" for i in issues)


def test_validate_ontology_allows_pack_default_weight_fallback():
    onto = {
        "version": "test",
        "defaults": {"fields": ["snippet"]},
        "packs": [
            {
                "id": "p",
                "name": "P",
                "enabled": True,
                "default_weight": 3,
                "rules": [
                    {"id": "r1", "type": "phrase", "pattern": "alpha"},
                ],
            }
        ],
    }
    errors = validate_ontology(onto)
    assert errors == []


def test_validate_ontology_requires_weight_when_no_fallback_available():
    onto = {
        "version": "test",
        "defaults": {"fields": ["snippet"]},
        "packs": [
            {
                "id": "p",
                "name": "P",
                "enabled": True,
                "rules": [
                    {"id": "r1", "type": "phrase", "pattern": "alpha"},
                ],
            }
        ],
    }
    errors = validate_ontology(onto)
    assert any("weight must be set" in e for e in errors)
