from backend.analysis.ontology import validate_ontology


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
