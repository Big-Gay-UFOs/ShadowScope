from pathlib import Path

from backend.analysis.ontology import load_ontology, validate_ontology, summarize_ontology, ontology_sha256


def test_default_ontology_valid():
    root = Path(__file__).resolve().parents[2]
    path = root / "ontology.json"
    obj = load_ontology(path)
    errs = validate_ontology(obj)
    assert errs == []
    summary = summarize_ontology(obj)
    assert summary["packs"] >= 1
    assert summary["total_rules"] >= 1
    h = ontology_sha256(obj)
    assert isinstance(h, str) and len(h) == 64