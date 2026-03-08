from __future__ import annotations

import json
from pathlib import Path

from backend.services.tagging import validate_ontology


def test_example_ontologies_validate():
    for name in [
        "ontology_sam_kwpair_demo.json",
        "ontology_sam_procurement_starter.json",
        "ontology_usaspending_starter.json",
    ]:
        path = Path("examples") / name
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        errs = validate_ontology(obj)
        assert not errs, f"{name} invalid: {errs}"
