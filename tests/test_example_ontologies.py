from __future__ import annotations

import json
from pathlib import Path

from backend.services.tagging import validate_ontology


def _load_example(name: str) -> dict:
    path = Path("examples") / name
    return json.loads(path.read_text(encoding="utf-8-sig"))


def test_example_ontologies_validate():
    for name in [
        "ontology_sam_kwpair_demo.json",
        "ontology_sam_procurement_starter.json",
        "ontology_sam_dod_foia_companion.json",
        "ontology_sam_procurement_plus_dod_foia.json",
        "ontology_usaspending_starter.json",
    ]:
        obj = _load_example(name)
        errs = validate_ontology(obj)
        assert not errs, f"{name} invalid: {errs}"


def test_starter_plus_dod_foia_contains_starter_and_companion_packs():
    starter = _load_example("ontology_sam_procurement_starter.json")
    dod = _load_example("ontology_sam_dod_foia_companion.json")
    plus = _load_example("ontology_sam_procurement_plus_dod_foia.json")

    starter_ids = {p.get("id") for p in starter.get("packs", []) if isinstance(p, dict)}
    dod_ids = {p.get("id") for p in dod.get("packs", []) if isinstance(p, dict)}
    plus_ids = {p.get("id") for p in plus.get("packs", []) if isinstance(p, dict)}

    expected_union = starter_ids | dod_ids
    assert expected_union.issubset(plus_ids)
    assert plus_ids == expected_union


def test_dod_companion_includes_precision_expansion_and_lore_suppressor_rules():
    dod = _load_example("ontology_sam_dod_foia_companion.json")
    packs = {p.get("id"): p for p in dod.get("packs", []) if isinstance(p, dict)}

    assert "sam_dod_hardened_subsurface_infrastructure" in packs

    hardened_rules = {
        r.get("id")
        for r in packs["sam_dod_hardened_subsurface_infrastructure"].get("rules", [])
        if isinstance(r, dict)
    }
    assert "subsurface_shaft_tunneling_context" in hardened_rules
    assert "site_hardened_infrastructure_pair_context" in hardened_rules

    noise_rules = {
        r.get("id")
        for r in packs["operational_noise_terms"].get("rules", [])
        if isinstance(r, dict)
    }
    assert "explicit_uap_lore_noise_terms" in noise_rules
