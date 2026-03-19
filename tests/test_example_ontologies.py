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
        "ontology_sam_hidden_program_proxy_companion.json",
        "ontology_sam_hidden_program_proxy_exploratory.json",
        "ontology_sam_procurement_plus_dod_foia.json",
        "ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json",
        "ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json",
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


def test_hidden_program_proxy_composites_include_expected_pack_unions():
    starter = _load_example("ontology_sam_procurement_starter.json")
    dod = _load_example("ontology_sam_dod_foia_companion.json")
    precision = _load_example("ontology_sam_hidden_program_proxy_companion.json")
    exploratory = _load_example("ontology_sam_hidden_program_proxy_exploratory.json")
    combined_precision = _load_example("ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json")
    combined_exploratory = _load_example("ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json")

    starter_ids = {p.get("id") for p in starter.get("packs", []) if isinstance(p, dict)}
    dod_ids = {p.get("id") for p in dod.get("packs", []) if isinstance(p, dict)}
    precision_ids = {p.get("id") for p in precision.get("packs", []) if isinstance(p, dict)}
    exploratory_ids = {p.get("id") for p in exploratory.get("packs", []) if isinstance(p, dict)}
    combined_precision_ids = {p.get("id") for p in combined_precision.get("packs", []) if isinstance(p, dict)}
    combined_exploratory_ids = {p.get("id") for p in combined_exploratory.get("packs", []) if isinstance(p, dict)}

    assert combined_precision_ids == (starter_ids | dod_ids | precision_ids)
    assert combined_exploratory_ids == (starter_ids | dod_ids | precision_ids | exploratory_ids)


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


def test_hidden_program_proxy_precision_contains_noise_and_classified_admin_packs():
    proxy = _load_example("ontology_sam_hidden_program_proxy_companion.json")
    packs = {p.get("id"): p for p in proxy.get("packs", []) if isinstance(p, dict)}

    assert "sam_proxy_noise_expansion" in packs
    assert "sam_proxy_classified_contract_security_admin" in packs

    noise_rules = {
        r.get("id")
        for r in packs["sam_proxy_noise_expansion"].get("rules", [])
        if isinstance(r, dict)
    }
    assert "security_training_noise" in noise_rules
    assert "generic_lab_supply_noise" in noise_rules

    admin_rules = {
        r.get("id")
        for r in packs["sam_proxy_classified_contract_security_admin"].get("rules", [])
        if isinstance(r, dict)
    }
    assert "dd254_classification_guide_contract_context" in admin_rules
    assert "visit_authorization_courier_access_context" in admin_rules
