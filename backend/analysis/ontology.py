from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple


_ALLOWED_RULE_TYPES = {"phrase", "regex"}
_ALLOWED_FIELDS = {"snippet", "place_text", "doc_id", "source_url", "raw_json"}


def load_ontology(path: Path) -> Dict[str, Any]:
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Ontology root must be a JSON object.")
    return data


def ontology_sha256(obj: Dict[str, Any]) -> str:
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_ontology(obj: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    if "version" not in obj or not isinstance(obj.get("version"), str):
        errors.append("root.version must be a string")
    if "packs" not in obj or not isinstance(obj.get("packs"), list):
        errors.append("root.packs must be a list")

    defaults = obj.get("defaults", {})
    if defaults and not isinstance(defaults, dict):
        errors.append("root.defaults must be an object if present")

    default_fields = defaults.get("fields", ["snippet", "place_text", "doc_id"]) if isinstance(defaults, dict) else ["snippet", "place_text", "doc_id"]
    if not isinstance(default_fields, list) or not all(isinstance(x, str) for x in default_fields):
        errors.append("defaults.fields must be a list of strings")
        default_fields = ["snippet", "place_text", "doc_id"]

    for f in default_fields:
        if f not in _ALLOWED_FIELDS:
            errors.append(f"defaults.fields contains unknown field: {f}")

    packs = obj.get("packs", [])
    pack_ids = set()
    for i, pack in enumerate(packs):
        if not isinstance(pack, dict):
            errors.append(f"packs[{i}] must be an object")
            continue

        pid = pack.get("id")
        if not isinstance(pid, str) or not pid:
            errors.append(f"packs[{i}].id must be a non-empty string")
        else:
            if pid in pack_ids:
                errors.append(f"duplicate pack id: {pid}")
            pack_ids.add(pid)

        if not isinstance(pack.get("name"), str) or not pack.get("name"):
            errors.append(f"packs[{i}].name must be a non-empty string")

        if "enabled" in pack and not isinstance(pack.get("enabled"), bool):
            errors.append(f"packs[{i}].enabled must be boolean")

        rules = pack.get("rules")
        if not isinstance(rules, list) or not rules:
            errors.append(f"packs[{i}].rules must be a non-empty list")
            continue

        rule_ids = set()
        for j, rule in enumerate(rules):
            if not isinstance(rule, dict):
                errors.append(f"packs[{i}].rules[{j}] must be an object")
                continue

            rid = rule.get("id")
            if not isinstance(rid, str) or not rid:
                errors.append(f"packs[{i}].rules[{j}].id must be a non-empty string")
            else:
                if rid in rule_ids:
                    errors.append(f"duplicate rule id in pack {pid}: {rid}")
                rule_ids.add(rid)

            rtype = rule.get("type")
            if rtype not in _ALLOWED_RULE_TYPES:
                errors.append(f"packs[{i}].rules[{j}].type must be one of {_ALLOWED_RULE_TYPES}")

            pattern = rule.get("pattern")
            if not isinstance(pattern, str) or not pattern:
                errors.append(f"packs[{i}].rules[{j}].pattern must be a non-empty string")

            weight = rule.get("weight")
            if not isinstance(weight, int):
                errors.append(f"packs[{i}].rules[{j}].weight must be an integer")

            fields = rule.get("fields", default_fields)
            if not isinstance(fields, list) or not all(isinstance(x, str) for x in fields):
                errors.append(f"packs[{i}].rules[{j}].fields must be a list of strings if present")
            else:
                for f in fields:
                    if f not in _ALLOWED_FIELDS:
                        errors.append(f"packs[{i}].rules[{j}].fields contains unknown field: {f}")

            # regex compile check
            if rtype == "regex" and isinstance(pattern, str) and pattern:
                try:
                    re.compile(pattern)
                except re.error as e:
                    errors.append(f"packs[{i}].rules[{j}].pattern regex compile error: {e}")

    return errors


def summarize_ontology(obj: Dict[str, Any]) -> Dict[str, Any]:
    packs = obj.get("packs", [])
    enabled = 0
    total_rules = 0
    for p in packs:
        if isinstance(p, dict) and p.get("enabled", True):
            enabled += 1
        if isinstance(p, dict) and isinstance(p.get("rules"), list):
            total_rules += len(p["rules"])
    return {
        "version": obj.get("version"),
        "packs": len(packs) if isinstance(packs, list) else 0,
        "packs_enabled": enabled,
        "total_rules": total_rules,
        "hash": ontology_sha256(obj),
    }