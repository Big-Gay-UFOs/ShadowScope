from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


_ALLOWED_RULE_TYPES = {"phrase", "regex"}
_ALLOWED_FIELDS = {"snippet", "place_text", "doc_id", "source_url", "raw_json"}
_DEFAULT_FIELDS = ["snippet", "place_text", "doc_id"]


def load_ontology(path: Path) -> Dict[str, Any]:
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError("Ontology root must be a JSON object.")
    return data


def ontology_sha256(obj: Dict[str, Any]) -> str:
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def resolve_default_fields(defaults: Any) -> List[str]:
    raw_fields = defaults.get("fields", list(_DEFAULT_FIELDS)) if isinstance(defaults, dict) else list(_DEFAULT_FIELDS)
    if not isinstance(raw_fields, list) or not all(isinstance(x, str) for x in raw_fields):
        return list(_DEFAULT_FIELDS)

    out: List[str] = []
    for field in raw_fields:
        if field in _ALLOWED_FIELDS and field not in out:
            out.append(field)
    return out or list(_DEFAULT_FIELDS)


def resolve_default_weight(defaults: Any) -> Optional[int]:
    if isinstance(defaults, dict) and isinstance(defaults.get("default_weight"), int):
        return int(defaults.get("default_weight"))
    return None


def resolve_pack_default_weight(pack: Any, fallback: Optional[int] = None) -> tuple[Optional[int], Optional[str]]:
    if isinstance(pack, dict):
        if isinstance(pack.get("default_weight"), int):
            return int(pack.get("default_weight")), "pack.default_weight"
        # Keep backward compatibility with existing ontologies that use pack.weight.
        if isinstance(pack.get("weight"), int):
            return int(pack.get("weight")), "pack.weight"
    if isinstance(fallback, int):
        return int(fallback), "defaults.default_weight"
    return None, None


def _regex_risk_warnings(pattern: str) -> List[str]:
    warnings: List[str] = []
    p = (pattern or "").strip()
    if not p:
        return warnings

    if p in {".*", ".+", "(?s).*", "(?s).+"}:
        warnings.append("pattern is a full wildcard and may match nearly everything")

    if len(p) > 400:
        warnings.append("pattern is very long; review for precision and runtime risk")

    # Heuristic nested-quantifier check, e.g. (a+)+ or (.*)+
    nested_quantifier = re.search(r"\((?:[^()\\]|\\.)*[+*](?:[^()\\]|\\.)*\)[+*{]", p)
    if nested_quantifier:
        warnings.append("pattern may contain nested quantifiers that can trigger catastrophic backtracking")

    if re.search(r"\.\*\.\*|\.\+\.\+", p):
        warnings.append("pattern contains repeated broad wildcards")

    return warnings


def lint_ontology(obj: Dict[str, Any], *, supplied_fields: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    supplied = {str(f) for f in (supplied_fields or []) if isinstance(f, str) and str(f).strip()}
    if not supplied:
        supplied = set(_ALLOWED_FIELDS)

    issues: List[Dict[str, Any]] = []

    defaults = obj.get("defaults", {})
    default_fields = resolve_default_fields(defaults)
    global_default_weight = resolve_default_weight(defaults)

    default_unknown = [f for f in default_fields if f not in _ALLOWED_FIELDS]
    for field in default_unknown:
        issues.append(
            {
                "type": "unknown_field_reference",
                "scope": "defaults.fields",
                "field": field,
                "message": f"Default field '{field}' is not supported by the tagger.",
            }
        )

    default_unsupplied = [f for f in default_fields if f not in supplied]
    if default_unsupplied:
        issues.append(
            {
                "type": "field_map_mismatch",
                "scope": "defaults.fields",
                "fields": sorted(default_unsupplied),
                "message": "Default fields include values that are never supplied by the taggable field map.",
            }
        )

    packs = obj.get("packs", [])
    if not isinstance(packs, list):
        return {
            "status": "ok",
            "issue_count": len(issues),
            "issues": issues,
            "supplied_fields": sorted(supplied),
        }

    for pack_idx, pack in enumerate(packs):
        if not isinstance(pack, dict):
            continue
        if pack.get("enabled", True) is False:
            continue

        pack_id = str(pack.get("id") or f"packs[{pack_idx}]")
        pack_default_weight, _ = resolve_pack_default_weight(pack, global_default_weight)

        rules = pack.get("rules", [])
        if not isinstance(rules, list):
            continue

        for rule_idx, rule in enumerate(rules):
            if not isinstance(rule, dict):
                continue

            rule_id = str(rule.get("id") or f"rules[{rule_idx}]")

            fields = rule.get("fields", default_fields)
            if not isinstance(fields, list) or not all(isinstance(x, str) for x in fields):
                fields = list(default_fields)
            if not fields:
                fields = list(default_fields)

            unknown_fields = [f for f in fields if f not in _ALLOWED_FIELDS]
            for field in unknown_fields:
                issues.append(
                    {
                        "type": "unknown_field_reference",
                        "scope": f"pack:{pack_id}/rule:{rule_id}",
                        "field": field,
                        "message": f"Rule references unsupported field '{field}'.",
                    }
                )

            unsupplied_fields = [f for f in fields if f in _ALLOWED_FIELDS and f not in supplied]
            if unsupplied_fields:
                issues.append(
                    {
                        "type": "field_not_supplied",
                        "scope": f"pack:{pack_id}/rule:{rule_id}",
                        "fields": sorted(set(unsupplied_fields)),
                        "message": "Rule targets fields that are never supplied by the taggable field map.",
                    }
                )

            weight = rule.get("weight", pack_default_weight)
            if not isinstance(weight, int):
                issues.append(
                    {
                        "type": "missing_weight",
                        "scope": f"pack:{pack_id}/rule:{rule_id}",
                        "message": "Rule has no integer weight and no usable default_weight fallback.",
                    }
                )

            if rule.get("type") == "regex" and isinstance(rule.get("pattern"), str):
                for msg in _regex_risk_warnings(str(rule.get("pattern"))):
                    issues.append(
                        {
                            "type": "regex_risk",
                            "scope": f"pack:{pack_id}/rule:{rule_id}",
                            "message": msg,
                            "pattern": rule.get("pattern"),
                        }
                    )

    return {
        "status": "ok",
        "issue_count": len(issues),
        "issues": issues,
        "supplied_fields": sorted(supplied),
    }


def validate_ontology(obj: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    if "version" not in obj or not isinstance(obj.get("version"), str):
        errors.append("root.version must be a string")
    if "packs" not in obj or not isinstance(obj.get("packs"), list):
        errors.append("root.packs must be a list")

    defaults = obj.get("defaults", {})
    if defaults and not isinstance(defaults, dict):
        errors.append("root.defaults must be an object if present")

    if isinstance(defaults, dict) and "default_weight" in defaults and not isinstance(defaults.get("default_weight"), int):
        errors.append("defaults.default_weight must be an integer if present")

    raw_default_fields = defaults.get("fields", list(_DEFAULT_FIELDS)) if isinstance(defaults, dict) else list(_DEFAULT_FIELDS)
    if not isinstance(raw_default_fields, list) or not all(isinstance(x, str) for x in raw_default_fields):
        errors.append("defaults.fields must be a list of strings")
        raw_default_fields = list(_DEFAULT_FIELDS)

    default_fields = resolve_default_fields(defaults)
    for field in raw_default_fields:
        if field not in _ALLOWED_FIELDS:
            errors.append(f"defaults.fields contains unknown field: {field}")

    global_default_weight = resolve_default_weight(defaults)

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

        if "default_weight" in pack and not isinstance(pack.get("default_weight"), int):
            errors.append(f"packs[{i}].default_weight must be an integer if present")
        if "weight" in pack and not isinstance(pack.get("weight"), int):
            errors.append(f"packs[{i}].weight must be an integer if present")

        pack_default_weight, _ = resolve_pack_default_weight(pack, global_default_weight)

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

            if "weight" in rule:
                if not isinstance(rule.get("weight"), int):
                    errors.append(f"packs[{i}].rules[{j}].weight must be an integer")
            elif not isinstance(pack_default_weight, int):
                errors.append(
                    f"packs[{i}].rules[{j}].weight must be set or a pack/default default_weight must be provided"
                )

            fields = rule.get("fields", default_fields)
            if not isinstance(fields, list) or not all(isinstance(x, str) for x in fields):
                errors.append(f"packs[{i}].rules[{j}].fields must be a list of strings if present")
            else:
                for field in fields:
                    if field not in _ALLOWED_FIELDS:
                        errors.append(f"packs[{i}].rules[{j}].fields contains unknown field: {field}")

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


ALLOWED_TAG_FIELDS = set(_ALLOWED_FIELDS)
DEFAULT_TAG_FIELDS = list(_DEFAULT_FIELDS)


__all__ = [
    "ALLOWED_TAG_FIELDS",
    "DEFAULT_TAG_FIELDS",
    "lint_ontology",
    "load_ontology",
    "ontology_sha256",
    "resolve_default_fields",
    "resolve_default_weight",
    "resolve_pack_default_weight",
    "summarize_ontology",
    "validate_ontology",
]
