from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class CompiledRule:
    pack_id: str
    rule_id: str
    rule_type: str           # "phrase" or "regex"
    pattern: str
    weight: int
    fields: List[str]
    regex: Optional[re.Pattern]


def _is_truthy_text(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""


def compile_for_tagging(ontology: Dict[str, Any]) -> Tuple[Dict[str, Any], List[CompiledRule]]:
    defaults = ontology.get("defaults", {}) if isinstance(ontology.get("defaults", {}), dict) else {}
    case_insensitive = bool(defaults.get("case_insensitive", True))
    default_fields = defaults.get("fields", ["snippet", "place_text", "doc_id"])
    if not isinstance(default_fields, list) or not all(isinstance(f, str) for f in default_fields):
        default_fields = ["snippet", "place_text", "doc_id"]

    flags = re.IGNORECASE if case_insensitive else 0

    compiled: List[CompiledRule] = []
    packs = ontology.get("packs", [])
    if not isinstance(packs, list):
        return {"case_insensitive": case_insensitive, "default_fields": default_fields}, compiled

    for pack in packs:
        if not isinstance(pack, dict):
            continue
        if pack.get("enabled", True) is False:
            continue

        pack_id = pack.get("id")
        if not isinstance(pack_id, str) or not pack_id:
            continue

        rules = pack.get("rules", [])
        if not isinstance(rules, list):
            continue

        for rule in rules:
            if not isinstance(rule, dict):
                continue
            rid = rule.get("id")
            rtype = rule.get("type")
            pattern = rule.get("pattern")
            weight = rule.get("weight")

            if not isinstance(rid, str) or not rid:
                continue
            if rtype not in ("phrase", "regex"):
                continue
            if not isinstance(pattern, str) or not pattern:
                continue
            if not isinstance(weight, int):
                continue

            fields = rule.get("fields", default_fields)
            if not isinstance(fields, list) or not all(isinstance(f, str) for f in fields):
                fields = list(default_fields)

            compiled_re = None
            if rtype == "regex":
                compiled_re = re.compile(pattern, flags=flags)

            compiled.append(
                CompiledRule(
                    pack_id=pack_id,
                    rule_id=rid,
                    rule_type=rtype,
                    pattern=pattern,
                    weight=weight,
                    fields=list(fields),
                    regex=compiled_re,
                )
            )

    meta = {"case_insensitive": case_insensitive, "default_fields": default_fields}
    return meta, compiled


def tag_fields(
    meta: Dict[str, Any],
    rules: List[CompiledRule],
    fields: Dict[str, Any],
) -> Dict[str, Any]:
    case_insensitive = bool(meta.get("case_insensitive", True))

    # normalize texts once
    text_map: Dict[str, str] = {}
    for k, v in fields.items():
        if _is_truthy_text(v):
            text_map[k] = str(v)
        else:
            text_map[k] = ""

    keywords_set = set()
    clauses: List[Dict[str, Any]] = []
    score = 0

    for r in rules:
        matched_rule = False

        for field in r.fields:
            text = text_map.get(field, "")
            if not text:
                continue

            if r.rule_type == "phrase":
                hay = text.lower() if case_insensitive else text
                needle = r.pattern.lower() if case_insensitive else r.pattern
                if needle in hay:
                    matched_rule = True
                    clauses.append(
                        {
                            "pack": r.pack_id,
                            "rule": r.rule_id,
                            "type": r.rule_type,
                            "weight": r.weight,
                            "field": field,
                            "match": r.pattern,
                        }
                    )
                    score += r.weight

            else:  # regex
                assert r.regex is not None
                m = r.regex.search(text)
                if m:
                    matched_rule = True
                    clauses.append(
                        {
                            "pack": r.pack_id,
                            "rule": r.rule_id,
                            "type": r.rule_type,
                            "weight": r.weight,
                            "field": field,
                            "match": m.group(0),
                        }
                    )
                    score += r.weight

        if matched_rule:
            keywords_set.add(f"{r.pack_id}:{r.rule_id}")

    # deterministic output
    keywords = sorted(keywords_set)
    clauses_sorted = sorted(
        clauses,
        key=lambda c: (
            c.get("pack", ""),
            c.get("rule", ""),
            c.get("field", ""),
            str(c.get("match", "")),
        ),
    )

    return {"keywords": keywords, "clauses": clauses_sorted, "score": score}