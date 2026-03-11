from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from backend.analysis.ontology import (
    ALLOWED_TAG_FIELDS,
    resolve_default_fields,
    resolve_default_weight,
    resolve_pack_default_weight,
)


@dataclass(frozen=True)
class CompiledRule:
    pack_id: str
    rule_id: str
    rule_type: str           # "phrase" or "regex"
    pattern: str
    weight: int
    weight_source: str
    fields: List[str]
    regex: Optional[re.Pattern]


def _is_truthy_text(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""


def _truncate_text(text: str, max_len: int, *, marker: str) -> str:
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    if len(marker) >= max_len:
        return text[:max_len]

    head_len = (max_len - len(marker)) // 2
    tail_len = max_len - len(marker) - head_len
    return text[:head_len] + marker + text[-tail_len:]


def _normalize_jsonish(
    value: Any,
    *,
    depth: int,
    max_depth: int,
    max_items: int,
    max_string_len: int,
) -> Any:
    if depth >= max_depth:
        return "__depth_truncated__"

    if isinstance(value, Mapping):
        items = sorted(((str(k), v) for k, v in value.items()), key=lambda kv: kv[0])
        if len(items) > max_items:
            keep_head = max_items // 2
            keep_tail = max_items - keep_head
            selected = items[:keep_head] + items[-keep_tail:]
            out = {
                k: _normalize_jsonish(
                    v,
                    depth=depth + 1,
                    max_depth=max_depth,
                    max_items=max_items,
                    max_string_len=max_string_len,
                )
                for k, v in selected
            }
            out["__truncated_keys__"] = len(items) - len(selected)
            return out
        return {
            k: _normalize_jsonish(
                v,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string_len=max_string_len,
            )
            for k, v in items
        }

    if isinstance(value, set):
        seq: Sequence[Any] = sorted(value, key=lambda x: str(x))
    elif isinstance(value, (list, tuple)):
        seq = value
    else:
        seq = ()

    if seq:
        if len(seq) > max_items:
            keep_head = max_items // 2
            keep_tail = max_items - keep_head
            head = seq[:keep_head]
            tail = seq[-keep_tail:]
            out = [
                _normalize_jsonish(
                    x,
                    depth=depth + 1,
                    max_depth=max_depth,
                    max_items=max_items,
                    max_string_len=max_string_len,
                )
                for x in head
            ]
            out.append(f"__truncated_items__:{len(seq) - len(head) - len(tail)}")
            out.extend(
                _normalize_jsonish(
                    x,
                    depth=depth + 1,
                    max_depth=max_depth,
                    max_items=max_items,
                    max_string_len=max_string_len,
                )
                for x in tail
            )
            return out
        return [
            _normalize_jsonish(
                x,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string_len=max_string_len,
            )
            for x in seq
        ]

    if isinstance(value, str):
        if len(value) <= max_string_len:
            return value
        overflow = len(value) - max_string_len
        marker = f"...<truncated:{overflow}chars>..."
        return _truncate_text(value, max_string_len, marker=marker)

    if isinstance(value, (int, float, bool)) or value is None:
        return value

    text = str(value)
    if len(text) <= max_string_len:
        return text
    overflow = len(text) - max_string_len
    marker = f"...<truncated:{overflow}chars>..."
    return _truncate_text(text, max_string_len, marker=marker)


def safe_json_text(obj: Any, max_len: int = 65536) -> str:
    """Deterministic JSON-ish text for ontology tagging/debugging.

    Guarantees:
    - stable key ordering for objects
    - bounded array/string/depth expansion
    - deterministic truncation with both prefix and suffix retained
    """
    if obj is None:
        return ""

    normalized = _normalize_jsonish(
        obj,
        depth=0,
        max_depth=6,
        max_items=64,
        max_string_len=2048,
    )
    text = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text

    overflow = len(text) - max_len
    marker = f"...<truncated_json:{overflow}chars>..."
    return _truncate_text(text, max_len, marker=marker)


def compile_for_tagging(ontology: Dict[str, Any]) -> Tuple[Dict[str, Any], List[CompiledRule]]:
    defaults = ontology.get("defaults", {}) if isinstance(ontology.get("defaults", {}), dict) else {}
    case_insensitive = bool(defaults.get("case_insensitive", True))
    default_fields = resolve_default_fields(defaults)
    global_default_weight = resolve_default_weight(defaults)

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

        pack_default_weight, pack_weight_source = resolve_pack_default_weight(pack, global_default_weight)

        rules = pack.get("rules", [])
        if not isinstance(rules, list):
            continue

        for rule in rules:
            if not isinstance(rule, dict):
                continue
            rid = rule.get("id")
            rtype = rule.get("type")
            pattern = rule.get("pattern")

            if not isinstance(rid, str) or not rid:
                continue
            if rtype not in ("phrase", "regex"):
                continue
            if not isinstance(pattern, str) or not pattern:
                continue

            if isinstance(rule.get("weight"), int):
                weight = int(rule.get("weight"))
                weight_source = "rule.weight"
            elif isinstance(pack_default_weight, int):
                weight = int(pack_default_weight)
                weight_source = str(pack_weight_source or "pack.default_weight")
            else:
                continue

            fields = rule.get("fields", default_fields)
            if not isinstance(fields, list) or not all(isinstance(f, str) for f in fields):
                fields = list(default_fields)

            filtered_fields = [f for f in fields if f in ALLOWED_TAG_FIELDS]
            if not filtered_fields:
                filtered_fields = list(default_fields)

            compiled_re = None
            if rtype == "regex":
                try:
                    compiled_re = re.compile(pattern, flags=flags)
                except re.error:
                    continue

            compiled.append(
                CompiledRule(
                    pack_id=pack_id,
                    rule_id=rid,
                    rule_type=rtype,
                    pattern=pattern,
                    weight=weight,
                    weight_source=weight_source,
                    fields=list(filtered_fields),
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
                            "weight_applied": r.weight,
                            "weight_source": r.weight_source,
                            "contribution": r.weight,
                            "field": field,
                            "pattern": r.pattern,
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
                            "weight_applied": r.weight,
                            "weight_source": r.weight_source,
                            "contribution": r.weight,
                            "field": field,
                            "pattern": r.pattern,
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
