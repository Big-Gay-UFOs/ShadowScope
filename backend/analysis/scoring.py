from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _norm_list(value: Any) -> list:
    # Normalize JSON-ish fields (some older rows may store {} instead of []).
    if value is None:
        return []
    if isinstance(value, dict):
        return []
    if isinstance(value, list):
        return value
    return []


def score_from_keywords_clauses(
    keywords: Any,
    clauses: Any,
    *,
    has_entity: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    kw = _norm_list(keywords)
    cl = _norm_list(clauses)

    clause_score = 0
    pack_hits = set()
    rule_hits = set()
    weighted: List[Dict[str, Any]] = []

    for c in cl:
        if not isinstance(c, dict):
            continue
        w = c.get("weight", 0)
        try:
            w_int = int(w)
        except Exception:
            w_int = 0

        clause_score += w_int

        pack = c.get("pack")
        rule = c.get("rule")
        if isinstance(pack, str) and pack:
            pack_hits.add(pack)
        if isinstance(pack, str) and isinstance(rule, str) and pack and rule:
            rule_hits.add((pack, rule))

        d = dict(c)
        d["weight"] = w_int
        weighted.append(d)

    # Backward-compatible fallback: if no clauses, use keywords count.
    keyword_score = 0
    if clause_score == 0 and len(kw) > 0:
        keyword_score = 3 * len(kw)

    entity_bonus = 10 if has_entity else 0
    score = clause_score + keyword_score + entity_bonus

    top_clauses = sorted(weighted, key=lambda x: x.get("weight", 0), reverse=True)[:5]

    details = {
        "clause_score": clause_score,
        "keyword_score": keyword_score,
        "entity_bonus": entity_bonus,
        "keyword_hits": len(kw),
        "pack_hits": len(pack_hits),
        "rule_hits": len(rule_hits),
        "top_clauses": top_clauses,
    }
    return score, details