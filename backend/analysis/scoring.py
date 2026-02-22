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


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def score_from_keywords_clauses(
    keywords: Any,
    clauses: Any,
    *,
    has_entity: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    # v1: sum clause weights + fallback keyword score + entity bonus
    kw = _norm_list(keywords)
    cl = _norm_list(clauses)

    clause_score = 0
    pack_hits = set()
    rule_hits = set()
    weighted: List[Dict[str, Any]] = []

    for c in cl:
        if not isinstance(c, dict):
            continue
        w_int = _to_int(c.get("weight", 0))
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

    keyword_score = 0
    if clause_score == 0 and len(kw) > 0:
        keyword_score = 3 * len(kw)

    entity_bonus = 10 if has_entity else 0
    score = clause_score + keyword_score + entity_bonus

    top_clauses = sorted(weighted, key=lambda x: x.get("weight", 0), reverse=True)[:5]
    details: Dict[str, Any] = {
        "scoring_version": "v1",
        "clause_score": clause_score,
        "keyword_score": keyword_score,
        "entity_bonus": entity_bonus,
        "keyword_hits": len(kw),
        "pack_hits": len(pack_hits),
        "rule_hits": len(rule_hits),
        "top_clauses": top_clauses,
    }
    return score, details


def score_from_keywords_clauses_v2(
    keywords: Any,
    clauses: Any,
    *,
    has_entity: bool = False,
    pair_bonus: int = 0,
    top_n: int = 6,
    rest_scale: float = 0.5,
) -> Tuple[int, Dict[str, Any]]:
    # v2: diminishing returns on clause weights + kw_pair synergy + fallback + entity bonus
    kw = _norm_list(keywords)
    cl = _norm_list(clauses)

    pack_hits = set()
    rule_hits = set()
    weighted: List[Dict[str, Any]] = []
    weights: List[int] = []

    for c in cl:
        if not isinstance(c, dict):
            continue
        w_int = _to_int(c.get("weight", 0))

        pack = c.get("pack")
        rule = c.get("rule")
        if isinstance(pack, str) and pack:
            pack_hits.add(pack)
        if isinstance(pack, str) and isinstance(rule, str) and pack and rule:
            rule_hits.add((pack, rule))

        d = dict(c)
        d["weight"] = w_int
        weighted.append(d)
        weights.append(w_int)

    weights_sorted = sorted(weights, reverse=True)
    clause_score_raw = sum(weights_sorted)

    top = weights_sorted[: max(0, int(top_n))]
    rest = weights_sorted[max(0, int(top_n)) :]
    clause_score = int(sum(top) + (rest_scale * sum(rest)))

    keyword_score = 0
    if clause_score == 0 and len(kw) > 0:
        keyword_score = 3 * len(kw)

    entity_bonus = 10 if has_entity else 0
    pair_bonus_int = _to_int(pair_bonus)

    score = clause_score + keyword_score + entity_bonus + pair_bonus_int

    top_clauses = sorted(weighted, key=lambda x: x.get("weight", 0), reverse=True)[:5]
    details: Dict[str, Any] = {
        "scoring_version": "v2",
        "clause_score_raw": clause_score_raw,
        "clause_score": clause_score,
        "keyword_score": keyword_score,
        "entity_bonus": entity_bonus,
        "pair_bonus": pair_bonus_int,
        "keyword_hits": len(kw),
        "pack_hits": len(pack_hits),
        "rule_hits": len(rule_hits),
        "top_n": int(top_n),
        "rest_scale": float(rest_scale),
        "top_clauses": top_clauses,
    }
    return score, details

