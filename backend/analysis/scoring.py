from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Tuple

from backend.services.lead_families import LEAD_FAMILY_TAXONOMY


_PROXY_PACK_PREFIX = "sam_proxy_"
_DOD_PACK_PREFIX = "sam_dod_"
_STARTER_PACKS = {"sam_procurement_starter"}
_SUPPRESSOR_PACKS = {"operational_noise_terms", "sam_proxy_noise_expansion"}
_LORE_RULE_IDS = {"explicit_uap_lore_noise_terms", "uap_lore_with_generic_secrecy_noise"}
_ROUTINE_NOISE_RULE_IDS = {
    "nsn_line_item_commodity_noise",
    "nsn_part_number_quantity_noise",
    "medical_clinical_procurement_noise",
    "event_hospitality_admin_noise",
    "admin_facility_ops_noise",
    "generic_lab_supply_noise",
    "security_training_noise",
    "generic_facility_maintenance_noise",
    "generic_university_outreach_noise",
    "generic_medical_clinical_noise",
}
_CORROBORATIVE_LINK_LANES = {
    "same_entity",
    "same_uei",
    "same_award_id",
    "same_contract_id",
    "same_doc_id",
    "sam_usaspending_candidate_join",
}
_STRUCTURAL_CORRELATION_LANES = {
    "same_agency",
    "same_psc",
    "same_naics",
    "same_place_region",
    "same_sam_naics",
}


def _build_family_indexes() -> tuple[dict[str, set[str]], dict[str, set[str]], dict[str, str]]:
    rule_index: dict[str, set[str]] = {}
    pack_index: dict[str, set[str]] = {}
    labels: dict[str, str] = {}
    for family, spec in LEAD_FAMILY_TAXONOMY.items():
        family_key = str(family or "").strip().lower()
        if not family_key:
            continue
        labels[family_key] = str(spec.get("label") or family_key.replace("_", " ")).strip()
        for rule_key, weight in dict(spec.get("rule_weights") or {}).items():
            if int(weight or 0) <= 0:
                continue
            normalized_rule = str(rule_key or "").strip().lower()
            if not normalized_rule:
                continue
            rule_index.setdefault(normalized_rule, set()).add(family_key)
        for pack_key, weight in dict(spec.get("pack_weights") or {}).items():
            if int(weight or 0) <= 0:
                continue
            normalized_pack = str(pack_key or "").strip().lower()
            if not normalized_pack:
                continue
            pack_index.setdefault(normalized_pack, set()).add(family_key)
    return rule_index, pack_index, labels


_FAMILY_RULE_INDEX, _FAMILY_PACK_INDEX, _FAMILY_LABELS = _build_family_indexes()


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


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _keyword_parts(value: Any) -> tuple[str, str]:
    text = str(value or "").strip().lower()
    if not text:
        return "", ""
    if text.startswith("kw:"):
        text = text[3:]
    pack, _, rule = text.partition(":")
    return pack.strip(), rule.strip()


def _classify_pack(pack: Any, *, allow_context: bool = False) -> str:
    text = str(pack or "").strip().lower()
    if not text:
        return "neutral"
    if text in _SUPPRESSOR_PACKS:
        return "suppressor"
    if text.startswith(_PROXY_PACK_PREFIX):
        return "proxy"
    if text.startswith(_DOD_PACK_PREFIX):
        return "dod"
    if text in _STARTER_PACKS:
        return "starter" if allow_context else "neutral"
    if allow_context:
        return "context"
    return "neutral"


def _is_routine_noise_rule(rule: Any) -> bool:
    return str(rule or "").strip().lower() in _ROUTINE_NOISE_RULE_IDS


def _families_for_signal(pack: Any, rule_key: Any) -> set[str]:
    normalized_rule = str(rule_key or "").strip().lower()
    normalized_pack = str(pack or "").strip().lower()
    out: set[str] = set()
    if normalized_rule:
        out.update(_FAMILY_RULE_INDEX.get(normalized_rule, set()))
    if normalized_pack:
        out.update(_FAMILY_PACK_INDEX.get(normalized_pack, set()))
    return out


def _family_label(family: Any) -> str:
    family_key = str(family or "").strip().lower()
    return _FAMILY_LABELS.get(family_key, family_key.replace("_", " "))


def _pair_keyword_metadata(keyword: Any) -> dict[str, Any]:
    pack, rule = _keyword_parts(keyword)
    rule_key = _rule_key(pack, rule)
    families = sorted(_families_for_signal(pack, rule_key))
    return {
        "keyword": str(keyword or "").strip(),
        "pack": pack,
        "rule": rule,
        "rule_key": rule_key,
        "bucket": _classify_pack(pack, allow_context=bool(rule)),
        "families": families,
    }


def _rule_key(pack: Any, rule: Any, match: Any = None) -> str:
    pack_text = str(pack or "").strip().lower()
    rule_text = str(rule or "").strip().lower()
    if rule_text:
        return f"{pack_text}:{rule_text}"
    match_text = str(match or "").strip().lower()
    if match_text:
        return f"{pack_text}:match:{match_text[:80]}"
    return pack_text


def _tiered_sum(values: list[int | float], *, top_n: int = 4, rest_scale: float = 0.5) -> int:
    ordered = sorted([float(v) for v in values if float(v) > 0.0], reverse=True)
    if not ordered:
        return 0
    top = ordered[: max(int(top_n), 0)]
    rest = ordered[max(int(top_n), 0):]
    total = sum(top) + (float(rest_scale) * sum(rest))
    return int(round(total))


def _same_keyword_from_correlation_key(correlation_key: Any) -> str:
    parts = str(correlation_key or "").split("|", 3)
    if len(parts) >= 4 and parts[0] == "same_keyword":
        suffix = parts[3]
        if suffix.startswith("kw:"):
            return suffix[3:]
    return ""


def _pack_rule_label(pack: Any, rule: Any) -> str:
    pack_text = str(pack or "").strip()
    rule_text = str(rule or "").strip()
    if pack_text and rule_text:
        return f"{pack_text}:{rule_text}"
    return pack_text or rule_text


def _clause_signal_magnitude(bucket: str, weight: int) -> int:
    magnitude = min(abs(int(weight)), 2)
    if bucket == "proxy":
        return 4 + magnitude
    if bucket == "dod":
        return 3 + magnitude
    if bucket == "context":
        return 2 + min(magnitude, 1)
    if bucket == "starter":
        return 1 + (1 if magnitude >= 2 else 0)
    return 0


def _copy_signal(
    *,
    label: str,
    contribution: int,
    bucket: str,
    signal_type: str,
    pack: Any = None,
    rule: Any = None,
    field: Any = None,
    match: Any = None,
    lane: Any = None,
    event_count: Any = None,
    score_signal: Any = None,
    weight: Any = None,
    is_lore: bool = False,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "label": str(label or "").strip(),
        "contribution": int(contribution),
        "bucket": bucket,
        "signal_type": signal_type,
    }
    if _has_text(pack):
        item["pack"] = str(pack).strip()
    if _has_text(rule):
        item["rule"] = str(rule).strip()
    if _has_text(field):
        item["field"] = str(field).strip()
    if _has_text(match):
        item["match"] = str(match).strip()
    if _has_text(lane):
        item["lane"] = str(lane).strip()
    if event_count is not None:
        item["event_count"] = _to_int(event_count)
    if score_signal is not None:
        item["score_signal"] = round(_to_float(score_signal), 4)
    if weight is not None:
        item["weight"] = _to_int(weight)
    if is_lore:
        item["is_lore"] = True
    return item


def _copy_penalty(
    *,
    label: str,
    penalty: int,
    signal_type: str,
    pack: Any = None,
    rule: Any = None,
    field: Any = None,
    match: Any = None,
    is_lore: bool = False,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "label": str(label or "").strip(),
        "penalty": int(penalty),
        "signal_type": signal_type,
    }
    if _has_text(pack):
        item["pack"] = str(pack).strip()
    if _has_text(rule):
        item["rule"] = str(rule).strip()
    if _has_text(field):
        item["field"] = str(field).strip()
    if _has_text(match):
        item["match"] = str(match).strip()
    if is_lore:
        item["is_lore"] = True
    return item


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
    rest = weights_sorted[max(0, int(top_n)):]
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


def score_from_keywords_clauses_v3(
    keywords: Any,
    clauses: Any,
    *,
    has_entity: bool = False,
    pair_bonus: int = 0,
    pair_count: int = 0,
    pair_count_total: int = 0,
    pair_strength: float = 0.0,
    correlations: Any = None,
    event_context: dict[str, Any] | None = None,
    allow_source_metadata_boosts: bool = True,
) -> Tuple[int, Dict[str, Any]]:
    return _score_from_keywords_clauses_v3_tuned(
        keywords,
        clauses,
        has_entity=has_entity,
        pair_bonus=pair_bonus,
        pair_count=pair_count,
        pair_count_total=pair_count_total,
        pair_strength=pair_strength,
        correlations=correlations,
        event_context=event_context,
        allow_source_metadata_boosts=allow_source_metadata_boosts,
    )


def _score_from_keywords_clauses_v3_tuned(
    keywords: Any,
    clauses: Any,
    *,
    has_entity: bool = False,
    pair_bonus: int = 0,
    pair_count: int = 0,
    pair_count_total: int = 0,
    pair_strength: float = 0.0,
    correlations: Any = None,
    event_context: dict[str, Any] | None = None,
    allow_source_metadata_boosts: bool = True,
) -> Tuple[int, Dict[str, Any]]:
    kw = _norm_list(keywords)
    cl = _norm_list(clauses)
    correlation_items = [dict(item) for item in _norm_list(correlations) if isinstance(item, dict)]
    context = dict(event_context or {})
    allow_source_metadata_boosts = bool(allow_source_metadata_boosts)

    pack_hits: set[str] = set()
    rule_hits: set[tuple[str, str]] = set()
    weighted: List[Dict[str, Any]] = []

    positive_rule_keys: set[str] = set()
    positive_noncontext_rule_keys: set[str] = set()
    positive_pack_keys: set[str] = set()
    starter_pack_keys: set[str] = set()
    context_pack_keys: set[str] = set()
    suppressor_rule_keys: set[str] = set()
    routine_noise_rule_hits: set[str] = set()
    family_signal_hits: dict[str, set[str]] = defaultdict(set)
    family_pair_hits: dict[str, int] = defaultdict(int)

    clause_signal_scores: list[int] = []
    keyword_signal_scores: list[int] = []
    starter_clause_scores: list[int] = []
    starter_keyword_scores: list[int] = []
    context_clause_scores: list[int] = []
    context_keyword_scores: list[int] = []
    suppressor_penalties: list[int] = []

    top_positive_signals: list[dict[str, Any]] = []
    top_suppressors: list[dict[str, Any]] = []
    top_clauses: list[dict[str, Any]] = []

    for clause in cl:
        if not isinstance(clause, dict):
            continue

        item = dict(clause)
        pack = str(item.get("pack") or "").strip()
        rule = str(item.get("rule") or "").strip()
        field = str(item.get("field") or "").strip()
        match = str(item.get("match") or "").strip()
        weight = _to_int(item.get("weight", 0))

        if pack:
            pack_hits.add(pack)
        if pack and rule:
            rule_hits.add((pack, rule))

        item["weight"] = weight
        weighted.append(item)

        bucket = _classify_pack(pack, allow_context=True)
        rule_key = _rule_key(pack, rule, match)
        label = _pack_rule_label(pack, rule) or field or "ontology_match"

        if bucket in {"proxy", "dod", "context", "starter"}:
            signal_points = _clause_signal_magnitude(bucket, weight)
            if weight < 0:
                if rule_key in suppressor_rule_keys:
                    continue
                suppressor_rule_keys.add(rule_key)
                if _is_routine_noise_rule(rule):
                    routine_noise_rule_hits.add(str(rule).strip().lower())
                penalty = max(signal_points, 1)
                suppressor_penalties.append(penalty)
                top_suppressors.append(
                    _copy_penalty(
                        label=label,
                        penalty=penalty,
                        signal_type="clause",
                        pack=pack,
                        rule=rule,
                        field=field,
                        match=match,
                    )
                )
                continue
            if rule_key in positive_rule_keys:
                continue
            positive_rule_keys.add(rule_key)
            pack_key = str(pack).strip().lower()
            if bucket == "starter":
                starter_pack_keys.add(pack_key)
                starter_clause_scores.append(signal_points)
            elif bucket == "context":
                context_pack_keys.add(pack_key)
                context_clause_scores.append(signal_points)
            else:
                positive_pack_keys.add(pack_key)
                positive_noncontext_rule_keys.add(rule_key)
                clause_signal_scores.append(signal_points)
                for family in _families_for_signal(pack, rule_key):
                    family_signal_hits[family].add(rule_key)
            top_positive_signals.append(
                _copy_signal(
                    label=label,
                    contribution=signal_points,
                    bucket="structural_context" if bucket in {"context", "starter"} else "proxy_relevance",
                    signal_type="clause",
                    pack=pack,
                    rule=rule,
                    field=field,
                    match=match,
                    weight=weight,
                )
            )
            top_clauses.append(item)
            continue

        if bucket == "suppressor":
            if rule_key in suppressor_rule_keys:
                continue
            suppressor_rule_keys.add(rule_key)
            if _is_routine_noise_rule(rule):
                routine_noise_rule_hits.add(str(rule).strip().lower())
            is_lore = str(rule or "").strip().lower() in _LORE_RULE_IDS
            penalty = 6 if is_lore else 4
            suppressor_penalties.append(penalty)
            top_suppressors.append(
                _copy_penalty(
                    label=label,
                    penalty=penalty,
                    signal_type="clause",
                    pack=pack,
                    rule=rule,
                    field=field,
                    match=match,
                    is_lore=is_lore,
                )
            )

    for keyword in kw:
        if not isinstance(keyword, str):
            continue
        pack, rule = _keyword_parts(keyword)
        rule_key = _rule_key(pack, rule)
        label = _pack_rule_label(pack, rule) or str(keyword).strip()
        bucket = _classify_pack(pack, allow_context=bool(rule))

        if bucket in {"proxy", "dod"}:
            if rule_key in suppressor_rule_keys or rule_key in positive_rule_keys:
                continue
            positive_rule_keys.add(rule_key)
            positive_noncontext_rule_keys.add(rule_key)
            positive_pack_keys.add(pack)
            signal_points = 2 if bucket == "proxy" else 1
            keyword_signal_scores.append(signal_points)
            for family in _families_for_signal(pack, rule_key):
                family_signal_hits[family].add(rule_key)
            top_positive_signals.append(
                _copy_signal(
                    label=label,
                    contribution=signal_points,
                    bucket="proxy_relevance",
                    signal_type="keyword",
                    pack=pack,
                    rule=rule,
                )
            )
            continue

        if bucket == "starter":
            if rule_key in suppressor_rule_keys or rule_key in positive_rule_keys:
                continue
            positive_rule_keys.add(rule_key)
            starter_pack_keys.add(pack)
            starter_keyword_scores.append(1)
            top_positive_signals.append(
                _copy_signal(
                    label=label,
                    contribution=1,
                    bucket="structural_context",
                    signal_type="keyword",
                    pack=pack,
                    rule=rule,
                )
            )
            continue

        if bucket == "context":
            if rule_key in suppressor_rule_keys or rule_key in positive_rule_keys:
                continue
            positive_rule_keys.add(rule_key)
            context_pack_keys.add(pack)
            signal_points = 1
            context_keyword_scores.append(signal_points)
            top_positive_signals.append(
                _copy_signal(
                    label=label,
                    contribution=signal_points,
                    bucket="structural_context",
                    signal_type="keyword",
                    pack=pack,
                    rule=rule,
                )
            )
            continue

        if bucket == "suppressor":
            if rule_key in suppressor_rule_keys:
                continue
            suppressor_rule_keys.add(rule_key)
            if _is_routine_noise_rule(rule):
                routine_noise_rule_hits.add(str(rule).strip().lower())
            is_lore = rule in _LORE_RULE_IDS
            penalty = 4 if is_lore else 3
            suppressor_penalties.append(penalty)
            top_suppressors.append(
                _copy_penalty(
                    label=label,
                    penalty=penalty,
                    signal_type="keyword",
                    pack=pack,
                    rule=rule,
                    is_lore=is_lore,
                )
            )

    clause_score_raw = int(sum(clause_signal_scores))
    clause_score = _tiered_sum(clause_signal_scores, top_n=4, rest_scale=0.5)
    keyword_score = int(sum(keyword_signal_scores))
    proxy_diversity_bonus = min(4, max(len(positive_pack_keys) - 1, 0))
    proxy_rule_diversity_bonus = min(2, max(len(positive_noncontext_rule_keys) - 2, 0))
    proxy_relevance_score = min(24, int(clause_score + keyword_score + proxy_diversity_bonus + proxy_rule_diversity_bonus))
    starter_clause_score = _tiered_sum(starter_clause_scores, top_n=2, rest_scale=0.25)
    starter_keyword_score = int(sum(starter_keyword_scores))
    starter_context_score = min(2, int(starter_clause_score + starter_keyword_score))
    nonstarter_context_clause_score = _tiered_sum(context_clause_scores, top_n=2, rest_scale=0.5)
    nonstarter_context_keyword_score = int(sum(context_keyword_scores))
    context_diversity_bonus = min(2, max(len(context_pack_keys) - 1, 0))
    nonstarter_context_score = min(
        3,
        int(nonstarter_context_clause_score + nonstarter_context_keyword_score + context_diversity_bonus),
    )
    context_clause_score = int(starter_clause_score + nonstarter_context_clause_score)
    context_keyword_score = int(starter_keyword_score + nonstarter_context_keyword_score)
    context_ontology_score = min(4, int(starter_context_score + nonstarter_context_score))

    corroboration_sources: list[dict[str, Any]] = []
    corroboration_signal_scores: list[int] = []
    corroboration_lanes: set[str] = set()
    non_pair_corroboration_lanes: set[str] = set()
    structural_lanes: set[str] = set()

    pair_bonus_input = max(_to_int(pair_bonus), 0)
    pair_items = [item for item in correlation_items if str(item.get("lane") or "") == "kw_pair"]
    pair_candidates: list[dict[str, Any]] = []
    pair_quality_counts: dict[str, int] = {
        "starter_only": 0,
        "proxy_pair": 0,
        "proxy_dod_companion": 0,
        "family_relevant": 0,
        "suppressed": 0,
    }
    starter_only_pair_count = 0
    pair_bonus_applied = 0
    pair_bonus_quality_cap = 0
    pair_bonus_suppressed = 0

    for item in pair_items:
        meta_items = [
            _pair_keyword_metadata(item.get("keyword_1")),
            _pair_keyword_metadata(item.get("keyword_2")),
        ]
        if any(meta.get("bucket") == "suppressor" for meta in meta_items):
            pair_quality_counts["suppressed"] += 1
            continue

        buckets = {
            str(meta.get("bucket") or "")
            for meta in meta_items
            if str(meta.get("bucket") or "") and str(meta.get("bucket") or "") != "neutral"
        }
        contains_proxy = "proxy" in buckets
        contains_dod = "dod" in buckets
        contains_starter = "starter" in buckets
        distinct_nonstarter_packs = {
            str(meta.get("pack") or "")
            for meta in meta_items
            if str(meta.get("bucket") or "") in {"proxy", "dod"}
        }
        shared_families = set(meta_items[0].get("families") or []) & set(meta_items[1].get("families") or [])
        if shared_families:
            for family in shared_families:
                family_pair_hits[family] += 1
        elif contains_proxy or contains_dod:
            union_families = set(meta_items[0].get("families") or []) | set(meta_items[1].get("families") or [])
            if len(union_families) == 1:
                family_pair_hits[next(iter(union_families))] += 1

        quality_labels: list[str] = []
        quality_cap = 0
        if buckets and buckets <= {"starter", "context"}:
            starter_only_pair_count += 1
            pair_quality_counts["starter_only"] += 1
            quality_cap = 1
            quality_labels.append("starter_only")
        elif contains_proxy or contains_dod:
            pair_quality_counts["proxy_pair"] += 1
            quality_cap = 3
            quality_labels.append("proxy_pair")
            if contains_proxy and contains_dod:
                pair_quality_counts["proxy_dod_companion"] += 1
                quality_cap += 1
                quality_labels.append("proxy_dod_companion")
            elif len(distinct_nonstarter_packs) >= 2:
                quality_cap += 1
                quality_labels.append("multi_pack")
            if shared_families:
                pair_quality_counts["family_relevant"] += 1
                quality_cap += 1
                quality_labels.append("family_relevant")
            if contains_starter and quality_cap > 2:
                quality_cap -= 1
                quality_labels.append("starter_mixed")
        elif buckets:
            quality_cap = 1
            quality_labels.append("context_only")

        if quality_cap <= 0:
            continue

        pair_candidates.append(
            {
                "label": str(
                    item.get("pair_label")
                    or item.get("pair_label_raw")
                    or item.get("correlation_key")
                    or "kw_pair corroboration"
                ).strip(),
                "quality_cap": int(quality_cap),
                "quality_labels": quality_labels,
                "shared_families": sorted(shared_families),
                "event_count": item.get("event_count"),
                "score_signal": item.get("score_signal"),
            }
        )

    link_lane_gate = proxy_relevance_score > 0 or bool(pair_candidates)
    for item in correlation_items:
        lane = str(item.get("lane") or "").strip()
        if not lane or lane == "kw_pair":
            continue

        if lane in _STRUCTURAL_CORRELATION_LANES:
            structural_lanes.add(lane)
            continue

        if lane == "same_keyword":
            keyword_value = _same_keyword_from_correlation_key(item.get("correlation_key"))
            pack, rule = _keyword_parts(keyword_value)
            bucket = _classify_pack(pack, allow_context=bool(rule))
            if bucket in {"suppressor", "starter"}:
                continue
            if bucket not in {"proxy", "dod", "context"}:
                continue
            contribution = 1 if bucket == "context" else 2
            corroboration_signal_scores.append(contribution)
            corroboration_lanes.add(lane)
            non_pair_corroboration_lanes.add(lane)
            corroboration_sources.append(
                _copy_signal(
                    label=_pack_rule_label(pack, rule) or keyword_value or "same_keyword",
                    contribution=contribution,
                    bucket="corroboration",
                    signal_type="correlation",
                    pack=pack,
                    rule=rule,
                    lane=lane,
                    event_count=item.get("event_count"),
                    score_signal=item.get("score_signal"),
                )
            )
            continue

        if lane in _CORROBORATIVE_LINK_LANES and link_lane_gate:
            if lane == "sam_usaspending_candidate_join":
                contribution = 3 if proxy_relevance_score >= 8 else 2
            elif lane in {"same_doc_id", "same_contract_id", "same_award_id"}:
                contribution = 2
            else:
                contribution = 1
            corroboration_signal_scores.append(contribution)
            corroboration_lanes.add(lane)
            non_pair_corroboration_lanes.add(lane)
            corroboration_sources.append(
                _copy_signal(
                    label=str(item.get("summary") or item.get("correlation_key") or lane).strip(),
                    contribution=contribution,
                    bucket="corroboration",
                    signal_type="correlation",
                    lane=lane,
                    event_count=item.get("event_count"),
                    score_signal=item.get("score_signal"),
                )
            )

    cross_lane_bonus = min(2, len(non_pair_corroboration_lanes))
    if cross_lane_bonus > 0:
        corroboration_signal_scores.append(cross_lane_bonus)
        corroboration_sources.append(
            _copy_signal(
                label="cross-lane corroboration",
                contribution=cross_lane_bonus,
                bucket="corroboration",
                signal_type="summary",
                lane=",".join(sorted(non_pair_corroboration_lanes)),
            )
        )

    family_relevant_families: list[dict[str, Any]] = []
    for family, hits in family_signal_hits.items():
        distinct_hits = len({str(item).strip().lower() for item in hits if str(item).strip()})
        if distinct_hits <= 0:
            continue
        bonus = 0
        if distinct_hits >= 2:
            bonus += 1
        if distinct_hits >= 3:
            bonus += 1
        if family_pair_hits.get(family, 0) > 0:
            bonus += 1
        if distinct_hits >= 2 and non_pair_corroboration_lanes:
            bonus += 1
        family_relevant_families.append(
            {
                "family": family,
                "label": _family_label(family),
                "distinct_signal_count": int(distinct_hits),
                "pair_count": int(family_pair_hits.get(family, 0)),
                "bonus": min(int(bonus), 4),
            }
        )
    family_relevant_families.sort(
        key=lambda item: (
            -_to_int(item.get("bonus", 0)),
            -_to_int(item.get("distinct_signal_count", 0)),
            str(item.get("family") or ""),
        )
    )
    family_relevance_bonus = _to_int(family_relevant_families[0].get("bonus")) if family_relevant_families else 0
    if family_relevance_bonus > 0:
        corroboration_signal_scores.append(family_relevance_bonus)
        corroboration_sources.append(
            _copy_signal(
                label="family-relevant pack cluster",
                contribution=family_relevance_bonus,
                bucket="corroboration",
                signal_type="ontology",
            )
        )

    if pair_bonus_input > 0 and pair_candidates:
        pair_candidates.sort(
            key=lambda item: (
                -_to_int(item.get("quality_cap", 0)),
                -_to_float(item.get("score_signal")),
                -_to_int(item.get("event_count", 0)),
                str(item.get("label") or ""),
            )
        )
        pair_bonus_quality_cap = _to_int(pair_candidates[0].get("quality_cap"))
        if len(pair_candidates) > 1 and _to_int(pair_candidates[1].get("quality_cap")) >= 3:
            pair_bonus_quality_cap += 1
        if pair_bonus_quality_cap >= 2:
            pair_bonus_quality_cap += cross_lane_bonus
        pair_bonus_quality_cap = min(int(pair_bonus_quality_cap), 8)
        pair_bonus_applied = min(pair_bonus_input, pair_bonus_quality_cap)
        pair_bonus_suppressed = max(pair_bonus_input - pair_bonus_applied, 0)
        if pair_bonus_applied > 0:
            corroboration_signal_scores.append(pair_bonus_applied)
            corroboration_lanes.add("kw_pair")
            top_pair = pair_candidates[0]
            pair_signal = _copy_signal(
                label=str(top_pair.get("label") or "kw_pair corroboration"),
                contribution=pair_bonus_applied,
                bucket="corroboration",
                signal_type="correlation",
                lane="kw_pair",
                event_count=pair_count or top_pair.get("event_count"),
                score_signal=top_pair.get("score_signal") or pair_strength,
            )
            pair_signal["pair_quality"] = list(top_pair.get("quality_labels") or [])
            if top_pair.get("shared_families"):
                pair_signal["families"] = [
                    {"family": family, "label": _family_label(family)}
                    for family in top_pair.get("shared_families") or []
                ]
            corroboration_sources.append(pair_signal)

    corroboration_diversity_bonus = min(2, max(len(corroboration_lanes) - 1, 0))
    corroboration_score = min(
        13,
        _tiered_sum(corroboration_signal_scores, top_n=5, rest_scale=0.5) + corroboration_diversity_bonus,
    )

    investigability_signals: list[dict[str, Any]] = []
    investigability_signal_scores: list[int] = []

    traceable_id_fields = (
        "doc_id",
        "document_id",
        "notice_id",
        "solicitation_number",
        "award_id",
        "source_record_id",
        "piid",
        "fain",
        "uri",
    )
    if any(_has_text(context.get(field)) for field in traceable_id_fields):
        investigability_signal_scores.append(2)
        investigability_signals.append(
            _copy_signal(
                label="traceable source identifiers",
                contribution=2,
                bucket="investigability",
                signal_type="field",
                field="record_id",
            )
        )

    if _has_text(context.get("source_url")):
        investigability_signal_scores.append(1)
        investigability_signals.append(
            _copy_signal(
                label="source URL available",
                contribution=1,
                bucket="investigability",
                signal_type="field",
                field="source_url",
            )
        )

    if any(
        _has_text(context.get(field))
        for field in (
            "awarding_agency_code",
            "awarding_agency_name",
            "funding_agency_code",
            "funding_agency_name",
            "contracting_office_code",
            "contracting_office_name",
        )
    ):
        investigability_signal_scores.append(1)
        investigability_signals.append(
            _copy_signal(
                label="agency or office handle",
                contribution=1,
                bucket="investigability",
                signal_type="field",
                field="agency",
            )
        )

    if context.get("occurred_at") is not None or context.get("created_at") is not None:
        investigability_signal_scores.append(1)
        investigability_signals.append(
            _copy_signal(
                label="time anchor present",
                contribution=1,
                bucket="investigability",
                signal_type="field",
                field="occurred_at",
            )
        )

    entity_bonus = 1 if has_entity else 0
    has_vendor_handle = any(_has_text(context.get(field)) for field in ("recipient_uei", "recipient_name", "recipient_cage_code"))
    if entity_bonus or (allow_source_metadata_boosts and has_vendor_handle):
        investigability_signal_scores.append(1)
        investigability_signals.append(
            _copy_signal(
                label="vendor or entity handle",
                contribution=1,
                bucket="investigability",
                signal_type="field",
                field="entity_id" if has_entity else "recipient_uei",
            )
        )

    investigability_score = min(6, int(sum(investigability_signal_scores)))

    structural_signals: list[dict[str, Any]] = []
    structural_signal_scores: list[int] = []

    if starter_context_score > 0:
        structural_signal_scores.append(starter_context_score)
        structural_signals.append(
            _copy_signal(
                label="starter ontology support",
                contribution=starter_context_score,
                bucket="structural_context",
                signal_type="ontology",
            )
        )

    if nonstarter_context_score > 0:
        structural_signal_scores.append(nonstarter_context_score)
        structural_signals.append(
            _copy_signal(
                label="non-starter context support",
                contribution=nonstarter_context_score,
                bucket="structural_context",
                signal_type="ontology",
            )
        )

    if allow_source_metadata_boosts and (_has_text(context.get("naics_code")) or _has_text(context.get("psc_code"))):
        structural_signal_scores.append(1)
        structural_signals.append(
            _copy_signal(
                label="procurement codes present",
                contribution=1,
                bucket="structural_context",
                signal_type="field",
                field="naics_code" if _has_text(context.get("naics_code")) else "psc_code",
            )
        )

    if allow_source_metadata_boosts and any(
        _has_text(context.get(field))
        for field in ("place_of_performance_state", "place_of_performance_country", "place_text")
    ):
        structural_signal_scores.append(1)
        structural_signals.append(
            _copy_signal(
                label="place context present",
                contribution=1,
                bucket="structural_context",
                signal_type="field",
                field="place_of_performance_state",
            )
        )

    if allow_source_metadata_boosts and (_has_text(context.get("notice_award_type")) or _has_text(context.get("category"))):
        structural_signal_scores.append(1)
        structural_signals.append(
            _copy_signal(
                label="notice or category context",
                contribution=1,
                bucket="structural_context",
                signal_type="field",
                field="notice_award_type" if _has_text(context.get("notice_award_type")) else "category",
            )
        )

    if allow_source_metadata_boosts and any(
        _has_text(context.get(field))
        for field in ("solicitation_number", "notice_id", "document_id", "award_id")
    ):
        structural_signal_scores.append(1)
        structural_signals.append(
            _copy_signal(
                label="procurement handle present",
                contribution=1,
                bucket="structural_context",
                signal_type="field",
                field="solicitation_number",
            )
        )

    if structural_lanes:
        structural_correlation_bonus = min(2, len(structural_lanes))
        structural_signal_scores.append(structural_correlation_bonus)
        structural_signals.append(
            _copy_signal(
                label="structural corroboration lanes",
                contribution=structural_correlation_bonus,
                bucket="structural_context",
                signal_type="correlation",
                lane=",".join(sorted(structural_lanes)),
            )
        )
    else:
        structural_correlation_bonus = 0

    structural_context_score = min(6, int(sum(structural_signal_scores)))

    original_investigability_score = int(investigability_score)
    original_structural_context_score = int(structural_context_score)
    weak_proxy_context_cap_reason = None
    if proxy_relevance_score <= 0 and corroboration_score <= 0:
        investigability_score = min(investigability_score, 2)
        structural_context_cap = 1 + min(int(context_ontology_score > 0), 1)
        structural_context_score = min(structural_context_score, structural_context_cap)
        weak_proxy_context_cap_reason = "context_without_proxy_or_corroboration"
    elif proxy_relevance_score < 8 and corroboration_score < 5:
        weak_investigability_cap = 4 + (1 if allow_source_metadata_boosts and has_vendor_handle else 0)
        investigability_score = min(investigability_score, weak_investigability_cap)
        structural_context_cap = 3 if nonstarter_context_score > 0 else 2
        if starter_only_pair_count > 0 and not non_pair_corroboration_lanes:
            structural_context_cap = min(structural_context_cap, 2)
        structural_context_score = min(structural_context_score, structural_context_cap)
        weak_proxy_context_cap_reason = "starter_heavy_or_weakly_corroborated"

    weak_proxy_investigability_delta = max(original_investigability_score - int(investigability_score), 0)
    weak_proxy_structural_delta = max(original_structural_context_score - int(structural_context_score), 0)
    weak_proxy_context_cap_applied = bool(weak_proxy_investigability_delta or weak_proxy_structural_delta)

    noise_penalty_core = _tiered_sum(suppressor_penalties, top_n=3, rest_scale=0.5)
    routine_noise_surcharge = min(len(routine_noise_rule_hits) * 2, 4)
    if routine_noise_rule_hits and (starter_only_pair_count > 0 or starter_context_score > 0):
        routine_noise_surcharge += 1
    noise_uncorroborated_surcharge = 4 if suppressor_penalties and proxy_relevance_score < 8 and corroboration_score < 4 else 0
    noise_penalty = min(20, int(noise_penalty_core + noise_uncorroborated_surcharge + routine_noise_surcharge))

    total_score = int(
        proxy_relevance_score
        + investigability_score
        + corroboration_score
        + structural_context_score
        - noise_penalty
    )

    positive_items = (
        top_positive_signals
        + corroboration_sources
        + investigability_signals
        + structural_signals
    )
    positive_items.sort(
        key=lambda item: (
            -_to_int(item.get("contribution", 0)),
            str(item.get("bucket") or ""),
            str(item.get("label") or ""),
        )
    )
    top_suppressors.sort(
        key=lambda item: (
            -_to_int(item.get("penalty", 0)),
            str(item.get("label") or ""),
        )
    )
    corroboration_sources.sort(
        key=lambda item: (
            -_to_int(item.get("contribution", 0)),
            -_to_int(item.get("event_count", 0)),
            str(item.get("label") or ""),
        )
    )

    details: Dict[str, Any] = {
        "scoring_version": "v3",
        "clause_score_raw": int(clause_score_raw),
        "clause_score": int(clause_score),
        "keyword_score": int(keyword_score),
        "entity_bonus": int(entity_bonus),
        "pair_bonus": int(pair_bonus_applied),
        "pair_bonus_applied": int(pair_bonus_applied),
        "pair_bonus_quality_cap": int(pair_bonus_quality_cap),
        "pair_bonus_suppressed": int(pair_bonus_suppressed),
        "pair_count": int(pair_count),
        "pair_count_total": int(pair_count_total),
        "pair_strength": round(_to_float(pair_strength), 4),
        "pair_signal_total": round(_to_float(pair_strength), 4),
        "pair_quality_counts": {key: int(value) for key, value in pair_quality_counts.items()},
        "starter_only_pair_count": int(starter_only_pair_count),
        "keyword_hits": len(kw),
        "pack_hits": len(pack_hits),
        "rule_hits": len(rule_hits),
        "has_noise": bool(suppressor_penalties),
        "noise_penalty": int(noise_penalty),
        "noise_penalty_applied": int(noise_penalty),
        "proxy_diversity_bonus": int(proxy_diversity_bonus),
        "proxy_rule_diversity_bonus": int(proxy_rule_diversity_bonus),
        "corroboration_diversity_bonus": int(corroboration_diversity_bonus),
        "cross_lane_bonus": int(cross_lane_bonus),
        "family_relevance_bonus": int(family_relevance_bonus),
        "family_relevant_families": family_relevant_families[:3],
        "structural_correlation_bonus": int(structural_correlation_bonus),
        "noise_uncorroborated_surcharge": int(noise_uncorroborated_surcharge),
        "routine_noise_surcharge": int(routine_noise_surcharge),
        "noise_penalty_core": int(noise_penalty_core),
        "context_clause_score": int(context_clause_score),
        "context_keyword_score": int(context_keyword_score),
        "starter_clause_score": int(starter_clause_score),
        "starter_keyword_score": int(starter_keyword_score),
        "starter_context_score": int(starter_context_score),
        "nonstarter_context_clause_score": int(nonstarter_context_clause_score),
        "nonstarter_context_keyword_score": int(nonstarter_context_keyword_score),
        "nonstarter_context_score": int(nonstarter_context_score),
        "context_diversity_bonus": int(context_diversity_bonus),
        "context_ontology_score": int(context_ontology_score),
        "positive_signal_count": len(positive_rule_keys),
        "suppressor_hit_count": len(suppressor_rule_keys),
        "routine_noise_hit_count": len(routine_noise_rule_hits),
        "proxy_relevance_score": int(proxy_relevance_score),
        "investigability_score": int(investigability_score),
        "corroboration_score": int(corroboration_score),
        "structural_context_score": int(structural_context_score),
        "weak_proxy_context_cap_applied": bool(weak_proxy_context_cap_applied),
        "weak_proxy_context_cap_reason": weak_proxy_context_cap_reason,
        "weak_proxy_investigability_delta": int(weak_proxy_investigability_delta),
        "weak_proxy_structural_delta": int(weak_proxy_structural_delta),
        "total_score": int(total_score),
        "top_positive_signals": positive_items[:8],
        "top_suppressors": top_suppressors[:8],
        "corroboration_sources": corroboration_sources[:6],
        "top_clauses": sorted(top_clauses, key=lambda item: item.get("weight", 0), reverse=True)[:5],
        "subscore_math": {
            "formula": "proxy_relevance_score + investigability_score + corroboration_score + structural_context_score - noise_penalty",
            "proxy_relevance_score": int(proxy_relevance_score),
            "investigability_score": int(investigability_score),
            "corroboration_score": int(corroboration_score),
            "structural_context_score": int(structural_context_score),
            "noise_penalty": int(noise_penalty),
            "total_score": int(total_score),
            "components": {
                "clause_score": int(clause_score),
                "keyword_score": int(keyword_score),
                "proxy_diversity_bonus": int(proxy_diversity_bonus),
                "proxy_rule_diversity_bonus": int(proxy_rule_diversity_bonus),
                "context_clause_score": int(context_clause_score),
                "context_keyword_score": int(context_keyword_score),
                "starter_context_score": int(starter_context_score),
                "nonstarter_context_score": int(nonstarter_context_score),
                "context_diversity_bonus": int(context_diversity_bonus),
                "context_ontology_score": int(context_ontology_score),
                "pair_bonus_applied": int(pair_bonus_applied),
                "pair_bonus_quality_cap": int(pair_bonus_quality_cap),
                "pair_bonus_suppressed": int(pair_bonus_suppressed),
                "corroboration_diversity_bonus": int(corroboration_diversity_bonus),
                "cross_lane_bonus": int(cross_lane_bonus),
                "family_relevance_bonus": int(family_relevance_bonus),
                "entity_bonus": int(entity_bonus),
                "structural_correlation_bonus": int(structural_correlation_bonus),
                "noise_penalty_core": int(noise_penalty_core),
                "routine_noise_surcharge": int(routine_noise_surcharge),
                "noise_uncorroborated_surcharge": int(noise_uncorroborated_surcharge),
                "weak_proxy_investigability_delta": int(weak_proxy_investigability_delta),
                "weak_proxy_structural_delta": int(weak_proxy_structural_delta),
            },
        },
    }
    return total_score, details
