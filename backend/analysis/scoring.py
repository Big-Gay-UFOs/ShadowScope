from __future__ import annotations

from typing import Any, Dict, List, Tuple


_PROXY_PACK_PREFIX = "sam_proxy_"
_DOD_PACK_PREFIX = "sam_dod_"
_SUPPRESSOR_PACKS = {"operational_noise_terms", "sam_proxy_noise_expansion"}
_LORE_RULE_IDS = {"explicit_uap_lore_noise_terms", "uap_lore_with_generic_secrecy_noise"}
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
    if allow_context:
        return "context"
    return "neutral"


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
    base = 0
    if bucket == "proxy":
        base = 4
    elif bucket == "dod":
        base = 3
    elif bucket == "context":
        base = 2
    return base + min(abs(int(weight)), 2)


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
) -> Tuple[int, Dict[str, Any]]:
    kw = _norm_list(keywords)
    cl = _norm_list(clauses)
    correlation_items = [dict(item) for item in _norm_list(correlations) if isinstance(item, dict)]
    context = dict(event_context or {})

    pack_hits: set[str] = set()
    rule_hits: set[tuple[str, str]] = set()
    weighted: List[Dict[str, Any]] = []

    positive_rule_keys: set[str] = set()
    positive_pack_keys: set[str] = set()
    context_pack_keys: set[str] = set()
    suppressor_rule_keys: set[str] = set()

    clause_signal_scores: list[int] = []
    keyword_signal_scores: list[int] = []
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

        if bucket in {"proxy", "dod", "context"}:
            signal_points = _clause_signal_magnitude(bucket, weight)
            if weight < 0:
                if rule_key in suppressor_rule_keys:
                    continue
                suppressor_rule_keys.add(rule_key)
                suppressor_penalties.append(signal_points)
                top_suppressors.append(
                    _copy_penalty(
                        label=label,
                        penalty=signal_points,
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
            if bucket == "context":
                context_pack_keys.add(str(pack).strip().lower())
                context_clause_scores.append(signal_points)
            else:
                positive_pack_keys.add(str(pack).strip().lower())
                clause_signal_scores.append(signal_points)
            top_positive_signals.append(
                _copy_signal(
                    label=label,
                    contribution=signal_points,
                    bucket="structural_context" if bucket == "context" else "proxy_relevance",
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
            if rule_key in suppressor_rule_keys:
                continue
            if rule_key in positive_rule_keys:
                continue
            positive_rule_keys.add(rule_key)
            positive_pack_keys.add(pack)
            signal_points = 2 if bucket == "proxy" else 1
            keyword_signal_scores.append(signal_points)
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
    proxy_diversity_bonus = min(3, max(len(positive_pack_keys) - 1, 0))
    proxy_relevance_score = min(20, int(clause_score + keyword_score + proxy_diversity_bonus))
    context_clause_score = _tiered_sum(context_clause_scores, top_n=3, rest_scale=0.5)
    context_keyword_score = int(sum(context_keyword_scores))
    context_diversity_bonus = min(2, max(len(context_pack_keys) - 1, 0))
    context_ontology_score = min(3, int(context_clause_score + context_keyword_score + context_diversity_bonus))

    corroboration_sources: list[dict[str, Any]] = []
    corroboration_signal_scores: list[int] = []
    corroboration_lanes: set[str] = set()
    structural_lanes: set[str] = set()

    pair_bonus_input = max(_to_int(pair_bonus), 0)
    pair_bonus_applied = 0
    if pair_bonus_input > 0:
        pair_items = [item for item in correlation_items if str(item.get("lane") or "") == "kw_pair"]
        suppressor_pair = False
        positive_pair = False
        for item in pair_items:
            kw1 = item.get("keyword_1")
            kw2 = item.get("keyword_2")
            if _classify_pack(_keyword_parts(kw1)[0]) == "suppressor" or _classify_pack(_keyword_parts(kw2)[0]) == "suppressor":
                suppressor_pair = True
                break
            if _classify_pack(_keyword_parts(kw1)[0]) in {"proxy", "dod"} or _classify_pack(_keyword_parts(kw2)[0]) in {"proxy", "dod"}:
                positive_pair = True
        if not suppressor_pair and (proxy_relevance_score > 0 or positive_pair):
            pair_bonus_applied = min(pair_bonus_input, 6)
            corroboration_signal_scores.append(pair_bonus_applied)
            corroboration_lanes.add("kw_pair")
            top_label = ""
            if pair_items:
                top_label = str(
                    pair_items[0].get("pair_label")
                    or pair_items[0].get("pair_label_raw")
                    or pair_items[0].get("correlation_key")
                    or ""
                ).strip()
            corroboration_sources.append(
                _copy_signal(
                    label=top_label or "kw_pair corroboration",
                    contribution=pair_bonus_applied,
                    bucket="corroboration",
                    signal_type="correlation",
                    lane="kw_pair",
                    event_count=pair_count or (pair_items[0].get("event_count") if pair_items else None),
                    score_signal=pair_items[0].get("score_signal") if pair_items else pair_strength,
                )
            )

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
            if bucket == "suppressor":
                continue
            if bucket not in {"proxy", "dod", "context"}:
                continue
            contribution = 1 if bucket == "context" else 2
            corroboration_signal_scores.append(contribution)
            corroboration_lanes.add(lane)
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

        if lane in _CORROBORATIVE_LINK_LANES and (proxy_relevance_score > 0 or pair_bonus_applied > 0):
            contribution = 2 if lane == "sam_usaspending_candidate_join" else 1
            corroboration_signal_scores.append(contribution)
            corroboration_lanes.add(lane)
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

    corroboration_diversity_bonus = min(2, max(len(corroboration_lanes) - 1, 0))
    corroboration_score = min(
        10,
        _tiered_sum(corroboration_signal_scores, top_n=4, rest_scale=0.5) + corroboration_diversity_bonus,
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
    if entity_bonus or any(_has_text(context.get(field)) for field in ("recipient_uei", "recipient_name", "recipient_cage_code")):
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

    if context_ontology_score > 0:
        structural_signal_scores.append(context_ontology_score)
        structural_signals.append(
            _copy_signal(
                label="starter or context ontology support",
                contribution=context_ontology_score,
                bucket="structural_context",
                signal_type="ontology",
            )
        )

    if _has_text(context.get("naics_code")) or _has_text(context.get("psc_code")):
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

    if any(_has_text(context.get(field)) for field in ("place_of_performance_state", "place_of_performance_country", "place_text")):
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

    if _has_text(context.get("notice_award_type")) or _has_text(context.get("category")):
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

    if any(_has_text(context.get(field)) for field in ("solicitation_number", "notice_id", "document_id", "award_id")):
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

    structural_context_score = min(5, int(sum(structural_signal_scores)))

    if proxy_relevance_score <= 0 and corroboration_score <= 0:
        investigability_score = min(investigability_score, 2)
        structural_context_cap = 1 + min(int(context_ontology_score), 1)
        structural_context_score = min(structural_context_score, structural_context_cap)

    noise_penalty_core = _tiered_sum(suppressor_penalties, top_n=3, rest_scale=0.5)
    noise_uncorroborated_surcharge = 4 if suppressor_penalties and proxy_relevance_score < 6 and corroboration_score < 3 else 0
    noise_penalty = min(18, int(noise_penalty_core + noise_uncorroborated_surcharge))

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
        "pair_count": int(pair_count),
        "pair_count_total": int(pair_count_total),
        "pair_strength": round(_to_float(pair_strength), 4),
        "pair_signal_total": round(_to_float(pair_strength), 4),
        "keyword_hits": len(kw),
        "pack_hits": len(pack_hits),
        "rule_hits": len(rule_hits),
        "has_noise": bool(suppressor_penalties),
        "noise_penalty": int(noise_penalty),
        "noise_penalty_applied": int(noise_penalty),
        "proxy_diversity_bonus": int(proxy_diversity_bonus),
        "corroboration_diversity_bonus": int(corroboration_diversity_bonus),
        "structural_correlation_bonus": int(structural_correlation_bonus),
        "noise_uncorroborated_surcharge": int(noise_uncorroborated_surcharge),
        "noise_penalty_core": int(noise_penalty_core),
        "context_clause_score": int(context_clause_score),
        "context_keyword_score": int(context_keyword_score),
        "context_diversity_bonus": int(context_diversity_bonus),
        "context_ontology_score": int(context_ontology_score),
        "positive_signal_count": len(positive_rule_keys),
        "suppressor_hit_count": len(suppressor_rule_keys),
        "proxy_relevance_score": int(proxy_relevance_score),
        "investigability_score": int(investigability_score),
        "corroboration_score": int(corroboration_score),
        "structural_context_score": int(structural_context_score),
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
                "context_clause_score": int(context_clause_score),
                "context_keyword_score": int(context_keyword_score),
                "context_diversity_bonus": int(context_diversity_bonus),
                "context_ontology_score": int(context_ontology_score),
                "pair_bonus_applied": int(pair_bonus_applied),
                "corroboration_diversity_bonus": int(corroboration_diversity_bonus),
                "entity_bonus": int(entity_bonus),
                "structural_correlation_bonus": int(structural_correlation_bonus),
                "noise_penalty_core": int(noise_penalty_core),
                "noise_uncorroborated_surcharge": int(noise_uncorroborated_surcharge),
            },
        },
    }
    return total_score, details
