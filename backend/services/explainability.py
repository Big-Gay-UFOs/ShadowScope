from __future__ import annotations

import math
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.db.models import Correlation, CorrelationLink


LANE_PRIORITY: tuple[str, ...] = (
    "kw_pair",
    "same_keyword",
    "same_entity",
    "same_uei",
    "same_award_id",
    "same_contract_id",
    "same_doc_id",
    "same_agency",
    "same_psc",
    "same_naics",
    "same_place_region",
    "same_sam_naics",
    "sam_usaspending_candidate_join",
)


def norm_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return []


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def coerce_number(value: Any, default: float = 0.0) -> int | float:
    numeric = safe_float(value, default=default)
    if math.isfinite(numeric) and abs(numeric - round(numeric)) < 1e-9:
        return int(round(numeric))
    return round(float(numeric), 6)


def humanize_keyword(keyword: Any) -> str:
    text = str(keyword or "").strip()
    if not text:
        return ""
    pack, sep, rule = text.partition(":")
    if not sep:
        return text.replace("_", " ")
    left = pack.replace("_", " ").strip()
    right = rule.replace("_", " ").strip()
    if left and right:
        return f"{left}: {right}"
    return left or right


def infer_correlation_lane(correlation_key: Any, lanes_hit: Any) -> str:
    if isinstance(lanes_hit, dict):
        lane = lanes_hit.get("lane")
        if isinstance(lane, str) and lane.strip():
            return lane.strip()
        for candidate in LANE_PRIORITY:
            payload = lanes_hit.get(candidate)
            if isinstance(payload, dict):
                return candidate
    key_text = str(correlation_key or "").strip()
    if "|" in key_text:
        return key_text.split("|", 1)[0].strip()
    return key_text or "unknown"


def correlation_lane_payload(lane: str, lanes_hit: Any) -> dict[str, Any]:
    if not isinstance(lanes_hit, dict):
        return {}
    if lanes_hit.get("lane") == lane:
        return lanes_hit
    payload = lanes_hit.get(lane)
    return payload if isinstance(payload, dict) else {}


def sort_lanes(lanes: list[str]) -> list[str]:
    unique = {str(lane).strip() for lane in lanes if str(lane).strip()}
    rank = {lane: idx for idx, lane in enumerate(LANE_PRIORITY)}
    return sorted(unique, key=lambda lane: (rank.get(lane, len(rank)), lane))


def extract_matched_ontology(clauses: Any) -> dict[str, Any]:
    normalized: list[dict[str, Any]] = []
    rule_stats: dict[tuple[str, str], dict[str, Any]] = {}

    for clause in norm_json_list(clauses):
        if not isinstance(clause, dict):
            continue

        item = dict(clause)
        pack = str(item.get("pack") or "").strip()
        rule = str(item.get("rule") or "").strip()
        field = str(item.get("field") or "").strip()
        match = str(item.get("match") or "").strip()
        weight = safe_int(item.get("weight"), default=0)

        item["weight"] = weight
        if pack:
            item["pack"] = pack
        if rule:
            item["rule"] = rule
        if field:
            item["field"] = field
        if match:
            item["match"] = match

        normalized.append(item)

        if pack and rule:
            entry = rule_stats.setdefault(
                (pack, rule),
                {
                    "pack": pack,
                    "rule": rule,
                    "count": 0,
                    "max_weight": 0,
                },
            )
            entry["count"] += 1
            entry["max_weight"] = max(safe_int(entry["max_weight"]), weight)

    normalized.sort(
        key=lambda item: (
            -safe_int(item.get("weight")),
            str(item.get("pack") or ""),
            str(item.get("rule") or ""),
            str(item.get("field") or ""),
            str(item.get("match") or ""),
        )
    )

    matched_rules = [
        f"{entry['pack']}:{entry['rule']}"
        for entry in sorted(
            rule_stats.values(),
            key=lambda item: (
                -safe_int(item.get("max_weight")),
                -safe_int(item.get("count")),
                str(item.get("pack") or ""),
                str(item.get("rule") or ""),
            ),
        )
    ]

    return {
        "matched_ontology_rules": matched_rules,
        "matched_ontology_clauses": normalized,
    }


def aggregate_matched_ontology(clause_lists: list[Any]) -> dict[str, Any]:
    clause_stats: dict[tuple[str, str], dict[str, Any]] = {}

    for clauses in clause_lists:
        extracted = extract_matched_ontology(clauses)
        seen_in_event: set[tuple[str, str]] = set()

        for clause in extracted.get("matched_ontology_clauses", []):
            if not isinstance(clause, dict):
                continue
            pack = str(clause.get("pack") or "").strip()
            rule = str(clause.get("rule") or "").strip()
            if not pack or not rule:
                continue

            key = (pack, rule)
            entry = clause_stats.setdefault(
                key,
                {
                    "pack": pack,
                    "rule": rule,
                    "event_count": 0,
                    "clause_count": 0,
                    "total_weight": 0,
                    "max_weight": 0,
                    "sample_fields": [],
                    "sample_matches": [],
                },
            )

            weight = safe_int(clause.get("weight"), default=0)
            entry["clause_count"] += 1
            entry["total_weight"] += weight
            entry["max_weight"] = max(safe_int(entry["max_weight"]), weight)

            if key not in seen_in_event:
                entry["event_count"] += 1
                seen_in_event.add(key)

            field = str(clause.get("field") or "").strip()
            if field and field not in entry["sample_fields"] and len(entry["sample_fields"]) < 3:
                entry["sample_fields"].append(field)

            match = str(clause.get("match") or "").strip()
            if match and match not in entry["sample_matches"] and len(entry["sample_matches"]) < 3:
                entry["sample_matches"].append(match)

    aggregated: list[dict[str, Any]] = []
    for entry in clause_stats.values():
        clause_count = max(safe_int(entry.get("clause_count")), 1)
        aggregated.append(
            {
                "pack": entry["pack"],
                "rule": entry["rule"],
                "event_count": safe_int(entry.get("event_count")),
                "clause_count": safe_int(entry.get("clause_count")),
                "avg_weight": round(float(entry.get("total_weight", 0)) / clause_count, 2),
                "max_weight": safe_int(entry.get("max_weight")),
                "sample_fields": list(entry.get("sample_fields") or []),
                "sample_matches": list(entry.get("sample_matches") or []),
            }
        )

    aggregated.sort(
        key=lambda item: (
            -safe_int(item.get("event_count")),
            -safe_float(item.get("avg_weight")),
            -safe_int(item.get("max_weight")),
            str(item.get("pack") or ""),
            str(item.get("rule") or ""),
        )
    )

    return {
        "matched_ontology_rules": [
            f"{item['pack']}:{item['rule']}"
            for item in aggregated
            if str(item.get("pack") or "").strip() and str(item.get("rule") or "").strip()
        ],
        "matched_ontology_clauses": aggregated,
    }


def _kw_pair_keywords_from_key(correlation_key: Any) -> tuple[str | None, str | None]:
    parts = str(correlation_key or "").split("|")
    if len(parts) == 3 and parts[0] == "kw_pair":
        return parts[1], parts[2]
    return None, None


def _kw_pair_metadata(
    *,
    correlation_key: Any,
    lane_payload: dict[str, Any],
    member_count: int,
    score_value: Any,
) -> dict[str, Any]:
    kw1 = str(lane_payload.get("keyword_1") or lane_payload.get("k1") or "").strip() or None
    kw2 = str(lane_payload.get("keyword_2") or lane_payload.get("k2") or "").strip() or None
    if not kw1 or not kw2:
        kw1, kw2 = _kw_pair_keywords_from_key(correlation_key)

    event_count = safe_int(lane_payload.get("event_count"), default=member_count)
    if event_count <= 0:
        event_count = member_count
    if event_count <= 0:
        event_count = safe_int(score_value, default=0)

    contribution = 0.0
    if event_count > 0:
        contribution = round(1.0 / math.sqrt(float(event_count)), 6)

    pair_label_raw = f"{kw1} + {kw2}" if kw1 and kw2 else ""
    pair_label = (
        f"{humanize_keyword(kw1)} + {humanize_keyword(kw2)}"
        if kw1 and kw2
        else pair_label_raw
    )

    return {
        "keyword_1": kw1,
        "keyword_2": kw2,
        "pair_label_raw": pair_label_raw,
        "pair_label": pair_label,
        "event_count": event_count,
        "contribution": contribution,
    }


def load_event_correlation_evidence(db: Session, *, event_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not event_ids:
        return {}

    member_counts = (
        db.query(
            CorrelationLink.correlation_id.label("correlation_id"),
            func.count(CorrelationLink.id).label("member_count"),
        )
        .group_by(CorrelationLink.correlation_id)
        .subquery()
    )

    rows = (
        db.query(
            CorrelationLink.event_id,
            Correlation.id,
            Correlation.correlation_key,
            Correlation.score,
            Correlation.window_days,
            Correlation.summary,
            Correlation.rationale,
            Correlation.lanes_hit,
            member_counts.c.member_count,
        )
        .join(Correlation, Correlation.id == CorrelationLink.correlation_id)
        .outerjoin(member_counts, member_counts.c.correlation_id == Correlation.id)
        .filter(CorrelationLink.event_id.in_(event_ids))
        .all()
    )

    by_event: dict[int, list[dict[str, Any]]] = {}
    lane_rank = {lane: idx for idx, lane in enumerate(LANE_PRIORITY)}

    for event_id, correlation_id, correlation_key, score, window_days, summary, rationale, lanes_hit, member_count in rows:
        lane = infer_correlation_lane(correlation_key, lanes_hit)
        payload = correlation_lane_payload(lane, lanes_hit)
        member_count_i = safe_int(member_count, default=0)
        score_signal = coerce_number(payload.get("score_signal", score), default=0.0)
        event_count = safe_int(payload.get("event_count"), default=member_count_i)

        item: dict[str, Any] = {
            "correlation_id": safe_int(correlation_id),
            "correlation_key": correlation_key,
            "lane": lane,
            "score_signal": score_signal,
            "score_signal_raw": score,
            "window_days": safe_int(window_days),
            "event_count": event_count if event_count > 0 else member_count_i,
            "summary": summary,
            "rationale": rationale,
            "contributes_pair_bonus": False,
        }

        if lane == "kw_pair":
            kw_pair = _kw_pair_metadata(
                correlation_key=correlation_key,
                lane_payload=payload,
                member_count=member_count_i,
                score_value=score,
            )
            item.update(kw_pair)
            item["contributes_pair_bonus"] = safe_int(kw_pair.get("event_count"), default=0) > 0

        by_event.setdefault(safe_int(event_id), []).append(item)

    for event_id in list(by_event.keys()):
        by_event[event_id].sort(
            key=lambda item: (
                1 if item.get("contributes_pair_bonus") else 0,
                safe_float(item.get("contribution"), default=0.0),
                safe_float(item.get("score_signal"), default=0.0),
                safe_int(item.get("event_count"), default=0),
                -lane_rank.get(str(item.get("lane") or ""), len(lane_rank)),
                safe_int(item.get("correlation_id"), default=0),
            ),
            reverse=True,
        )

    return by_event


def enrich_lead_score_details(
    *,
    clauses: Any,
    base_details: dict[str, Any] | None,
    correlations: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    details = dict(base_details or {})
    correlation_items = [dict(item) for item in (correlations or []) if isinstance(item, dict)]
    matched = extract_matched_ontology(clauses)
    kw_pairs = [item for item in correlation_items if str(item.get("lane") or "") == "kw_pair"]

    details["contributing_correlations"] = correlation_items
    details["contributing_lanes"] = sort_lanes([str(item.get("lane") or "") for item in correlation_items])
    details["matched_ontology_rules"] = matched["matched_ontology_rules"]
    details["matched_ontology_clauses"] = matched["matched_ontology_clauses"]
    details["pair_count"] = safe_int(details.get("pair_count"), default=len(kw_pairs))
    details["pair_strength"] = round(
        safe_float(
            details.get("pair_strength"),
            default=sum(safe_float(item.get("contribution"), default=0.0) for item in kw_pairs),
        ),
        4,
    )
    details["pair_bonus_applied"] = safe_int(
        details.get("pair_bonus_applied"),
        default=safe_int(details.get("pair_bonus"), default=0),
    )
    details["noise_penalty_applied"] = safe_int(
        details.get("noise_penalty_applied"),
        default=safe_int(details.get("noise_penalty"), default=0),
    )
    if "top_clauses" not in details:
        details["top_clauses"] = matched["matched_ontology_clauses"][:5]
    return details
