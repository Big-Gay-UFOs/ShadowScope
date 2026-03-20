from __future__ import annotations

import math
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import aliased
from sqlalchemy.orm import Session

from backend.db.models import Correlation, CorrelationLink, Event
from backend.services.investigator_filters import event_place_region_label
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


def optional_number(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return coerce_number(value, default=0.0)


def optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return safe_int(value)


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
        "c12": optional_int(lane_payload.get("c12")),
        "keyword_1_df": optional_int(lane_payload.get("keyword_1_df")),
        "keyword_2_df": optional_int(lane_payload.get("keyword_2_df")),
        "total_events": optional_int(lane_payload.get("total_events")),
        "score_kind": str(lane_payload.get("score_kind")).strip() or None
        if lane_payload.get("score_kind") is not None
        else None,
        "score_secondary": optional_number(lane_payload.get("score_secondary")),
        "score_secondary_kind": str(lane_payload.get("score_secondary_kind")).strip() or None
        if lane_payload.get("score_secondary_kind") is not None
        else None,
    }


def _candidate_join_metadata(lane_payload: dict[str, Any], score_value: Any) -> dict[str, Any]:
    return {
        "confidence_score": optional_number(lane_payload.get("confidence_score") or score_value),
        "likely_incumbent": bool(lane_payload.get("likely_incumbent")),
        "time_delta_days": optional_number(lane_payload.get("time_delta_days")),
        "evidence_types": [str(item) for item in norm_json_list(lane_payload.get("evidence_types")) if str(item).strip()],
        "matched_values": lane_payload.get("matched_values") if isinstance(lane_payload.get("matched_values"), dict) else {},
        "candidate_join_evidence": [
            dict(item)
            for item in norm_json_list(lane_payload.get("evidence"))
            if isinstance(item, dict)
        ],
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
        elif lane == "sam_usaspending_candidate_join":
            item.update(_candidate_join_metadata(payload, score))

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


def _agency_label_from_values(
    *,
    awarding_name: Any,
    awarding_code: Any,
    funding_name: Any,
    funding_code: Any,
    contracting_name: Any,
    contracting_code: Any,
) -> str | None:
    candidates = (
        (awarding_name, awarding_code),
        (funding_name, funding_code),
        (contracting_name, contracting_code),
    )
    for name, code in candidates:
        name_text = str(name or "").strip()
        code_text = str(code or "").strip().upper()
        if name_text and code_text:
            return f"{name_text} ({code_text})"
        if name_text or code_text:
            return name_text or code_text
    return None


def _place_region_label_from_values(*, state: Any, country: Any) -> str | None:
    state_text = str(state or "").strip().upper()
    country_text = str(country or "").strip().upper()
    if state_text and country_text:
        return f"{state_text}, {country_text}"
    if state_text:
        return state_text
    if country_text:
        return country_text
    return None


def _append_unique_value(bucket: list[Any], value: Any, *, limit: int = 5) -> None:
    if value is None:
        return
    text = str(value).strip()
    if not text:
        return
    if text in {str(item).strip() for item in bucket}:
        return
    if len(bucket) >= int(limit):
        return
    bucket.append(value)


def _linked_source_row_sort_key(row: Any) -> tuple[int, int, str, str, str, int]:
    return (
        safe_int(getattr(row, "target_event_id", None), default=0),
        str(getattr(row, "source", None) or "").strip(),
        str(getattr(row, "doc_id", None) or "").strip(),
        str(getattr(row, "award_id", None) or getattr(row, "solicitation_number", None) or "").strip(),
        safe_int(getattr(row, "linked_event_id", None), default=0),
        safe_int(getattr(row, "correlation_id", None), default=0),
    )


def build_event_context_payload(event: Event | None) -> dict[str, Any]:
    if event is None:
        return {}
    return {
        "category": event.category,
        "source": event.source,
        "source_url": event.source_url,
        "doc_id": event.doc_id,
        "award_id": event.award_id,
        "generated_unique_award_id": event.generated_unique_award_id,
        "piid": event.piid,
        "fain": event.fain,
        "uri": event.uri,
        "source_record_id": event.source_record_id,
        "recipient_name": event.recipient_name,
        "recipient_uei": event.recipient_uei,
        "recipient_cage_code": event.recipient_cage_code,
        "entity_id": event.entity_id,
        "awarding_agency_code": event.awarding_agency_code,
        "awarding_agency_name": event.awarding_agency_name,
        "funding_agency_code": event.funding_agency_code,
        "funding_agency_name": event.funding_agency_name,
        "contracting_office_code": event.contracting_office_code,
        "contracting_office_name": event.contracting_office_name,
        "psc_code": event.psc_code,
        "naics_code": event.naics_code,
        "notice_award_type": event.notice_award_type,
        "place_of_performance_state": event.place_of_performance_state,
        "place_of_performance_country": event.place_of_performance_country,
        "place_text": event.place_text,
        "place_region": event_place_region_label(event),
        "solicitation_number": event.solicitation_number,
        "notice_id": event.notice_id,
        "document_id": event.document_id,
        "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
        "created_at": event.created_at.isoformat() if event.created_at else None,
    }


def load_event_linked_source_summary(
    db: Session,
    *,
    event_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not event_ids:
        return {}

    target_link = aliased(CorrelationLink)
    member_link = aliased(CorrelationLink)

    rows = (
        db.query(
            target_link.event_id.label("target_event_id"),
            target_link.correlation_id.label("correlation_id"),
            Correlation.correlation_key,
            Correlation.lanes_hit,
            Event.id.label("linked_event_id"),
            Event.source,
            Event.doc_id,
            Event.award_id,
            Event.solicitation_number,
            Event.source_url,
            Event.recipient_name,
            Event.recipient_uei,
            Event.awarding_agency_name,
            Event.awarding_agency_code,
            Event.funding_agency_name,
            Event.funding_agency_code,
            Event.contracting_office_name,
            Event.contracting_office_code,
            Event.place_of_performance_state,
            Event.place_of_performance_country,
        )
        .join(Correlation, Correlation.id == target_link.correlation_id)
        .join(member_link, member_link.correlation_id == target_link.correlation_id)
        .join(Event, Event.id == member_link.event_id)
        .filter(target_link.event_id.in_(event_ids))
        .filter(member_link.event_id != target_link.event_id)
        .all()
    )
    rows = sorted(rows, key=_linked_source_row_sort_key)

    by_event: dict[int, dict[str, Any]] = {}
    for row in rows:
        target_event_id = safe_int(getattr(row, "target_event_id", None), default=0)
        correlation_id = safe_int(getattr(row, "correlation_id", None), default=0)
        linked_event_id = safe_int(getattr(row, "linked_event_id", None), default=0)
        if target_event_id <= 0 or correlation_id <= 0 or linked_event_id <= 0:
            continue

        lane = infer_correlation_lane(getattr(row, "correlation_key", None), getattr(row, "lanes_hit", None))
        linked_record = {
            "event_id": linked_event_id,
            "source": getattr(row, "source", None),
            "doc_id": getattr(row, "doc_id", None),
            "award_id": getattr(row, "award_id", None),
            "solicitation_number": getattr(row, "solicitation_number", None),
            "source_url": getattr(row, "source_url", None),
            "recipient_name": getattr(row, "recipient_name", None),
            "recipient_uei": getattr(row, "recipient_uei", None),
            "agency": _agency_label_from_values(
                awarding_name=getattr(row, "awarding_agency_name", None),
                awarding_code=getattr(row, "awarding_agency_code", None),
                funding_name=getattr(row, "funding_agency_name", None),
                funding_code=getattr(row, "funding_agency_code", None),
                contracting_name=getattr(row, "contracting_office_name", None),
                contracting_code=getattr(row, "contracting_office_code", None),
            ),
            "place_region": _place_region_label_from_values(
                state=getattr(row, "place_of_performance_state", None),
                country=getattr(row, "place_of_performance_country", None),
            ),
            "lane": lane,
        }

        event_bucket = by_event.setdefault(
            target_event_id,
            {
                "linked_records_by_correlation": {},
                "_source_summary": {},
            },
        )
        correlation_bucket = event_bucket["linked_records_by_correlation"].setdefault(correlation_id, [])
        if linked_event_id not in {safe_int(item.get("event_id"), default=0) for item in correlation_bucket}:
            correlation_bucket.append(linked_record)

        source = str(linked_record.get("source") or "").strip() or "unknown"
        source_bucket = event_bucket["_source_summary"].setdefault(
            source,
            {
                "source": source,
                "linked_event_count": 0,
                "lanes": set(),
                "sample_event_ids": [],
                "sample_doc_ids": [],
                "sample_award_ids": [],
                "sample_recipients": [],
                "sample_agencies": [],
                "_seen_event_ids": set(),
            },
        )
        if linked_event_id not in source_bucket["_seen_event_ids"]:
            source_bucket["_seen_event_ids"].add(linked_event_id)
            source_bucket["linked_event_count"] += 1
        source_bucket["lanes"].add(lane)
        _append_unique_value(source_bucket["sample_event_ids"], linked_event_id)
        _append_unique_value(source_bucket["sample_doc_ids"], linked_record.get("doc_id"))
        _append_unique_value(source_bucket["sample_award_ids"], linked_record.get("award_id") or linked_record.get("solicitation_number"))
        _append_unique_value(source_bucket["sample_recipients"], linked_record.get("recipient_name") or linked_record.get("recipient_uei"))
        _append_unique_value(source_bucket["sample_agencies"], linked_record.get("agency"))

    out: dict[int, dict[str, Any]] = {}
    for event_id, payload in by_event.items():
        source_summary = []
        for source_payload in payload.get("_source_summary", {}).values():
            source_summary.append(
                {
                    "source": source_payload.get("source"),
                    "linked_event_count": safe_int(source_payload.get("linked_event_count"), default=0),
                    "lanes": sorted(str(item) for item in source_payload.get("lanes", set()) if str(item).strip()),
                    "sample_event_ids": list(source_payload.get("sample_event_ids") or []),
                    "sample_doc_ids": list(source_payload.get("sample_doc_ids") or []),
                    "sample_award_ids": list(source_payload.get("sample_award_ids") or []),
                    "sample_recipients": list(source_payload.get("sample_recipients") or []),
                    "sample_agencies": list(source_payload.get("sample_agencies") or []),
                }
            )
        source_summary.sort(
            key=lambda item: (
                -safe_int(item.get("linked_event_count"), default=0),
                str(item.get("source") or ""),
            )
        )
        out[event_id] = {
            "linked_source_summary": source_summary,
            "linked_records_by_correlation": {
                int(correlation_id): list(records)
                for correlation_id, records in payload.get("linked_records_by_correlation", {}).items()
            },
        }
    return out


def enrich_lead_score_details(
    *,
    clauses: Any,
    base_details: dict[str, Any] | None,
    correlations: list[dict[str, Any]] | None,
    event_context: dict[str, Any] | None = None,
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
    details.setdefault("pair_bonus_quality_cap", safe_int(details.get("pair_bonus_applied"), default=0))
    details.setdefault("pair_bonus_suppressed", 0)
    details["noise_penalty_applied"] = safe_int(
        details.get("noise_penalty_applied"),
        default=safe_int(details.get("noise_penalty"), default=0),
    )
    details.setdefault("starter_only_pair_count", 0)
    details.setdefault("pair_quality_counts", {})
    details.setdefault("cross_lane_bonus", 0)
    details.setdefault("family_relevance_bonus", 0)
    details.setdefault("family_relevant_families", [])
    details.setdefault("starter_context_score", 0)
    details.setdefault("nonstarter_context_score", 0)
    details.setdefault("routine_noise_surcharge", 0)
    details.setdefault("routine_noise_hit_count", 0)
    details.setdefault("weak_proxy_context_cap_applied", False)
    details.setdefault("weak_proxy_context_cap_reason", None)
    details.setdefault("weak_proxy_investigability_delta", 0)
    details.setdefault("weak_proxy_structural_delta", 0)
    if "top_clauses" not in details:
        details["top_clauses"] = matched["matched_ontology_clauses"][:5]
    if "corroboration_sources" not in details:
        details["corroboration_sources"] = [
            dict(item)
            for item in correlation_items[:5]
            if isinstance(item, dict)
        ]
    if "top_positive_signals" not in details:
        positive_signals: list[dict[str, Any]] = []
        for clause in details.get("top_clauses") or []:
            if not isinstance(clause, dict):
                continue
            positive_signals.append(
                {
                    "label": humanize_keyword(
                        f"{clause.get('pack') or ''}:{clause.get('rule') or ''}".strip(":")
                    ),
                    "bucket": "proxy_relevance",
                    "signal_type": "clause",
                    "contribution": safe_int(clause.get("weight"), default=safe_int(clause.get("avg_weight"), default=0)),
                    "pack": clause.get("pack"),
                    "rule": clause.get("rule"),
                    "field": clause.get("field"),
                    "match": clause.get("match"),
                }
            )
        details["top_positive_signals"] = positive_signals[:5]
    details.setdefault("top_suppressors", [])
    if details.get("scoring_version") == "v3" and "subscore_math" not in details:
        proxy_relevance = safe_int(details.get("proxy_relevance_score"), default=0)
        investigability = safe_int(details.get("investigability_score"), default=0)
        corroboration = safe_int(details.get("corroboration_score"), default=0)
        structural = safe_int(details.get("structural_context_score"), default=0)
        noise = safe_int(details.get("noise_penalty"), default=0)
        total = safe_int(
            details.get("total_score"),
            default=proxy_relevance + investigability + corroboration + structural - noise,
        )
        details["subscore_math"] = {
            "formula": "proxy_relevance_score + investigability_score + corroboration_score + structural_context_score - noise_penalty",
            "proxy_relevance_score": proxy_relevance,
            "investigability_score": investigability,
            "corroboration_score": corroboration,
            "structural_context_score": structural,
            "noise_penalty": noise,
            "total_score": total,
            "components": {
                "pair_bonus_quality_cap": safe_int(details.get("pair_bonus_quality_cap"), default=0),
                "pair_bonus_suppressed": safe_int(details.get("pair_bonus_suppressed"), default=0),
                "cross_lane_bonus": safe_int(details.get("cross_lane_bonus"), default=0),
                "family_relevance_bonus": safe_int(details.get("family_relevance_bonus"), default=0),
                "starter_context_score": safe_int(details.get("starter_context_score"), default=0),
                "nonstarter_context_score": safe_int(details.get("nonstarter_context_score"), default=0),
                "routine_noise_surcharge": safe_int(details.get("routine_noise_surcharge"), default=0),
                "weak_proxy_investigability_delta": safe_int(details.get("weak_proxy_investigability_delta"), default=0),
                "weak_proxy_structural_delta": safe_int(details.get("weak_proxy_structural_delta"), default=0),
            },
        }
        details.setdefault("total_score", total)
    if event_context:
        details["event_context"] = dict(event_context)
    return details
