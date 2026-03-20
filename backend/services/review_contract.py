from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from backend.db.models import Event, LeadSnapshot, LeadSnapshotItem
from backend.services.investigator_filters import event_place_region_label


RANKED_LEAD_REVIEW_CONTRACT_VERSION = "ranked_lead.review_row.v1"

CANONICAL_RANKED_LEAD_REVIEW_FIELDS: tuple[str, ...] = (
    "snapshot_id",
    "snapshot_item_id",
    "snapshot_scoring_version",
    "rank",
    "score",
    "scoring_version",
    "lead_family",
    "lead_family_label",
    "secondary_lead_families",
    "why_summary",
    "score_details",
    "top_positive_signals",
    "top_suppressors",
    "corroboration_summary",
    "contributing_lanes",
    "linked_source_summary",
    "candidate_join_evidence",
    "event_id",
    "event_hash",
    "entity_id",
    "category",
    "source",
    "doc_id",
    "source_url",
    "snippet",
    "occurred_at",
    "created_at",
    "place_text",
    "place_region",
    "solicitation_number",
    "notice_id",
    "document_id",
    "award_id",
    "piid",
    "generated_unique_award_id",
    "source_record_id",
    "awarding_agency_code",
    "awarding_agency_name",
    "funding_agency_code",
    "funding_agency_name",
    "contracting_office_code",
    "contracting_office_name",
    "recipient_name",
    "recipient_uei",
    "recipient_parent_uei",
    "recipient_duns",
    "recipient_cage_code",
    "vendor_name",
    "vendor_uei",
    "vendor_parent_uei",
    "vendor_duns",
    "vendor_cage_code",
    "psc_code",
    "psc_description",
    "naics_code",
    "naics_description",
    "has_core_identifiers",
    "has_agency_target",
    "has_vendor_context",
    "has_classification_context",
    "has_foia_handles",
    "completeness_summary",
)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _norm_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _score_part(details: dict[str, Any], key: str, default: Any = 0) -> Any:
    value = details.get(key, default)
    return default if value is None else value


def _normalize_review_window_timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def list_text(values: list[Any], *, limit: int = 5) -> str:
    return "; ".join([str(value) for value in values[: int(limit)] if str(value).strip()])


def top_clauses_text(details: dict[str, Any], *, limit: int = 5) -> str:
    items = details.get("matched_ontology_clauses") or details.get("top_clauses") or []
    out: list[str] = []
    if isinstance(items, list):
        for clause in items[: int(limit)]:
            if not isinstance(clause, dict):
                continue
            pack = clause.get("pack") or ""
            rule = clause.get("rule") or ""
            weight = clause.get("weight")
            avg_weight = clause.get("avg_weight")
            event_count = clause.get("event_count")
            if pack and rule and avg_weight is not None:
                out.append(f"{pack}:{rule}(events={event_count},avg={avg_weight})")
            elif pack and rule:
                out.append(f"{pack}:{rule}({weight})")
            elif pack:
                out.append(f"{pack}({weight})")
            else:
                out.append(f"clause({weight})")
    return "; ".join(out)


def correlation_text(correlation: dict[str, Any]) -> str:
    lane = str(correlation.get("lane") or "")
    label = str(correlation.get("pair_label") or correlation.get("correlation_key") or "")
    event_count = correlation.get("event_count")
    score_signal = correlation.get("score_signal")
    if label and event_count is not None and score_signal is not None:
        return f"{lane}:{label}(signal={score_signal},n={event_count})"
    if label:
        return f"{lane}:{label}" if lane else label
    return lane or "correlation"


def signal_text(signal: dict[str, Any]) -> str:
    label = str(signal.get("label") or "").strip()
    bucket = str(signal.get("bucket") or "").strip()
    contribution = signal.get("contribution")
    if label and contribution is not None:
        return f"{bucket}:{label}(+{contribution})" if bucket else f"{label}(+{contribution})"
    if label:
        return label
    return bucket or "signal"


def suppressor_text(signal: dict[str, Any]) -> str:
    label = str(signal.get("label") or "").strip()
    penalty = signal.get("penalty")
    if label and penalty is not None:
        return f"{label}(-{penalty})"
    return label or "suppressor"


def family_assignment_text(assignment: dict[str, Any]) -> str:
    family = str(assignment.get("family") or "").strip()
    score = assignment.get("score")
    rationale = str(assignment.get("rationale") or "").strip()
    if family and score is not None:
        if rationale:
            return f"{family}({score}): {rationale}"
        return f"{family}({score})"
    return family or rationale or "lead_family"


def candidate_join_text(entry: dict[str, Any]) -> str:
    evidence_types = [str(item) for item in (entry.get("evidence_types") or []) if str(item).strip()]
    linked_sources = [str(item) for item in (entry.get("linked_sources") or []) if str(item).strip()]
    parts = ["candidate"]
    if evidence_types:
        parts.append("evidence=" + ",".join(evidence_types))
    if linked_sources:
        parts.append("sources=" + ",".join(linked_sources))
    if entry.get("likely_incumbent"):
        parts.append("likely_incumbent=true")
    score_signal = entry.get("score_signal")
    if score_signal is not None:
        parts.append(f"score={score_signal}")
    return " | ".join(parts)


def linked_source_text(entry: dict[str, Any]) -> str:
    source = str(entry.get("source") or "").strip()
    count = entry.get("linked_event_count")
    lanes = [str(item) for item in (entry.get("lanes") or []) if str(item).strip()]
    if source and count is not None and lanes:
        return f"{source}(n={count},lanes={','.join(lanes)})"
    if source and count is not None:
        return f"{source}(n={count})"
    return source or "linked_source"


def why_summary(details: dict[str, Any]) -> str:
    if str(details.get("scoring_version") or "").strip().lower() == "v3":
        why_bits = [
            f"proxy={_score_part(details, 'proxy_relevance_score', 0)}",
            f"investigability={_score_part(details, 'investigability_score', 0)}",
            f"corroboration={_score_part(details, 'corroboration_score', 0)}",
            f"structural={_score_part(details, 'structural_context_score', 0)}",
            f"noise_penalty=-{_score_part(details, 'noise_penalty_applied', _score_part(details, 'noise_penalty', 0))}",
        ]
        if details.get("lead_family"):
            why_bits.insert(0, f"lead_family={details.get('lead_family')}")
        pair_suppressed = _score_part(details, "pair_bonus_suppressed", 0)
        if pair_suppressed:
            why_bits.append(f"pair_cap=-{pair_suppressed}")
        routine_noise = _score_part(details, "routine_noise_surcharge", 0)
        if routine_noise:
            why_bits.append(f"routine_noise=-{routine_noise}")
        if details.get("weak_proxy_context_cap_applied"):
            why_bits.append("context capped for weak proxy/corroboration")
        top_positive_signals = details.get("top_positive_signals") or []
        top_suppressors = details.get("top_suppressors") or []
        corroboration_sources = details.get("corroboration_sources") or []
        if top_positive_signals:
            why_bits.append("signals: " + list_text([signal_text(signal) for signal in top_positive_signals], limit=5))
        if top_suppressors:
            why_bits.append("suppressors: " + list_text([suppressor_text(signal) for signal in top_suppressors], limit=5))
        if corroboration_sources:
            why_bits.append("corroboration: " + list_text([signal_text(signal) for signal in corroboration_sources], limit=5))
        return " | ".join(why_bits)

    clause_score = _score_part(details, "clause_score", 0)
    clause_score_raw = _score_part(details, "clause_score_raw", None)
    keyword_score = _score_part(details, "keyword_score", 0)
    entity_bonus = _score_part(details, "entity_bonus", 0)
    pair_bonus = _score_part(details, "pair_bonus_applied", _score_part(details, "pair_bonus", 0))
    pair_count = _score_part(details, "pair_count", 0)
    pair_strength = _score_part(details, "pair_strength", 0.0)
    noise_penalty = _score_part(details, "noise_penalty_applied", _score_part(details, "noise_penalty", 0))
    matched_rules = details.get("matched_ontology_rules") or []
    contributing_correlations = details.get("contributing_correlations") or []

    why_bits: list[str] = []
    if details.get("lead_family"):
        why_bits.append(f"lead_family={details.get('lead_family')}")
    if clause_score_raw is not None:
        why_bits.append(f"clauses={clause_score} (raw={clause_score_raw})")
    else:
        why_bits.append(f"clauses={clause_score}")
    if keyword_score:
        why_bits.append(f"keywords={keyword_score}")
    if entity_bonus:
        why_bits.append(f"entity_bonus={entity_bonus}")
    if pair_bonus:
        why_bits.append(f"pair_bonus={pair_bonus} (pairs={pair_count}, strength={pair_strength})")
    if noise_penalty:
        why_bits.append(f"noise_penalty=-{noise_penalty}")
    if matched_rules:
        why_bits.append(f"rules: {list_text(matched_rules)}")
    if contributing_correlations:
        why_bits.append(
            "correlations: " + list_text([correlation_text(correlation) for correlation in contributing_correlations], limit=5)
        )
    return " | ".join(why_bits)


def serialize_event_procurement_context(event: Event | None) -> dict[str, Any]:
    if event is None:
        return {
            "event_id": None,
            "event_hash": None,
            "entity_id": None,
            "category": None,
            "source": None,
            "doc_id": None,
            "source_url": None,
            "snippet": None,
            "occurred_at": None,
            "created_at": None,
            "place_text": None,
            "place_region": None,
            "solicitation_number": None,
            "notice_id": None,
            "document_id": None,
            "award_id": None,
            "piid": None,
            "generated_unique_award_id": None,
            "source_record_id": None,
            "awarding_agency_code": None,
            "awarding_agency_name": None,
            "funding_agency_code": None,
            "funding_agency_name": None,
            "contracting_office_code": None,
            "contracting_office_name": None,
            "recipient_name": None,
            "recipient_uei": None,
            "recipient_parent_uei": None,
            "recipient_duns": None,
            "recipient_cage_code": None,
            "vendor_name": None,
            "vendor_uei": None,
            "vendor_parent_uei": None,
            "vendor_duns": None,
            "vendor_cage_code": None,
            "psc_code": None,
            "psc_description": None,
            "naics_code": None,
            "naics_description": None,
        }

    return {
        "event_id": int(event.id),
        "event_hash": event.hash,
        "entity_id": event.entity_id,
        "category": event.category,
        "source": event.source,
        "doc_id": event.doc_id,
        "source_url": event.source_url,
        "snippet": event.snippet,
        "occurred_at": _iso(event.occurred_at),
        "created_at": _iso(event.created_at),
        "place_text": event.place_text,
        "place_region": event_place_region_label(event),
        "solicitation_number": event.solicitation_number,
        "notice_id": event.notice_id,
        "document_id": event.document_id,
        "award_id": event.award_id,
        "piid": event.piid,
        "generated_unique_award_id": event.generated_unique_award_id,
        "source_record_id": event.source_record_id,
        "awarding_agency_code": event.awarding_agency_code,
        "awarding_agency_name": event.awarding_agency_name,
        "funding_agency_code": event.funding_agency_code,
        "funding_agency_name": event.funding_agency_name,
        "contracting_office_code": event.contracting_office_code,
        "contracting_office_name": event.contracting_office_name,
        "recipient_name": event.recipient_name,
        "recipient_uei": event.recipient_uei,
        "recipient_parent_uei": event.recipient_parent_uei,
        "recipient_duns": event.recipient_duns,
        "recipient_cage_code": event.recipient_cage_code,
        "vendor_name": event.recipient_name,
        "vendor_uei": event.recipient_uei,
        "vendor_parent_uei": event.recipient_parent_uei,
        "vendor_duns": event.recipient_duns,
        "vendor_cage_code": event.recipient_cage_code,
        "psc_code": event.psc_code,
        "psc_description": event.psc_description,
        "naics_code": event.naics_code,
        "naics_description": event.naics_description,
    }


def build_review_row_completeness(row: dict[str, Any]) -> dict[str, Any]:
    core_identifier_fields = [
        field
        for field in (
            "doc_id",
            "solicitation_number",
            "notice_id",
            "document_id",
            "award_id",
            "piid",
            "generated_unique_award_id",
            "source_record_id",
        )
        if _norm_text(row.get(field))
    ]
    agency_fields = [
        field
        for field in (
            "awarding_agency_code",
            "awarding_agency_name",
            "funding_agency_code",
            "funding_agency_name",
            "contracting_office_code",
            "contracting_office_name",
        )
        if _norm_text(row.get(field))
    ]
    vendor_fields = [
        field
        for field in (
            "recipient_name",
            "recipient_uei",
            "recipient_parent_uei",
            "recipient_duns",
            "recipient_cage_code",
        )
        if _norm_text(row.get(field))
    ]
    classification_fields = [
        field
        for field in (
            "psc_code",
            "psc_description",
            "naics_code",
            "naics_description",
        )
        if _norm_text(row.get(field))
    ]
    foia_handle_fields = [
        field
        for field in (
            "source_url",
            "doc_id",
            "solicitation_number",
            "notice_id",
            "document_id",
            "award_id",
            "piid",
            "generated_unique_award_id",
            "source_record_id",
        )
        if _norm_text(row.get(field))
    ]

    flags = {
        "has_core_identifiers": bool(core_identifier_fields),
        "has_agency_target": bool(agency_fields),
        "has_vendor_context": bool(vendor_fields),
        "has_classification_context": bool(classification_fields),
        "has_foia_handles": bool(foia_handle_fields),
    }
    missing_context_categories = [name for name, present in flags.items() if not present]

    return {
        **flags,
        "present_core_identifier_fields": core_identifier_fields,
        "present_agency_fields": agency_fields,
        "present_vendor_fields": vendor_fields,
        "present_classification_fields": classification_fields,
        "present_foia_handle_fields": foia_handle_fields,
        "missing_context_categories": missing_context_categories,
    }


def serialize_ranked_lead_review_row(
    *,
    snapshot: LeadSnapshot | None,
    item: LeadSnapshotItem | None,
    event: Event | None,
    details: dict[str, Any] | None,
    rank: Optional[int] = None,
    score: Optional[int] = None,
) -> dict[str, Any]:
    event_context = serialize_event_procurement_context(event)
    if item is not None and _norm_text(getattr(item, "event_hash", None)):
        event_context["event_hash"] = item.event_hash
    details_dict = _norm_dict(details)
    corroboration_summary = _norm_dict(details_dict.get("corroboration_summary"))
    row = {
        "snapshot_id": None if snapshot is None else int(snapshot.id),
        "snapshot_item_id": None if item is None else int(item.id),
        "snapshot_scoring_version": None if snapshot is None else getattr(snapshot, "scoring_version", None),
        "rank": int(rank) if rank is not None else (int(item.rank) if item is not None else None),
        "score": int(score) if score is not None else (int(item.score) if item is not None else None),
        "scoring_version": details_dict.get("scoring_version") or (None if snapshot is None else getattr(snapshot, "scoring_version", None)),
        "lead_family": details_dict.get("lead_family"),
        "lead_family_label": details_dict.get("lead_family_label"),
        "secondary_lead_families": _norm_list(details_dict.get("secondary_lead_families")),
        "why_summary": why_summary(details_dict) if details_dict else None,
        "score_details": details_dict,
        "top_positive_signals": _norm_list(details_dict.get("top_positive_signals")),
        "top_suppressors": _norm_list(details_dict.get("top_suppressors")),
        "corroboration_summary": corroboration_summary,
        "contributing_lanes": _norm_list(details_dict.get("contributing_lanes")),
        "linked_source_summary": _norm_list(corroboration_summary.get("linked_source_summary")),
        "candidate_join_evidence": _norm_list(corroboration_summary.get("candidate_join_evidence")),
        **event_context,
    }
    completeness = build_review_row_completeness(row)
    row.update(
        {
            "has_core_identifiers": completeness["has_core_identifiers"],
            "has_agency_target": completeness["has_agency_target"],
            "has_vendor_context": completeness["has_vendor_context"],
            "has_classification_context": completeness["has_classification_context"],
            "has_foia_handles": completeness["has_foia_handles"],
            "completeness_summary": completeness,
        }
    )
    return {field: row.get(field) for field in CANONICAL_RANKED_LEAD_REVIEW_FIELDS}


def review_row_csv_safe(row: dict[str, Any]) -> dict[str, Any]:
    csv_row: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (dict, list)):
            csv_row[key] = json.dumps(value, ensure_ascii=False)
        else:
            csv_row[key] = value
    return csv_row


def review_effective_window(rows: list[dict[str, Any]]) -> dict[str, Any]:
    parsed: list[datetime] = []
    for row in rows:
        timestamp = row.get("occurred_at") or row.get("created_at")
        if not isinstance(timestamp, str) or not timestamp.strip():
            continue
        normalized = _normalize_review_window_timestamp(timestamp)
        if normalized is None:
            continue
        parsed.append(normalized)
    if not parsed:
        return {
            "basis": "occurred_at_or_created_at",
            "earliest": None,
            "latest": None,
            "span_days": None,
        }
    earliest = min(parsed)
    latest = max(parsed)
    span_days = max(int((latest - earliest).total_seconds() // 86400), 0)
    return {
        "basis": "occurred_at_or_created_at",
        "earliest": earliest.isoformat(),
        "latest": latest.isoformat(),
        "span_days": span_days,
    }


def review_completeness_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    fields = (
        "has_core_identifiers",
        "has_agency_target",
        "has_vendor_context",
        "has_classification_context",
        "has_foia_handles",
    )
    return {
        field: sum(1 for row in rows if bool(row.get(field)))
        for field in fields
    }


__all__ = [
    "CANONICAL_RANKED_LEAD_REVIEW_FIELDS",
    "RANKED_LEAD_REVIEW_CONTRACT_VERSION",
    "build_review_row_completeness",
    "candidate_join_text",
    "correlation_text",
    "family_assignment_text",
    "linked_source_text",
    "list_text",
    "review_completeness_counts",
    "review_effective_window",
    "review_row_csv_safe",
    "serialize_event_procurement_context",
    "serialize_ranked_lead_review_row",
    "signal_text",
    "suppressor_text",
    "top_clauses_text",
    "why_summary",
]
