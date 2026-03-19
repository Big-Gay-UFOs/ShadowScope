from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.analysis.scoring import (
    score_from_keywords_clauses,
    score_from_keywords_clauses_v2,
    score_from_keywords_clauses_v3,
)
from backend.connectors.samgov_context import extract_sam_context_fields
from backend.db.models import (
    AnalysisRun,
    Correlation,
    CorrelationLink,
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
    get_session_factory,
)


SUPPORTED_SCORING_VERSIONS: tuple[str, ...] = ("v1", "v2", "v3")
DEFAULT_SCORING_VERSION = "v3"

_DOD_PACK_PREFIX = "sam_dod_"
_FOIA_MATRIX_BONUS_CAP = 3
_MAX_COMPARISON_VERSIONS = 2


def normalize_scoring_version(scoring_version: str | None) -> str:
    value = str(scoring_version or DEFAULT_SCORING_VERSION).strip().lower()
    if value not in SUPPORTED_SCORING_VERSIONS:
        allowed = ", ".join(SUPPORTED_SCORING_VERSIONS)
        raise ValueError(f"Unsupported scoring_version '{scoring_version}'. Expected one of: {allowed}")
    return value


def normalize_comparison_versions(versions: Optional[list[str]]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in versions or []:
        version = normalize_scoring_version(raw)
        if version in seen:
            continue
        normalized.append(version)
        seen.add(version)
    if not normalized:
        return []
    if len(normalized) != _MAX_COMPARISON_VERSIONS:
        raise ValueError("compare_scoring_versions must contain exactly two distinct scoring versions")
    return normalized


def _norm_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, dict):
        return []
    if isinstance(value, list):
        return value
    return []


def _norm_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_present(*values: Any) -> Optional[str]:
    for value in values:
        text = _norm_text(value)
        if text:
            return text
    return None


def _score_number(details: dict[str, Any], key: str) -> int:
    try:
        return int(details.get(key) or 0)
    except Exception:
        return 0


def _dod_keyword_metrics(keywords: list[Any]) -> tuple[int, int]:
    lane_ids: set[str] = set()
    keyword_hits = 0
    for item in keywords:
        if not isinstance(item, str):
            continue
        pack, _, _rule = item.partition(":")
        if not pack.startswith(_DOD_PACK_PREFIX):
            continue
        lane_ids.add(pack)
        keyword_hits += 1
    return len(lane_ids), keyword_hits


def _foia_matrix_bonus(*, dod_lane_count: int, pair_count: int) -> int:
    if dod_lane_count <= 0:
        return 0
    lane_component = min(int(dod_lane_count), 2)
    pair_component = 1 if int(pair_count) > 0 else 0
    return min(_FOIA_MATRIX_BONUS_CAP, lane_component + pair_component)


def _foia_potential_tier(*, dod_lane_count: int, dod_keyword_hit_count: int, pair_count: int) -> str:
    if dod_lane_count >= 3 and dod_keyword_hit_count >= 4 and pair_count >= 1:
        return "high"
    if dod_lane_count >= 2 and dod_keyword_hit_count >= 2:
        return "medium"
    if dod_lane_count >= 1 and dod_keyword_hit_count >= 2 and pair_count >= 1:
        return "medium"
    return "low"


def _structural_context_details(event: Event) -> dict[str, Any]:
    raw = event.raw_json if isinstance(event.raw_json, dict) else {}
    ctx = extract_sam_context_fields(raw)

    core_fields = {
        "notice_type": _first_present(event.notice_award_type, ctx.get("sam_notice_type")),
        "solicitation_number": _first_present(event.solicitation_number, ctx.get("sam_solicitation_number")),
        "naics_code": _first_present(event.naics_code, ctx.get("sam_naics_code")),
        "set_aside_code": _first_present(ctx.get("sam_set_aside_code"), raw.get("typeOfSetAside")),
    }
    research_fields = {
        "agency_path_code": _first_present(ctx.get("sam_agency_path_code"), raw.get("fullParentPathCode")),
        "response_deadline": _first_present(ctx.get("sam_response_deadline"), raw.get("responseDeadLine")),
        "place_state": _first_present(event.place_of_performance_state, ctx.get("sam_place_state_code")),
        "place_country": _first_present(event.place_of_performance_country, ctx.get("sam_place_country_code")),
        "office_code": _first_present(ctx.get("sam_office_code"), raw.get("officeCode")),
    }
    identity_fields = {
        "recipient_name": _first_present(event.recipient_name, raw.get("recipient_name"), raw.get("Recipient Name")),
        "recipient_uei": _first_present(event.recipient_uei, raw.get("recipient_uei"), raw.get("recipientUEI"), raw.get("uei")),
        "recipient_cage_code": _first_present(
            event.recipient_cage_code,
            raw.get("recipient_cage_code"),
            raw.get("recipientCageCode"),
            raw.get("cage"),
            raw.get("cageCode"),
        ),
        "recipient_id": _first_present(raw.get("recipient_id"), raw.get("recipientId")),
    }

    core_present = [name for name, value in core_fields.items() if value]
    research_present = [name for name, value in research_fields.items() if value]
    identity_present = [name for name, value in identity_fields.items() if value]

    core_score = min(
        sum(
            {
                "notice_type": 2,
                "solicitation_number": 2,
                "naics_code": 2,
                "set_aside_code": 1,
            }[name]
            for name in core_present
        ),
        6,
    )
    research_score = min(
        sum(
            {
                "agency_path_code": 2,
                "response_deadline": 1,
                "place_state": 1,
                "place_country": 1,
                "office_code": 1,
            }[name]
            for name in research_present
        ),
        4,
    )

    identity_score = 0
    if identity_fields["recipient_name"]:
        identity_score += 1
    if identity_fields["recipient_uei"] or identity_fields["recipient_cage_code"] or identity_fields["recipient_id"]:
        identity_score += 2
    identity_score = min(identity_score, 3)

    structural_context_score = core_score + research_score + identity_score
    if structural_context_score >= 8:
        context_label = "high"
    elif structural_context_score >= 4:
        context_label = "medium"
    else:
        context_label = "low"

    return {
        "structural_core_score": int(core_score),
        "structural_research_score": int(research_score),
        "structural_identity_score": int(identity_score),
        "structural_context_score": int(structural_context_score),
        "structural_core_fields": core_present,
        "structural_research_fields": research_present,
        "structural_identity_fields": identity_present,
        "structural_context_label": context_label,
    }


def _lead_family(
    *,
    clause_score: int,
    keyword_score: int,
    pair_count: int,
    dod_lane_count: int,
    structural_context_score: int,
    structural_core_score: int,
) -> str:
    if dod_lane_count >= 2 and (pair_count > 0 or structural_core_score >= 4):
        return "foia_contextual"
    if structural_context_score >= 7 and pair_count > 0:
        return "high_context_pair_supported"
    if structural_context_score >= 7:
        return "high_context_structural"
    if pair_count > 0:
        return "pair_supported"
    if clause_score > 0:
        return "clause_driven"
    if keyword_score > 0:
        return "keyword_only"
    return "low_signal"


def _comparison_state(baseline_rank: Optional[int], target_rank: Optional[int]) -> str:
    if baseline_rank is None and target_rank is None:
        return "absent"
    if baseline_rank is None:
        return "entered_target"
    if target_rank is None:
        return "dropped_from_target"
    return "shared"


def build_scoring_delta_explanation(
    baseline_details: Optional[dict[str, Any]],
    target_details: Optional[dict[str, Any]],
) -> str:
    baseline = baseline_details if isinstance(baseline_details, dict) else {}
    target = target_details if isinstance(target_details, dict) else {}

    parts: list[str] = []
    for key, label in (
        ("clause_score", "clauses"),
        ("keyword_score", "keywords"),
        ("entity_bonus", "entity"),
        ("pair_bonus", "pair"),
        ("structural_context_score", "structural"),
        ("foia_matrix_bonus", "foia"),
        ("noise_penalty", "noise"),
    ):
        base_value = _score_number(baseline, key)
        target_value = _score_number(target, key)
        delta = target_value - base_value
        if delta == 0:
            continue
        sign = "+" if delta > 0 else ""
        parts.append(f"{label} {sign}{delta}")

    baseline_family = baseline.get("lead_family")
    target_family = target.get("lead_family")
    if baseline_family != target_family and (baseline_family or target_family):
        parts.append(f"lead_family {baseline_family or 'n/a'} -> {target_family or 'n/a'}")

    if not parts:
        return "No material score-component change."
    return "; ".join(parts[:4])


def compute_leads(
    db: Session,
    *,
    scan_limit: int = 5000,
    limit: int = 200,
    min_score: int = 1,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    pair_bonus_multiplier: int = 6,
    pair_bonus_cap: int = 12,
    noise_pair_bonus_cap: int = 2,
    noise_penalty: int = 8,
) -> Tuple[List[Tuple[int, Event, Dict[str, Any]]], int]:
    scoring_version = normalize_scoring_version(scoring_version)
    rows = db.execute(select(Event).order_by(Event.id.desc()).limit(int(scan_limit))).scalars().all()
    scanned = len(rows)

    pair_counts: Dict[int, int] = {}
    pair_strength: Dict[int, float] = {}

    pair_aware = scoring_version in {"v2", "v3"}
    if pair_aware:
        ids = [int(e.id) for e in rows]
        if ids:
            like_pat = f"kw_pair|{source}|%|pair:%" if source else "kw_pair|%|%|pair:%"
            q = (
                db.query(CorrelationLink.event_id, Correlation.score)
                .join(Correlation, Correlation.id == CorrelationLink.correlation_id)
                .filter(Correlation.correlation_key.like(like_pat))
                .filter(CorrelationLink.event_id.in_(ids))
            )
            for event_id, cscore in q.all():
                eid = int(event_id)
                try:
                    n = int(cscore or 0)
                except Exception:
                    n = 0
                if n <= 0:
                    continue
                pair_counts[eid] = pair_counts.get(eid, 0) + 1
                pair_strength[eid] = pair_strength.get(eid, 0.0) + (1.0 / math.sqrt(float(n)))

    scored: List[Tuple[int, Event, Dict[str, Any]]] = []
    for event in rows:
        if source and event.source != source:
            continue
        if exclude_source and event.source == exclude_source:
            continue

        kw_list = _norm_list(event.keywords)
        has_noise = any(
            (
                isinstance(keyword, str)
                and keyword.startswith("operational_noise_terms:")
            )
            or keyword == "operational_noise_terms:nasa_sponsoring_agreement_noise"
            for keyword in kw_list
        )
        dod_lane_count, dod_keyword_hit_count = _dod_keyword_metrics(kw_list)
        pair_n = pair_counts.get(int(event.id), 0)
        strength = pair_strength.get(int(event.id), 0.0)
        pair_bonus = 0
        if pair_aware:
            pair_bonus = int(round(float(pair_bonus_multiplier) * float(strength)))
            if pair_bonus > int(pair_bonus_cap):
                pair_bonus = int(pair_bonus_cap)
            if has_noise:
                pair_bonus = min(int(pair_bonus), int(noise_pair_bonus_cap))

        foia_matrix_bonus = 0
        if scoring_version in {"v2", "v3"}:
            foia_matrix_bonus = _foia_matrix_bonus(dod_lane_count=dod_lane_count, pair_count=pair_n)
            if has_noise:
                foia_matrix_bonus = min(int(foia_matrix_bonus), 1)

        if scoring_version == "v2":
            score, details = score_from_keywords_clauses_v2(
                event.keywords,
                event.clauses,
                has_entity=bool(event.entity_id),
                pair_bonus=int(pair_bonus),
            )
            score = int(score) + int(foia_matrix_bonus)
        elif scoring_version == "v3":
            structural = _structural_context_details(event)
            score, details = score_from_keywords_clauses_v3(
                event.keywords,
                event.clauses,
                has_entity=bool(event.entity_id),
                pair_bonus=int(pair_bonus),
                structural_core_score=int(structural["structural_core_score"]),
                structural_research_score=int(structural["structural_research_score"]),
                structural_identity_score=int(structural["structural_identity_score"]),
                foia_matrix_bonus=int(foia_matrix_bonus),
            )
            details.update(structural)
        else:
            score, details = score_from_keywords_clauses(
                event.keywords,
                event.clauses,
                has_entity=bool(event.entity_id),
            )

        if has_noise and scoring_version in {"v2", "v3"}:
            score = int(score) - int(noise_penalty)
            details["noise_penalty"] = int(noise_penalty)
            details["pair_bonus_cap_due_to_noise"] = int(noise_pair_bonus_cap)

        details.setdefault("scoring_version", scoring_version)
        details.setdefault("pair_bonus", int(pair_bonus))
        details.setdefault("pair_count", int(pair_n))
        details.setdefault("pair_strength", round(float(strength), 4))
        details.setdefault("has_noise", bool(has_noise))
        details.setdefault("noise_penalty", 0)
        details.setdefault("foia_matrix_bonus", int(foia_matrix_bonus))
        details.setdefault("structural_core_score", 0)
        details.setdefault("structural_research_score", 0)
        details.setdefault("structural_identity_score", 0)
        details.setdefault("structural_context_score", 0)
        details.setdefault("structural_core_fields", [])
        details.setdefault("structural_research_fields", [])
        details.setdefault("structural_identity_fields", [])
        details.setdefault("structural_context_label", "low")
        details["dod_lane_count"] = int(dod_lane_count)
        details["dod_keyword_hit_count"] = int(dod_keyword_hit_count)
        details["foia_matrix_bonus"] = int(foia_matrix_bonus)
        details["foia_potential_tier"] = _foia_potential_tier(
            dod_lane_count=int(dod_lane_count),
            dod_keyword_hit_count=int(dod_keyword_hit_count),
            pair_count=int(pair_n),
        )
        details["lead_family"] = _lead_family(
            clause_score=_score_number(details, "clause_score"),
            keyword_score=_score_number(details, "keyword_score"),
            pair_count=int(pair_n),
            dod_lane_count=int(dod_lane_count),
            structural_context_score=_score_number(details, "structural_context_score"),
            structural_core_score=_score_number(details, "structural_core_score"),
        )

        if int(score) >= int(min_score):
            scored.append((int(score), event, details))

    scored.sort(key=lambda item: (item[0], item[1].id), reverse=True)
    return scored[: int(limit)], scanned


def compare_lead_scoring_versions(
    db: Session,
    *,
    versions: list[str],
    scan_limit: int = 5000,
    limit: int = 200,
    min_score: int = 1,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
) -> dict[str, Any]:
    baseline_version, target_version = normalize_comparison_versions(versions)

    ranked_by_version: dict[str, list[dict[str, Any]]] = {}
    scanned_by_version: dict[str, int] = {}
    for version in (baseline_version, target_version):
        ranked, scanned = compute_leads(
            db,
            scan_limit=scan_limit,
            limit=limit,
            min_score=min_score,
            source=source,
            exclude_source=exclude_source,
            scoring_version=version,
        )
        scanned_by_version[version] = int(scanned)
        rows: list[dict[str, Any]] = []
        for rank, (score, event, details) in enumerate(ranked, start=1):
            rows.append(
                {
                    "rank": int(rank),
                    "score": int(score),
                    "event": event,
                    "details": details,
                }
            )
        ranked_by_version[version] = rows

    baseline_rows = ranked_by_version[baseline_version]
    target_rows = ranked_by_version[target_version]
    baseline_by_event = {int(item["event"].id): item for item in baseline_rows}
    target_by_event = {int(item["event"].id): item for item in target_rows}

    items: list[dict[str, Any]] = []
    for event_id in sorted(set(baseline_by_event) | set(target_by_event)):
        baseline = baseline_by_event.get(event_id)
        target = target_by_event.get(event_id)
        event = (target or baseline)["event"]
        baseline_rank = int(baseline["rank"]) if baseline else None
        target_rank = int(target["rank"]) if target else None
        baseline_score = int(baseline["score"]) if baseline else None
        target_score = int(target["score"]) if target else None
        baseline_details = baseline.get("details") if baseline else {}
        target_details = target.get("details") if target else {}
        lead_family = (target_details or {}).get("lead_family") or (baseline_details or {}).get("lead_family")

        items.append(
            {
                "event_id": int(event.id),
                "event_hash": event.hash,
                "doc_id": event.doc_id,
                "source": event.source,
                "source_url": event.source_url,
                "snippet": event.snippet,
                "lead_family": lead_family,
                "baseline_version": baseline_version,
                "target_version": target_version,
                f"{baseline_version}_rank": baseline_rank,
                f"{baseline_version}_score": baseline_score,
                f"{target_version}_rank": target_rank,
                f"{target_version}_score": target_score,
                "baseline_rank": baseline_rank,
                "baseline_score": baseline_score,
                "target_rank": target_rank,
                "target_score": target_score,
                "delta_rank": (
                    int(baseline_rank) - int(target_rank)
                    if baseline_rank is not None and target_rank is not None
                    else None
                ),
                "delta_score": (
                    int(target_score) - int(baseline_score)
                    if baseline_score is not None and target_score is not None
                    else None
                ),
                "comparison_state": _comparison_state(baseline_rank, target_rank),
                "explanation_delta": build_scoring_delta_explanation(baseline_details, target_details),
                "baseline_score_details": baseline_details if isinstance(baseline_details, dict) else {},
                "target_score_details": target_details if isinstance(target_details, dict) else {},
            }
        )

    items.sort(
        key=lambda item: (
            item.get("target_rank") is None,
            item.get("target_rank") if item.get("target_rank") is not None else 10**9,
            item.get("baseline_rank") is None,
            item.get("baseline_rank") if item.get("baseline_rank") is not None else 10**9,
            -(item.get("target_score") if item.get("target_score") is not None else item.get("baseline_score") or 0),
            item.get("event_id") or 0,
        )
    )

    state_counts = {
        "shared": sum(1 for item in items if item["comparison_state"] == "shared"),
        "entered_target": sum(1 for item in items if item["comparison_state"] == "entered_target"),
        "dropped_from_target": sum(1 for item in items if item["comparison_state"] == "dropped_from_target"),
    }
    return {
        "baseline_version": baseline_version,
        "target_version": target_version,
        "versions": [baseline_version, target_version],
        "source": source,
        "exclude_source": exclude_source,
        "min_score": int(min_score),
        "limit": int(limit),
        "scan_limit": int(scan_limit),
        "scanned": scanned_by_version,
        "count": len(items),
        "state_counts": state_counts,
        "items": items,
    }


def create_lead_snapshot(
    *,
    analysis_run_id: Optional[int] = None,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    min_score: int = 1,
    limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    notes: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    scoring_version = normalize_scoring_version(scoring_version)
    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()
    try:
        if analysis_run_id is not None:
            ok = db.execute(select(AnalysisRun.id).where(AnalysisRun.id == analysis_run_id)).scalar_one_or_none()
            if ok is None:
                raise ValueError(
                    f"analysis_run_id {analysis_run_id} not found in analysis_runs. "
                    f"Run 'ss ontology apply ...' and use the printed analysis_run_id, or omit --analysis-run-id."
                )

        ranked, scanned = compute_leads(
            db,
            scan_limit=scan_limit,
            limit=limit,
            min_score=min_score,
            source=source,
            exclude_source=exclude_source,
            scoring_version=scoring_version,
        )

        snap = LeadSnapshot(
            analysis_run_id=analysis_run_id,
            source=source,
            min_score=int(min_score),
            limit=int(limit),
            scoring_version=scoring_version,
            notes=notes,
        )
        db.add(snap)
        db.commit()
        db.refresh(snap)

        inserted = 0
        for idx, (score, event, details) in enumerate(ranked, start=1):
            item = LeadSnapshotItem(
                snapshot_id=snap.id,
                event_id=event.id,
                event_hash=event.hash,
                rank=idx,
                score=int(score),
                score_details=details,
            )
            db.add(item)
            inserted += 1

        db.commit()
        return {
            "status": "ok",
            "snapshot_id": snap.id,
            "analysis_run_id": analysis_run_id,
            "source": source,
            "exclude_source": exclude_source,
            "min_score": int(min_score),
            "limit": int(limit),
            "scan_limit": int(scan_limit),
            "scoring_version": scoring_version,
            "scanned": int(scanned),
            "items": int(inserted),
        }
    finally:
        db.close()


__all__ = [
    "DEFAULT_SCORING_VERSION",
    "SUPPORTED_SCORING_VERSIONS",
    "build_scoring_delta_explanation",
    "compare_lead_scoring_versions",
    "compute_leads",
    "create_lead_snapshot",
    "normalize_comparison_versions",
    "normalize_scoring_version",
]
