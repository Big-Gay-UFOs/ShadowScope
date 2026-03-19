from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.analysis.scoring import (
    score_from_keywords_clauses,
    score_from_keywords_clauses_v2,
    score_from_keywords_clauses_v3,
)
from backend.correlate.scorer import (
    DEFAULT_KW_PAIR_BONUS_MIN_EVENT_COUNT,
    DEFAULT_KW_PAIR_BONUS_MIN_SIGNAL,
    kw_pair_bonus_contribution,
    kw_pair_event_count,
    kw_pair_score_signal,
)
from backend.db.models import (
    AnalysisRun,
    Correlation,
    CorrelationLink,
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
    get_session_factory,
)
from backend.services.explainability import (
    enrich_lead_score_details,
    load_event_linked_source_summary,
    load_event_correlation_evidence,
)
from backend.services.lead_families import classify_lead_families
from backend.services.investigator_filters import event_time_expr, investigator_event_conditions


SUPPORTED_SCORING_VERSIONS: tuple[str, ...] = ("v1", "v2", "v3")
DEFAULT_SCORING_VERSION = "v3"

_DOD_PACK_PREFIX = "sam_dod_"
_NOISE_PACK_PREFIXES = ("operational_noise_terms:", "sam_proxy_noise_expansion:")
_FOIA_MATRIX_BONUS_CAP = 3
_VALID_SCORING_VERSIONS = set(SUPPORTED_SCORING_VERSIONS)
_MAX_COMPARISON_VERSIONS = 2
_MIN_SORT_DT = datetime.min.replace(tzinfo=timezone.utc)


def normalize_scoring_version(value: Any, *, default: str = DEFAULT_SCORING_VERSION) -> str:
    raw = str(value or default).strip().lower() or default
    if raw not in _VALID_SCORING_VERSIONS:
        allowed = ", ".join(SUPPORTED_SCORING_VERSIONS)
        raise ValueError(f"Unsupported scoring_version '{value}'. Expected one of: {allowed}")
    return raw


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


def _normalized_source_key(value: Any) -> str:
    return "".join(ch for ch in str(value or "").strip().lower() if ch.isalnum())


def _supports_v3_source_metadata_boosts(source: Any) -> bool:
    return _normalized_source_key(source) == "samgov"


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


def _score_number(details: Optional[dict[str, Any]], key: str) -> int:
    try:
        return int((details or {}).get(key) or 0)
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



def _legacy_pair_bonus_contribution(event_count: int) -> float:
    if int(event_count) <= 0:
        return 0.0
    return 1.0 / math.sqrt(float(event_count))


def _lead_candidate_conditions(
    *,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    date_from: Optional[Any] = None,
    date_to: Optional[Any] = None,
    occurred_after: Optional[Any] = None,
    occurred_before: Optional[Any] = None,
    created_after: Optional[Any] = None,
    created_before: Optional[Any] = None,
    since_days: Optional[int] = None,
    entity_id: Optional[int] = None,
    keyword: Optional[str] = None,
    agency: Optional[str] = None,
    psc: Optional[str] = None,
    naics: Optional[str] = None,
    award_id: Optional[str] = None,
    recipient_uei: Optional[str] = None,
    place_region: Optional[str] = None,
) -> list[Any]:
    conditions = investigator_event_conditions(
        source=source,
        date_from=date_from,
        date_to=date_to,
        entity_id=entity_id,
        keyword=keyword,
        agency=agency,
        psc=psc,
        naics=naics,
        award_id=award_id,
        recipient_uei=recipient_uei,
        place_region=place_region,
    )

    if exclude_source:
        conditions.append(Event.source != exclude_source)

    if occurred_after is not None:
        conditions.append(Event.occurred_at >= occurred_after)
    if occurred_before is not None:
        conditions.append(Event.occurred_at <= occurred_before)
    if created_after is not None:
        conditions.append(Event.created_at >= created_after)
    if created_before is not None:
        conditions.append(Event.created_at <= created_before)
    if since_days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=max(int(since_days), 0))
        conditions.append(event_time_expr(Event) >= since)

    return conditions


def _lead_candidate_order_by() -> tuple[Any, ...]:
    relevant_at = event_time_expr(Event)
    return (
        relevant_at.desc().nullslast(),
        Event.occurred_at.desc().nullslast(),
        Event.created_at.desc().nullslast(),
        Event.id.desc(),
    )


def _sortable_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return _MIN_SORT_DT


def _lead_sort_values(event: Event) -> tuple[Any, ...]:
    relevant_at = _sortable_dt(event.occurred_at or event.created_at)
    occurred_at = _sortable_dt(event.occurred_at)
    created_at = _sortable_dt(event.created_at)
    return relevant_at, occurred_at, created_at, int(event.id)


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
        if key == "noise_penalty":
            # noise_penalty is stored as a positive magnitude but reduces the total score.
            delta = -delta
        if delta == 0:
            continue
        sign = "+" if delta > 0 else ""
        parts.append(f"{label} {sign}{delta}")

    baseline_family = _first_present(baseline.get("lead_family"))
    target_family = _first_present(target.get("lead_family"))
    if baseline_family != target_family and (baseline_family or target_family):
        parts.append(f"lead_family {baseline_family or 'n/a'} -> {target_family or 'n/a'}")

    if not parts:
        return "No material score-component change."
    return "; ".join(parts[:4])


def _event_scoring_context(event: Event) -> dict[str, Any]:
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
        "solicitation_number": event.solicitation_number,
        "notice_id": event.notice_id,
        "document_id": event.document_id,
        "occurred_at": event.occurred_at,
        "created_at": event.created_at,
    }



def compute_leads(
    db: Session,
    *,
    scan_limit: int = 5000,
    limit: int = 200,
    min_score: int = 1,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    date_from: Optional[Any] = None,
    date_to: Optional[Any] = None,
    occurred_after: Optional[Any] = None,
    occurred_before: Optional[Any] = None,
    created_after: Optional[Any] = None,
    created_before: Optional[Any] = None,
    since_days: Optional[int] = None,
    entity_id: Optional[int] = None,
    keyword: Optional[str] = None,
    agency: Optional[str] = None,
    psc: Optional[str] = None,
    naics: Optional[str] = None,
    award_id: Optional[str] = None,
    recipient_uei: Optional[str] = None,
    place_region: Optional[str] = None,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    pair_bonus_multiplier: int = 6,
    pair_bonus_cap: int = 12,
    noise_pair_bonus_cap: int = 2,
    noise_penalty: int = 8,
    pair_signal_threshold: float = DEFAULT_KW_PAIR_BONUS_MIN_SIGNAL,
    pair_event_count_threshold: int = DEFAULT_KW_PAIR_BONUS_MIN_EVENT_COUNT,
) -> Tuple[List[Tuple[int, Event, Dict[str, Any]]], int]:
    scoring_version = normalize_scoring_version(scoring_version)
    conditions = _lead_candidate_conditions(
        source=source,
        exclude_source=exclude_source,
        date_from=date_from,
        date_to=date_to,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        created_after=created_after,
        created_before=created_before,
        since_days=since_days,
        entity_id=entity_id,
        keyword=keyword,
        agency=agency,
        psc=psc,
        naics=naics,
        award_id=award_id,
        recipient_uei=recipient_uei,
        place_region=place_region,
    )

    rows = (
        db.execute(
            select(Event)
            .where(*conditions)
            .order_by(*_lead_candidate_order_by())
            .limit(max(int(scan_limit), 0))
        )
        .scalars()
        .all()
    )
    scanned = len(rows)
    event_ids = [int(e.id) for e in rows]
    correlations_by_event = load_event_correlation_evidence(db, event_ids=event_ids) if event_ids else {}
    pair_counts: Dict[int, int] = {}
    pair_counts_total: Dict[int, int] = {}
    pair_strength: Dict[int, float] = {}

    is_v2 = scoring_version == "v2"
    is_v3 = scoring_version == "v3"
    needs_pair_metrics = is_v2 or is_v3

    if needs_pair_metrics and event_ids:
        like_pat = f"kw_pair|{source}|%|pair:%" if source else "kw_pair|%|%|pair:%"
        q = (
            db.query(CorrelationLink.event_id, Correlation.score, Correlation.lanes_hit)
            .join(Correlation, Correlation.id == CorrelationLink.correlation_id)
            .filter(Correlation.correlation_key.like(like_pat))
            .filter(CorrelationLink.event_id.in_(event_ids))
        )
        for event_id, cscore, lanes_hit in q.all():
            eid = int(event_id)
            event_count = kw_pair_event_count(lanes_hit, fallback_score=cscore)
            if event_count <= 0:
                continue

            pair_counts_total[eid] = pair_counts_total.get(eid, 0) + 1

            score_signal = kw_pair_score_signal(lanes_hit)
            contribution = kw_pair_bonus_contribution(
                score_signal=score_signal,
                event_count=event_count,
                min_signal=float(pair_signal_threshold),
                min_event_count=int(pair_event_count_threshold),
            )
            if contribution <= 0 and score_signal is None:
                contribution = _legacy_pair_bonus_contribution(event_count)

            if contribution <= 0:
                continue

            pair_counts[eid] = pair_counts.get(eid, 0) + 1
            pair_strength[eid] = pair_strength.get(eid, 0.0) + float(contribution)

    scored: List[Tuple[int, Event, Dict[str, Any]]] = []
    for e in rows:

        kw_list = _norm_list(e.keywords)
        has_noise = any(
            (isinstance(k, str) and k.startswith(_NOISE_PACK_PREFIXES))
            or k == "operational_noise_terms:nasa_sponsoring_agreement_noise"
            for k in kw_list
        )
        dod_lane_count, dod_keyword_hit_count = _dod_keyword_metrics(kw_list)

        correlations = correlations_by_event.get(int(e.id), [])
        pair_n = pair_counts.get(int(e.id), 0)
        pair_n_total = pair_counts_total.get(int(e.id), 0)
        strength = pair_strength.get(int(e.id), 0.0)

        raw_pair_bonus = int(round(float(pair_bonus_multiplier) * float(strength)))
        if raw_pair_bonus > int(pair_bonus_cap):
            raw_pair_bonus = int(pair_bonus_cap)

        if is_v2:
            pair_bonus = int(raw_pair_bonus)

            if has_noise:
                pair_bonus = min(int(pair_bonus), int(noise_pair_bonus_cap))

            score, details = score_from_keywords_clauses_v2(
                e.keywords,
                e.clauses,
                has_entity=bool(e.entity_id),
                pair_bonus=int(pair_bonus),
            )
            details["pair_count"] = int(pair_n)
            details["pair_count_total"] = int(pair_n_total)
            details["pair_strength"] = round(float(strength), 4)
            details["pair_signal_total"] = round(float(strength), 4)
            details["pair_signal_threshold"] = float(pair_signal_threshold)
            details["pair_event_count_threshold"] = int(pair_event_count_threshold)
            details["has_noise"] = bool(has_noise)
            if has_noise:
                score = int(score) - int(noise_penalty)
                details["noise_penalty"] = int(noise_penalty)
                details["pair_bonus_cap_due_to_noise"] = int(noise_pair_bonus_cap)
        elif is_v3:
            score, details = score_from_keywords_clauses_v3(
                e.keywords,
                e.clauses,
                has_entity=bool(e.entity_id),
                pair_bonus=int(raw_pair_bonus),
                pair_count=int(pair_n),
                pair_count_total=int(pair_n_total),
                pair_strength=float(strength),
                correlations=correlations,
                event_context=_event_scoring_context(e),
                allow_source_metadata_boosts=_supports_v3_source_metadata_boosts(e.source),
            )
        else:
            score, details = score_from_keywords_clauses(
                e.keywords,
                e.clauses,
                has_entity=bool(e.entity_id),
            )

        foia_matrix_bonus = 0
        if is_v2:
            foia_matrix_bonus = _foia_matrix_bonus(dod_lane_count=dod_lane_count, pair_count=pair_n)
            if has_noise:
                foia_matrix_bonus = min(int(foia_matrix_bonus), 1)
            score = int(score) + int(foia_matrix_bonus)

        details.setdefault("pair_count", int(pair_n))
        details.setdefault("pair_count_total", int(pair_n_total))
        details.setdefault("pair_strength", round(float(strength), 4))
        details.setdefault("pair_signal_total", round(float(strength), 4))
        details.setdefault("pair_signal_threshold", float(pair_signal_threshold))
        details.setdefault("pair_event_count_threshold", int(pair_event_count_threshold))
        details.setdefault("has_noise", bool(has_noise))
        details.setdefault("pair_bonus_raw", int(raw_pair_bonus))
        details["dod_lane_count"] = int(dod_lane_count)
        details["dod_keyword_hit_count"] = int(dod_keyword_hit_count)
        details["foia_matrix_bonus"] = int(foia_matrix_bonus)
        details["foia_potential_tier"] = _foia_potential_tier(
            dod_lane_count=int(dod_lane_count),
            dod_keyword_hit_count=int(dod_keyword_hit_count),
            pair_count=int(pair_n),
        )
        details = enrich_lead_score_details(
            clauses=e.clauses,
            base_details=details,
            correlations=correlations,
        )

        if int(score) >= int(min_score):
            scored.append((int(score), e, details))

    scored.sort(key=lambda t: (t[0],) + _lead_sort_values(t[1]), reverse=True)
    selected = scored[: int(limit)]
    selected_event_ids = [int(event.id) for _score, event, _details in selected]
    linked_source_context = (
        load_event_linked_source_summary(db, event_ids=selected_event_ids)
        if selected_event_ids
        else {}
    )

    enriched_selected: list[tuple[int, Event, dict[str, Any]]] = []
    for score, event, details in selected:
        context = linked_source_context.get(int(event.id), {})
        details = classify_lead_families(
            details=details,
            linked_source_summary=context.get("linked_source_summary"),
            linked_records_by_correlation=context.get("linked_records_by_correlation"),
        )
        enriched_selected.append((score, event, details))

    return enriched_selected, scanned


def compare_lead_scoring_versions(
    db: Session,
    *,
    versions: list[str],
    scan_limit: int = 5000,
    limit: int = 200,
    min_score: int = 1,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    date_from: Optional[Any] = None,
    date_to: Optional[Any] = None,
    occurred_after: Optional[Any] = None,
    occurred_before: Optional[Any] = None,
    created_after: Optional[Any] = None,
    created_before: Optional[Any] = None,
    since_days: Optional[int] = None,
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
            date_from=date_from,
            date_to=date_to,
            occurred_after=occurred_after,
            occurred_before=occurred_before,
            created_after=created_after,
            created_before=created_before,
            since_days=since_days,
            scoring_version=version,
        )
        scanned_by_version[version] = int(scanned)
        ranked_by_version[version] = [
            {
                "rank": int(rank),
                "score": int(score),
                "event": event,
                "details": details,
            }
            for rank, (score, event, details) in enumerate(ranked, start=1)
        ]

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
        lead_family = _first_present(
            (target_details or {}).get("lead_family"),
            (baseline_details or {}).get("lead_family"),
        )

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
    date_from: Optional[Any] = None,
    date_to: Optional[Any] = None,
    occurred_after: Optional[Any] = None,
    occurred_before: Optional[Any] = None,
    created_after: Optional[Any] = None,
    created_before: Optional[Any] = None,
    since_days: Optional[int] = None,
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
            ok = (
                db.execute(select(AnalysisRun.id).where(AnalysisRun.id == analysis_run_id))
                .scalar_one_or_none()
            )
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
            date_from=date_from,
            date_to=date_to,
            occurred_after=occurred_after,
            occurred_before=occurred_before,
            created_after=created_after,
            created_before=created_before,
            since_days=since_days,
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
        for idx, (score, e, details) in enumerate(ranked, start=1):
            item = LeadSnapshotItem(
                snapshot_id=snap.id,
                event_id=e.id,
                event_hash=e.hash,
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
            "date_from": date_from.isoformat() if hasattr(date_from, "isoformat") else date_from,
            "date_to": date_to.isoformat() if hasattr(date_to, "isoformat") else date_to,
            "occurred_after": occurred_after.isoformat() if hasattr(occurred_after, "isoformat") else occurred_after,
            "occurred_before": occurred_before.isoformat() if hasattr(occurred_before, "isoformat") else occurred_before,
            "created_after": created_after.isoformat() if hasattr(created_after, "isoformat") else created_after,
            "created_before": created_before.isoformat() if hasattr(created_before, "isoformat") else created_before,
            "since_days": int(since_days) if since_days is not None else None,
            "min_score": int(min_score),
            "limit": int(limit),
            "scan_limit": int(scan_limit),
            "scoring_version": scoring_version,
            "scanned": int(scanned),
            "items": int(inserted),
        }
    finally:
        db.close()
