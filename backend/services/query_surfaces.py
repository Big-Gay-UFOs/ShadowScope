from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Float, Integer, cast, func, or_, select
from sqlalchemy.orm import Session

from backend.db.models import Correlation, CorrelationLink, Event
from backend.services.explainability import (
    coerce_number,
    correlation_lane_payload,
    infer_correlation_lane,
    safe_float,
    safe_int,
)
from backend.services.investigator_filters import (
    event_place_region_label,
    event_time_expr,
    investigator_event_conditions,
    investigator_event_filters_present,
)
from backend.services.lead_families import lead_family_label, lead_matches_family, summarize_lead_family_groups
from backend.services.kw_pair_clusters import list_kw_pair_clusters
from backend.services.leads import compute_leads
from backend.services.review_contract import serialize_ranked_lead_review_row


_EVENT_SORT_FIELDS: dict[str, Any] = {
    "occurred_at": "occurred_at",
    "created_at": "created_at",
    "id": "id",
    "source": "source",
}

_LEAD_SORT_FIELDS = {"score", "occurred_at", "created_at", "id", "pair_strength", "pair_count", "source", "lead_family"}
_CORRELATION_SORT_FIELDS = {"score", "score_signal", "event_count", "created_at", "id"}


def _norm_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return []


def _normalize_sort_dir(value: str | None) -> str:
    text = str(value or "desc").strip().lower()
    return "asc" if text == "asc" else "desc"


def _apply_event_sort(stmt, *, sort_by: str | None, sort_dir: str | None):
    direction = _normalize_sort_dir(sort_dir)
    sort_key = str(sort_by or "occurred_at").strip().lower()
    sort_field = _EVENT_SORT_FIELDS.get(sort_key, "occurred_at")

    if sort_field == "created_at":
        primary = Event.created_at
    elif sort_field == "id":
        primary = Event.id
    elif sort_field == "source":
        primary = func.lower(Event.source)
    else:
        primary = event_time_expr(Event)

    if direction == "asc":
        return stmt.order_by(primary.asc().nullsfirst(), Event.id.asc())
    return stmt.order_by(primary.desc().nullslast(), Event.id.desc())


def _serialize_event(event: Event) -> dict[str, Any]:
    return {
        "id": event.id,
        "entity_id": event.entity_id,
        "category": event.category,
        "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
        "lat": event.lat,
        "lon": event.lon,
        "source": event.source,
        "source_url": event.source_url,
        "doc_id": event.doc_id,
        "award_id": event.award_id,
        "generated_unique_award_id": event.generated_unique_award_id,
        "piid": event.piid,
        "fain": event.fain,
        "uri": event.uri,
        "transaction_id": event.transaction_id,
        "modification_number": event.modification_number,
        "source_record_id": event.source_record_id,
        "recipient_name": event.recipient_name,
        "recipient_uei": event.recipient_uei,
        "recipient_parent_uei": event.recipient_parent_uei,
        "recipient_duns": event.recipient_duns,
        "recipient_cage_code": event.recipient_cage_code,
        "awarding_agency_code": event.awarding_agency_code,
        "awarding_agency_name": event.awarding_agency_name,
        "funding_agency_code": event.funding_agency_code,
        "funding_agency_name": event.funding_agency_name,
        "contracting_office_code": event.contracting_office_code,
        "contracting_office_name": event.contracting_office_name,
        "psc_code": event.psc_code,
        "psc_description": event.psc_description,
        "naics_code": event.naics_code,
        "naics_description": event.naics_description,
        "notice_award_type": event.notice_award_type,
        "place_of_performance_city": event.place_of_performance_city,
        "place_of_performance_state": event.place_of_performance_state,
        "place_of_performance_country": event.place_of_performance_country,
        "place_of_performance_zip": event.place_of_performance_zip,
        "place_region": event_place_region_label(event),
        "solicitation_number": event.solicitation_number,
        "notice_id": event.notice_id,
        "document_id": event.document_id,
        "keywords": _norm_list(event.keywords),
        "clauses": _norm_list(event.clauses),
        "place_text": event.place_text,
        "snippet": event.snippet,
        "raw_json": event.raw_json,
        "hash": event.hash,
        "created_at": event.created_at.isoformat() if event.created_at else None,
    }


def query_events(
    db: Session,
    *,
    limit: int = 50,
    offset: int = 0,
    source: str | None = None,
    exclude_source: str | None = None,
    days: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    entity_id: int | None = None,
    keyword: str | None = None,
    has_entity: bool | None = None,
    award_id: str | None = None,
    contract_id: str | None = None,
    document_id: str | None = None,
    notice_id: str | None = None,
    solicitation_number: str | None = None,
    recipient_uei: str | None = None,
    agency: str | None = None,
    psc: str | None = None,
    naics: str | None = None,
    notice_award_type: str | None = None,
    place_region: str | None = None,
    place_state: str | None = None,
    place_country: str | None = None,
    sort_by: str | None = "occurred_at",
    sort_dir: str | None = "desc",
) -> dict[str, Any]:
    stmt = select(Event)

    effective_place_region = place_region
    if not effective_place_region and (place_state or place_country):
        pieces = [str(place_state or "").strip(), str(place_country or "").strip()]
        effective_place_region = ", ".join([piece for piece in pieces if piece]) or None

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
        place_region=effective_place_region,
    )

    if exclude_source:
        conditions.append(Event.source != exclude_source)

    if has_entity is True:
        conditions.append(Event.entity_id.is_not(None))
    elif has_entity is False:
        conditions.append(Event.entity_id.is_(None))

    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=max(int(days), 1))
        conditions.append(event_time_expr(Event) >= since)

    if contract_id:
        cid = str(contract_id).strip()
        if cid:
            conditions.append(or_(Event.piid == cid, Event.fain == cid, Event.uri == cid))

    if document_id:
        doc = str(document_id).strip()
        if doc:
            conditions.append(or_(Event.document_id == doc, Event.doc_id == doc))

    if notice_id:
        notice_value = str(notice_id).strip()
        if notice_value:
            conditions.append(Event.notice_id == notice_value)

    if solicitation_number:
        solicitation_value = str(solicitation_number).strip()
        if solicitation_value:
            conditions.append(Event.solicitation_number == solicitation_value)

    if notice_award_type:
        notice_type = str(notice_award_type).strip().lower()
        if notice_type:
            conditions.append(func.lower(Event.notice_award_type) == notice_type)

    stmt = stmt.where(*conditions)
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    stmt = _apply_event_sort(stmt, sort_by=sort_by, sort_dir=sort_dir)
    rows = db.execute(stmt.offset(max(int(offset), 0)).limit(max(int(limit), 0))).scalars().all()
    return {
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "items": [_serialize_event(event) for event in rows],
    }


def _lead_matches_correlation_filters(
    details: dict[str, Any],
    *,
    lane: str | None = None,
    min_event_count: int | None = None,
    min_score_signal: float | None = None,
) -> bool:
    lane_value = str(lane or "").strip().lower()
    correlations = details.get("contributing_correlations") or []
    if not lane_value and min_event_count is None and min_score_signal is None:
        return True

    for correlation in correlations:
        if not isinstance(correlation, dict):
            continue
        if lane_value and str(correlation.get("lane") or "").strip().lower() != lane_value:
            continue
        if min_event_count is not None and safe_int(correlation.get("event_count"), default=0) < int(min_event_count):
            continue
        if min_score_signal is not None:
            score_signal = safe_float(
                correlation.get("score_signal"),
                default=safe_float(correlation.get("score_signal_raw"), default=0.0),
            )
            if score_signal < float(min_score_signal):
                continue
        return True
    return False


def _lead_sort_value(item: tuple[int, Event, dict[str, Any]], sort_by: str) -> Any:
    score, event, details = item
    if sort_by == "created_at":
        return _lead_sort_datetime(event.created_at)
    if sort_by == "occurred_at":
        return _lead_sort_datetime(event.occurred_at or event.created_at)
    if sort_by == "id":
        return int(event.id)
    if sort_by == "pair_strength":
        return safe_float(details.get("pair_strength"), default=0.0)
    if sort_by == "pair_count":
        return safe_int(details.get("pair_count"), default=0)
    if sort_by == "source":
        return str(event.source or "")
    if sort_by == "lead_family":
        return str(details.get("lead_family") or "")
    return int(score)


def _lead_sort_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime.min.replace(tzinfo=timezone.utc)


def _lead_sort_components(event: Event) -> tuple[Any, ...]:
    relevant_at = _lead_sort_datetime(event.occurred_at or event.created_at)
    occurred_at = _lead_sort_datetime(event.occurred_at)
    created_at = _lead_sort_datetime(event.created_at)
    return relevant_at, occurred_at, created_at, int(event.id)


def query_leads(
    db: Session,
    *,
    limit: int = 50,
    offset: int = 0,
    min_score: int = 1,
    scan_limit: int = 5000,
    scoring_version: str = "v2",
    source: str | None = None,
    exclude_source: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    occurred_after: datetime | None = None,
    occurred_before: datetime | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    since_days: int | None = None,
    entity_id: int | None = None,
    keyword: str | None = None,
    agency: str | None = None,
    psc: str | None = None,
    naics: str | None = None,
    award_id: str | None = None,
    recipient_uei: str | None = None,
    place_region: str | None = None,
    lead_family: str | None = None,
    lane: str | None = None,
    min_event_count: int | None = None,
    min_score_signal: float | None = None,
    include_details: bool = True,
    group_by_family: bool = False,
    sort_by: str | None = "score",
    sort_dir: str | None = "desc",
) -> dict[str, Any]:
    desired = max(int(scan_limit), max(int(limit), 0) + max(int(offset), 0))
    ranked, scanned = compute_leads(
        db,
        scan_limit=desired,
        limit=desired,
        min_score=min_score,
        source=source,
        exclude_source=exclude_source,
        scoring_version=scoring_version,
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

    filtered = [
        item
        for item in ranked
        if _lead_matches_correlation_filters(
            item[2],
            lane=lane,
            min_event_count=min_event_count,
            min_score_signal=min_score_signal,
        )
        and lead_matches_family(item[2], lead_family)
    ]

    sort_key = str(sort_by or "score").strip().lower()
    if sort_key not in _LEAD_SORT_FIELDS:
        sort_key = "score"
    reverse = _normalize_sort_dir(sort_dir) == "desc"
    filtered.sort(
        key=lambda item: (_lead_sort_value(item, sort_key),) + _lead_sort_components(item[1]),
        reverse=reverse,
    )

    family_group_rows = [
        {
            "rank": idx,
            "score": int(score),
            "event_id": int(event.id),
            "lead_family": details.get("lead_family"),
            "secondary_lead_families": details.get("secondary_lead_families") or [],
        }
        for idx, (score, event, details) in enumerate(filtered, start=1)
    ]
    family_groups = (
        summarize_lead_family_groups(family_group_rows, lead_family_filter=lead_family)
        if group_by_family
        else []
    )

    sliced = filtered[max(int(offset), 0): max(int(offset), 0) + max(int(limit), 0)]
    items: list[dict[str, Any]] = []
    start_rank = max(int(offset), 0) + 1
    for idx, (score, event, details) in enumerate(sliced, start=start_rank):
        review_row = serialize_ranked_lead_review_row(
            snapshot=None,
            item=None,
            event=event,
            details=details,
            rank=idx,
            score=int(score),
        )
        payload = {
            **review_row,
            "id": event.id,
            "keywords": _norm_list(event.keywords),
            "clauses": _norm_list(event.clauses),
            "lead_family_label": review_row.get("lead_family_label") or lead_family_label(details.get("lead_family")),
            "corroboration_summary": details.get("corroboration_summary") or {},
            "pair_bonus_applied": details.get("pair_bonus_applied", details.get("pair_bonus", 0)),
            "noise_penalty_applied": details.get("noise_penalty_applied", details.get("noise_penalty", 0)),
            "contributing_lanes": details.get("contributing_lanes") or [],
            "matched_ontology_rules": details.get("matched_ontology_rules") or [],
        }
        if not include_details:
            payload.pop("score_details", None)
        items.append(payload)

    return {
        "total": len(filtered),
        "limit": int(limit),
        "offset": int(offset),
        "scanned": int(scanned),
        "items": items,
        "family_groups": family_groups,
    }


def query_correlations(
    db: Session,
    *,
    source: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    entity_id: int | None = None,
    keyword: str | None = None,
    min_score: float | None = None,
    agency: str | None = None,
    psc: str | None = None,
    naics: str | None = None,
    award_id: str | None = None,
    recipient_uei: str | None = None,
    place_region: str | None = None,
    lane: str | None = None,
    window_days: int | None = None,
    min_event_count: int | None = None,
    min_score_signal: float | None = None,
    limit: int = 50,
    offset: int = 0,
    sort_by: str | None = "score_signal",
    sort_dir: str | None = "desc",
) -> dict[str, Any]:
    lane_value = str(lane or "").strip().lower() or None
    if lane_value == "kw_pair":
        return list_kw_pair_clusters(
            db,
            source=source,
            window_days=window_days,
            min_score=min_score_signal if min_score_signal is not None else min_score,
            min_event_count=min_event_count,
            limit=limit,
            offset=offset,
            include_events=False,
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
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    total_counts = (
        select(
            CorrelationLink.correlation_id.label("correlation_id"),
            func.count(CorrelationLink.id).label("total_event_count"),
        )
        .group_by(CorrelationLink.correlation_id)
        .subquery()
    )

    requested_event_filters = investigator_event_filters_present(
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

    filtered_counts = None
    if requested_event_filters:
        filtered_counts = (
            select(
                CorrelationLink.correlation_id.label("correlation_id"),
                func.count(CorrelationLink.id).label("matched_event_count"),
            )
            .join(Event, Event.id == CorrelationLink.event_id)
            .where(
                *investigator_event_conditions(
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
            )
            .group_by(CorrelationLink.correlation_id)
            .subquery()
        )

    stmt = (
        select(
            Correlation,
            func.coalesce(total_counts.c.total_event_count, 0).label("total_event_count"),
            (
                func.coalesce(filtered_counts.c.matched_event_count, 0)
                if filtered_counts is not None
                else func.coalesce(total_counts.c.total_event_count, 0)
            ).label("matched_event_count"),
        )
        .outerjoin(total_counts, total_counts.c.correlation_id == Correlation.id)
    )

    if filtered_counts is not None:
        stmt = stmt.join(filtered_counts, filtered_counts.c.correlation_id == Correlation.id)

    if lane_value:
        stmt = stmt.where(Correlation.correlation_key.like(f"{lane_value}|%"))
    if window_days is not None:
        stmt = stmt.where(Correlation.window_days == int(window_days))
    if min_score is not None:
        stmt = stmt.where(cast(Correlation.score, Float) >= float(min_score))
    if min_score_signal is not None:
        stmt = stmt.where(cast(Correlation.score, Float) >= float(min_score_signal))

    if min_event_count is not None and int(min_event_count) > 0:
        count_column = filtered_counts.c.matched_event_count if filtered_counts is not None else total_counts.c.total_event_count
        stmt = stmt.where(func.coalesce(count_column, 0) >= int(min_event_count))

    sort_key = str(sort_by or "score_signal").strip().lower()
    if sort_key not in _CORRELATION_SORT_FIELDS:
        sort_key = "score_signal"
    direction = _normalize_sort_dir(sort_dir)

    if sort_key == "event_count":
        primary = cast(
            filtered_counts.c.matched_event_count if filtered_counts is not None else total_counts.c.total_event_count,
            Integer,
        )
    elif sort_key == "created_at":
        primary = Correlation.created_at
    elif sort_key == "id":
        primary = Correlation.id
    else:
        primary = cast(Correlation.score, Float)

    if direction == "asc":
        stmt = stmt.order_by(primary.asc().nullsfirst(), Correlation.id.asc())
    else:
        stmt = stmt.order_by(primary.desc().nullslast(), Correlation.id.desc())

    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(stmt.offset(max(int(offset), 0)).limit(max(int(limit), 0))).all()

    items: list[dict[str, Any]] = []
    for correlation, total_event_count, matched_event_count in rows:
        lane_name = infer_correlation_lane(correlation.correlation_key, correlation.lanes_hit)
        payload = correlation_lane_payload(lane_name, correlation.lanes_hit)
        score_signal = coerce_number(payload.get("score_signal", correlation.score), default=0.0)
        visible_count = safe_int(matched_event_count if requested_event_filters else total_event_count, default=0)
        items.append(
            {
                "id": correlation.id,
                "lane": lane_name,
                "correlation_key": correlation.correlation_key,
                "score": correlation.score,
                "score_signal": score_signal,
                "window_days": correlation.window_days,
                "radius_km": correlation.radius_km,
                "lanes_hit": correlation.lanes_hit,
                "summary": correlation.summary,
                "rationale": correlation.rationale,
                "created_at": correlation.created_at,
                "event_count": visible_count,
                "matched_event_count": safe_int(matched_event_count, default=0),
                "total_event_count": safe_int(total_event_count, default=0),
            }
        )

    return {
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "items": items,
    }


__all__ = ["query_correlations", "query_events", "query_leads"]
