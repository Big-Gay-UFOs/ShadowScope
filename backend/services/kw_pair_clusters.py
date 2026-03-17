from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

from sqlalchemy.orm import Session

from backend.db.models import Correlation, CorrelationLink, Entity, Event
from backend.services.explainability import (
    aggregate_matched_ontology,
    coerce_number,
    correlation_lane_payload,
    humanize_keyword,
    infer_correlation_lane,
    safe_float,
    safe_int,
)
from backend.services.investigator_filters import (
    event_place_region_label,
    investigator_event_conditions,
    investigator_event_filters_present,
)


def _best_agency(ev: Event) -> dict[str, Any] | None:
    candidates = (
        (ev.awarding_agency_code, ev.awarding_agency_name),
        (ev.funding_agency_code, ev.funding_agency_name),
        (ev.contracting_office_code, ev.contracting_office_name),
    )
    for code, name in candidates:
        code_text = str(code or "").strip().upper()
        name_text = str(name or "").strip()
        if code_text or name_text:
            label = name_text or code_text
            if name_text and code_text:
                label = f"{name_text} ({code_text})"
            return {
                "agency_code": code_text or None,
                "agency_name": name_text or None,
                "label": label,
            }
    return None


def _psc_info(ev: Event) -> dict[str, Any] | None:
    code = str(ev.psc_code or "").strip().upper()
    desc = str(ev.psc_description or "").strip()
    if not code and not desc:
        return None
    label = code or desc
    if code and desc:
        label = f"{code} {desc}"
    return {"psc_code": code or None, "psc_description": desc or None, "label": label}


def _naics_info(ev: Event) -> dict[str, Any] | None:
    code = str(ev.naics_code or "").strip().upper()
    desc = str(ev.naics_description or "").strip()
    if not code and not desc:
        return None
    label = code or desc
    if code and desc:
        label = f"{code} {desc}"
    return {"naics_code": code or None, "naics_description": desc or None, "label": label}


def _top_counts(bucket: dict[tuple[Any, ...], dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        bucket.values(),
        key=lambda item: (
            -safe_int(item.get("count"), default=0),
            str(item.get("label") or ""),
        ),
    )


def _cluster_source(correlation_key: Any, member_events: list[dict[str, Any]]) -> str | None:
    parts = str(correlation_key or "").split("|")
    if len(parts) >= 2 and parts[0] == "kw_pair":
        source = str(parts[1] or "").strip()
        if source and source != "*":
            return source
    if member_events:
        first_source = str(member_events[0].get("source") or "").strip()
        return first_source or None
    return None


def _sort_items(items: list[dict[str, Any]], *, sort_by: str | None, sort_dir: str | None) -> None:
    sort_key = str(sort_by or "score_signal").strip().lower()
    if sort_key not in {"score", "score_signal", "event_count", "created_at", "id", "pair_label"}:
        sort_key = "score_signal"
    reverse = str(sort_dir or "desc").strip().lower() != "asc"

    def key(item: dict[str, Any]) -> tuple[Any, ...]:
        if sort_key == "event_count":
            primary = safe_int(item.get("event_count"), default=0)
        elif sort_key == "created_at":
            primary = str(item.get("created_at") or "")
        elif sort_key == "id":
            primary = safe_int(item.get("correlation_id"), default=0)
        elif sort_key == "pair_label":
            primary = str(item.get("pair_label") or item.get("pair_label_raw") or "")
        else:
            primary = safe_float(item.get("score_signal"), default=safe_float(item.get("score"), default=0.0))
        return (primary, safe_int(item.get("event_count"), default=0), safe_int(item.get("correlation_id"), default=0))

    items.sort(key=key, reverse=reverse)


def list_kw_pair_clusters(
    db: Session,
    *,
    source: Optional[str] = None,
    window_days: Optional[int] = None,
    min_score: Optional[float] = None,
    min_event_count: Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
    include_events: bool = False,
    correlation_id: Optional[int] = None,
    date_from: Any = None,
    date_to: Any = None,
    entity_id: Optional[int] = None,
    keyword: Optional[str] = None,
    agency: Optional[str] = None,
    psc: Optional[str] = None,
    naics: Optional[str] = None,
    award_id: Optional[str] = None,
    recipient_uei: Optional[str] = None,
    place_region: Optional[str] = None,
    sort_by: str | None = "score_signal",
    sort_dir: str | None = "desc",
) -> dict[str, Any]:
    q = db.query(Correlation).filter(Correlation.correlation_key.like("kw_pair|%"))

    if correlation_id is not None:
        q = q.filter(Correlation.id == int(correlation_id))

    if source:
        q = q.filter(Correlation.correlation_key.like(f"kw_pair|{source}|%"))

    if window_days is not None:
        q = q.filter(Correlation.window_days == int(window_days))

    correlations = q.all()
    corr_ids = [safe_int(c.id) for c in correlations]
    requested_filters = investigator_event_filters_present(
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

    member_rows: dict[int, list[tuple[Event, Entity | None]]] = defaultdict(list)
    if corr_ids:
        rows_query = (
            db.query(CorrelationLink.correlation_id, Event, Entity)
            .join(Event, Event.id == CorrelationLink.event_id)
            .outerjoin(Entity, Entity.id == Event.entity_id)
            .filter(CorrelationLink.correlation_id.in_(corr_ids))
            .order_by(CorrelationLink.correlation_id.asc(), Event.id.asc())
        )
        if requested_filters:
            rows_query = rows_query.filter(
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
        rows = rows_query.all()
        for corr_id_value, event, entity in rows:
            member_rows[safe_int(corr_id_value)].append((event, entity))

    items: list[dict[str, Any]] = []
    for correlation in correlations:
        corr_id = safe_int(correlation.id)
        lane = infer_correlation_lane(correlation.correlation_key, correlation.lanes_hit)
        if lane != "kw_pair":
            continue

        payload = correlation_lane_payload(lane, correlation.lanes_hit)
        raw_score_signal = payload.get("score_signal", correlation.score)
        score_signal = coerce_number(raw_score_signal, default=0.0)

        members = member_rows.get(corr_id, [])
        if requested_filters and not members:
            continue

        total_event_count = safe_int(payload.get("event_count"), default=len(members))
        if total_event_count <= 0:
            total_event_count = len(members)
        matched_event_count = len(members)
        visible_event_count = matched_event_count if requested_filters else total_event_count

        if min_event_count is not None and visible_event_count < int(min_event_count):
            continue
        if min_score is not None and safe_float(score_signal, default=0.0) < float(min_score):
            continue

        keyword_1 = str(payload.get("keyword_1") or payload.get("k1") or "").strip() or None
        keyword_2 = str(payload.get("keyword_2") or payload.get("k2") or "").strip() or None
        pair_label_raw = f"{keyword_1} + {keyword_2}" if keyword_1 and keyword_2 else ""
        pair_label = (
            f"{humanize_keyword(keyword_1)} + {humanize_keyword(keyword_2)}"
            if keyword_1 and keyword_2
            else pair_label_raw
        )

        member_events: list[dict[str, Any]] = []
        entity_counts: dict[tuple[Any, ...], dict[str, Any]] = {}
        agency_counts: dict[tuple[Any, ...], dict[str, Any]] = {}
        psc_counts: dict[tuple[Any, ...], dict[str, Any]] = {}
        naics_counts: dict[tuple[Any, ...], dict[str, Any]] = {}
        clause_lists: list[Any] = []

        for event, entity in members:
            clause_lists.append(event.clauses)

            entity_name = (
                str(getattr(entity, "name", "") or "").strip()
                or str(event.recipient_name or "").strip()
            )
            entity_uei = (
                str(getattr(entity, "uei", "") or "").strip()
                or str(event.recipient_uei or "").strip()
            )
            entity_key = (entity_name, entity_uei)
            entity_id_value = safe_int(getattr(entity, "id", None), default=0) or None
            if entity_name or entity_id_value:
                entity_entry = entity_counts.setdefault(
                    entity_key,
                    {
                        "entity_id": entity_id_value,
                        "name": entity_name or None,
                        "uei": entity_uei or None,
                        "label": entity_name or entity_uei or "linked entity",
                        "count": 0,
                    },
                )
                if entity_entry.get("entity_id") is None and entity_id_value is not None:
                    entity_entry["entity_id"] = entity_id_value
                entity_entry["count"] += 1

            agency_info = _best_agency(event)
            if agency_info:
                agency_key = (agency_info.get("agency_code"), agency_info.get("agency_name"), agency_info.get("label"))
                agency_entry = agency_counts.setdefault(
                    agency_key,
                    {
                        **agency_info,
                        "count": 0,
                    },
                )
                agency_entry["count"] += 1

            psc_info = _psc_info(event)
            if psc_info:
                psc_key = (psc_info.get("psc_code"), psc_info.get("psc_description"), psc_info.get("label"))
                psc_entry = psc_counts.setdefault(
                    psc_key,
                    {
                        **psc_info,
                        "count": 0,
                    },
                )
                psc_entry["count"] += 1

            naics_info = _naics_info(event)
            if naics_info:
                naics_key = (naics_info.get("naics_code"), naics_info.get("naics_description"), naics_info.get("label"))
                naics_entry = naics_counts.setdefault(
                    naics_key,
                    {
                        **naics_info,
                        "count": 0,
                    },
                )
                naics_entry["count"] += 1

            member_event = {
                "id": safe_int(event.id),
                "hash": event.hash,
                "source": event.source,
                "doc_id": event.doc_id,
                "source_url": event.source_url,
                "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "snippet": event.snippet,
                "place_text": event.place_text,
                "place_region": event_place_region_label(event),
                "entity": None
                if not (entity_name or entity_id_value)
                else {
                    "entity_id": entity_id_value,
                    "name": entity_name or None,
                    "uei": entity_uei or None,
                },
                "award_id": event.award_id,
                "generated_unique_award_id": event.generated_unique_award_id,
                "recipient_name": event.recipient_name,
                "recipient_uei": event.recipient_uei,
                "awarding_agency_code": event.awarding_agency_code,
                "awarding_agency_name": event.awarding_agency_name,
                "psc_code": event.psc_code,
                "psc_description": event.psc_description,
                "naics_code": event.naics_code,
                "naics_description": event.naics_description,
            }
            member_events.append(member_event)

        ontology = aggregate_matched_ontology(clause_lists)
        member_event_ids = [safe_int(item.get("id")) for item in member_events]
        member_event_hashes = [str(item.get("hash") or "") for item in member_events if str(item.get("hash") or "")]
        source_value = _cluster_source(correlation.correlation_key, member_events)

        item = {
            "correlation_id": corr_id,
            "correlation_key": correlation.correlation_key,
            "lane": "kw_pair",
            "source": source_value,
            "window_days": safe_int(correlation.window_days),
            "score": correlation.score,
            "score_signal": score_signal,
            "score_signal_raw": correlation.score,
            "event_count": visible_event_count,
            "matched_event_count": matched_event_count,
            "total_event_count": total_event_count,
            "keyword_1": keyword_1,
            "keyword_2": keyword_2,
            "pair_label_raw": pair_label_raw,
            "pair_label": pair_label,
            "scoring_version": "v2",
            "pair_bonus_applied": True,
            "noise_penalty_applied": False,
            "contributing_lanes": ["kw_pair"],
            "contributing_correlations": [
                {
                    "correlation_id": corr_id,
                    "correlation_key": correlation.correlation_key,
                    "lane": "kw_pair",
                    "score_signal": score_signal,
                    "event_count": visible_event_count,
                    "pair_label": pair_label,
                }
            ],
            "member_event_ids": member_event_ids,
            "member_event_hashes": member_event_hashes,
            "member_events": member_events if include_events else [],
            "top_entities": _top_counts(entity_counts),
            "top_agencies": _top_counts(agency_counts),
            "top_psc": _top_counts(psc_counts),
            "top_naics": _top_counts(naics_counts),
            "matched_ontology_rules": ontology["matched_ontology_rules"],
            "matched_ontology_clauses": ontology["matched_ontology_clauses"],
            "summary": correlation.summary,
            "rationale": correlation.rationale,
            "created_at": correlation.created_at.isoformat() if correlation.created_at else None,
        }
        items.append(item)

    _sort_items(items, sort_by=sort_by, sort_dir=sort_dir)

    total = len(items)
    start = max(int(offset), 0)
    end = start + max(int(limit), 0)
    return {
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "items": items[start:end],
    }


def get_kw_pair_cluster(db: Session, *, correlation_id: int) -> dict[str, Any] | None:
    payload = list_kw_pair_clusters(
        db,
        correlation_id=int(correlation_id),
        limit=1,
        offset=0,
        include_events=True,
    )
    items = payload.get("items") or []
    return items[0] if items else None
