from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.api.deps import get_db_session
from backend.db.models import Correlation, CorrelationLink, Entity, Event
from backend.services.investigator_filters import event_place_region_label
from backend.services.kw_pair_clusters import get_kw_pair_cluster
from backend.services.query_surfaces import query_correlations

router = APIRouter(prefix="/correlations", tags=["correlations"])


@router.get("/")
def list_correlations(
    source: Optional[str] = Query("USAspending", description="Event source filter (blank for all)"),
    date_from: Optional[datetime] = Query(None, description="Filter linked events by inclusive start datetime"),
    date_to: Optional[datetime] = Query(None, description="Filter linked events by inclusive end datetime"),
    entity_id: Optional[int] = Query(None, description="Filter linked events by entity_id"),
    keyword: Optional[str] = Query(None, description="Filter linked events by keyword tag"),
    min_score: Optional[int] = Query(None, ge=0, description="Minimum numeric score"),
    agency: Optional[str] = Query(None, description="Filter linked events by agency code or name"),
    psc: Optional[str] = Query(None, description="Filter linked events by PSC code or description"),
    naics: Optional[str] = Query(None, description="Filter linked events by NAICS code or description"),
    award_id: Optional[str] = Query(None, description="Filter linked events by award id"),
    recipient_uei: Optional[str] = Query(None, description="Filter linked events by recipient UEI"),
    place_region: Optional[str] = Query(None, description="Filter linked events by state/country region"),
    lane: Optional[str] = Query(None, description="Filter by correlation lane (e.g., same_entity, same_uei)"),
    window_days: Optional[int] = Query(None, ge=1, description="Filter by window_days"),
    min_event_count: Optional[int] = Query(None, ge=0, description="Minimum linked events within the active filters"),
    min_score_signal: Optional[float] = Query(None, ge=0, description="Minimum score_signal / lane score"),
    sort_by: Optional[str] = Query("score_signal", description="Sort by score_signal, event_count, created_at, or id"),
    sort_dir: Optional[str] = Query("desc", description="Sort direction asc or desc"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    return query_correlations(
        db,
        source=source or None,
        date_from=date_from,
        date_to=date_to,
        entity_id=entity_id,
        keyword=keyword,
        min_score=min_score,
        agency=agency,
        psc=psc,
        naics=naics,
        award_id=award_id,
        recipient_uei=recipient_uei,
        place_region=place_region,
        lane=lane,
        window_days=window_days,
        min_event_count=min_event_count,
        min_score_signal=min_score_signal,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@router.get("/{correlation_id}")
def get_correlation(correlation_id: int, db: Session = Depends(get_db_session)) -> Dict[str, Any]:
    correlation = db.get(Correlation, int(correlation_id))
    if not correlation:
        raise HTTPException(status_code=404, detail="Correlation not found")

    lane = str(correlation.correlation_key or "").split("|", 1)[0] if correlation.correlation_key else None
    if lane == "kw_pair":
        item = get_kw_pair_cluster(db, correlation_id=int(correlation_id))
        if item is None:
            raise HTTPException(status_code=404, detail="Correlation not found")
        return item

    links = (
        db.query(CorrelationLink, Event, Entity)
        .join(Event, Event.id == CorrelationLink.event_id)
        .outerjoin(Entity, Entity.id == Event.entity_id)
        .filter(CorrelationLink.correlation_id == int(correlation_id))
        .order_by(Event.id.asc())
        .all()
    )

    events: List[Dict[str, Any]] = []
    for (_link, event, entity) in links:
        events.append(
            {
                "id": event.id,
                "hash": event.hash,
                "source": event.source,
                "doc_id": event.doc_id,
                "source_url": event.source_url,
                "occurred_at": event.occurred_at,
                "created_at": event.created_at,
                "snippet": event.snippet,
                "place_text": event.place_text,
                "place_region": event_place_region_label(event),
                "award_id": event.award_id,
                "generated_unique_award_id": event.generated_unique_award_id,
                "recipient_name": event.recipient_name,
                "recipient_uei": event.recipient_uei,
                "awarding_agency_code": event.awarding_agency_code,
                "awarding_agency_name": event.awarding_agency_name,
                "psc_code": event.psc_code,
                "naics_code": event.naics_code,
                "entity": None
                if entity is None
                else {"id": entity.id, "name": entity.name, "uei": entity.uei},
            }
        )

    return {
        "id": correlation.id,
        "lane": lane,
        "correlation_key": correlation.correlation_key,
        "score": correlation.score,
        "window_days": correlation.window_days,
        "radius_km": correlation.radius_km,
        "lanes_hit": correlation.lanes_hit,
        "summary": correlation.summary,
        "rationale": correlation.rationale,
        "created_at": correlation.created_at,
        "events": events,
        "event_count": len(events),
    }

