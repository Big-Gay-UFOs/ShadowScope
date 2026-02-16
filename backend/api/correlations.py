from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Integer, cast, func
from sqlalchemy.orm import Session
from backend.api.deps import get_db_session
from backend.db.models import Correlation, CorrelationLink, Entity, Event

router = APIRouter(prefix="/correlations", tags=["correlations"])


def _try_int(v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


@router.get("/")
def list_correlations(
    source: Optional[str] = Query("USAspending", description="Event source filter (blank for all)"),
    window_days: Optional[int] = Query(None, ge=1, description="Filter by window_days"),
    min_score: Optional[int] = Query(None, ge=0, description="Minimum numeric score (best-effort; score stored as text)"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    """
    List correlations with lightweight metadata.
    """
    q = db.query(Correlation)

    if window_days is not None:
        q = q.filter(Correlation.window_days == int(window_days))

    # Best-effort numeric filter: only applies when score is parseable as integer
    if min_score is not None:
        q = q.filter(cast(Correlation.score, Integer) >= int(min_score))

    # Source filter: join through links->events (exists)
    if source:
        q = q.join(CorrelationLink, CorrelationLink.correlation_id == Correlation.id)\
             .join(Event, Event.id == CorrelationLink.event_id)\
             .filter(Event.source == source)\
             .distinct()

    total = q.count()

    rows = (
        q.order_by(Correlation.id.desc())
        .offset(int(offset))
        .limit(int(limit))
        .all()
    )

    items: List[Dict[str, Any]] = []
    for c in rows:
        items.append(
            {
                "id": c.id,
                "score": c.score,
                "window_days": c.window_days,
                "radius_km": c.radius_km,
                "lanes_hit": c.lanes_hit,
                "summary": c.summary,
                "rationale": c.rationale,
                "created_at": c.created_at,
            }
        )

    return {"total": total, "limit": limit, "offset": offset, "items": items}


@router.get("/{correlation_id}")
def get_correlation(
    correlation_id: int,
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    c = db.get(Correlation, int(correlation_id))
    if not c:
        raise HTTPException(status_code=404, detail="Correlation not found")

    # fetch linked events + entity details
    links = (
        db.query(CorrelationLink, Event, Entity)
        .join(Event, Event.id == CorrelationLink.event_id)
        .outerjoin(Entity, Entity.id == Event.entity_id)
        .filter(CorrelationLink.correlation_id == int(correlation_id))
        .order_by(Event.id.asc())
        .all()
    )

    events: List[Dict[str, Any]] = []
    for (_link, ev, ent) in links:
        events.append(
            {
                "id": ev.id,
                "hash": ev.hash,
                "source": ev.source,
                "doc_id": ev.doc_id,
                "occurred_at": ev.occurred_at,
                "created_at": ev.created_at,
                "snippet": ev.snippet,
                "place_text": ev.place_text,
                "entity": None
                if ent is None
                else {
                    "id": ent.id,
                    "name": ent.name,
                    "uei": ent.uei,
                },
            }
        )

    return {
        "id": c.id,
        "score": c.score,
        "window_days": c.window_days,
        "radius_km": c.radius_km,
        "lanes_hit": c.lanes_hit,
        "summary": c.summary,
        "rationale": c.rationale,
        "created_at": c.created_at,
        "events": events,
        "event_count": len(events),
    }