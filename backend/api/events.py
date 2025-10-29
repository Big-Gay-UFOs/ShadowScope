"""Events API endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.api.deps import get_db_session
from backend.db.models import Event
from backend.services.export import export_events

router = APIRouter(prefix="/events", tags=["events"])


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    entity_id: Optional[int] = None
    category: str
    occurred_at: Optional[datetime] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    source: str
    source_url: Optional[str] = None
    doc_id: Optional[str] = None
    keywords: Optional[List[str]] = None
    clauses: Optional[List[str]] = None
    place_text: Optional[str] = None
    snippet: Optional[str] = None
    raw_json: Optional[dict] = None
    hash: str
    created_at: Optional[datetime] = None


class EventListResponse(BaseModel):
    items: List[EventOut]
    total: int


@router.get("", response_model=EventListResponse)
def list_events(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    source: Optional[str] = Query(None),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    q: Optional[str] = Query(None, description="Filter by snippet text"),
    db: Session = Depends(get_db_session),
) -> EventListResponse:
    conditions = []
    if source:
        conditions.append(Event.source == source)
    if date_from:
        conditions.append(Event.occurred_at >= date_from)
    if date_to:
        conditions.append(Event.occurred_at <= date_to)
    if q:
        conditions.append(Event.snippet.ilike(f"%{q}%"))

    base_query = select(Event).where(*conditions)
    total_stmt = select(func.count()).select_from(Event).where(*conditions)
    total = db.execute(total_stmt).scalar_one()
    items = (
        db.execute(
            base_query.order_by(Event.occurred_at.desc().nullslast(), Event.id.desc())
            .offset(offset)
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return EventListResponse(items=items, total=total)


@router.get("/export")
def export_events_endpoint():
    results = export_events()
    csv_path = results["csv"]
    return FileResponse(
        path=csv_path,
        media_type="text/csv",
        filename=csv_path.name,
    )


__all__ = ["router"]
