from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import select
import os
from typing import Any

from backend.api.deps import get_db_session
from backend.db.models import Entity, Event, session_scope

router = APIRouter(prefix="/api", tags=["core"])


def _norm_list(value: Any) -> list:
    """Normalize JSON-ish fields (some older rows may store {} instead of [])."""
    if value is None:
        return []
    if isinstance(value, dict):
        return []
    if isinstance(value, list):
        return value
    return []


@router.get("/ping")
def ping():
    return {"message": "pong"}


@router.get("/entities")
def list_entities(db: Session = Depends(get_db_session)):
    rows = db.query(Entity).order_by(Entity.id).all()
    return [
        {
            "id": r.id,
            "name": r.name,
            "cage": r.cage,
            "uei": r.uei,
            "parent": r.parent,
            "type": r.type,
            "sponsor": r.sponsor,
            "sites": r.sites_json,
        }
        for r in rows
    ]


@router.get("/events")
def list_events(limit: int = 50):
    database_url = os.getenv("DATABASE_URL", "sqlite:///./dev.db")
    with session_scope(database_url) as s:
        rows = (
            s.execute(select(Event).order_by(Event.id.desc()).limit(limit))
            .scalars()
            .all()
        )

    return [
        {
            "id": e.id,
            "entity_id": e.entity_id,
            "category": e.category,
            "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
            "lat": e.lat,
            "lon": e.lon,
            "source": e.source,
            "source_url": e.source_url,
            "doc_id": e.doc_id,
            "keywords": _norm_list(e.keywords),
            "clauses": _norm_list(e.clauses),
            "place_text": e.place_text,
            "snippet": e.snippet,
            "raw_json": e.raw_json,
            "hash": e.hash,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }
        for e in rows
    ]


@router.get("/leads")
def list_leads(
    limit: int = 50,
    min_score: int = 1,
    source: str | None = None,
    exclude_source: str | None = None,
):
    database_url = os.getenv("DATABASE_URL", "sqlite:///./dev.db")
    with session_scope(database_url) as s:
        rows = (
            s.execute(select(Event).order_by(Event.id.desc()).limit(2000))
            .scalars()
            .all()
        )

    def score(e: Event) -> int:
        k = _norm_list(e.keywords)
        return (10 if e.entity_id else 0) + (3 * len(k))

    scored = []
    for e in rows:
        # Apply requested filters
        if source and e.source != source:
            continue
        if exclude_source and e.source == exclude_source:
            continue

        sc = score(e)
        if sc >= min_score:
            scored.append((sc, e))

    scored.sort(key=lambda t: (t[0], t[1].id), reverse=True)
    top = scored[:limit]

    return [
        {
            "score": sc,
            "id": e.id,
            "entity_id": e.entity_id,
            "category": e.category,
            "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
            "source": e.source,
            "doc_id": e.doc_id,
            "place_text": e.place_text,
            "snippet": e.snippet,
            "keywords": _norm_list(e.keywords),
            "source_url": e.source_url,
        }
        for sc, e in top
    ]
