from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.api.deps import get_db_session
from backend.analysis.scoring import score_from_keywords_clauses
from backend.db.models import AnalysisRun, Entity, Event
from backend.search.opensearch import opensearch_search

router = APIRouter(prefix="/api", tags=["core"])


def _norm_list(value: Any) -> list:
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
def list_events(limit: int = 50, db: Session = Depends(get_db_session)):
    rows = db.execute(select(Event).order_by(Event.id.desc()).limit(limit)).scalars().all()
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


@router.get("/analysis-runs")
def list_analysis_runs(
    limit: int = 50,
    analysis_type: Optional[str] = None,
    db: Session = Depends(get_db_session),
):
    q = db.query(AnalysisRun).order_by(AnalysisRun.id.desc()).limit(limit)
    if analysis_type:
        q = q.filter(AnalysisRun.analysis_type == analysis_type)

    rows = q.all()
    return [
        {
            "id": r.id,
            "analysis_type": r.analysis_type,
            "status": r.status,
            "source": r.source,
            "days": r.days,
            "ontology_version": r.ontology_version,
            "ontology_hash": r.ontology_hash,
            "dry_run": bool(getattr(r, "dry_run", False)),
            "scanned": r.scanned,
            "updated": r.updated,
            "unchanged": r.unchanged,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "ended_at": r.ended_at.isoformat() if r.ended_at else None,
            "error": r.error,
        }
        for r in rows
    ]


@router.get("/leads")
def list_leads(
    limit: int = 50,
    min_score: int = 1,
    source: str | None = None,
    exclude_source: str | None = None,
    include_details: bool = True,
    db: Session = Depends(get_db_session),
):
    # Pull a window of recent events and rank them in Python.
    rows = db.execute(select(Event).order_by(Event.id.desc()).limit(5000)).scalars().all()

    scored = []
    for e in rows:
        if source and e.source != source:
            continue
        if exclude_source and e.source == exclude_source:
            continue

        sc, details = score_from_keywords_clauses(e.keywords, e.clauses, has_entity=bool(e.entity_id))
        if sc >= min_score:
            scored.append((sc, e, details))

    scored.sort(key=lambda t: (t[0], t[1].id), reverse=True)
    top = scored[:limit]

    out = []
    for sc, e, details in top:
        item = {
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
            "clauses": _norm_list(e.clauses),
            "source_url": e.source_url,
        }
        if include_details:
            item["score_details"] = details
        out.append(item)

    return out


@router.get("/search")
def search(
    q: str,
    limit: int = 50,
    source: str | None = None,
    category: str | None = None,
):
    try:
        return opensearch_search(q=q, limit=limit, source=source, category=category)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))