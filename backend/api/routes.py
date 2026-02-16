from backend.api.correlations import router as correlations_router
from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from backend.api.deps import get_db_session
from backend.analysis.scoring import score_from_keywords_clauses
from backend.db.models import (
    AnalysisRun,
    Entity,
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
)
from backend.search.opensearch import opensearch_search
from backend.services.deltas import lead_deltas

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


@router.get("/lead-snapshots")
def list_lead_snapshots(
    limit: int = 50,
    analysis_run_id: Optional[int] = None,
    source: Optional[str] = None,
    db: Session = Depends(get_db_session),
):
    q = db.query(LeadSnapshot).order_by(LeadSnapshot.id.desc()).limit(limit)
    if analysis_run_id is not None:
        q = q.filter(LeadSnapshot.analysis_run_id == analysis_run_id)
    if source:
        q = q.filter(LeadSnapshot.source == source)

    snaps = q.all()
    ids = [int(s.id) for s in snaps]

    counts = {}
    if ids:
        rows = db.execute(
            select(LeadSnapshotItem.snapshot_id, func.count(LeadSnapshotItem.id))
            .where(LeadSnapshotItem.snapshot_id.in_(ids))
            .group_by(LeadSnapshotItem.snapshot_id)
        ).all()
        counts = {int(sid): int(cnt) for sid, cnt in rows}

    return [
        {
            "id": int(s.id),
            "analysis_run_id": s.analysis_run_id,
            "source": s.source,
            "min_score": s.min_score,
            "limit": s.limit,
            "scoring_version": s.scoring_version,
            "notes": s.notes,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "items": counts.get(int(s.id), 0),
        }
        for s in snaps
    ]


@router.get("/lead-snapshots/{snapshot_id}")
def get_lead_snapshot(snapshot_id: int, db: Session = Depends(get_db_session)):
    snap = db.execute(select(LeadSnapshot).where(LeadSnapshot.id == snapshot_id)).scalar_one_or_none()
    if snap is None:
        raise HTTPException(status_code=404, detail=f"lead_snapshot {snapshot_id} not found")

    count = db.execute(
        select(func.count(LeadSnapshotItem.id)).where(LeadSnapshotItem.snapshot_id == snapshot_id)
    ).scalar_one()

    return {
        "id": int(snap.id),
        "analysis_run_id": snap.analysis_run_id,
        "source": snap.source,
        "min_score": snap.min_score,
        "limit": snap.limit,
        "scoring_version": snap.scoring_version,
        "notes": snap.notes,
        "created_at": snap.created_at.isoformat() if snap.created_at else None,
        "items": int(count),
    }


@router.get("/lead-snapshots/{snapshot_id}/items")
def list_lead_snapshot_items(
    snapshot_id: int,
    limit: int = 200,
    include_score_details: bool = True,
    db: Session = Depends(get_db_session),
):
    snap = db.execute(select(LeadSnapshot.id).where(LeadSnapshot.id == snapshot_id)).scalar_one_or_none()
    if snap is None:
        raise HTTPException(status_code=404, detail=f"lead_snapshot {snapshot_id} not found")

    rows = (
        db.query(LeadSnapshotItem, Event)
        .join(Event, LeadSnapshotItem.event_id == Event.id)
        .filter(LeadSnapshotItem.snapshot_id == snapshot_id)
        .order_by(LeadSnapshotItem.rank.asc())
        .limit(limit)
        .all()
    )

    out = []
    for item, ev in rows:
        d = {
            "snapshot_id": int(item.snapshot_id),
            "rank": int(item.rank),
            "score": int(item.score),
            "event_id": int(item.event_id),
            "event_hash": item.event_hash,
            "event": {
                "id": int(ev.id),
                "hash": ev.hash,
                "source": ev.source,
                "doc_id": ev.doc_id,
                "source_url": ev.source_url,
                "snippet": ev.snippet,
                "place_text": ev.place_text,
                "occurred_at": ev.occurred_at.isoformat() if ev.occurred_at else None,
                "created_at": ev.created_at.isoformat() if ev.created_at else None,
            },
        }
        if include_score_details:
            d["score_details"] = item.score_details
        out.append(d)

    return out


@router.get("/lead-deltas")
def get_lead_deltas(
    from_snapshot_id: int,
    to_snapshot_id: int,
    db: Session = Depends(get_db_session),
):
    try:
        return lead_deltas(db, from_snapshot_id=from_snapshot_id, to_snapshot_id=to_snapshot_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


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
api_router.include_router(correlations_router)
