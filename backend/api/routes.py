from typing import Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.api.deps import get_db_session
from backend.api.correlations import router as correlations_router
from backend.services.explainability import enrich_lead_score_details, load_event_correlation_evidence
from backend.services.leads import normalize_scoring_version
from backend.db.models import AnalysisRun, Entity, Event, LeadSnapshot, LeadSnapshotItem
from backend.search.opensearch import opensearch_search
from backend.services.deltas import lead_deltas
from backend.services.query_surfaces import query_events, query_leads

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
def list_events(
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
    agency_code: str | None = None,
    psc: str | None = None,
    naics: str | None = None,
    notice_award_type: str | None = None,
    place_region: str | None = None,
    place_state: str | None = None,
    place_country: str | None = None,
    sort_by: str | None = "occurred_at",
    sort_dir: str | None = "desc",
    db: Session = Depends(get_db_session),
):
    payload = query_events(
        db,
        limit=limit,
        offset=offset,
        source=source,
        exclude_source=exclude_source,
        days=days,
        date_from=date_from,
        date_to=date_to,
        entity_id=entity_id,
        keyword=keyword,
        has_entity=has_entity,
        award_id=award_id,
        contract_id=contract_id,
        document_id=document_id,
        notice_id=notice_id,
        solicitation_number=solicitation_number,
        recipient_uei=recipient_uei,
        agency=agency or agency_code,
        psc=psc,
        naics=naics,
        notice_award_type=notice_award_type,
        place_region=place_region,
        place_state=place_state,
        place_country=place_country,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    return payload["items"]
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
    offset: int = 0,
    min_score: int = 1,
    scan_limit: int = 5000,
    scoring_version: str = "v2",
    source: str | None = None,
    exclude_source: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    entity_id: int | None = None,
    keyword: str | None = None,
    agency: str | None = None,
    psc: str | None = None,
    naics: str | None = None,
    award_id: str | None = None,
    recipient_uei: str | None = None,
    place_region: str | None = None,
    lane: str | None = None,
    min_event_count: int | None = None,
    min_score_signal: float | None = None,
    sort_by: str | None = "score",
    sort_dir: str | None = "desc",
    include_details: bool = True,
    db: Session = Depends(get_db_session),
):
    try:
        limit_i = int(limit)
        offset_i = int(offset)
        scan_i = int(scan_limit)
        min_i = int(min_score)
    except Exception:
        raise HTTPException(status_code=400, detail='limit, offset, scan_limit, and min_score must be integers')

    if limit_i < 1 or limit_i > 200:
        raise HTTPException(status_code=400, detail='limit must be between 1 and 200')
    if offset_i < 0:
        raise HTTPException(status_code=400, detail='offset must be >= 0')
    if scan_i < 1 or scan_i > 5000:
        raise HTTPException(status_code=400, detail='scan_limit must be between 1 and 5000')
    if scan_i < (limit_i + offset_i):
        scan_i = limit_i + offset_i
    if min_i < 0:
        min_i = 0

    try:
        scoring_version = normalize_scoring_version(scoring_version)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    payload = query_leads(
        db,
        limit=limit_i,
        offset=offset_i,
        min_score=min_i,
        scan_limit=scan_i,
        scoring_version=scoring_version,
        source=source,
        exclude_source=exclude_source,
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
        lane=lane,
        min_event_count=min_event_count,
        min_score_signal=min_score_signal,
        include_details=include_details,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    return payload["items"]
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
        rows = (
            db.execute(
                select(LeadSnapshotItem.snapshot_id, func.count(LeadSnapshotItem.id))
                .where(LeadSnapshotItem.snapshot_id.in_(ids))
                .group_by(LeadSnapshotItem.snapshot_id)
            )
            .all()
        )
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

    count = db.execute(select(func.count(LeadSnapshotItem.id)).where(LeadSnapshotItem.snapshot_id == snapshot_id)).scalar_one()

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

    event_ids = [int(item.event_id) for item, _ev in rows]
    correlations_by_event = load_event_correlation_evidence(db, event_ids=event_ids)

    out = []
    for item, ev in rows:
        details = enrich_lead_score_details(
            clauses=ev.clauses,
            base_details=item.score_details if isinstance(item.score_details, dict) else {},
            correlations=correlations_by_event.get(int(item.event_id), []),
        )
        d = {
            "snapshot_id": int(item.snapshot_id),
            "rank": int(item.rank),
            "score": int(item.score),
            "event_id": int(item.event_id),
            "event_hash": item.event_hash,
            "scoring_version": details.get("scoring_version"),
            "pair_bonus_applied": details.get("pair_bonus_applied", details.get("pair_bonus", 0)),
            "noise_penalty_applied": details.get("noise_penalty_applied", details.get("noise_penalty", 0)),
            "contributing_lanes": details.get("contributing_lanes") or [],
            "matched_ontology_rules": details.get("matched_ontology_rules") or [],
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
            d["score_details"] = details
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


# Mount correlations API under /api/correlations/*
router.include_router(correlations_router)


