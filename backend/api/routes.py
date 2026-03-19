from typing import Any, Optional
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select, or_, cast, String
from sqlalchemy.orm import Session

from backend.api.deps import get_db_session
from backend.api.correlations import router as correlations_router
from backend.services.leads import DEFAULT_SCORING_VERSION, SUPPORTED_SCORING_VERSIONS, compute_leads, normalize_scoring_version
from backend.db.models import AnalysisRun, Entity, Event, LeadSnapshot, LeadSnapshotItem
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
def list_events(
    limit: int = 50,
    source: str | None = None,
    exclude_source: str | None = None,
    days: int | None = None,
    entity_id: int | None = None,
    keyword: str | None = None,
    has_entity: bool | None = None,
    award_id: str | None = None,
    contract_id: str | None = None,
    document_id: str | None = None,
    notice_id: str | None = None,
    solicitation_number: str | None = None,
    recipient_uei: str | None = None,
    agency_code: str | None = None,
    psc: str | None = None,
    naics: str | None = None,
    notice_award_type: str | None = None,
    place_state: str | None = None,
    place_country: str | None = None,
    db: Session = Depends(get_db_session),
):
    q = select(Event)

    if source:
        q = q.where(Event.source == source)
    if exclude_source:
        q = q.where(Event.source != exclude_source)
    if entity_id is not None:
        q = q.where(Event.entity_id == int(entity_id))
    if has_entity is True:
        q = q.where(Event.entity_id != None)  # noqa: E711
    elif has_entity is False:
        q = q.where(Event.entity_id == None)  # noqa: E711

    if days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=max(int(days), 1))
        q = q.where(or_(Event.created_at >= since, Event.occurred_at >= since))

    if keyword:
        kw = str(keyword).strip()
        if kw:
            kw_esc = kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            q = q.where(cast(Event.keywords, String).like(f'%"{kw_esc}"%', escape="\\"))

    if award_id:
        award = str(award_id).strip()
        if award:
            q = q.where(or_(Event.award_id == award, Event.generated_unique_award_id == award))

    if contract_id:
        cid = str(contract_id).strip()
        if cid:
            q = q.where(or_(Event.piid == cid, Event.fain == cid, Event.uri == cid))

    if document_id:
        doc = str(document_id).strip()
        if doc:
            q = q.where(or_(Event.document_id == doc, Event.doc_id == doc))

    if notice_id:
        n = str(notice_id).strip()
        if n:
            q = q.where(Event.notice_id == n)

    if solicitation_number:
        s = str(solicitation_number).strip()
        if s:
            q = q.where(Event.solicitation_number == s)

    if recipient_uei:
        uei = str(recipient_uei).strip().upper()
        if uei:
            q = q.where(func.upper(Event.recipient_uei) == uei)

    if agency_code:
        ac = str(agency_code).strip().upper()
        if ac:
            q = q.where(
                or_(
                    func.upper(Event.awarding_agency_code) == ac,
                    func.upper(Event.funding_agency_code) == ac,
                    func.upper(Event.contracting_office_code) == ac,
                )
            )

    if psc:
        psc_code = str(psc).strip().upper()
        if psc_code:
            q = q.where(func.upper(Event.psc_code) == psc_code)

    if naics:
        naics_code = str(naics).strip().upper()
        if naics_code:
            q = q.where(func.upper(Event.naics_code) == naics_code)

    if notice_award_type:
        ntype = str(notice_award_type).strip().lower()
        if ntype:
            q = q.where(func.lower(Event.notice_award_type) == ntype)

    if place_state:
        st = str(place_state).strip().upper()
        if st:
            q = q.where(func.upper(Event.place_of_performance_state) == st)

    if place_country:
        ctry = str(place_country).strip().upper()
        if ctry:
            q = q.where(func.upper(Event.place_of_performance_country) == ctry)

    rows = db.execute(q.order_by(Event.id.desc()).limit(int(limit))).scalars().all()
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
            "award_id": e.award_id,
            "generated_unique_award_id": e.generated_unique_award_id,
            "piid": e.piid,
            "fain": e.fain,
            "uri": e.uri,
            "transaction_id": e.transaction_id,
            "modification_number": e.modification_number,
            "source_record_id": e.source_record_id,
            "recipient_name": e.recipient_name,
            "recipient_uei": e.recipient_uei,
            "recipient_parent_uei": e.recipient_parent_uei,
            "recipient_duns": e.recipient_duns,
            "recipient_cage_code": e.recipient_cage_code,
            "awarding_agency_code": e.awarding_agency_code,
            "awarding_agency_name": e.awarding_agency_name,
            "funding_agency_code": e.funding_agency_code,
            "funding_agency_name": e.funding_agency_name,
            "contracting_office_code": e.contracting_office_code,
            "contracting_office_name": e.contracting_office_name,
            "psc_code": e.psc_code,
            "psc_description": e.psc_description,
            "naics_code": e.naics_code,
            "naics_description": e.naics_description,
            "notice_award_type": e.notice_award_type,
            "place_of_performance_city": e.place_of_performance_city,
            "place_of_performance_state": e.place_of_performance_state,
            "place_of_performance_country": e.place_of_performance_country,
            "place_of_performance_zip": e.place_of_performance_zip,
            "solicitation_number": e.solicitation_number,
            "notice_id": e.notice_id,
            "document_id": e.document_id,
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
    scan_limit: int = 5000,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    source: str | None = None,
    exclude_source: str | None = None,
    include_details: bool = True,
    db: Session = Depends(get_db_session),
):
    # Defensive bounds to prevent request-driven full table scans
    try:
        limit_i = int(limit)
        scan_i = int(scan_limit)
        min_i = int(min_score)
    except Exception:
        raise HTTPException(status_code=400, detail='limit, scan_limit, and min_score must be integers')

    if limit_i < 1 or limit_i > 200:
        raise HTTPException(status_code=400, detail='limit must be between 1 and 200')
    if scan_i < 1 or scan_i > 5000:
        raise HTTPException(status_code=400, detail='scan_limit must be between 1 and 5000')
    if scan_i < limit_i:
        scan_i = limit_i
    if min_i < 0:
        min_i = 0

    try:
        sv = normalize_scoring_version(scoring_version)
    except ValueError:
        allowed = " or ".join(SUPPORTED_SCORING_VERSIONS)
        raise HTTPException(status_code=400, detail=f"scoring_version must be {allowed}")

    limit = limit_i
    scan_limit = scan_i
    min_score = min_i
    scoring_version = sv

    ranked, _scanned = compute_leads(
        db,
        scan_limit=scan_limit,
        limit=limit,
        min_score=min_score,
        source=source,
        exclude_source=exclude_source,
        scoring_version=scoring_version,
    )
    out = []
    for sc, e, details in ranked:
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


# Mount correlations API under /api/correlations/*
router.include_router(correlations_router)
