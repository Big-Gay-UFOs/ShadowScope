from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.analysis.scoring import score_from_keywords_clauses
from backend.db.models import AnalysisRun, Event, LeadSnapshot, LeadSnapshotItem, get_session_factory


def _norm_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, dict):
        return []
    if isinstance(value, list):
        return value
    return []


def compute_leads(
    db: Session,
    *,
    scan_limit: int = 5000,
    limit: int = 200,
    min_score: int = 1,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
) -> Tuple[List[Tuple[int, Event, Dict[str, Any]]], int]:
    rows = db.execute(select(Event).order_by(Event.id.desc()).limit(int(scan_limit))).scalars().all()
    scanned = len(rows)

    scored: List[Tuple[int, Event, Dict[str, Any]]] = []
    for e in rows:
        if source and e.source != source:
            continue
        if exclude_source and e.source == exclude_source:
            continue

        score, details = score_from_keywords_clauses(e.keywords, e.clauses, has_entity=bool(e.entity_id))
        if score >= int(min_score):
            scored.append((int(score), e, details))

    scored.sort(key=lambda t: (t[0], t[1].id), reverse=True)
    return scored[: int(limit)], scanned


def create_lead_snapshot(
    *,
    analysis_run_id: Optional[int] = None,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    min_score: int = 1,
    limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = "v1",
    notes: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    try:
        if analysis_run_id is not None:
            ok = db.execute(select(AnalysisRun.id).where(AnalysisRun.id == analysis_run_id)).scalar_one_or_none()
            if ok is None:
                raise ValueError(
                    f"analysis_run_id {analysis_run_id} not found in analysis_runs. "
                    f"Run 'ss ontology apply ...' and use the printed analysis_run_id, or omit --analysis-run-id."
                )

        ranked, scanned = compute_leads(
            db,
            scan_limit=scan_limit,
            limit=limit,
            min_score=min_score,
            source=source,
            exclude_source=exclude_source,
        )

        snap = LeadSnapshot(
            analysis_run_id=analysis_run_id,
            source=source,
            min_score=int(min_score),
            limit=int(limit),
            scoring_version=str(scoring_version),
            notes=notes,
        )
        db.add(snap)
        db.commit()
        db.refresh(snap)

        inserted = 0
        for idx, (score, e, details) in enumerate(ranked, start=1):
            item = LeadSnapshotItem(
                snapshot_id=snap.id,
                event_id=e.id,
                event_hash=e.hash,
                rank=idx,
                score=int(score),
                score_details=details,
            )
            db.add(item)
            inserted += 1

        db.commit()

        return {
            "status": "ok",
            "snapshot_id": snap.id,
            "analysis_run_id": analysis_run_id,
            "source": source,
            "exclude_source": exclude_source,
            "min_score": int(min_score),
            "limit": int(limit),
            "scan_limit": int(scan_limit),
            "scoring_version": str(scoring_version),
            "scanned": int(scanned),
            "items": int(inserted),
        }
    finally:
        db.close()