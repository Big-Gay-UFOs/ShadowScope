from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.exc import OperationalError

from backend.db.models import (
    AnalysisRun,
    Correlation,
    Entity,
    Event,
    IngestRun,
    LeadSnapshot,
    LeadSnapshotItem,
    get_engine,
    get_session_factory,
)


def doctor_status(
    *,
    days: int = 30,
    source: str | None = "USAspending",
    scan_limit: int = 5000,
    max_keywords_per_event: int = 10,
    database_url: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    window_days = max(int(days), 1)
    since = now - timedelta(days=window_days)

    try:
        engine = get_engine(database_url)
        safe_url = engine.url.render_as_string(hide_password=True)
    except Exception as e:
        return {
            "db": {"status": "error", "url": None, "error": str(e)},
            "window": {"days": window_days, "since": since.isoformat(), "source": source or "*"},
            "hints": ["Unable to create DB engine. Check DATABASE_URL / connectivity."],
        }

    SessionFactory = get_session_factory(database_url)

    def _count(db, stmt) -> int:
        return int(db.execute(stmt).scalar_one())

    hints: list[str] = []

    try:
        with SessionFactory() as db:
            # --- Totals ---
            events_total = _count(db, select(func.count()).select_from(Event))
            entities_total = _count(db, select(func.count()).select_from(Entity))
            correlations_total = _count(db, select(func.count()).select_from(Correlation))
            snapshots_total = _count(db, select(func.count()).select_from(LeadSnapshot))
            snapshot_items_total = _count(db, select(func.count()).select_from(LeadSnapshotItem))

            # --- Windowed events ---
            ev_where = [Event.created_at >= since]
            if source:
                ev_where.append(Event.source == source)

            events_window = _count(db, select(func.count()).select_from(Event).where(*ev_where))
            events_with_entity_window = _count(
                db,
                select(func.count()).select_from(Event).where(*ev_where, Event.entity_id.isnot(None)),
            )

            # --- Correlations by lane (use correlation_key prefixes; include both source and '*' forms) ---
            lane_prefixes = ["kw_pair", "same_keyword", "same_uei", "same_entity"]
            lane_counts: dict[str, int] = {}
            for lane in lane_prefixes:
                if source:
                    patterns = [f"{lane}|{source}|%", f"{lane}|*|%"]
                else:
                    patterns = [f"{lane}|%"]
                lane_counts[lane] = _count(
                    db,
                    select(func.count())
                    .select_from(Correlation)
                    .where(
                        Correlation.correlation_key.isnot(None),
                        or_(*[Correlation.correlation_key.like(p) for p in patterns]),
                    ),
                )

            # --- Sample recent events for keyword diagnostics ---
            q = select(Event.id, Event.keywords, Event.entity_id).where(*ev_where).order_by(Event.created_at.desc()).limit(int(scan_limit))
            rows = db.execute(q).all()

            scanned_events = len(rows)
            events_with_keywords = 0
            events_keywords_gt_max = 0
            kw_counter: Counter[str] = Counter()

            for _eid, keywords, _entity_id in rows:
                kws: list[str] = []
                if isinstance(keywords, list):
                    kws = [str(x) for x in keywords if x is not None and str(x).strip() != ""]
                if kws:
                    events_with_keywords += 1
                    kw_counter.update(kws)
                if len(kws) > int(max_keywords_per_event):
                    events_keywords_gt_max += 1

            unique_keywords = len(kw_counter)
            top_keywords = [{"keyword": k, "count": int(v)} for k, v in kw_counter.most_common(10)]
            coverage_pct = round((events_with_keywords / scanned_events) * 100.0, 1) if scanned_events else 0.0

            # --- Last runs ---
            ingest_q = select(IngestRun).order_by(IngestRun.id.desc())
            if source:
                ingest_q = ingest_q.where(IngestRun.source == source)
            ingest = db.execute(ingest_q.limit(1)).scalars().first()

            ont_q = (
                select(AnalysisRun)
                .where(AnalysisRun.analysis_type == "ontology_apply")
                .order_by(AnalysisRun.id.desc())
            )
            if source:
                ont_q = ont_q.where(AnalysisRun.source == source)
            ontology_apply = db.execute(ont_q.limit(1)).scalars().first()

            snap_q = select(LeadSnapshot).order_by(LeadSnapshot.id.desc())
            if source:
                snap_q = snap_q.where(LeadSnapshot.source == source)
            lead_snapshot = db.execute(snap_q.limit(1)).scalars().first()

            lead_snapshot_items = 0
            if lead_snapshot is not None:
                lead_snapshot_items = _count(
                    db,
                    select(func.count()).select_from(LeadSnapshotItem).where(LeadSnapshotItem.snapshot_id == lead_snapshot.id),
                )

    except OperationalError as e:
        return {
            "db": {"status": "error", "url": safe_url, "error": str(e)},
            "window": {"days": window_days, "since": since.isoformat(), "source": source or "*"},
            "hints": ["Database schema missing or inaccessible. Try: ss db init"],
        }

    # --- Hints / failure heuristics ---
    if events_total == 0:
        hints.append("No events in DB. Try: ss ingest usaspending --days 30 --pages 1")
    elif events_window == 0:
        hints.append(f"No events in last {window_days} days for source={source or '*'}; increase --days or run ingest.")

    if events_window > 0 and events_with_keywords == 0:
        hints.append("No keywords tagged on recent events. Try: ss ontology apply --path ontology.json --days 30 --source USAspending")

    if lane_counts.get("kw_pair", 0) == 0:
        if events_with_keywords == 0:
            hints.append("kw_pair correlations require keywords. Run ontology apply first, then rebuild keyword-pairs.")
        else:
            hints.append("No kw_pair correlations found. Try: ss correlate rebuild-keyword-pairs --window-days 30 --source USAspending --min-events 3")
            if scanned_events and (events_keywords_gt_max / scanned_events) >= 0.2:
                hints.append(
                    f"Many events have >{int(max_keywords_per_event)} keywords; pair explosion guard may suppress pairs. Consider raising --max-keywords-per-event or tightening ontology."
                )

    if events_window > 0 and events_with_entity_window == 0:
        hints.append("No entities linked on recent events. Try: ss entities link --source USAspending --days 30")

    if snapshots_total == 0:
        hints.append("No lead snapshots found. Try: ss leads snapshot --source USAspending --min-score 1 --limit 200")

    payload: dict[str, Any] = {
        "db": {"status": "ok", "url": safe_url},
        "window": {"days": window_days, "since": since.isoformat(), "source": source or "*"},
        "counts": {
            "events_total": int(events_total),
            "events_window": int(events_window),
            "events_with_entity_window": int(events_with_entity_window),
            "entities_total": int(entities_total),
            "correlations_total": int(correlations_total),
            "lead_snapshots_total": int(snapshots_total),
            "lead_snapshot_items_total": int(snapshot_items_total),
        },
        "keywords": {
            "scanned_events": int(scanned_events),
            "events_with_keywords": int(events_with_keywords),
            "coverage_pct": float(coverage_pct),
            "unique_keywords": int(unique_keywords),
            "top_keywords": top_keywords,
            "events_keywords_gt_max": int(events_keywords_gt_max),
            "max_keywords_per_event": int(max_keywords_per_event),
        },
        "correlations": {"by_lane": lane_counts},
        "last_runs": {
            "ingest": (
                {
                    "id": int(ingest.id),
                    "source": ingest.source,
                    "status": ingest.status,
                    "started_at": ingest.started_at.isoformat() if ingest.started_at else None,
                    "ended_at": ingest.ended_at.isoformat() if ingest.ended_at else None,
                    "fetched": int(ingest.fetched or 0),
                    "inserted": int(ingest.inserted or 0),
                    "normalized": int(ingest.normalized or 0),
                }
                if ingest
                else None
            ),
            "ontology_apply": (
                {
                    "id": int(ontology_apply.id),
                    "status": ontology_apply.status,
                    "source": ontology_apply.source,
                    "days": int(ontology_apply.days or 0),
                    "scanned": int(ontology_apply.scanned or 0),
                    "updated": int(ontology_apply.updated or 0),
                    "unchanged": int(ontology_apply.unchanged or 0),
                    "ended_at": ontology_apply.ended_at.isoformat() if ontology_apply.ended_at else None,
                }
                if ontology_apply
                else None
            ),
            "lead_snapshot": (
                {
                    "id": int(lead_snapshot.id),
                    "source": lead_snapshot.source,
                    "created_at": lead_snapshot.created_at.isoformat() if lead_snapshot.created_at else None,
                    "items": int(lead_snapshot_items),
                }
                if lead_snapshot
                else None
            ),
        },
        "hints": hints,
    }

    return payload
