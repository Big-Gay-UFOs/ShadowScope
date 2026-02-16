from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.db.models import Correlation, CorrelationLink, Entity, Event, get_session_factory


def not_implemented() -> None:
    raise NotImplementedError("Correlation engine placeholder was replaced by rebuild_entity_correlations().")


def rebuild_entity_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "USAspending",
    min_events: int = 2,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Rebuild correlations using the simplest lane:
      - same entity_id within the last `window_days`

    Derived-table style:
      - non-dry-run: deletes all correlations + links, then rebuilds
      - dry-run: computes counts only
    """
    window_days = int(window_days)
    min_events = int(min_events)
    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if min_events < 2:
        raise ValueError("min_events must be >= 2")

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        groups_q = (
            db.query(
                Event.entity_id,
                func.count(Event.id),
                func.min(ts),
                func.max(ts),
            )
            .filter(Event.entity_id.isnot(None))
            .filter(ts >= since)
        )
        if source:
            groups_q = groups_q.filter(Event.source == source)

        groups = groups_q.group_by(Event.entity_id).all()
        eligible = [(eid, int(cnt), tmin, tmax) for (eid, cnt, tmin, tmax) in groups if int(cnt) >= min_events]

        deleted_correlations = 0
        deleted_links = 0
        if not dry_run:
            deleted_links = db.query(CorrelationLink).delete(synchronize_session=False)
            deleted_correlations = db.query(Correlation).delete(synchronize_session=False)
            db.commit()

        correlations_created = 0
        links_created = 0
        entities_seen = len(groups)

        for (entity_id, cnt, tmin, tmax) in eligible:
            ent = db.get(Entity, int(entity_id))
            ent_name = ent.name if ent else f"entity_id={entity_id}"
            ent_uei = ent.uei if ent else None

            lanes_hit = {
                "lane": "same_entity",
                "entity_id": int(entity_id),
                "uei": ent_uei,
                "event_count": cnt,
                "since": since.isoformat(),
                "until": now.isoformat(),
            }

            corr = Correlation(
                score=str(cnt),  # score column is varchar in current schema
                window_days=window_days,
                radius_km=float(radius_km),
                lanes_hit=lanes_hit,
                summary=f"{cnt} events share entity {ent_name}",
                rationale=f"Grouped events with entity_id={entity_id} within last {window_days} days (min_events={min_events}).",
                created_at=now,
            )

            if dry_run:
                correlations_created += 1
                links_created += cnt
                continue

            db.add(corr)
            db.flush()

            ids_q = db.query(Event.id).filter(Event.entity_id == int(entity_id)).filter(ts >= since)
            if source:
                ids_q = ids_q.filter(Event.source == source)
            event_ids = [r[0] for r in ids_q.all()]

            for ev_id in event_ids:
                db.add(CorrelationLink(correlation_id=int(corr.id), event_id=int(ev_id)))

            correlations_created += 1
            links_created += len(event_ids)

        if not dry_run:
            db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "window_days": window_days,
            "min_events": min_events,
            "since": since.isoformat(),
            "entities_seen": entities_seen,
            "eligible_entities": len(eligible),
            "deleted_correlations": deleted_correlations,
            "deleted_links": deleted_links,
            "correlations_created": correlations_created,
            "links_created": links_created,
        }
    finally:
        db.close()