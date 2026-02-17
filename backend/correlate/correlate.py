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
    Idempotent rebuild (same-entity lane) using correlation_key:
      key format: same_entity|<source>|<window_days>|entity:<entity_id>

    - Updates existing correlations if key exists
    - Creates new ones if missing
    - Deletes stale ones for this lane/window/source
    """
    window_days = int(window_days)
    min_events = int(min_events)
    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if min_events < 2:
        raise ValueError("min_events must be >= 2")

    lane = "same_entity"
    src_key = source if source else "*"
    key_prefix = f"{lane}|{src_key}|{window_days}|entity:"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        groups_q = (
            db.query(Event.entity_id, func.count(Event.id))
            .filter(Event.entity_id.isnot(None))
            .filter(ts >= since)
        )
        if source:
            groups_q = groups_q.filter(Event.source == source)
        groups = groups_q.group_by(Event.entity_id).all()

        eligible = [(int(eid), int(cnt)) for (eid, cnt) in groups if int(cnt) >= min_events]
        eligible_keys = set(f"{key_prefix}{eid}" for (eid, _cnt) in eligible)

        # Load existing correlations for this lane/window/source
        existing = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like(f"{key_prefix}%"))
            .all()
        )
        existing_by_key = {c.correlation_key: c for c in existing if c.correlation_key}

        correlations_created = 0
        correlations_updated = 0
        correlations_deleted = 0
        links_created = 0
        links_deleted = 0

        for (entity_id, cnt) in eligible:
            key = f"{key_prefix}{entity_id}"

            ent = db.get(Entity, int(entity_id))
            ent_name = ent.name if ent else f"entity_id={entity_id}"
            ent_uei = ent.uei if ent else None

            lanes_hit = {
                "lane": lane,
                "entity_id": int(entity_id),
                "uei": ent_uei,
                "event_count": cnt,
                "since": since.isoformat(),
                "until": now.isoformat(),
            }

            if dry_run:
                continue

            c = existing_by_key.get(key)
            if c is None:
                c = Correlation(
                    correlation_key=key,
                    score=str(cnt),
                    window_days=window_days,
                    radius_km=float(radius_km),
                    lanes_hit=lanes_hit,
                    summary=f"{cnt} events share entity {ent_name}",
                    rationale=f"Grouped events with entity_id={entity_id} within last {window_days} days (min_events={min_events}).",
                    created_at=now,
                )
                db.add(c)
                db.flush()
                correlations_created += 1
            else:
                c.score = str(cnt)
                c.window_days = window_days
                c.radius_km = float(radius_km)
                c.lanes_hit = lanes_hit
                c.summary = f"{cnt} events share entity {ent_name}"
                c.rationale = f"Grouped events with entity_id={entity_id} within last {window_days} days (min_events={min_events})."
                c.created_at = now
                correlations_updated += 1

            # rebuild links for this correlation
            links_deleted += db.query(CorrelationLink).filter(CorrelationLink.correlation_id == int(c.id)).delete(synchronize_session=False)

            ids_q = db.query(Event.id).filter(Event.entity_id == int(entity_id)).filter(ts >= since)
            if source:
                ids_q = ids_q.filter(Event.source == source)
            event_ids = [r[0] for r in ids_q.all()]

            for ev_id in event_ids:
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(ev_id)))
            links_created += len(event_ids)

        # Delete stale correlations for this lane/window/source
        if not dry_run:
            stale_keys = [k for k in existing_by_key.keys() if k not in eligible_keys]
            if stale_keys:
                correlations_deleted = db.query(Correlation).filter(Correlation.correlation_key.in_(stale_keys)).delete(synchronize_session=False)

        if not dry_run:
            db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "window_days": window_days,
            "min_events": min_events,
            "since": since.isoformat(),
            "entities_seen": len(groups),
            "eligible_entities": len(eligible),
            "correlations_created": correlations_created,
            "correlations_updated": correlations_updated,
            "correlations_deleted": correlations_deleted,
            "links_created": links_created,
            "links_deleted": links_deleted,
        }
    finally:
        db.close()