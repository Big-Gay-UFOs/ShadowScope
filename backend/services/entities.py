from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.db.models import Entity, Event, get_session_factory


def _clean_name(name: str) -> str:
    # canonicalize whitespace + case for matching
    return " ".join(name.strip().split())


def _extract_usaspending_recipient(raw_json: Any) -> Optional[str]:
    # USAspending connector currently requests "Recipient Name" in fields
    if not isinstance(raw_json, dict):
        return None
    v = raw_json.get("Recipient Name") or raw_json.get("recipient_name") or raw_json.get("recipient")
    if not isinstance(v, str) or not v.strip():
        return None
    return _clean_name(v)


def _get_or_create_entity(db: Session, *, name: str) -> Tuple[Entity, bool]:
    # case-insensitive match on name; if duplicates exist, take the first
    key = name.lower()
    ent = db.query(Entity).filter(func.lower(Entity.name) == key).order_by(Entity.id.asc()).first()
    if ent is not None:
        return ent, False

    ent = Entity(name=name)
    db.add(ent)
    db.flush()  # allocate ent.id without committing yet
    return ent, True


def link_entities_from_events(
    *,
    source: str = "USAspending",
    days: int = 30,
    batch: int = 500,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Link events -> entities for a given source.
    - only processes events where entity_id is NULL
    - derives entity name from raw_json (USAspending: "Recipient Name")
    - idempotent: running again should do 0 updates once linked
    """
    since = datetime.now(timezone.utc) - timedelta(days=max(int(days), 1))

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    scanned = 0
    linked = 0
    skipped_no_name = 0
    entities_created = 0
    last_id = 0

    try:
        while True:
            rows = (
                db.query(Event)
                .filter(Event.id > last_id)
                .filter(Event.source == source)
                .filter(Event.entity_id == None)  # noqa: E711
                .filter(Event.created_at >= since)
                .order_by(Event.id.asc())
                .limit(int(batch))
                .all()
            )
            if not rows:
                break

            for ev in rows:
                scanned += 1
                last_id = int(ev.id)

                name = _extract_usaspending_recipient(ev.raw_json)
                if not name:
                    skipped_no_name += 1
                    continue

                ent, created = _get_or_create_entity(db, name=name)
                if created:
                    entities_created += 1

                linked += 1
                if not dry_run:
                    ev.entity_id = ent.id

            if not dry_run:
                db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "days": int(days),
            "since": since.isoformat(),
            "scanned": scanned,
            "linked": linked,
            "skipped_no_name": skipped_no_name,
            "entities_created": entities_created,
        }
    finally:
        db.close()