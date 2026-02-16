from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.db.models import Entity, Event, get_session_factory


def _clean_text(value: str) -> str:
    return " ".join(value.strip().split())


def _clean_id(value: str) -> str:
    return _clean_text(value).upper()


def _extract_usaspending_identity(raw_json: Any) -> Dict[str, Optional[str]]:
    if not isinstance(raw_json, dict):
        return {"name": None, "uei": None, "duns": None, "recipient_id": None}

    name = raw_json.get("Recipient Name") or raw_json.get("recipient_name") or raw_json.get("recipient")
    uei = raw_json.get("Recipient UEI") or raw_json.get("recipient_uei") or raw_json.get("uei")
    duns = raw_json.get("Recipient DUNS Number") or raw_json.get("recipient_duns") or raw_json.get("duns")
    recipient_id = raw_json.get("recipient_id") or raw_json.get("prime_award_recipient_id")

    out: Dict[str, Optional[str]] = {"name": None, "uei": None, "duns": None, "recipient_id": None}
    if isinstance(name, str) and name.strip():
        out["name"] = _clean_text(name)
    if isinstance(uei, str) and uei.strip():
        out["uei"] = _clean_id(uei)
    if isinstance(duns, str) and duns.strip():
        out["duns"] = _clean_id(duns)
    if isinstance(recipient_id, str) and recipient_id.strip():
        out["recipient_id"] = _clean_text(recipient_id)
    return out


def _get_or_create_entity(
    db: Session,
    *,
    name: str,
    uei: Optional[str] = None,
    meta: Optional[Dict[str, str]] = None,
) -> Tuple[Entity, bool]:
    ent: Optional[Entity] = None

    # Prefer UEI match (strongest id)
    if uei:
        ent = db.query(Entity).filter(Entity.uei == uei).order_by(Entity.id.asc()).first()

    # Fall back to case-insensitive name match
    if ent is None:
        key = name.lower()
        ent = db.query(Entity).filter(func.lower(Entity.name) == key).order_by(Entity.id.asc()).first()

    if ent is not None:
        changed = False

        if uei and not ent.uei:
            ent.uei = uei
            changed = True

        if meta:

            cur_sites = ent.sites_json if isinstance(ent.sites_json, dict) else {}

            merged_sites = dict(cur_sites)

            sites_changed = False


            for k, v in meta.items():

                if v and not merged_sites.get(k):

                    merged_sites[k] = v

                    sites_changed = True


            if sites_changed:

                # IMPORTANT: reassign so SQLAlchemy reliably persists JSON updates

                ent.sites_json = merged_sites

                changed = True        if changed:
            db.flush()

        return ent, False

    ent = Entity(name=name, uei=uei)
    if meta:
        ent.sites_json = dict(meta)
    db.add(ent)
    db.flush()
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
    - USAspending identity priority: UEI > DUNS > recipient_id > normalized name
    - idempotent: running again should do 0 updates once linked
    """
    since = datetime.now(timezone.utc) - timedelta(days=max(int(days), 1))
    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    scanned = 0
    linked = 0
    skipped_no_name = 0  # keep key name stable for CLI output
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

                ident = _extract_usaspending_identity(ev.raw_json)

                # Choose a display name (prefer actual recipient name)
                name = ident["name"]
                if not name:
                    if ident["uei"]:
                        name = f"UEI:{ident['uei']}"
                    elif ident["duns"]:
                        name = f"DUNS:{ident['duns']}"
                    elif ident["recipient_id"]:
                        name = f"RID:{ident['recipient_id']}"
                    else:
                        skipped_no_name += 1
                        continue

                meta: Dict[str, str] = {}
                if ident["duns"]:
                    meta["duns"] = ident["duns"]
                if ident["recipient_id"]:
                    meta["recipient_id"] = ident["recipient_id"]

                ent, created = _get_or_create_entity(db, name=name, uei=ident["uei"], meta=meta or None)
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