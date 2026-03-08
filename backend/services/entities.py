from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import String, cast, func, or_
from sqlalchemy.orm import Session

from backend.db.models import Entity, Event, get_session_factory


def _clean_text(value: str) -> str:
    return " ".join(value.strip().split())


def _clean_id(value: str) -> str:
    return _clean_text(value).upper()


def _as_clean_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    v = _clean_text(value)
    return v if v else None


def _as_clean_optional_id(value: Any) -> Optional[str]:
    v = _as_clean_optional_text(value)
    return _clean_id(v) if v else None


def _extract_usaspending_identity(raw_json: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "name": None,
        "uei": None,
        "duns": None,
        "cage": None,
        "recipient_id": None,
        "meta": {},
    }
    if not isinstance(raw_json, dict):
        return out

    name = (
        raw_json.get("Recipient Name")
        or raw_json.get("recipient_name")
        or raw_json.get("recipient")
        or raw_json.get("recipientName")
    )
    uei = (
        raw_json.get("Recipient UEI")
        or raw_json.get("recipient_uei")
        or raw_json.get("uei")
        or raw_json.get("UEI")
    )
    duns = (
        raw_json.get("Recipient DUNS Number")
        or raw_json.get("recipient_duns")
        or raw_json.get("duns")
        or raw_json.get("DUNS")
    )
    cage = (
        raw_json.get("Recipient CAGE Code")
        or raw_json.get("Recipient CAGE")
        or raw_json.get("recipient_cage")
        or raw_json.get("cage_code")
        or raw_json.get("cage")
        or raw_json.get("CAGE")
    )
    recipient_id = raw_json.get("recipient_id") or raw_json.get("prime_award_recipient_id")

    out["name"] = _as_clean_optional_text(name)
    out["uei"] = _as_clean_optional_id(uei)
    out["duns"] = _as_clean_optional_id(duns)
    out["cage"] = _as_clean_optional_id(cage)
    out["recipient_id"] = _as_clean_optional_text(recipient_id)

    if out["recipient_id"]:
        out["meta"]["recipient_id"] = out["recipient_id"]
    if out["duns"]:
        out["meta"]["duns"] = out["duns"]
    if out["cage"]:
        out["meta"]["cage"] = out["cage"]

    return out


def _extract_samgov_identity(raw_json: Any) -> Dict[str, Any]:
    """
    Best-effort SAM.gov identity extraction.

    Priority:
    1) recipient/awardee identity if present
    2) agency/office identity via fullParentPathName/fullParentPathCode
    """
    out = _extract_usaspending_identity(raw_json)
    if not isinstance(raw_json, dict):
        return out

    parent_name = (
        raw_json.get("fullParentPathName")
        or raw_json.get("parentPathName")
        or raw_json.get("organization")
    )
    parent_code = (
        raw_json.get("fullParentPathCode")
        or raw_json.get("parentPathCode")
        or raw_json.get("organizationCode")
    )
    org_type = raw_json.get("organizationType")

    parent_name_clean = _as_clean_optional_text(parent_name)
    parent_code_clean = _as_clean_optional_id(parent_code)
    org_type_clean = _as_clean_optional_text(org_type)

    if not out["name"] and parent_name_clean:
        out["name"] = parent_name_clean

    if parent_code_clean:
        if not out["recipient_id"]:
            out["recipient_id"] = parent_code_clean
            out["meta"].setdefault("recipient_id", parent_code_clean)
        out["meta"]["sam_parent_path_code"] = parent_code_clean

    if parent_name_clean:
        out["meta"]["sam_parent_path_name"] = parent_name_clean
    if org_type_clean:
        out["meta"]["sam_organization_type"] = org_type_clean

    return out


def _find_entity_by_sites_value(db: Session, key: str, value: str) -> Optional[Entity]:
    sites_text = cast(Entity.sites_json, String)
    p1 = f'%"{key}":"{value}"%'
    p2 = f'%"{key}": "{value}"%'
    return (
        db.query(Entity)
        .filter(or_(sites_text.like(p1), sites_text.like(p2)))
        .order_by(Entity.id.asc())
        .first()
    )


def _get_or_create_entity(
    db: Session,
    *,
    name: str,
    uei: Optional[str] = None,
    duns: Optional[str] = None,
    cage: Optional[str] = None,
    entity_type: Optional[str] = None,
    meta: Optional[Dict[str, str]] = None,
) -> Tuple[Entity, bool]:
    ent: Optional[Entity] = None
    meta = dict(meta or {})

    if uei:
        ent = db.query(Entity).filter(Entity.uei == uei).order_by(Entity.id.asc()).first()

    if ent is None and duns:
        ent = _find_entity_by_sites_value(db, "duns", duns)

    if ent is None and cage:
        ent = db.query(Entity).filter(Entity.cage == cage).order_by(Entity.id.asc()).first()

    if ent is None:
        for meta_key in ("recipient_id", "sam_parent_path_code"):
            meta_value = meta.get(meta_key)
            if meta_value:
                ent = _find_entity_by_sites_value(db, meta_key, meta_value)
                if ent is not None:
                    break

    if ent is None:
        ent = (
            db.query(Entity)
            .filter(func.lower(Entity.name) == name.lower())
            .order_by(Entity.id.asc())
            .first()
        )

    if ent is not None:
        changed = False

        if uei and not ent.uei:
            ent.uei = uei
            changed = True
        if cage and not ent.cage:
            ent.cage = cage
            changed = True
        if entity_type and not ent.type:
            ent.type = entity_type
            changed = True

        if meta:
            cur_sites = ent.sites_json if isinstance(ent.sites_json, dict) else {}
            merged = dict(cur_sites)
            sites_changed = False
            for k, v in meta.items():
                if v and not merged.get(k):
                    merged[k] = v
                    sites_changed = True
            if sites_changed:
                ent.sites_json = merged
                changed = True

        if changed:
            db.flush()
        return ent, False

    ent = Entity(name=name, uei=uei, cage=cage, type=entity_type)
    if meta:
        ent.sites_json = dict(meta)
    db.add(ent)
    db.flush()
    return ent, True


def _extract_identity(raw_json: Any, source: str) -> Dict[str, Any]:
    if source == "SAM.gov":
        return _extract_samgov_identity(raw_json)
    return _extract_usaspending_identity(raw_json)


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
    - source-aware identity extraction
    - idempotent once linked
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

                ident = _extract_identity(ev.raw_json, source)
                display_name = ident["name"]

                if not display_name:
                    if ident["uei"]:
                        display_name = f"UEI:{ident['uei']}"
                    elif ident["cage"]:
                        display_name = f"CAGE:{ident['cage']}"
                    elif ident["duns"]:
                        display_name = f"DUNS:{ident['duns']}"
                    elif ident["recipient_id"]:
                        prefix = "SAM" if source == "SAM.gov" else "RID"
                        display_name = f"{prefix}:{ident['recipient_id']}"
                    else:
                        skipped_no_name += 1
                        continue

                meta: Dict[str, str] = dict(ident.get("meta") or {})
                if ident["recipient_id"]:
                    meta.setdefault("recipient_id", ident["recipient_id"])
                if ident["duns"]:
                    meta.setdefault("duns", ident["duns"])
                if ident["cage"]:
                    meta.setdefault("cage", ident["cage"])

                ent, created = _get_or_create_entity(
                    db,
                    name=display_name,
                    uei=ident["uei"],
                    duns=ident["duns"],
                    cage=ident["cage"],
                    entity_type=meta.get("sam_organization_type"),
                    meta=meta or None,
                )
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
