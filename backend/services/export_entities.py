from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select

from backend.db.models import Entity, Event, get_session_factory
from backend.runtime import EXPORTS_DIR, ensure_runtime_directories


def _raw_get(raw: Any, keys: list[str]) -> str | None:
    if not isinstance(raw, dict):
        return None
    for k in keys:
        v = raw.get(k)
        if v is not None and str(v).strip() != "":
            return str(v)

    lower_map = {str(k).lower(): k for k in raw.keys()}
    for k in keys:
        lk = str(k).lower()
        if lk in lower_map:
            v = raw.get(lower_map[lk])
            if v is not None and str(v).strip() != "":
                return str(v)
    return None


def export_entities_bundle(
    *,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> dict[str, Any]:
    """
    Export:
      1) entities list
      2) event -> entity mapping (includes recipient identifiers from raw_json when present)

    output:
      - None: uses data/exports/
      - directory path: writes files into that directory
      - file path with suffix: uses that stem as base name (writes multiple files with that base)
    """
    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)

    with SessionFactory() as db:
        entities = db.execute(select(Entity).order_by(Entity.id.asc())).scalars().all()
        events = (
            db.execute(
                select(Event)
                .where(Event.entity_id.isnot(None))
                .order_by(Event.id.asc())
            )
            .scalars()
            .all()
        )

    entities_by_id: dict[int, Entity] = {int(e.id): e for e in entities}

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = f"entities_{ts}"
    export_dir = EXPORTS_DIR

    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent if output.parent else Path(".")
            export_dir.mkdir(parents=True, exist_ok=True)
            base = output.stem or base
        else:
            export_dir = output
            export_dir.mkdir(parents=True, exist_ok=True)

    entities_csv = export_dir / f"{base}.csv"
    entities_json = export_dir / f"{base}.json"
    mapping_csv = export_dir / f"event_{base}.csv"
    mapping_json = export_dir / f"event_{base}.json"

    entity_rows: list[dict[str, Any]] = []
    for ent in entities:
        entity_rows.append(
            {
                "entity_id": int(ent.id),
                "name": ent.name,
                "uei": ent.uei,
                "cage": ent.cage,
                "parent": ent.parent,
                "type": ent.type,
                "sponsor": ent.sponsor,
                "created_at": ent.created_at.isoformat() if ent.created_at else None,
                "sites_json": json.dumps(ent.sites_json or {}, ensure_ascii=False),
            }
        )

    mapping_rows: list[dict[str, Any]] = []
    for ev in events:
        ent = entities_by_id.get(int(ev.entity_id)) if ev.entity_id is not None else None
        raw = ev.raw_json if isinstance(ev.raw_json, dict) else {}
        mapping_rows.append(
            {
                "event_id": int(ev.id),
                "event_hash": ev.hash,
                "source": ev.source,
                "doc_id": ev.doc_id,
                "occurred_at": ev.occurred_at.isoformat() if ev.occurred_at else None,
                "created_at": ev.created_at.isoformat() if ev.created_at else None,
                "entity_id": int(ev.entity_id) if ev.entity_id is not None else None,
                "entity_name": None if ent is None else ent.name,
                "entity_uei": None if ent is None else ent.uei,
                "entity_cage": None if ent is None else ent.cage,
                "recipient_name": _raw_get(raw, ["Recipient Name", "recipient_name", "recipient"]),
                "recipient_uei": _raw_get(raw, ["UEI", "uei", "recipient_uei"]),
                "recipient_duns": _raw_get(raw, ["DUNS", "duns", "recipient_duns"]),
                "recipient_cage": _raw_get(raw, ["CAGE", "cage", "recipient_cage"]),
            }
        )

    _write_csv(entities_csv, entity_rows)
    _write_csv(mapping_csv, mapping_rows)

    entities_payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "count": len(entity_rows),
        "items": entity_rows,
    }
    mapping_payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "count": len(mapping_rows),
        "items": mapping_rows,
    }

    entities_json.write_text(json.dumps(entities_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    mapping_json.write_text(json.dumps(mapping_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "entities_csv": entities_csv,
        "entities_json": entities_json,
        "event_entities_csv": mapping_csv,
        "event_entities_json": mapping_json,
        "entities_count": len(entity_rows),
        "event_entities_count": len(mapping_rows),
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


__all__ = ["export_entities_bundle"]
