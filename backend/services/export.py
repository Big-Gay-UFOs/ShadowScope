"""Export utilities for ShadowScope datasets."""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from backend.db.models import Event, get_session_factory
from backend.runtime import EXPORTS_DIR, ensure_runtime_directories


def export_events(
    database_url: Optional[str] = None, output: Optional[Path] = None
) -> Dict[str, Path]:
    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as session:
        events = (
            session.query(Event)
            .order_by(Event.occurred_at.desc().nullslast(), Event.id.desc())
            .all()
        )

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent or Path(".")
            export_dir.mkdir(parents=True, exist_ok=True)
            base_name = output.stem or f"events_{timestamp}"
            csv_path = export_dir / f"{base_name}.csv"
        else:
            export_dir = output
            export_dir.mkdir(parents=True, exist_ok=True)
            base_name = f"events_{timestamp}"
            csv_path = export_dir / f"{base_name}.csv"
    else:
        export_dir = EXPORTS_DIR
        export_dir.mkdir(parents=True, exist_ok=True)
        base_name = f"events_{timestamp}"
        csv_path = export_dir / f"{base_name}.csv"
    jsonl_path = csv_path.with_suffix(".jsonl")

    rows = [_serialize_event(event) for event in events]
    _write_csv(csv_path, rows)
    _write_jsonl(jsonl_path, rows)

    return {"csv": csv_path, "jsonl": jsonl_path, "count": len(rows)}


def _serialize_event(event: Event) -> Dict[str, object]:
    return {
        "id": event.id,
        "entity_id": event.entity_id,
        "category": event.category,
        "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
        "lat": event.lat,
        "lon": event.lon,
        "source": event.source,
        "source_url": event.source_url,
        "doc_id": event.doc_id,
        "keywords": json.dumps(event.keywords or []),
        "clauses": json.dumps(event.clauses or []),
        "place_text": event.place_text,
        "snippet": event.snippet,
        "raw_json": json.dumps(event.raw_json or {}),
        "hash": event.hash,
        "created_at": event.created_at.isoformat() if event.created_at else None,
    }


def _write_csv(path: Path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows):
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


__all__ = ["export_events"]
