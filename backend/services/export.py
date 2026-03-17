"""Export utilities for ShadowScope datasets."""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from backend.db.models import get_session_factory
from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.query_surfaces import query_events


def export_events(
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
    *,
    limit: Optional[int] = None,
    offset: int = 0,
    source: Optional[str] = None,
    date_from: Any = None,
    date_to: Any = None,
    entity_id: Optional[int] = None,
    keyword: Optional[str] = None,
    agency: Optional[str] = None,
    psc: Optional[str] = None,
    naics: Optional[str] = None,
    award_id: Optional[str] = None,
    recipient_uei: Optional[str] = None,
    place_region: Optional[str] = None,
    sort_by: Optional[str] = "occurred_at",
    sort_dir: Optional[str] = "desc",
) -> Dict[str, Path]:
    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as session:
        if limit is None:
            probe = query_events(
                session,
                limit=1,
                offset=offset,
                source=source,
                date_from=date_from,
                date_to=date_to,
                entity_id=entity_id,
                keyword=keyword,
                agency=agency,
                psc=psc,
                naics=naics,
                award_id=award_id,
                recipient_uei=recipient_uei,
                place_region=place_region,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
            effective_limit = max(int(probe.get("total") or 0) - int(offset), 0)
        else:
            effective_limit = max(int(limit), 0)

        payload = query_events(
            session,
            limit=effective_limit,
            offset=offset,
            source=source,
            date_from=date_from,
            date_to=date_to,
            entity_id=entity_id,
            keyword=keyword,
            agency=agency,
            psc=psc,
            naics=naics,
            award_id=award_id,
            recipient_uei=recipient_uei,
            place_region=place_region,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
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

    rows = list(payload.get("items") or [])
    _write_csv(csv_path, rows)
    _write_jsonl(jsonl_path, rows)

    return {"csv": csv_path, "jsonl": jsonl_path, "count": len(rows)}



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
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


__all__ = ["export_events"]


