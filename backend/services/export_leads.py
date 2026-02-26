"""Export utilities for lead snapshots and lead deltas."""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import select

from backend.db.models import Event, LeadSnapshot, LeadSnapshotItem, get_session_factory
from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.deltas import compute_lead_deltas


def export_lead_snapshot(
    *,
    snapshot_id: int,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Export a lead snapshot (lead_snapshots + lead_snapshot_items + event metadata)
    to CSV + JSON.

    output:
      - None: uses data/exports/
      - directory path: writes lead_snapshot_<id>_<ts>.csv/.json into that directory
      - file path with suffix: uses that stem as base name and writes both .csv and .json
    """
    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)

    with SessionFactory() as db:
        snap = db.execute(
            select(LeadSnapshot).where(LeadSnapshot.id == int(snapshot_id))
        ).scalar_one_or_none()
        if snap is None:
            raise ValueError(f"lead_snapshot {snapshot_id} not found")

        items = (
            db.execute(
                select(LeadSnapshotItem)
                .where(LeadSnapshotItem.snapshot_id == int(snapshot_id))
                .order_by(LeadSnapshotItem.rank.asc())
            )
            .scalars()
            .all()
        )

        event_ids = [int(i.event_id) for i in items]
        events_by_id: dict[int, Event] = {}
        if event_ids:
            rows = (
                db.execute(select(Event).where(Event.id.in_(event_ids)))
                .scalars()
                .all()
            )
            events_by_id = {int(e.id): e for e in rows}

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_name = f"lead_snapshot_{int(snapshot_id)}_{ts}"
    export_dir = EXPORTS_DIR

    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent if output.parent else Path(".")
            export_dir.mkdir(parents=True, exist_ok=True)
            base_name = output.stem or base_name
        else:
            export_dir = output
            export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / f"{base_name}.csv"
    json_path = export_dir / f"{base_name}.json"

    rows_out = []
    for it in items:
        e = events_by_id.get(int(it.event_id))
        rows_out.append(
            {
                "snapshot_id": int(snapshot_id),
                "rank": int(it.rank),
                "score": int(it.score),
                "event_id": int(it.event_id),
                "event_hash": it.event_hash,
                "source": None if e is None else e.source,
                "doc_id": None if e is None else e.doc_id,
                "source_url": None if e is None else e.source_url,
                "occurred_at": None
                if (e is None or e.occurred_at is None)
                else e.occurred_at.isoformat(),
                "created_at": None
                if (e is None or e.created_at is None)
                else e.created_at.isoformat(),
                "entity_id": None if e is None else e.entity_id,
                "snippet": None if e is None else (e.snippet or ""),
                "place_text": None if e is None else (e.place_text or ""),
                "score_details_json": json.dumps(it.score_details or {}, ensure_ascii=False),
            }
        )

    _write_csv(csv_path, rows_out)

    max_items = getattr(snap, "max_items", None)
    if max_items is None:
        max_items = getattr(snap, "limit", 0)

    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "snapshot": {
            "id": int(snapshot_id),
            "created_at": snap.created_at.isoformat() if snap.created_at else None,
            "analysis_run_id": getattr(snap, "analysis_run_id", None),
            "source": getattr(snap, "source", None),
            "min_score": int(getattr(snap, "min_score", 0)),
            "max_items": int(max_items or 0),
            "scoring_version": getattr(snap, "scoring_version", None),
            "notes": getattr(snap, "notes", None),
        },
        "count": len(rows_out),
        "items": rows_out,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"csv": csv_path, "json": json_path, "count": len(rows_out), "snapshot_id": int(snapshot_id)}


def export_lead_deltas(
    *,
    from_snapshot_id: int,
    to_snapshot_id: int,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Export lead deltas between two snapshots to CSV + JSON.

    CSV: one row per change/new/removed with flattened columns for easy analysis.
    JSON: full structured payload from compute_lead_deltas plus exported_at and file metadata.
    """
    ensure_runtime_directories()
    deltas = compute_lead_deltas(
        from_snapshot_id=int(from_snapshot_id),
        to_snapshot_id=int(to_snapshot_id),
        database_url=database_url,
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_name = f"lead_deltas_{int(from_snapshot_id)}_{int(to_snapshot_id)}_{ts}"
    export_dir = EXPORTS_DIR

    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent if output.parent else Path(".")
            export_dir.mkdir(parents=True, exist_ok=True)
            base_name = output.stem or base_name
        else:
            export_dir = output
            export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / f"{base_name}.csv"
    json_path = export_dir / f"{base_name}.json"

    def _event_cols(ev: Optional[dict]) -> dict:
        if not ev:
            return {
                "source": None,
                "doc_id": None,
                "source_url": None,
                "occurred_at": None,
                "created_at": None,
                "snippet": None,
                "place_text": None,
            }
        return {
            "source": ev.get("source"),
            "doc_id": ev.get("doc_id"),
            "source_url": ev.get("source_url"),
            "occurred_at": ev.get("occurred_at"),
            "created_at": ev.get("created_at"),
            "snippet": ev.get("snippet"),
            "place_text": ev.get("place_text"),
        }

    rows = []

    for it in deltas.get("new", []):
        ev = it.get("event")
        rows.append(
            {
                "change_type": "new",
                "event_hash": it.get("event_hash"),
                "event_id": it.get("event_id"),
                "from_rank": None,
                "from_score": None,
                "to_rank": it.get("rank"),
                "to_score": it.get("score"),
                "delta_rank": None,
                "delta_score": None,
                "from_score_details_json": None,
                "to_score_details_json": json.dumps(it.get("score_details") or {}, ensure_ascii=False),
                **_event_cols(ev),
            }
        )

    for it in deltas.get("removed", []):
        ev = it.get("event")
        rows.append(
            {
                "change_type": "removed",
                "event_hash": it.get("event_hash"),
                "event_id": it.get("event_id"),
                "from_rank": it.get("rank"),
                "from_score": it.get("score"),
                "to_rank": None,
                "to_score": None,
                "delta_rank": None,
                "delta_score": None,
                "from_score_details_json": json.dumps(it.get("score_details") or {}, ensure_ascii=False),
                "to_score_details_json": None,
                **_event_cols(ev),
            }
        )

    for it in deltas.get("changed", []):
        ev = it.get("event")
        frm = it.get("from") or {}
        to = it.get("to") or {}
        d = it.get("delta") or {}
        rows.append(
            {
                "change_type": "changed",
                "event_hash": it.get("event_hash"),
                "event_id": it.get("event_id"),
                "from_rank": (frm.get("rank")),
                "from_score": (frm.get("score")),
                "to_rank": (to.get("rank")),
                "to_score": (to.get("score")),
                "delta_rank": d.get("rank"),
                "delta_score": d.get("score"),
                "from_score_details_json": json.dumps(frm.get("score_details") or {}, ensure_ascii=False),
                "to_score_details_json": json.dumps(to.get("score_details") or {}, ensure_ascii=False),
                **_event_cols(ev),
            }
        )

    _write_csv(csv_path, rows)

    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "from_snapshot_id": int(from_snapshot_id),
        "to_snapshot_id": int(to_snapshot_id),
        **deltas,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"csv": csv_path, "json": json_path, "count": len(rows), "from_snapshot_id": int(from_snapshot_id), "to_snapshot_id": int(to_snapshot_id)}


def _write_csv(path: Path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


__all__ = ["export_lead_snapshot", "export_lead_deltas"]
