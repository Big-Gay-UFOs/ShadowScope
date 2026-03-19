from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.db.models import Event, LeadSnapshot, LeadSnapshotItem, get_session_factory


def _event_meta(db: Session, event_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not event_ids:
        return {}
    rows = db.execute(select(Event).where(Event.id.in_(event_ids))).scalars().all()
    out: Dict[int, Dict[str, Any]] = {}
    for event in rows:
        out[int(event.id)] = {
            "id": int(event.id),
            "hash": event.hash,
            "source": event.source,
            "doc_id": event.doc_id,
            "source_url": event.source_url,
            "snippet": event.snippet,
            "place_text": event.place_text,
            "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
            "created_at": event.created_at.isoformat() if event.created_at else None,
        }
    return out


def _snapshot_meta(snapshot: LeadSnapshot) -> Dict[str, Any]:
    return {
        "id": int(snapshot.id),
        "analysis_run_id": getattr(snapshot, "analysis_run_id", None),
        "source": getattr(snapshot, "source", None),
        "min_score": int(getattr(snapshot, "min_score", 0) or 0),
        "limit": int(getattr(snapshot, "limit", 0) or 0),
        "scoring_version": getattr(snapshot, "scoring_version", None),
        "notes": getattr(snapshot, "notes", None),
        "created_at": snapshot.created_at.isoformat() if getattr(snapshot, "created_at", None) else None,
    }


def _load_snapshot(db: Session, snapshot_id: int) -> LeadSnapshot:
    snapshot = db.execute(select(LeadSnapshot).where(LeadSnapshot.id == snapshot_id)).scalar_one_or_none()
    if snapshot is None:
        raise ValueError(f"lead_snapshot {snapshot_id} not found")
    return snapshot


def _load_items(db: Session, snapshot_id: int) -> List[LeadSnapshotItem]:
    return (
        db.execute(
            select(LeadSnapshotItem)
            .where(LeadSnapshotItem.snapshot_id == snapshot_id)
            .order_by(LeadSnapshotItem.rank.asc())
        )
        .scalars()
        .all()
    )


def lead_deltas(db: Session, *, from_snapshot_id: int, to_snapshot_id: int) -> Dict[str, Any]:
    from_snapshot = _load_snapshot(db, from_snapshot_id)
    to_snapshot = _load_snapshot(db, to_snapshot_id)

    from_items = _load_items(db, from_snapshot_id)
    to_items = _load_items(db, to_snapshot_id)

    before = {item.event_hash: item for item in from_items}
    after = {item.event_hash: item for item in to_items}

    before_keys = set(before.keys())
    after_keys = set(after.keys())

    new_keys = sorted(after_keys - before_keys)
    removed_keys = sorted(before_keys - after_keys)
    common_keys = sorted(before_keys & after_keys)

    changed: List[Dict[str, Any]] = []
    for key in common_keys:
        before_item = before[key]
        after_item = after[key]
        if int(before_item.score) != int(after_item.score) or int(before_item.rank) != int(after_item.rank):
            changed.append(
                {
                    "event_hash": key,
                    "event_id": int(after_item.event_id),
                    "from": {"rank": int(before_item.rank), "score": int(before_item.score), "score_details": before_item.score_details},
                    "to": {"rank": int(after_item.rank), "score": int(after_item.score), "score_details": after_item.score_details},
                    "delta": {"rank": int(after_item.rank) - int(before_item.rank), "score": int(after_item.score) - int(before_item.score)},
                }
            )

    event_ids: list[int] = []
    for key in (new_keys + removed_keys):
        event_ids.append(int((after.get(key) or before.get(key)).event_id))  # type: ignore[union-attr]
    for item in changed:
        event_ids.append(int(item["event_id"]))
    event_ids = sorted(set(event_ids))

    meta = _event_meta(db, event_ids)

    def _item_out(item: LeadSnapshotItem) -> Dict[str, Any]:
        return {
            "event_hash": item.event_hash,
            "event_id": int(item.event_id),
            "rank": int(item.rank),
            "score": int(item.score),
            "score_details": item.score_details,
            "event": meta.get(int(item.event_id)),
        }

    new_items = [_item_out(after[key]) for key in new_keys]
    removed_items = [_item_out(before[key]) for key in removed_keys]

    for item in changed:
        item["event"] = meta.get(int(item["event_id"]))

    return {
        "from_snapshot_id": int(from_snapshot_id),
        "to_snapshot_id": int(to_snapshot_id),
        "from_snapshot": _snapshot_meta(from_snapshot),
        "to_snapshot": _snapshot_meta(to_snapshot),
        "scoring_versions_match": getattr(from_snapshot, "scoring_version", None) == getattr(to_snapshot, "scoring_version", None),
        "counts": {
            "from": len(from_items),
            "to": len(to_items),
            "new": len(new_items),
            "removed": len(removed_items),
            "changed": len(changed),
        },
        "new": new_items,
        "removed": removed_items,
        "changed": changed,
    }


def compute_lead_deltas(*, from_snapshot_id: int, to_snapshot_id: int, database_url: Optional[str] = None) -> Dict[str, Any]:
    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()
    try:
        return lead_deltas(db, from_snapshot_id=from_snapshot_id, to_snapshot_id=to_snapshot_id)
    finally:
        db.close()
