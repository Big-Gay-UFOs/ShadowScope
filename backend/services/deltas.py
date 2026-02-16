from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.db.models import Event, LeadSnapshot, LeadSnapshotItem, get_session_factory


def _event_meta(db: Session, event_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not event_ids:
        return {}
    rows = db.execute(select(Event).where(Event.id.in_(event_ids))).scalars().all()
    out: Dict[int, Dict[str, Any]] = {}
    for e in rows:
        out[int(e.id)] = {
            "id": int(e.id),
            "hash": e.hash,
            "source": e.source,
            "doc_id": e.doc_id,
            "source_url": e.source_url,
            "snippet": e.snippet,
            "place_text": e.place_text,
            "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }
    return out


def _load_snapshot(db: Session, snapshot_id: int) -> LeadSnapshot:
    snap = db.execute(select(LeadSnapshot).where(LeadSnapshot.id == snapshot_id)).scalar_one_or_none()
    if snap is None:
        raise ValueError(f"lead_snapshot {snapshot_id} not found")
    return snap


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
    _load_snapshot(db, from_snapshot_id)
    _load_snapshot(db, to_snapshot_id)

    a_items = _load_items(db, from_snapshot_id)
    b_items = _load_items(db, to_snapshot_id)

    a = {i.event_hash: i for i in a_items}
    b = {i.event_hash: i for i in b_items}

    a_keys = set(a.keys())
    b_keys = set(b.keys())

    new_keys = sorted(b_keys - a_keys)
    removed_keys = sorted(a_keys - b_keys)
    common_keys = sorted(a_keys & b_keys)

    changed: List[Dict[str, Any]] = []
    for k in common_keys:
        ai = a[k]
        bi = b[k]
        if int(ai.score) != int(bi.score) or int(ai.rank) != int(bi.rank):
            changed.append(
                {
                    "event_hash": k,
                    "event_id": int(bi.event_id),
                    "from": {"rank": int(ai.rank), "score": int(ai.score), "score_details": ai.score_details},
                    "to": {"rank": int(bi.rank), "score": int(bi.score), "score_details": bi.score_details},
                    "delta": {"rank": int(bi.rank) - int(ai.rank), "score": int(bi.score) - int(ai.score)},
                }
            )

    # event metadata for anything referenced
    event_ids = []
    for k in (new_keys + removed_keys):
        event_ids.append(int((b.get(k) or a.get(k)).event_id))  # type: ignore[union-attr]
    for c in changed:
        event_ids.append(int(c["event_id"]))
    event_ids = sorted(set(event_ids))

    meta = _event_meta(db, event_ids)

    def _item_out(i: LeadSnapshotItem) -> Dict[str, Any]:
        return {
            "event_hash": i.event_hash,
            "event_id": int(i.event_id),
            "rank": int(i.rank),
            "score": int(i.score),
            "score_details": i.score_details,
            "event": meta.get(int(i.event_id)),
        }

    new_items = [_item_out(b[k]) for k in new_keys]
    removed_items = [_item_out(a[k]) for k in removed_keys]

    # attach meta to changed
    for c in changed:
        c["event"] = meta.get(int(c["event_id"]))

    return {
        "from_snapshot_id": int(from_snapshot_id),
        "to_snapshot_id": int(to_snapshot_id),
        "counts": {
            "from": len(a_items),
            "to": len(b_items),
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