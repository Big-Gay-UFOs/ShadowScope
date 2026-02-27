"""Export utilities for lead snapshots and lead deltas."""
from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import select

from backend.db.models import Correlation, CorrelationLink, Event, LeadSnapshot, LeadSnapshotItem, get_session_factory
from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.deltas import compute_lead_deltas


def _score_part(details: dict[str, Any], key: str, default: Any = 0) -> Any:
    v = details.get(key, default)
    return default if v is None else v


def _top_clauses_text(details: dict[str, Any], limit: int = 5) -> str:
    items = details.get("top_clauses") or []
    out: list[str] = []
    if isinstance(items, list):
        for c in items[: int(limit)]:
            if not isinstance(c, dict):
                continue
            pack = c.get("pack") or ""
            rule = c.get("rule") or ""
            w = c.get("weight")
            if pack and rule:
                out.append(f"{pack}:{rule}({w})")
            elif pack:
                out.append(f"{pack}({w})")
            else:
                out.append(f"clause({w})")
    return "; ".join(out)


def _fetch_kw_pair_correlations(db, *, event_ids: list[int], source: Optional[str]) -> dict[int, list[dict[str, Any]]]:
    if not event_ids:
        return {}
    like_pat = f"kw_pair|{source}|%|pair:%" if source else "kw_pair|%|%|pair:%"

    q = (
        db.query(
            CorrelationLink.event_id,
            Correlation.id,
            Correlation.score,
            Correlation.window_days,
            Correlation.correlation_key,
            Correlation.lanes_hit,
        )
        .join(Correlation, Correlation.id == CorrelationLink.correlation_id)
        .filter(Correlation.correlation_key.like(like_pat))
        .filter(CorrelationLink.event_id.in_(event_ids))
    )

    by_event: dict[int, list[dict[str, Any]]] = {}
    for event_id, cid, cscore, window_days, ckey, lanes_hit in q.all():
        eid = int(event_id)
        lh = lanes_hit if isinstance(lanes_hit, dict) else {}

        if lh.get("lane") == "kw_pair":
            kw1 = lh.get("keyword_1") or lh.get("k1")
            kw2 = lh.get("keyword_2") or lh.get("k2")
            ec = lh.get("event_count")
        else:
            maybe = lh.get("kw_pair") if isinstance(lh, dict) else None
            if isinstance(maybe, dict):
                kw1 = maybe.get("keyword_1") or maybe.get("k1")
                kw2 = maybe.get("keyword_2") or maybe.get("k2")
                ec = maybe.get("event_count")
            else:
                kw1 = None
                kw2 = None
                ec = None

        if ec is None:
            try:
                ec = int(cscore or 0)
            except Exception:
                ec = 0

        try:
            ec_i = int(ec)
        except Exception:
            ec_i = 0

        if not kw1 or not kw2 or ec_i <= 0:
            continue

        contrib = 0.0
        try:
            contrib = 1.0 / math.sqrt(float(ec_i))
        except Exception:
            contrib = 0.0

        by_event.setdefault(eid, []).append(
            {
                "correlation_id": int(cid),
                "correlation_key": ckey,
                "window_days": int(window_days or 0),
                "keyword_1": str(kw1),
                "keyword_2": str(kw2),
                "event_count": int(ec_i),
                "contribution": round(float(contrib), 6),
            }
        )

    for eid in list(by_event.keys()):
        by_event[eid].sort(key=lambda d: (d.get("contribution", 0.0), -d.get("event_count", 0)), reverse=True)

    return by_event


def export_lead_snapshot(
    *,
    snapshot_id: int,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Export a lead snapshot (lead_snapshots + lead_snapshot_items + event metadata)
    to CSV + JSON.

    Adds explainability fields:
      - score component columns (clause/entity/pair/noise)
      - top clause hits
      - top kw_pair correlations contributing to pair_bonus
      - why_summary (human readable)
    """
    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)

    with SessionFactory() as db:
        snap = db.execute(select(LeadSnapshot).where(LeadSnapshot.id == int(snapshot_id))).scalar_one_or_none()
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
            rows = db.execute(select(Event).where(Event.id.in_(event_ids))).scalars().all()
            events_by_id = {int(e.id): e for e in rows}

        kw_pairs_by_event = _fetch_kw_pair_correlations(db, event_ids=event_ids, source=getattr(snap, "source", None))

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

    rows_out: list[dict[str, Any]] = []
    for it in items:
        e = events_by_id.get(int(it.event_id))
        details = it.score_details if isinstance(it.score_details, dict) else {}

        clause_score = _score_part(details, "clause_score", 0)
        clause_score_raw = _score_part(details, "clause_score_raw", None)
        keyword_score = _score_part(details, "keyword_score", 0)
        entity_bonus = _score_part(details, "entity_bonus", 0)
        pair_bonus = _score_part(details, "pair_bonus", 0)
        pair_count = _score_part(details, "pair_count", 0)
        pair_strength = _score_part(details, "pair_strength", 0.0)
        has_noise = bool(_score_part(details, "has_noise", False))
        noise_penalty = _score_part(details, "noise_penalty", 0)

        top_clauses_text = _top_clauses_text(details, limit=5)

        pairs = kw_pairs_by_event.get(int(it.event_id), []) or []
        top_pairs = pairs[:5]
        top_pairs_text = "; ".join([f"{p['keyword_1']}+{p['keyword_2']}(n={p['event_count']})" for p in top_pairs])

        why_bits: list[str] = []
        if clause_score_raw is not None:
            why_bits.append(f"clauses={clause_score} (raw={clause_score_raw})")
        else:
            why_bits.append(f"clauses={clause_score}")
        if keyword_score:
            why_bits.append(f"keywords={keyword_score}")
        if entity_bonus:
            why_bits.append(f"entity_bonus={entity_bonus}")
        if pair_bonus:
            why_bits.append(f"pair_bonus={pair_bonus} (pairs={pair_count}, strength={pair_strength})")
        if has_noise and noise_penalty:
            why_bits.append(f"noise_penalty=-{noise_penalty}")
        if top_pairs_text:
            why_bits.append(f"top_pairs: {top_pairs_text}")
        if top_clauses_text:
            why_bits.append(f"top_clauses: {top_clauses_text}")

        why_summary = " | ".join(why_bits)

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
                "occurred_at": None if (e is None or e.occurred_at is None) else e.occurred_at.isoformat(),
                "created_at": None if (e is None or e.created_at is None) else e.created_at.isoformat(),
                "entity_id": None if e is None else e.entity_id,
                "snippet": None if e is None else (e.snippet or ""),
                "place_text": None if e is None else (e.place_text or ""),
                "scoring_version": details.get("scoring_version"),
                "clause_score": clause_score,
                "clause_score_raw": clause_score_raw,
                "keyword_score": keyword_score,
                "entity_bonus": entity_bonus,
                "pair_bonus": pair_bonus,
                "pair_count": pair_count,
                "pair_strength": pair_strength,
                "has_noise": has_noise,
                "noise_penalty": noise_penalty,
                "top_clauses_text": top_clauses_text,
                "top_kw_pairs_text": top_pairs_text,
                "top_kw_pairs_json": json.dumps(top_pairs, ensure_ascii=False),
                "why_summary": why_summary,
                "score_details_json": json.dumps(details or {}, ensure_ascii=False),
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

    rows: list[dict[str, Any]] = []

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
