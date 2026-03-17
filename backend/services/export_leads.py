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
from backend.services.explainability import enrich_lead_score_details, load_event_correlation_evidence


def _score_part(details: dict[str, Any], key: str, default: Any = 0) -> Any:
    v = details.get(key, default)
    return default if v is None else v


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _list_text(values: list[Any], *, limit: int = 5) -> str:
    return "; ".join([str(v) for v in values[: int(limit)] if str(v).strip()])


def _top_clauses_text(details: dict[str, Any], limit: int = 5) -> str:
    items = details.get("matched_ontology_clauses") or details.get("top_clauses") or []
    out: list[str] = []
    if isinstance(items, list):
        for clause in items[: int(limit)]:
            if not isinstance(clause, dict):
                continue
            pack = clause.get("pack") or ""
            rule = clause.get("rule") or ""
            weight = clause.get("weight")
            avg_weight = clause.get("avg_weight")
            event_count = clause.get("event_count")
            if pack and rule and avg_weight is not None:
                out.append(f"{pack}:{rule}(events={event_count},avg={avg_weight})")
            elif pack and rule:
                out.append(f"{pack}:{rule}({weight})")
            elif pack:
                out.append(f"{pack}({weight})")
            else:
                out.append(f"clause({weight})")
    return "; ".join(out)


def _correlation_text(correlation: dict[str, Any]) -> str:
    lane = str(correlation.get("lane") or "")
    label = str(correlation.get("pair_label") or correlation.get("correlation_key") or "")
    event_count = correlation.get("event_count")
    score_signal = correlation.get("score_signal")
    if label and event_count is not None and score_signal is not None:
        return f"{lane}:{label}(signal={score_signal},n={event_count})"
    if label:
        return f"{lane}:{label}" if lane else label
    return lane or "correlation"


def _why_summary(details: dict[str, Any]) -> str:
    clause_score = _score_part(details, "clause_score", 0)
    clause_score_raw = _score_part(details, "clause_score_raw", None)
    keyword_score = _score_part(details, "keyword_score", 0)
    entity_bonus = _score_part(details, "entity_bonus", 0)
    pair_bonus = _score_part(details, "pair_bonus_applied", _score_part(details, "pair_bonus", 0))
    pair_count = _score_part(details, "pair_count", 0)
    pair_strength = _score_part(details, "pair_strength", 0.0)
    noise_penalty = _score_part(details, "noise_penalty_applied", _score_part(details, "noise_penalty", 0))
    matched_rules = details.get("matched_ontology_rules") or []
    contributing_correlations = details.get("contributing_correlations") or []

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
    if noise_penalty:
        why_bits.append(f"noise_penalty=-{noise_penalty}")
    if matched_rules:
        why_bits.append(f"rules: {_list_text(matched_rules)}")
    if contributing_correlations:
        why_bits.append(
            "correlations: " + _list_text([_correlation_text(c) for c in contributing_correlations], limit=5)
        )
    return " | ".join(why_bits)


def _flatten_details(prefix: str, details: dict[str, Any]) -> dict[str, Any]:
    contributing_lanes = details.get("contributing_lanes") or []
    contributing_correlations = details.get("contributing_correlations") or []
    matched_rules = details.get("matched_ontology_rules") or []
    matched_clauses = details.get("matched_ontology_clauses") or []
    return {
        f"{prefix}_scoring_version": details.get("scoring_version"),
        f"{prefix}_clause_score": _score_part(details, "clause_score", 0),
        f"{prefix}_clause_score_raw": _score_part(details, "clause_score_raw", None),
        f"{prefix}_keyword_score": _score_part(details, "keyword_score", 0),
        f"{prefix}_entity_bonus": _score_part(details, "entity_bonus", 0),
        f"{prefix}_pair_bonus": _score_part(details, "pair_bonus", 0),
        f"{prefix}_pair_bonus_applied": _score_part(details, "pair_bonus_applied", _score_part(details, "pair_bonus", 0)),
        f"{prefix}_pair_count": _score_part(details, "pair_count", 0),
        f"{prefix}_pair_strength": _score_part(details, "pair_strength", 0.0),
        f"{prefix}_noise_penalty": _score_part(details, "noise_penalty", 0),
        f"{prefix}_noise_penalty_applied": _score_part(details, "noise_penalty_applied", _score_part(details, "noise_penalty", 0)),
        f"{prefix}_contributing_lanes_text": _list_text([str(v) for v in contributing_lanes], limit=20),
        f"{prefix}_contributing_lanes_json": _json_text(contributing_lanes),
        f"{prefix}_contributing_correlations_text": _list_text([_correlation_text(c) for c in contributing_correlations], limit=5),
        f"{prefix}_contributing_correlations_json": _json_text(contributing_correlations),
        f"{prefix}_matched_ontology_rules_text": _list_text([str(v) for v in matched_rules], limit=10),
        f"{prefix}_matched_ontology_rules_json": _json_text(matched_rules),
        f"{prefix}_matched_ontology_clauses_json": _json_text(matched_clauses),
        f"{prefix}_why_summary": _why_summary(details),
        f"{prefix}_score_details_json": _json_text(details or {}),
    }


def _load_event_context(database_url: Optional[str], event_ids: list[int]) -> tuple[dict[int, Event], dict[int, list[dict[str, Any]]]]:
    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        events_by_id: dict[int, Event] = {}
        if event_ids:
            rows = db.execute(select(Event).where(Event.id.in_(event_ids))).scalars().all()
            events_by_id = {int(event.id): event for event in rows}
        correlations_by_event = load_event_correlation_evidence(db, event_ids=event_ids)
    return events_by_id, correlations_by_event


def export_lead_snapshot(
    *,
    snapshot_id: int,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Export a lead snapshot (lead_snapshots + lead_snapshot_items + event metadata)
    to CSV + JSON.
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
    events_by_id, correlations_by_event = _load_event_context(database_url, event_ids)

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
        event = events_by_id.get(int(it.event_id))
        details = it.score_details if isinstance(it.score_details, dict) else {}
        details = enrich_lead_score_details(
            clauses=None if event is None else event.clauses,
            base_details=details,
            correlations=correlations_by_event.get(int(it.event_id), []),
        )

        pair_correlations = [
            c for c in (details.get("contributing_correlations") or []) if str(c.get("lane") or "") == "kw_pair"
        ]
        top_pairs = pair_correlations[:5]
        top_pairs_text = "; ".join(
            [
                f"{p.get('pair_label') or p.get('pair_label_raw') or p.get('correlation_key')}(n={p.get('event_count')})"
                for p in top_pairs
            ]
        )
        matched_rules = details.get("matched_ontology_rules") or []
        matched_rules_text = _list_text([str(v) for v in matched_rules], limit=10)
        contributing_lanes = details.get("contributing_lanes") or []
        contributing_lanes_text = _list_text([str(v) for v in contributing_lanes], limit=20)
        contributing_correlations = details.get("contributing_correlations") or []
        contributing_correlations_text = _list_text([_correlation_text(c) for c in contributing_correlations], limit=5)
        top_clauses_text = _top_clauses_text(details, limit=5)
        why_summary = _why_summary(details)

        rows_out.append(
            {
                "snapshot_id": int(snapshot_id),
                "rank": int(it.rank),
                "score": int(it.score),
                "event_id": int(it.event_id),
                "event_hash": it.event_hash,
                "source": None if event is None else event.source,
                "doc_id": None if event is None else event.doc_id,
                "source_url": None if event is None else event.source_url,
                "occurred_at": None if (event is None or event.occurred_at is None) else event.occurred_at.isoformat(),
                "created_at": None if (event is None or event.created_at is None) else event.created_at.isoformat(),
                "entity_id": None if event is None else event.entity_id,
                "snippet": None if event is None else (event.snippet or ""),
                "place_text": None if event is None else (event.place_text or ""),
                "scoring_version": details.get("scoring_version"),
                "clause_score": _score_part(details, "clause_score", 0),
                "clause_score_raw": _score_part(details, "clause_score_raw", None),
                "keyword_score": _score_part(details, "keyword_score", 0),
                "entity_bonus": _score_part(details, "entity_bonus", 0),
                "pair_bonus": _score_part(details, "pair_bonus", 0),
                "pair_bonus_applied": _score_part(details, "pair_bonus_applied", _score_part(details, "pair_bonus", 0)),
                "pair_count": _score_part(details, "pair_count", 0),
                "pair_strength": _score_part(details, "pair_strength", 0.0),
                "has_noise": bool(_score_part(details, "has_noise", False)),
                "noise_penalty": _score_part(details, "noise_penalty", 0),
                "noise_penalty_applied": _score_part(details, "noise_penalty_applied", _score_part(details, "noise_penalty", 0)),
                "contributing_lanes_text": contributing_lanes_text,
                "contributing_lanes_json": _json_text(contributing_lanes),
                "contributing_correlations_text": contributing_correlations_text,
                "contributing_correlations_json": _json_text(contributing_correlations),
                "matched_ontology_rules_text": matched_rules_text,
                "matched_ontology_rules_json": _json_text(matched_rules),
                "matched_ontology_clauses_json": _json_text(details.get("matched_ontology_clauses") or []),
                "top_clauses_text": top_clauses_text,
                "top_kw_pairs_text": top_pairs_text,
                "top_kw_pairs_json": _json_text(top_pairs),
                "why_summary": why_summary,
                "score_details_json": _json_text(details or {}),
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
    """
    ensure_runtime_directories()
    deltas = compute_lead_deltas(
        from_snapshot_id=int(from_snapshot_id),
        to_snapshot_id=int(to_snapshot_id),
        database_url=database_url,
    )

    event_ids: list[int] = []
    for item in deltas.get("new", []):
        event_ids.append(int(item.get("event_id") or 0))
    for item in deltas.get("removed", []):
        event_ids.append(int(item.get("event_id") or 0))
    for item in deltas.get("changed", []):
        event_ids.append(int(item.get("event_id") or 0))
    event_ids = [event_id for event_id in sorted(set(event_ids)) if event_id > 0]
    events_by_id, correlations_by_event = _load_event_context(database_url, event_ids)

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

    def _enrich(event_id: Any, details: Any) -> dict[str, Any]:
        event = events_by_id.get(int(event_id or 0))
        base_details = details if isinstance(details, dict) else {}
        return enrich_lead_score_details(
            clauses=None if event is None else event.clauses,
            base_details=base_details,
            correlations=correlations_by_event.get(int(event_id or 0), []),
        )

    rows: list[dict[str, Any]] = []

    for item in deltas.get("new", []):
        event = item.get("event")
        to_details = _enrich(item.get("event_id"), item.get("score_details"))
        rows.append(
            {
                "change_type": "new",
                "event_hash": item.get("event_hash"),
                "event_id": item.get("event_id"),
                "from_rank": None,
                "from_score": None,
                "to_rank": item.get("rank"),
                "to_score": item.get("score"),
                "delta_rank": None,
                "delta_score": None,
                **_flatten_details("from", {}),
                **_flatten_details("to", to_details),
                **_event_cols(event),
            }
        )

    for item in deltas.get("removed", []):
        event = item.get("event")
        from_details = _enrich(item.get("event_id"), item.get("score_details"))
        rows.append(
            {
                "change_type": "removed",
                "event_hash": item.get("event_hash"),
                "event_id": item.get("event_id"),
                "from_rank": item.get("rank"),
                "from_score": item.get("score"),
                "to_rank": None,
                "to_score": None,
                "delta_rank": None,
                "delta_score": None,
                **_flatten_details("from", from_details),
                **_flatten_details("to", {}),
                **_event_cols(event),
            }
        )

    for item in deltas.get("changed", []):
        event = item.get("event")
        frm = item.get("from") or {}
        to = item.get("to") or {}
        delta = item.get("delta") or {}
        from_details = _enrich(item.get("event_id"), frm.get("score_details"))
        to_details = _enrich(item.get("event_id"), to.get("score_details"))
        rows.append(
            {
                "change_type": "changed",
                "event_hash": item.get("event_hash"),
                "event_id": item.get("event_id"),
                "from_rank": frm.get("rank"),
                "from_score": frm.get("score"),
                "to_rank": to.get("rank"),
                "to_score": to.get("score"),
                "delta_rank": delta.get("rank"),
                "delta_score": delta.get("score"),
                **_flatten_details("from", from_details),
                **_flatten_details("to", to_details),
                **_event_cols(event),
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
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


__all__ = ["export_lead_snapshot", "export_lead_deltas"]
