from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy.orm import Session

from backend.db.models import (
    Correlation,
    CorrelationLink,
    Entity,
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
    get_session_factory,
)
from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.explainability import (
    aggregate_matched_ontology,
    coerce_number,
    correlation_lane_payload,
    enrich_lead_score_details,
    extract_matched_ontology,
    infer_correlation_lane,
    load_event_correlation_evidence,
    safe_int,
)
from backend.services.investigator_filters import event_place_region_label


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _best_agency(event: Event) -> dict[str, Any] | None:
    candidates = (
        (event.awarding_agency_code, event.awarding_agency_name, "awarding"),
        (event.funding_agency_code, event.funding_agency_name, "funding"),
        (event.contracting_office_code, event.contracting_office_name, "contracting_office"),
    )
    for code, name, role in candidates:
        code_text = str(code or "").strip().upper()
        name_text = str(name or "").strip()
        if code_text or name_text:
            label = name_text or code_text
            if name_text and code_text:
                label = f"{name_text} ({code_text})"
            return {
                "role": role,
                "agency_code": code_text or None,
                "agency_name": name_text or None,
                "label": label,
            }
    return None


def _event_record(event: Event, entity: Entity | None) -> dict[str, Any]:
    ontology = extract_matched_ontology(event.clauses)
    agency = _best_agency(event)
    entity_name = str(getattr(entity, "name", "") or "").strip() or str(event.recipient_name or "").strip()
    entity_uei = str(getattr(entity, "uei", "") or "").strip() or str(event.recipient_uei or "").strip()
    entity_id_value = safe_int(getattr(entity, "id", None), default=0) or None
    return {
        "event_id": int(event.id),
        "hash": event.hash,
        "source": event.source,
        "doc_id": event.doc_id,
        "source_url": event.source_url,
        "occurred_at": _iso(event.occurred_at),
        "created_at": _iso(event.created_at),
        "snippet": event.snippet,
        "place_text": event.place_text,
        "place_region": event_place_region_label(event),
        "identifiers": {
            "award_id": event.award_id,
            "generated_unique_award_id": event.generated_unique_award_id,
            "piid": event.piid,
            "fain": event.fain,
            "uri": event.uri,
            "transaction_id": event.transaction_id,
            "modification_number": event.modification_number,
            "source_record_id": event.source_record_id,
            "solicitation_number": event.solicitation_number,
            "notice_id": event.notice_id,
            "document_id": event.document_id,
            "recipient_uei": event.recipient_uei,
            "entity_id": entity_id_value,
        },
        "recipient": {
            "name": event.recipient_name,
            "uei": event.recipient_uei,
            "parent_uei": event.recipient_parent_uei,
            "duns": event.recipient_duns,
            "cage_code": event.recipient_cage_code,
            "entity_name": entity_name or None,
            "entity_uei": entity_uei or None,
        },
        "agency": agency,
        "psc": {
            "code": event.psc_code,
            "description": event.psc_description,
        },
        "naics": {
            "code": event.naics_code,
            "description": event.naics_description,
        },
        "matched_ontology_rules": ontology.get("matched_ontology_rules") or [],
        "matched_ontology_clauses": ontology.get("matched_ontology_clauses") or [],
    }


def _dedupe_preserve(values: list[Any]) -> list[Any]:
    out: list[Any] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(value)
    return out


def _aggregate_identifiers(records: list[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[str, list[Any]] = {
        "event_ids": [],
        "hashes": [],
        "doc_ids": [],
        "award_ids": [],
        "generated_unique_award_ids": [],
        "piids": [],
        "fains": [],
        "uris": [],
        "transaction_ids": [],
        "solicitation_numbers": [],
        "notice_ids": [],
        "recipient_ueis": [],
        "entity_ids": [],
        "place_regions": [],
    }
    for record in records:
        buckets["event_ids"].append(record.get("event_id"))
        buckets["hashes"].append(record.get("hash"))
        buckets["doc_ids"].append(record.get("doc_id"))
        buckets["place_regions"].append(record.get("place_region"))
        identifiers = record.get("identifiers") or {}
        buckets["award_ids"].append(identifiers.get("award_id"))
        buckets["generated_unique_award_ids"].append(identifiers.get("generated_unique_award_id"))
        buckets["piids"].append(identifiers.get("piid"))
        buckets["fains"].append(identifiers.get("fain"))
        buckets["uris"].append(identifiers.get("uri"))
        buckets["transaction_ids"].append(identifiers.get("transaction_id"))
        buckets["solicitation_numbers"].append(identifiers.get("solicitation_number"))
        buckets["notice_ids"].append(identifiers.get("notice_id"))
        buckets["recipient_ueis"].append(identifiers.get("recipient_uei"))
        buckets["entity_ids"].append(identifiers.get("entity_id"))
    return {key: _dedupe_preserve(values) for key, values in buckets.items()}


def _aggregate_agencies(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[tuple[str, str, str]] = Counter()
    for record in records:
        agency = record.get("agency") or {}
        label = str(agency.get("label") or "").strip()
        code = str(agency.get("agency_code") or "").strip().upper()
        role = str(agency.get("role") or "").strip()
        if label or code:
            counter[(label, code, role)] += 1
    rows = []
    for (label, code, role), count in counter.most_common():
        rows.append({"label": label or code or None, "agency_code": code or None, "role": role or None, "count": int(count)})
    return rows


def _aggregate_recipients(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[tuple[str, str]] = Counter()
    for record in records:
        recipient = record.get("recipient") or {}
        label = str(recipient.get("entity_name") or recipient.get("name") or recipient.get("uei") or "").strip()
        uei = str(recipient.get("entity_uei") or recipient.get("uei") or "").strip().upper()
        if label or uei:
            counter[(label, uei)] += 1
    rows = []
    for (label, uei), count in counter.most_common():
        rows.append({"label": label or uei or None, "uei": uei or None, "count": int(count)})
    return rows


def _aggregate_urls(records: list[dict[str, Any]]) -> list[str]:
    return [str(value) for value in _dedupe_preserve([record.get("source_url") for record in records])]


def _time_window_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    timeline: list[dict[str, Any]] = []
    parsed: list[datetime] = []
    for record in records:
        ts = record.get("occurred_at") or record.get("created_at")
        if ts is None:
            continue
        ts_text = _iso(ts)
        try:
            dt = datetime.fromisoformat(str(ts_text).replace("Z", "+00:00"))
        except ValueError:
            continue
        parsed.append(dt)
        timeline.append(
            {
                "timestamp": dt.isoformat(),
                "event_id": record.get("event_id"),
                "source": record.get("source"),
                "doc_id": record.get("doc_id"),
                "label": record.get("snippet") or record.get("doc_id") or record.get("hash"),
            }
        )
    timeline.sort(key=lambda item: (item.get("timestamp") or "", safe_int(item.get("event_id"), default=0)))
    if not parsed:
        return {"earliest": None, "latest": None, "span_days": 0, "timeline": timeline}
    earliest = min(parsed)
    latest = max(parsed)
    span_days = max(int((latest - earliest).total_seconds() // 86400), 0)
    return {
        "earliest": earliest.isoformat(),
        "latest": latest.isoformat(),
        "span_days": span_days,
        "timeline": timeline[:20],
    }


def _load_correlation_members(db: Session, correlation_id: int) -> list[tuple[Event, Entity | None]]:
    return [
        (event, entity)
        for _link, event, entity in (
            db.query(CorrelationLink, Event, Entity)
            .join(Event, Event.id == CorrelationLink.event_id)
            .outerjoin(Entity, Entity.id == Event.entity_id)
            .filter(CorrelationLink.correlation_id == int(correlation_id))
            .order_by(Event.id.asc())
            .all()
        )
    ]


def _base_payload(package_type: str) -> dict[str, Any]:
    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "package_type": package_type,
        "review_guardrails": {
            "evidence_only": True,
            "claims_inferred": False,
            "foia_letter_generated": False,
        },
    }


def _resolve_output_path(output: Optional[Path], base_name: str) -> Path:
    ensure_runtime_directories()
    export_dir = EXPORTS_DIR
    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent if output.parent else Path('.')
            export_dir.mkdir(parents=True, exist_ok=True)
            return export_dir / (output.stem + '.json')
        export_dir = output
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir / f"{base_name}.json"


def _write_payload(payload: dict[str, Any], *, output: Optional[Path], base_name: str) -> Path:
    path = _resolve_output_path(output, base_name)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return path


def export_correlation_evidence_package(
    *,
    correlation_id: int,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> dict[str, Any]:
    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        correlation = db.get(Correlation, int(correlation_id))
        if correlation is None:
            raise ValueError(f"correlation {correlation_id} not found")

        lane = infer_correlation_lane(correlation.correlation_key, correlation.lanes_hit)
        lane_payload = correlation_lane_payload(lane, correlation.lanes_hit)
        members = _load_correlation_members(db, int(correlation_id))
        records = [_event_record(event, entity) for event, entity in members]
        ontology = aggregate_matched_ontology([event.clauses for event, _entity in members])

    payload = _base_payload("correlation_evidence_package")
    payload.update(
        {
            "correlation": {
                "id": int(correlation.id),
                "lane": lane,
                "correlation_key": correlation.correlation_key,
                "score": correlation.score,
                "score_signal": coerce_number(lane_payload.get("score_signal", correlation.score), default=0.0),
                "window_days": safe_int(correlation.window_days, default=0),
                "summary": correlation.summary,
                "rationale": correlation.rationale,
                "matched_values": lane_payload.get("matched_values") if isinstance(lane_payload.get("matched_values"), dict) else {},
                "lanes_hit": correlation.lanes_hit,
                "created_at": _iso(correlation.created_at),
            },
            "source_record_ids": [record.get("event_id") for record in records],
            "source_urls": _aggregate_urls(records),
            "matched_identifiers": _aggregate_identifiers(records),
            "agencies": _aggregate_agencies(records),
            "vendors_or_recipients": _aggregate_recipients(records),
            "matched_ontology_rules": ontology.get("matched_ontology_rules") or [],
            "matched_ontology_clauses": ontology.get("matched_ontology_clauses") or [],
            "time_window_summary": _time_window_summary(records),
            "source_records": records,
        }
    )

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    json_path = _write_payload(payload, output=output, base_name=f"evidence_package_correlation_{int(correlation_id)}_{ts}")
    return {"json": json_path, "package_type": payload["package_type"], "source_record_count": len(records), "correlation_id": int(correlation_id)}


def export_lead_evidence_package(
    *,
    snapshot_id: int,
    lead_event_id: Optional[int] = None,
    lead_rank: Optional[int] = None,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> dict[str, Any]:
    if lead_event_id is None and lead_rank is None:
        raise ValueError("lead_event_id or lead_rank is required for a lead evidence package")

    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        snapshot = db.get(LeadSnapshot, int(snapshot_id))
        if snapshot is None:
            raise ValueError(f"lead_snapshot {snapshot_id} not found")

        query = (
            db.query(LeadSnapshotItem, Event, Entity)
            .join(Event, Event.id == LeadSnapshotItem.event_id)
            .outerjoin(Entity, Entity.id == Event.entity_id)
            .filter(LeadSnapshotItem.snapshot_id == int(snapshot_id))
        )
        if lead_event_id is not None:
            query = query.filter(LeadSnapshotItem.event_id == int(lead_event_id))
        else:
            query = query.filter(LeadSnapshotItem.rank == int(lead_rank))
        row = query.first()
        if row is None:
            if lead_event_id is not None:
                raise ValueError(f"event_id {lead_event_id} not found in lead_snapshot {snapshot_id}")
            raise ValueError(f"rank {lead_rank} not found in lead_snapshot {snapshot_id}")

        item, event, entity = row
        correlations_by_event = load_event_correlation_evidence(db, event_ids=[int(event.id)])
        details = enrich_lead_score_details(
            clauses=event.clauses,
            base_details=item.score_details if isinstance(item.score_details, dict) else {},
            correlations=correlations_by_event.get(int(event.id), []),
        )

        correlation_ids = [safe_int(entry.get("correlation_id"), default=0) for entry in (details.get("contributing_correlations") or [])]
        correlation_ids = [corr_id for corr_id in correlation_ids if corr_id > 0]
        correlation_rows = {corr.id: corr for corr in db.query(Correlation).filter(Correlation.id.in_(correlation_ids)).all()} if correlation_ids else {}

        source_records_by_id: dict[int, dict[str, Any]] = {}
        lead_record = _event_record(event, entity)
        source_records_by_id[int(event.id)] = lead_record
        supporting_correlations: list[dict[str, Any]] = []

        for correlation_info in details.get("contributing_correlations") or []:
            if not isinstance(correlation_info, dict):
                continue
            corr_id = safe_int(correlation_info.get("correlation_id"), default=0)
            corr_obj = correlation_rows.get(corr_id)
            members = _load_correlation_members(db, corr_id) if corr_obj is not None else []
            lane = infer_correlation_lane(
                correlation_info.get("correlation_key") if corr_obj is None else corr_obj.correlation_key,
                {} if corr_obj is None else corr_obj.lanes_hit,
            )
            member_ids: list[int] = []
            for member_event, member_entity in members:
                member_ids.append(int(member_event.id))
                source_records_by_id.setdefault(int(member_event.id), _event_record(member_event, member_entity))
            supporting_correlations.append(
                {
                    "correlation_id": corr_id or None,
                    "lane": correlation_info.get("lane") or lane,
                    "correlation_key": correlation_info.get("correlation_key") if corr_obj is None else corr_obj.correlation_key,
                    "score_signal": correlation_info.get("score_signal"),
                    "event_count": correlation_info.get("event_count"),
                    "pair_label": correlation_info.get("pair_label"),
                    "summary": None if corr_obj is None else corr_obj.summary,
                    "rationale": None if corr_obj is None else corr_obj.rationale,
                    "member_event_ids": member_ids,
                }
            )

        records = list(source_records_by_id.values())
        ontology = aggregate_matched_ontology([record.get("matched_ontology_clauses") for record in records])

    payload = _base_payload("lead_evidence_package")
    payload.update(
        {
            "snapshot": {
                "id": int(snapshot.id),
                "source": snapshot.source,
                "min_score": int(snapshot.min_score or 0),
                "scoring_version": snapshot.scoring_version,
                "created_at": _iso(snapshot.created_at),
                "notes": snapshot.notes,
            },
            "lead": {
                "event_id": int(event.id),
                "event_hash": event.hash,
                "rank": int(item.rank),
                "score": int(item.score),
                "source": event.source,
                "doc_id": event.doc_id,
                "source_url": event.source_url,
                "contributing_lanes": details.get("contributing_lanes") or [],
                "score_details": details,
            },
            "supporting_correlations": supporting_correlations,
            "source_record_ids": [record.get("event_id") for record in records],
            "source_urls": _aggregate_urls(records),
            "matched_identifiers": _aggregate_identifiers(records),
            "agencies": _aggregate_agencies(records),
            "vendors_or_recipients": _aggregate_recipients(records),
            "matched_ontology_rules": ontology.get("matched_ontology_rules") or [],
            "matched_ontology_clauses": ontology.get("matched_ontology_clauses") or [],
            "time_window_summary": _time_window_summary(records),
            "source_records": records,
        }
    )

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    json_path = _write_payload(payload, output=output, base_name=f"evidence_package_lead_{int(snapshot_id)}_{int(event.id)}_{ts}")
    return {"json": json_path, "package_type": payload["package_type"], "source_record_count": len(records), "snapshot_id": int(snapshot_id), "event_id": int(event.id)}


def export_evidence_package(
    *,
    snapshot_id: Optional[int] = None,
    lead_event_id: Optional[int] = None,
    lead_rank: Optional[int] = None,
    correlation_id: Optional[int] = None,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> dict[str, Any]:
    if correlation_id is not None:
        if snapshot_id is not None or lead_event_id is not None or lead_rank is not None:
            raise ValueError("Choose either correlation_id or a lead target, not both")
        return export_correlation_evidence_package(
            correlation_id=int(correlation_id),
            database_url=database_url,
            output=output,
        )

    if snapshot_id is None:
        raise ValueError("snapshot_id is required when exporting a lead evidence package")

    return export_lead_evidence_package(
        snapshot_id=int(snapshot_id),
        lead_event_id=lead_event_id,
        lead_rank=lead_rank,
        database_url=database_url,
        output=output,
    )


__all__ = ["export_evidence_package", "export_correlation_evidence_package", "export_lead_evidence_package"]
