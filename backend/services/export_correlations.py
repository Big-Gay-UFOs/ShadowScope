from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from backend.db.models import Correlation, CorrelationLink, Entity, Event, get_session_factory
from backend.services.kw_pair_clusters import list_kw_pair_clusters
from backend.services.query_surfaces import query_correlations
from pathlib import Path


def export_correlations(
    *,
    out_path: str,
    source: Optional[str] = "USAspending",
    date_from: Any = None,
    date_to: Any = None,
    entity_id: Optional[int] = None,
    keyword: Optional[str] = None,
    min_score: Optional[int] = None,
    agency: Optional[str] = None,
    psc: Optional[str] = None,
    naics: Optional[str] = None,
    award_id: Optional[str] = None,
    recipient_uei: Optional[str] = None,
    place_region: Optional[str] = None,
    lane: Optional[str] = None,
    window_days: Optional[int] = None,
    min_event_count: Optional[int] = None,
    min_score_signal: Optional[float] = None,
    limit: int = 500,
    offset: int = 0,
    sort_by: Optional[str] = "score_signal",
    sort_dir: Optional[str] = "desc",
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    try:
        lane_value = str(lane or "").strip().lower() or None
        if lane_value == "kw_pair":
            payload = list_kw_pair_clusters(
                db,
                source=source,
                window_days=window_days,
                min_score=min_score_signal if min_score_signal is not None else min_score,
                min_event_count=min_event_count,
                limit=int(limit),
                offset=int(offset),
                include_events=True,
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
            items = list(payload.get("items") or [])
        else:
            selected = query_correlations(
                db,
                source=source,
                date_from=date_from,
                date_to=date_to,
                entity_id=entity_id,
                keyword=keyword,
                min_score=min_score,
                agency=agency,
                psc=psc,
                naics=naics,
                award_id=award_id,
                recipient_uei=recipient_uei,
                place_region=place_region,
                lane=lane,
                window_days=window_days,
                min_event_count=min_event_count,
                min_score_signal=min_score_signal,
                limit=int(limit),
                offset=int(offset),
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
            items = []
            for item in selected.get("items") or []:
                corr_id = int(item.get("id"))
                correlation = db.get(Correlation, corr_id)
                if correlation is None:
                    continue
                links = (
                    db.query(Event, Entity)
                    .join(CorrelationLink, CorrelationLink.event_id == Event.id)
                    .outerjoin(Entity, Entity.id == Event.entity_id)
                    .filter(CorrelationLink.correlation_id == corr_id)
                    .order_by(Event.id.asc())
                    .all()
                )
                events = []
                for event, entity in links:
                    events.append(
                        {
                            "id": event.id,
                            "hash": event.hash,
                            "source": event.source,
                            "doc_id": event.doc_id,
                            "source_url": event.source_url,
                            "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
                            "created_at": event.created_at.isoformat() if event.created_at else None,
                            "snippet": event.snippet,
                            "place_text": event.place_text,
                            "entity": None if entity is None else {"id": entity.id, "name": entity.name, "uei": entity.uei},
                        }
                    )
                items.append(
                    {
                        **item,
                        "created_at": correlation.created_at.isoformat() if correlation.created_at else None,
                        "events": events,
                    }
                )

        payload = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "date_from": date_from.isoformat() if hasattr(date_from, "isoformat") else date_from,
            "date_to": date_to.isoformat() if hasattr(date_to, "isoformat") else date_to,
            "entity_id": entity_id,
            "keyword": keyword,
            "min_score": min_score,
            "agency": agency,
            "psc": psc,
            "naics": naics,
            "award_id": award_id,
            "recipient_uei": recipient_uei,
            "place_region": place_region,
            "lane": lane,
            "window_days": window_days,
            "min_event_count": min_event_count,
            "min_score_signal": min_score_signal,
            "limit": int(limit),
            "offset": int(offset),
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "count": len(items),
            "items": items,
        }

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        return {"status": "ok", "out_path": out_path, "count": len(items)}
    finally:
        db.close()
def export_kw_pairs(
    *,
    database_url: str | None = None,
    output: Path | None = None,
    limit: int = 200,
    min_event_count: int = 2,
) -> dict:
    """
    Export kw_pair clusters to CSV + JSON.

    Items are ranked investigator-first by score_signal, then event_count.
    """
    from datetime import datetime, timezone
    import csv
    import json

    from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
    from backend.services.kw_pair_clusters import list_kw_pair_clusters

    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)

    with SessionFactory() as db:
        payload = list_kw_pair_clusters(
            db,
            limit=int(limit),
            offset=0,
            min_event_count=int(min_event_count),
            include_events=True,
        )

    items = list(payload.get("items") or [])

    rows_out: list[dict[str, Any]] = []
    for item in items:
        rows_out.append(
            {
                "correlation_id": item.get("correlation_id"),
                "correlation_key": item.get("correlation_key"),
                "lane": item.get("lane"),
                "source": item.get("source"),
                "window_days": item.get("window_days"),
                "score": item.get("score"),
                "score_signal": item.get("score_signal"),
                "event_count": item.get("event_count"),
                "keyword_1": item.get("keyword_1"),
                "keyword_2": item.get("keyword_2"),
                "pair_label": item.get("pair_label"),
                "scoring_version": item.get("scoring_version"),
                "pair_bonus_applied": item.get("pair_bonus_applied"),
                "noise_penalty_applied": item.get("noise_penalty_applied"),
                "member_event_ids": ";".join([str(v) for v in item.get("member_event_ids") or []]),
                "member_event_hashes": ";".join([str(v) for v in item.get("member_event_hashes") or []]),
                "member_events_json": json.dumps(item.get("member_events") or [], ensure_ascii=False),
                "top_entities_text": "; ".join([str(v.get("label") or v.get("name") or "") for v in (item.get("top_entities") or [])[:5]]),
                "top_entities_json": json.dumps(item.get("top_entities") or [], ensure_ascii=False),
                "top_agencies_text": "; ".join([str(v.get("label") or "") for v in (item.get("top_agencies") or [])[:5]]),
                "top_agencies_json": json.dumps(item.get("top_agencies") or [], ensure_ascii=False),
                "top_psc_text": "; ".join([str(v.get("label") or "") for v in (item.get("top_psc") or [])[:5]]),
                "top_psc_json": json.dumps(item.get("top_psc") or [], ensure_ascii=False),
                "top_naics_text": "; ".join([str(v.get("label") or "") for v in (item.get("top_naics") or [])[:5]]),
                "top_naics_json": json.dumps(item.get("top_naics") or [], ensure_ascii=False),
                "contributing_lanes_json": json.dumps(item.get("contributing_lanes") or [], ensure_ascii=False),
                "contributing_correlations_json": json.dumps(item.get("contributing_correlations") or [], ensure_ascii=False),
                "matched_ontology_rules_text": "; ".join([str(v) for v in (item.get("matched_ontology_rules") or [])[:10]]),
                "matched_ontology_rules_json": json.dumps(item.get("matched_ontology_rules") or [], ensure_ascii=False),
                "matched_ontology_clauses_json": json.dumps(item.get("matched_ontology_clauses") or [], ensure_ascii=False),
                "summary": item.get("summary"),
                "rationale": item.get("rationale"),
            }
        )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = "kw_pairs_" + ts
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

    csv_path = export_dir / (base + ".csv")
    json_path = export_dir / (base + ".json")

    if rows_out:
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
            writer.writeheader()
            writer.writerows(rows_out)
    else:
        csv_path.write_text("", encoding="utf-8")

    json_payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "limit": int(limit),
        "min_event_count": int(min_event_count),
        "count": len(items),
        "items": items,
    }
    json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"csv": csv_path, "json": json_path, "count": len(items)}

def _candidate_join_items(
    *,
    db: Session,
    window_days: int | None = None,
    min_score: int | None = None,
    limit: int = 200,
    incumbent_only: bool = False,
) -> list[dict[str, Any]]:
    from sqlalchemy import Integer, cast

    from backend.correlate.candidate_joins import CANDIDATE_JOIN_LANE

    q = db.query(Correlation).filter(Correlation.correlation_key.like(f"{CANDIDATE_JOIN_LANE}|%"))
    if window_days is not None:
        q = q.filter(Correlation.window_days == int(window_days))
    if min_score is not None:
        q = q.filter(cast(Correlation.score, Integer) >= int(min_score))

    rows = q.order_by(cast(Correlation.score, Integer).desc(), Correlation.id.desc()).all()

    items: list[dict[str, Any]] = []
    for c in rows:
        lh = c.lanes_hit if isinstance(c.lanes_hit, dict) else {}
        if incumbent_only and not bool(lh.get("likely_incumbent")):
            continue

        links = (
            db.query(Event)
            .join(CorrelationLink, CorrelationLink.event_id == Event.id)
            .filter(CorrelationLink.correlation_id == int(c.id))
            .order_by(Event.id.asc())
            .all()
        )
        sam = next((ev for ev in links if ev.source == "SAM.gov"), None)
        usa = next((ev for ev in links if ev.source == "USAspending"), None)
        if sam is None or usa is None:
            continue

        evidence_types = lh.get("evidence_types") if isinstance(lh.get("evidence_types"), list) else []
        matched_values = lh.get("matched_values") if isinstance(lh.get("matched_values"), dict) else {}
        evidence = lh.get("evidence") if isinstance(lh.get("evidence"), list) else []

        items.append(
            {
                "correlation_id": int(c.id),
                "correlation_key": c.correlation_key,
                "score": int(c.score or 0),
                "window_days": int(c.window_days or 0),
                "history_days": lh.get("history_days"),
                "likely_incumbent": bool(lh.get("likely_incumbent")),
                "time_delta_days": lh.get("time_delta_days"),
                "evidence_types": evidence_types,
                "matched_values": matched_values,
                "evidence": evidence,
                "summary": c.summary,
                "rationale": c.rationale,
                "created_at": c.created_at.isoformat() if c.created_at else None,
                "sam_event": {
                    "id": int(sam.id),
                    "hash": sam.hash,
                    "doc_id": sam.doc_id,
                    "source_url": sam.source_url,
                    "solicitation_number": sam.solicitation_number,
                    "notice_id": sam.notice_id,
                    "recipient_name": sam.recipient_name,
                    "recipient_uei": sam.recipient_uei,
                    "awarding_agency_code": sam.awarding_agency_code,
                    "awarding_agency_name": sam.awarding_agency_name,
                    "psc_code": sam.psc_code,
                    "naics_code": sam.naics_code,
                    "place_region": lh.get("matched_values", {}).get("place_region"),
                    "occurred_at": sam.occurred_at.isoformat() if sam.occurred_at else None,
                    "created_at": sam.created_at.isoformat() if sam.created_at else None,
                    "snippet": sam.snippet,
                },
                "usaspending_event": {
                    "id": int(usa.id),
                    "hash": usa.hash,
                    "doc_id": usa.doc_id,
                    "source_url": usa.source_url,
                    "award_id": usa.award_id,
                    "generated_unique_award_id": usa.generated_unique_award_id,
                    "piid": usa.piid,
                    "recipient_name": usa.recipient_name,
                    "recipient_uei": usa.recipient_uei,
                    "awarding_agency_code": usa.awarding_agency_code,
                    "awarding_agency_name": usa.awarding_agency_name,
                    "psc_code": usa.psc_code,
                    "naics_code": usa.naics_code,
                    "occurred_at": usa.occurred_at.isoformat() if usa.occurred_at else None,
                    "created_at": usa.created_at.isoformat() if usa.created_at else None,
                    "snippet": usa.snippet,
                },
            }
        )

    items = items[: int(limit)]
    return items


def summarize_candidate_joins(
    *,
    database_url: str | None = None,
    window_days: int | None = None,
    min_score: int | None = None,
    limit: int = 20,
    incumbent_only: bool = False,
) -> dict[str, Any]:
    from collections import Counter

    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        items = _candidate_join_items(
            db=db,
            window_days=window_days,
            min_score=min_score,
            limit=limit,
            incumbent_only=incumbent_only,
        )

    evidence_counter: Counter[str] = Counter()
    score_bands = {"strong": 0, "medium": 0, "candidate": 0}
    likely_incumbent = 0
    for item in items:
        evidence_counter.update([str(x) for x in item.get("evidence_types") or []])
        score = int(item.get("score") or 0)
        if score >= 80:
            score_bands["strong"] += 1
        elif score >= 60:
            score_bands["medium"] += 1
        else:
            score_bands["candidate"] += 1
        if item.get("likely_incumbent"):
            likely_incumbent += 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_days": window_days,
        "min_score": min_score,
        "limit": int(limit),
        "incumbent_only": bool(incumbent_only),
        "count": len(items),
        "likely_incumbent_count": int(likely_incumbent),
        "score_bands": score_bands,
        "evidence_type_counts": dict(evidence_counter),
        "items": items,
    }


def export_candidate_joins(
    *,
    database_url: str | None = None,
    output: Path | None = None,
    window_days: int | None = None,
    min_score: int | None = None,
    limit: int = 200,
    incumbent_only: bool = False,
) -> dict[str, Any]:
    import csv

    from backend.runtime import EXPORTS_DIR, ensure_runtime_directories

    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        items = _candidate_join_items(
            db=db,
            window_days=window_days,
            min_score=min_score,
            limit=limit,
            incumbent_only=incumbent_only,
        )

    rows_out: list[dict[str, Any]] = []
    for item in items:
        sam = item.get("sam_event") or {}
        usa = item.get("usaspending_event") or {}
        rows_out.append(
            {
                "correlation_id": item.get("correlation_id"),
                "correlation_key": item.get("correlation_key"),
                "score": item.get("score"),
                "window_days": item.get("window_days"),
                "history_days": item.get("history_days"),
                "likely_incumbent": item.get("likely_incumbent"),
                "time_delta_days": item.get("time_delta_days"),
                "evidence_types": ";".join([str(x) for x in item.get("evidence_types") or []]),
                "matched_values_json": json.dumps(item.get("matched_values") or {}, ensure_ascii=False),
                "sam_event_id": sam.get("id"),
                "sam_event_hash": sam.get("hash"),
                "sam_doc_id": sam.get("doc_id"),
                "sam_source_url": sam.get("source_url"),
                "sam_solicitation_number": sam.get("solicitation_number"),
                "sam_recipient_name": sam.get("recipient_name"),
                "sam_recipient_uei": sam.get("recipient_uei"),
                "sam_awarding_agency_code": sam.get("awarding_agency_code"),
                "sam_awarding_agency_name": sam.get("awarding_agency_name"),
                "sam_psc_code": sam.get("psc_code"),
                "sam_naics_code": sam.get("naics_code"),
                "sam_occurred_at": sam.get("occurred_at"),
                "sam_snippet": sam.get("snippet"),
                "usaspending_event_id": usa.get("id"),
                "usaspending_event_hash": usa.get("hash"),
                "usaspending_doc_id": usa.get("doc_id"),
                "usaspending_source_url": usa.get("source_url"),
                "usaspending_award_id": usa.get("award_id"),
                "usaspending_generated_unique_award_id": usa.get("generated_unique_award_id"),
                "usaspending_piid": usa.get("piid"),
                "usaspending_recipient_name": usa.get("recipient_name"),
                "usaspending_recipient_uei": usa.get("recipient_uei"),
                "usaspending_awarding_agency_code": usa.get("awarding_agency_code"),
                "usaspending_awarding_agency_name": usa.get("awarding_agency_name"),
                "usaspending_psc_code": usa.get("psc_code"),
                "usaspending_naics_code": usa.get("naics_code"),
                "usaspending_occurred_at": usa.get("occurred_at"),
                "usaspending_snippet": usa.get("snippet"),
                "summary": item.get("summary"),
                "rationale": item.get("rationale"),
            }
        )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = "candidate_joins_" + ts
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

    csv_path = export_dir / (base + ".csv")
    json_path = export_dir / (base + ".json")

    if rows_out:
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
            writer.writeheader()
            writer.writerows(rows_out)
    else:
        csv_path.write_text("", encoding="utf-8")

    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "window_days": window_days,
        "min_score": min_score,
        "limit": int(limit),
        "incumbent_only": bool(incumbent_only),
        "count": len(items),
        "items": items,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"csv": csv_path, "json": json_path, "count": len(items)}

