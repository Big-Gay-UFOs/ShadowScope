from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.exc import OperationalError

from backend.connectors.samgov_context import extract_sam_context_fields
from backend.db.models import (
    AnalysisRun,
    Correlation,
    Entity,
    Event,
    IngestRun,
    LeadSnapshot,
    LeadSnapshotItem,
    get_engine,
    get_session_factory,
)
from backend.services.entities import _extract_identity


def _top_counter_rows(counter: Counter[str], key_name: str, limit: int = 10) -> list[dict[str, Any]]:
    return [{key_name: k, "count": int(v)} for k, v in counter.most_common(int(limit))]


def doctor_status(
    *,
    days: int = 30,
    source: str | None = "USAspending",
    scan_limit: int = 5000,
    max_keywords_per_event: int = 10,
    database_url: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    window_days = max(int(days), 1)
    since = now - timedelta(days=window_days)

    try:
        engine = get_engine(database_url)
        safe_url = engine.url.render_as_string(hide_password=True)
    except Exception as e:
        return {
            "db": {"status": "error", "url": None, "error": str(e)},
            "window": {"days": window_days, "since": since.isoformat(), "source": source or "*"},
            "hints": ["Unable to create DB engine. Check DATABASE_URL / connectivity."],
        }

    SessionFactory = get_session_factory(database_url)

    def _count(db, stmt) -> int:
        return int(db.execute(stmt).scalar_one())

    hints: list[str] = []

    try:
        with SessionFactory() as db:
            # --- Totals ---
            events_total = _count(db, select(func.count()).select_from(Event))
            entities_total = _count(db, select(func.count()).select_from(Entity))
            correlations_total = _count(db, select(func.count()).select_from(Correlation))
            snapshots_total = _count(db, select(func.count()).select_from(LeadSnapshot))
            snapshot_items_total = _count(db, select(func.count()).select_from(LeadSnapshotItem))

            # --- Windowed events ---
            event_ts = func.coalesce(Event.occurred_at, Event.created_at)
            ev_where = [event_ts >= since]
            if source:
                ev_where.append(Event.source == source)

            events_window = _count(db, select(func.count()).select_from(Event).where(*ev_where))
            events_with_entity_window = _count(
                db,
                select(func.count()).select_from(Event).where(*ev_where, Event.entity_id.isnot(None)),
            )
            entity_window_coverage_pct = (
                round((events_with_entity_window / events_window) * 100.0, 1) if events_window else 0.0
            )

            # --- Correlations by lane (use correlation_key prefixes; include both source and '*' forms) ---
            lane_prefixes = ["kw_pair", "same_keyword", "same_uei", "same_entity", "same_sam_naics"]
            lane_counts: dict[str, int] = {}
            for lane in lane_prefixes:
                if source:
                    patterns = [f"{lane}|{source}|%", f"{lane}|*|%"]
                else:
                    patterns = [f"{lane}|%"]
                lane_counts[lane] = _count(
                    db,
                    select(func.count())
                    .select_from(Correlation)
                    .where(
                        Correlation.window_days == window_days,
                        Correlation.correlation_key.isnot(None),
                        or_(*[Correlation.correlation_key.like(p) for p in patterns]),
                    ),
                )

            # --- Sample recent events for keyword/entity/context diagnostics ---
            q = (
                select(Event.id, Event.keywords, Event.entity_id, Event.raw_json, Event.source)
                .where(*ev_where)
                .order_by(event_ts.desc())
                .limit(int(scan_limit))
            )
            rows = db.execute(q).all()

            scanned_events = len(rows)
            events_with_keywords = 0
            events_keywords_gt_max = 0
            events_with_identity_signal = 0
            events_with_identity_signal_linked = 0
            events_with_name_signal = 0
            events_with_name_signal_linked = 0
            kw_counter: Counter[str] = Counter()

            sam_context_scanned_events = 0
            sam_context_depth_total = 0
            events_with_research_context = 0
            events_with_core_procurement_context = 0
            sam_context_field_counts: Counter[str] = Counter()
            sam_notice_type_counter: Counter[str] = Counter()
            sam_naics_counter: Counter[str] = Counter()
            sam_set_aside_counter: Counter[str] = Counter()

            sam_context_fields = [
                "sam_agency_path_code",
                "sam_notice_type",
                "sam_solicitation_number",
                "sam_naics_code",
                "sam_set_aside_code",
                "sam_response_deadline",
            ]
            sam_core_fields = [
                "sam_notice_type",
                "sam_naics_code",
                "sam_set_aside_code",
                "sam_solicitation_number",
            ]

            for _eid, keywords, _entity_id, raw_json, row_source in rows:
                kws: list[str] = []
                if isinstance(keywords, list):
                    kws = [str(x) for x in keywords if x is not None and str(x).strip() != ""]
                if kws:
                    events_with_keywords += 1
                    kw_counter.update(kws)
                if len(kws) > int(max_keywords_per_event):
                    events_keywords_gt_max += 1

                identity_signal = False
                name_signal = False
                try:
                    ident = _extract_identity(raw_json, row_source or (source or "USAspending"))
                    if isinstance(ident, dict):
                        meta = ident.get("meta")
                        identity_signal = bool(
                            ident.get("uei")
                            or ident.get("duns")
                            or ident.get("cage")
                            or ident.get("recipient_id")
                            or (isinstance(meta, dict) and meta.get("sam_parent_path_code"))
                        )
                        name_signal = bool(ident.get("name"))
                except Exception:
                    identity_signal = False
                    name_signal = False

                if identity_signal:
                    events_with_identity_signal += 1
                    if _entity_id is not None:
                        events_with_identity_signal_linked += 1
                if name_signal:
                    events_with_name_signal += 1
                    if _entity_id is not None:
                        events_with_name_signal_linked += 1

                row_is_sam = (row_source == "SAM.gov") or (source == "SAM.gov")
                if row_is_sam:
                    ctx = extract_sam_context_fields(raw_json if isinstance(raw_json, dict) else {})
                    sam_context_scanned_events += 1

                    present_count = 0
                    for field_name in sam_context_fields:
                        if ctx.get(field_name):
                            present_count += 1
                            sam_context_field_counts[field_name] += 1
                    sam_context_depth_total += present_count

                    core_present_count = 0
                    for field_name in sam_core_fields:
                        if ctx.get(field_name):
                            core_present_count += 1
                    if present_count >= 3:
                        events_with_research_context += 1
                    if core_present_count >= 2:
                        events_with_core_procurement_context += 1

                    notice_type = ctx.get("sam_notice_type")
                    if notice_type:
                        sam_notice_type_counter[str(notice_type)] += 1
                    naics_code = ctx.get("sam_naics_code")
                    if naics_code:
                        sam_naics_counter[str(naics_code)] += 1
                    set_aside_code = ctx.get("sam_set_aside_code")
                    if set_aside_code:
                        sam_set_aside_counter[str(set_aside_code)] += 1

            unique_keywords = len(kw_counter)
            top_keywords = [{"keyword": k, "count": int(v)} for k, v in kw_counter.most_common(10)]
            coverage_pct = round((events_with_keywords / scanned_events) * 100.0, 1) if scanned_events else 0.0
            identity_signal_coverage_pct = (
                round((events_with_identity_signal_linked / events_with_identity_signal) * 100.0, 1)
                if events_with_identity_signal
                else 0.0
            )
            name_signal_coverage_pct = (
                round((events_with_name_signal_linked / events_with_name_signal) * 100.0, 1)
                if events_with_name_signal
                else 0.0
            )

            if sam_context_scanned_events:
                avg_context_fields_per_event = round(sam_context_depth_total / sam_context_scanned_events, 2)
                research_context_coverage_pct = round(
                    (events_with_research_context / sam_context_scanned_events) * 100.0, 1
                )
                core_procurement_context_coverage_pct = round(
                    (events_with_core_procurement_context / sam_context_scanned_events) * 100.0, 1
                )
                sam_context_coverage_by_field = {
                    k: round((sam_context_field_counts.get(k, 0) / sam_context_scanned_events) * 100.0, 1)
                    for k in sam_context_fields
                }
            else:
                avg_context_fields_per_event = 0.0
                research_context_coverage_pct = 0.0
                core_procurement_context_coverage_pct = 0.0
                sam_context_coverage_by_field = {k: 0.0 for k in sam_context_fields}

            top_notice_types = _top_counter_rows(sam_notice_type_counter, "notice_type", limit=10)
            top_naics_codes = _top_counter_rows(sam_naics_counter, "naics_code", limit=10)
            top_set_aside_codes = _top_counter_rows(sam_set_aside_counter, "set_aside_code", limit=10)

            # --- Last runs ---
            ingest_q = select(IngestRun).order_by(IngestRun.id.desc())
            if source:
                ingest_q = ingest_q.where(IngestRun.source == source)
            ingest = db.execute(ingest_q.limit(1)).scalars().first()

            ont_q = (
                select(AnalysisRun)
                .where(AnalysisRun.analysis_type == "ontology_apply")
                .order_by(AnalysisRun.id.desc())
            )
            if source:
                ont_q = ont_q.where(AnalysisRun.source == source)
            ontology_apply = db.execute(ont_q.limit(1)).scalars().first()

            snap_q = select(LeadSnapshot).order_by(LeadSnapshot.id.desc())
            if source:
                snap_q = snap_q.where(LeadSnapshot.source == source)
            lead_snapshot = db.execute(snap_q.limit(1)).scalars().first()

            lead_snapshot_items = 0
            if lead_snapshot is not None:
                lead_snapshot_items = _count(
                    db,
                    select(func.count()).select_from(LeadSnapshotItem).where(LeadSnapshotItem.snapshot_id == lead_snapshot.id),
                )

    except OperationalError as e:
        return {
            "db": {"status": "error", "url": safe_url, "error": str(e)},
            "window": {"days": window_days, "since": since.isoformat(), "source": source or "*"},
            "hints": ["Database schema missing or inaccessible. Try: ss db init"],
        }

    # --- Hints / failure heuristics ---
    hint_source = source or "USAspending"
    if events_total == 0:
        hints.append("No events in DB. Try: ss ingest usaspending --days 30 --pages 1")
    elif events_window == 0:
        hints.append(f"No events in last {window_days} days for source={source or '*'}; increase --days or run ingest.")

    if events_window > 0 and events_with_keywords == 0:
        hints.append(
            f'No keywords tagged on recent events. Try: ss ontology apply --path ontology.json --days {window_days} --source "{hint_source}"'
        )

    if lane_counts.get("kw_pair", 0) == 0:
        if events_with_keywords == 0:
            hints.append("kw_pair correlations require keywords. Run ontology apply first, then rebuild keyword-pairs.")
        else:
            hints.append(
                f'No kw_pair correlations found. Try: ss correlate rebuild-keyword-pairs --window-days {window_days} --source "{hint_source}" --min-events 2'
            )
            if scanned_events and (events_keywords_gt_max / scanned_events) >= 0.2:
                hints.append(
                    f"Many events have >{int(max_keywords_per_event)} keywords; pair explosion guard may suppress pairs. Consider raising --max-keywords-per-event or tightening ontology."
                )

    if events_window > 0 and events_with_entity_window == 0:
        hints.append(f'No entities linked on recent events. Try: ss entities link --source "{hint_source}" --days {window_days}')
    elif events_window > 0 and entity_window_coverage_pct < 25.0:
        hints.append(
            f'Entity coverage is low ({entity_window_coverage_pct}%). Try: ss entities link --source "{hint_source}" --days {window_days}'
        )

    if events_with_identity_signal > 0 and identity_signal_coverage_pct < 50.0:
        hints.append(
            "Low identity-based entity linkage in sampled events. Inspect recipient_id/UEI/CAGE fields in raw_json and rerun entity linking."
        )

    sam_mode = (source == "SAM.gov") or (source is None and sam_context_scanned_events > 0)
    if sam_mode and sam_context_scanned_events > 0:
        if events_with_research_context == 0:
            hints.append(
                "SAM context depth is low in sampled events. Verify SAM normalization and inspect canonical sam_* fields in raw_json."
            )
        naics_pct = sam_context_coverage_by_field.get("sam_naics_code", 0.0)
        if naics_pct < 30.0:
            hints.append(
                "SAM NAICS coverage in sampled events is low. Verify upstream fields and review sam_naics_code extraction in normalization."
            )

    if lead_snapshot is None:
        hints.append(f'No lead snapshots found. Try: ss leads snapshot --source "{hint_source}" --min-score 1 --limit 200')

    payload: dict[str, Any] = {
        "db": {"status": "ok", "url": safe_url},
        "window": {"days": window_days, "since": since.isoformat(), "source": source or "*"},
        "counts": {
            "events_total": int(events_total),
            "events_window": int(events_window),
            "events_with_entity_window": int(events_with_entity_window),
            "entities_total": int(entities_total),
            "correlations_total": int(correlations_total),
            "lead_snapshots_total": int(snapshots_total),
            "lead_snapshot_items_total": int(snapshot_items_total),
        },
        "entities": {
            "window_linked_coverage_pct": float(entity_window_coverage_pct),
            "sample_scanned_events": int(scanned_events),
            "sample_events_with_identity_signal": int(events_with_identity_signal),
            "sample_events_with_identity_signal_linked": int(events_with_identity_signal_linked),
            "sample_identity_signal_coverage_pct": float(identity_signal_coverage_pct),
            "sample_events_with_name": int(events_with_name_signal),
            "sample_events_with_name_linked": int(events_with_name_signal_linked),
            "sample_name_coverage_pct": float(name_signal_coverage_pct),
        },
        "keywords": {
            "scanned_events": int(scanned_events),
            "events_with_keywords": int(events_with_keywords),
            "coverage_pct": float(coverage_pct),
            "unique_keywords": int(unique_keywords),
            "top_keywords": top_keywords,
            "events_keywords_gt_max": int(events_keywords_gt_max),
            "max_keywords_per_event": int(max_keywords_per_event),
        },
        "sam_context": {
            "scanned_events": int(sam_context_scanned_events),
            "events_with_research_context": int(events_with_research_context),
            "research_context_coverage_pct": float(research_context_coverage_pct),
            "events_with_core_procurement_context": int(events_with_core_procurement_context),
            "core_procurement_context_coverage_pct": float(core_procurement_context_coverage_pct),
            "avg_context_fields_per_event": float(avg_context_fields_per_event),
            "coverage_by_field_pct": sam_context_coverage_by_field,
            "top_notice_types": top_notice_types,
            "top_naics_codes": top_naics_codes,
            "top_set_aside_codes": top_set_aside_codes,
        },
        "correlations": {"by_lane": lane_counts},
        "last_runs": {
            "ingest": (
                {
                    "id": int(ingest.id),
                    "source": ingest.source,
                    "status": ingest.status,
                    "started_at": ingest.started_at.isoformat() if ingest.started_at else None,
                    "ended_at": ingest.ended_at.isoformat() if ingest.ended_at else None,
                    "fetched": int(ingest.fetched or 0),
                    "inserted": int(ingest.inserted or 0),
                    "normalized": int(ingest.normalized or 0),
                }
                if ingest
                else None
            ),
            "ontology_apply": (
                {
                    "id": int(ontology_apply.id),
                    "status": ontology_apply.status,
                    "source": ontology_apply.source,
                    "days": int(ontology_apply.days or 0),
                    "scanned": int(ontology_apply.scanned or 0),
                    "updated": int(ontology_apply.updated or 0),
                    "unchanged": int(ontology_apply.unchanged or 0),
                    "ended_at": ontology_apply.ended_at.isoformat() if ontology_apply.ended_at else None,
                }
                if ontology_apply
                else None
            ),
            "lead_snapshot": (
                {
                    "id": int(lead_snapshot.id),
                    "source": lead_snapshot.source,
                    "created_at": lead_snapshot.created_at.isoformat() if lead_snapshot.created_at else None,
                    "items": int(lead_snapshot_items),
                }
                if lead_snapshot
                else None
            ),
        },
        "hints": hints,
    }

    return payload
