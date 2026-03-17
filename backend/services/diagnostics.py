from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import func, select

from backend.connectors.samgov_context import extract_sam_context_fields
from backend.db.models import Event, LeadSnapshot, LeadSnapshotItem, get_session_factory
from backend.runtime import EXPORTS_DIR
from backend.services.bundle import inspect_bundle
from backend.services.doctor import doctor_status


def _coalesce_event_ts() -> Any:
    return func.coalesce(Event.occurred_at, Event.created_at)


def _find_latest_sam_bundle(bundle_path: Optional[Path] = None) -> Optional[Path]:
    if bundle_path is not None:
        path = Path(bundle_path).expanduser()
        return path if path.exists() else None

    candidates = [
        EXPORTS_DIR / "validation" / "samgov",
        EXPORTS_DIR / "smoke" / "samgov",
    ]
    bundle_dirs: list[Path] = []
    for root in candidates:
        if not root.exists() or not root.is_dir():
            continue
        for child in root.iterdir():
            if child.is_dir():
                bundle_dirs.append(child)

    if not bundle_dirs:
        return None

    bundle_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return bundle_dirs[0]


def diagnose_samgov(
    *,
    days: int = 30,
    scan_limit: int = 5000,
    max_keywords_per_event: int = 10,
    database_url: Optional[str] = None,
    bundle_path: Optional[Path] = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    window_days = max(int(days), 1)
    since = now - timedelta(days=window_days)

    doc = doctor_status(
        days=window_days,
        source="SAM.gov",
        scan_limit=int(scan_limit),
        max_keywords_per_event=int(max_keywords_per_event),
        database_url=database_url,
    )

    latest_bundle_dir = _find_latest_sam_bundle(bundle_path=bundle_path)
    bundle_inspection: dict[str, Any] | None = None
    if latest_bundle_dir is not None:
        bundle_inspection = inspect_bundle(latest_bundle_dir)

    SessionFactory = get_session_factory(database_url)
    event_ts = _coalesce_event_ts()

    untagged_event_ids: list[int] = []
    no_entity_event_ids: list[int] = []
    no_lead_value_event_ids: list[int] = []
    low_context_event_ids: list[int] = []
    sample_doc_ids: list[str] = []

    latest_snapshot_id: Optional[int] = None
    db_query_error: Optional[str] = None
    rows: list[Any] = []

    try:
        with SessionFactory() as db:
            rows = db.execute(
                select(Event.id, Event.doc_id, Event.keywords, Event.entity_id, Event.raw_json)
                .where(event_ts >= since, Event.source == "SAM.gov")
                .order_by(event_ts.desc())
                .limit(int(scan_limit))
            ).all()

            for event_id, doc_id, keywords, entity_id, raw_json in rows:
                eid = int(event_id)
                kws = keywords if isinstance(keywords, list) else []
                if len([k for k in kws if str(k).strip()]) == 0:
                    untagged_event_ids.append(eid)
                if entity_id is None:
                    no_entity_event_ids.append(eid)

                ctx = extract_sam_context_fields(raw_json if isinstance(raw_json, dict) else {})
                present = 0
                for key in (
                    "sam_notice_type",
                    "sam_naics_code",
                    "sam_set_aside_code",
                    "sam_solicitation_number",
                    "sam_agency_path_code",
                    "sam_response_deadline",
                ):
                    if ctx.get(key):
                        present += 1
                if present < 3:
                    low_context_event_ids.append(eid)

                if doc_id:
                    sample_doc_ids.append(str(doc_id))

            latest_snapshot = db.execute(
                select(LeadSnapshot)
                .where(LeadSnapshot.source == "SAM.gov")
                .order_by(LeadSnapshot.id.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest_snapshot is not None:
                latest_snapshot_id = int(latest_snapshot.id)
                covered_ids = set(
                    int(v)
                    for (v,) in db.execute(
                        select(LeadSnapshotItem.event_id).where(LeadSnapshotItem.snapshot_id == int(latest_snapshot.id))
                    ).all()
                )
                for event_id, _doc_id, _keywords, _entity_id, _raw_json in rows:
                    eid = int(event_id)
                    if eid not in covered_ids:
                        no_lead_value_event_ids.append(eid)
            else:
                no_lead_value_event_ids = [int(event_id) for event_id, *_rest in rows]
    except Exception as exc:
        db_query_error = f"{type(exc).__name__}: {exc}"

    counts = doc.get("counts") or {}
    events_window = int(counts.get("events_window") or 0)
    keyword_cov = float((doc.get("keywords") or {}).get("coverage_pct") or 0.0)
    entity_cov = float((doc.get("entities") or {}).get("window_linked_coverage_pct") or 0.0)

    rate_limit_retries = 0
    bundle_integrity_status = None
    workflow_status = None
    bundle_quality = None
    required_failure_categories: list[str] = []
    advisory_failure_categories: list[str] = []
    if isinstance(bundle_inspection, dict):
        bundle_integrity_status = bundle_inspection.get("bundle_integrity_status") or bundle_inspection.get("status")
        workflow_status = bundle_inspection.get("workflow_status")
        manifest = bundle_inspection.get("manifest") if isinstance(bundle_inspection.get("manifest"), dict) else {}
        ingest_diag = manifest.get("ingest_diagnostics") if isinstance(manifest.get("ingest_diagnostics"), dict) else {}
        rate_limit_retries = int(ingest_diag.get("rate_limit_retries") or 0)
        quality_payload = manifest.get("quality") if isinstance(manifest.get("quality"), dict) else {}
        bundle_quality = quality_payload.get("quality")
        required_failure_categories = list(quality_payload.get("required_failure_categories") or [])
        advisory_failure_categories = list(quality_payload.get("advisory_failure_categories") or [])

    recommendations: list[str] = []
    if events_window == 0:
        recommendations.append(
            "No SAM events in window. Try a wider window/pages: ss workflow samgov-validate --days 90 --pages 5 --limit 250 --window-days 90 --json"
        )
    if len(untagged_event_ids) > 0:
        recommendations.append(
            f'Untagged SAM events detected ({len(untagged_event_ids)}). Next: ss ontology apply --path .\\examples\\ontology_sam_procurement_starter.json --days {window_days} --source "SAM.gov"'
        )
    if len(no_entity_event_ids) > 0:
        recommendations.append(
            f'Events without entities detected ({len(no_entity_event_ids)}). Next: ss entities link --source "SAM.gov" --days {window_days}'
        )
    if len(no_lead_value_event_ids) > 0:
        recommendations.append(
            f'Events missing latest lead-snapshot coverage ({len(no_lead_value_event_ids)}). Next: ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200'
        )
    if rate_limit_retries > 0:
        recommendations.append(
            "Rate-limit retries observed. Tune SAM_API_TIMEOUT_SECONDS, SAM_API_MAX_RETRIES, and SAM_API_BACKOFF_BASE for larger runs."
        )
    if latest_bundle_dir is None:
        recommendations.append(
            "No SAM bundle found. Run ss workflow samgov-smoke --json or ss workflow samgov-validate --json to generate a normalized bundle."
        )
    if db_query_error:
        recommendations.append(
            "SAM diagnostics query failed while inspecting events/snapshots. Verify DATABASE_URL and local schema health (for example: ss db init)."
        )

    if (doc.get("db") or {}).get("status") != "ok" or db_query_error:
        classification = "broken"
    elif rate_limit_retries > 0:
        classification = "rate_limited_degraded"
    elif events_window == 0:
        classification = "sparse_valid"
    elif keyword_cov < 40.0 or entity_cov < 40.0:
        classification = "partially_useful"
    else:
        classification = "healthy"

    return {
        "generated_at": now.isoformat(),
        "source": "SAM.gov",
        "classification": classification,
        "window": {
            "days": window_days,
            "since": since.isoformat(),
        },
        "doctor": doc,
        "bundle": {
            "latest_bundle_dir": latest_bundle_dir,
            "inspection": bundle_inspection,
            "bundle_integrity_status": bundle_integrity_status,
            "workflow_status": workflow_status,
            "bundle_quality": bundle_quality,
            "required_failure_categories": required_failure_categories,
            "advisory_failure_categories": advisory_failure_categories,
        },
        "gaps": {
            "untagged_events": len(untagged_event_ids),
            "events_without_entities": len(no_entity_event_ids),
            "events_without_lead_value": len(no_lead_value_event_ids),
            "low_context_events": len(low_context_event_ids),
            "latest_snapshot_id": latest_snapshot_id,
            "sample_doc_ids": sample_doc_ids[:10],
            "sample_untagged_event_ids": untagged_event_ids[:20],
            "sample_no_entity_event_ids": no_entity_event_ids[:20],
            "sample_no_lead_value_event_ids": no_lead_value_event_ids[:20],
        },
        "rate_limit_retries": rate_limit_retries,
        "db_query_error": db_query_error,
        "recommendations": recommendations,
    }


__all__ = ["diagnose_samgov"]
