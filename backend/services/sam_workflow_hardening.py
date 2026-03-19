from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.leads import DEFAULT_SCORING_VERSION
from backend.services.bundle import (
    SAM_BUNDLE_RESULTS_DIR,
    SAM_BUNDLE_VERSION,
    flatten_bundle_files,
    normalize_sam_exports,
    render_sam_bundle_report,
    write_bundle_manifest,
)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_validation_mode(mode: str) -> str:
    value = str(mode or "smoke").strip().lower()
    return value if value in {"smoke", "larger"} else "smoke"


def _classify_sam_quality(
    *,
    failed_required_checks: list[dict[str, Any]],
    warning_checks: list[dict[str, Any]],
    events_window: int,
    events_with_keywords: int,
    events_with_entity: int,
    snapshot_items: int,
    ingest_nonzero: bool,
    ingest_request_diag: dict[str, Any],
) -> dict[str, Any]:
    rate_limit_retries = _safe_int(ingest_request_diag.get("rate_limit_retries"))
    retry_attempts_total = _safe_int(ingest_request_diag.get("retry_attempts_total"))

    partially_useful = (
        events_window > 0
        and (
            events_with_keywords > 0
            or events_with_entity > 0
            or snapshot_items > 0
        )
    )

    reason_codes: list[str] = []

    if failed_required_checks:
        quality = "hard_failure"
        reason_codes.extend([f"required_check_failed:{item.get('name')}" for item in failed_required_checks])
    elif warning_checks:
        if rate_limit_retries > 0:
            quality = "rate_limited_degraded"
        elif partially_useful:
            quality = "partially_useful"
        elif events_window > 0:
            quality = "sparse_valid"
        else:
            quality = "degraded"
        reason_codes.extend([f"warning_check_failed:{item.get('name')}" for item in warning_checks])
    else:
        if not ingest_nonzero and events_window == 0:
            quality = "sparse_valid"
            reason_codes.append("empty_window_or_no_fresh_ingest")
        elif rate_limit_retries > 0 and retry_attempts_total > 0:
            quality = "rate_limited_degraded"
            reason_codes.append("rate_limit_retries_observed")
        else:
            quality = "healthy"

    operator_messages: list[str] = []
    if rate_limit_retries > 0:
        operator_messages.append(
            "SAM.gov retries/429s were observed. Consider tuning SAM_API_TIMEOUT_SECONDS, "
            "SAM_API_MAX_RETRIES, and SAM_API_BACKOFF_BASE for larger windows."
        )
    if events_window == 0:
        operator_messages.append(
            "No SAM.gov events were found in the diagnostic window. This can be sparse-valid, "
            "or may require larger --days/--pages."
        )
    elif partially_useful and (warning_checks or failed_required_checks):
        operator_messages.append(
            "Run produced usable artifacts but some quality gates missed thresholds. Treat as partially useful."
        )

    return {
        "quality": quality,
        "partially_useful": bool(partially_useful),
        "rate_limit_retries": rate_limit_retries,
        "retry_attempts_total": retry_attempts_total,
        "reason_codes": reason_codes,
        "operator_messages": operator_messages,
    }


def run_samgov_smoke_workflow_hardened(
    *,
    ingest_days: int = 30,
    pages: int = 2,
    page_size: int = 100,
    max_records: Optional[int] = 50,
    start_page: int = 1,
    keywords: Optional[list[str]] = None,
    api_key: Optional[str] = None,
    ontology_path: Path = Path("examples/ontology_sam_procurement_starter.json"),
    ontology_days: int = 30,
    entity_days: int = 30,
    entity_batch: int = 500,
    window_days: int = 30,
    min_events_entity: int = 2,
    min_events_keywords: int = 2,
    max_events_keywords: int = 200,
    max_keywords_per_event: int = 10,
    min_score: int = 1,
    snapshot_limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    compare_scoring_versions: Optional[list[str]] = None,
    notes: Optional[str] = None,
    bundle_root: Optional[Path] = None,
    database_url: Optional[str] = None,
    require_nonzero: bool = True,
    skip_ingest: bool = False,
    threshold_overrides: Optional[dict[str, Any]] = None,
    validation_mode: str = "smoke",
    workflow_type: str = "samgov-smoke",
) -> dict[str, Any]:
    from backend.services import workflow as workflow_module

    ensure_runtime_directories()
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    mode = _normalize_validation_mode(validation_mode)

    default_root = EXPORTS_DIR / "smoke" / "samgov"
    if mode == "larger":
        default_root = EXPORTS_DIR / "validation" / "samgov"
    root = Path(bundle_root).expanduser() if bundle_root else default_root
    bundle_dir = root / stamp
    bundle_dir.mkdir(parents=True, exist_ok=True)

    workflow_error: Optional[str] = None
    try:
        workflow_res = workflow_module.run_samgov_workflow(
            ingest_days=int(ingest_days),
            pages=int(pages),
            page_size=int(page_size),
            max_records=max_records,
            start_page=int(start_page),
            keywords=keywords,
            api_key=api_key,
            ontology_path=Path(ontology_path),
            ontology_days=int(ontology_days),
            entity_days=int(entity_days),
            entity_batch=int(entity_batch),
            window_days=int(window_days),
            min_events_entity=int(min_events_entity),
            min_events_keywords=int(min_events_keywords),
            max_events_keywords=int(max_events_keywords),
            max_keywords_per_event=int(max_keywords_per_event),
            min_score=int(min_score),
            snapshot_limit=int(snapshot_limit),
            scan_limit=int(scan_limit),
            scoring_version=str(scoring_version),
            compare_scoring_versions=list(compare_scoring_versions or []),
            notes=notes,
            output=bundle_dir / "exports" / "samgov_bundle.csv",
            export_events_flag=True,
            database_url=database_url,
            skip_ingest=bool(skip_ingest),
            abort_on_ingest_skip=True,
        )
    except Exception as exc:
        workflow_error = str(exc)
        workflow_res = {
            "source": "SAM.gov",
            "status": "failed",
            "error": workflow_error,
            "exports": {},
        }

    if isinstance(workflow_res, dict):
        workflow_res["exports"] = normalize_sam_exports(
            workflow_exports=(workflow_res.get("exports") if isinstance(workflow_res.get("exports"), dict) else {}),
            bundle_dir=bundle_dir,
        )

    status = workflow_res.get("status") if isinstance(workflow_res, dict) else None
    ingest = workflow_res.get("ingest") if isinstance(workflow_res, dict) else {}
    ingest_nonzero = (
        _safe_int((ingest or {}).get("fetched")) > 0
        or _safe_int((ingest or {}).get("inserted")) > 0
        or _safe_int((ingest or {}).get("normalized")) > 0
    )

    thresholds = workflow_module._resolve_sam_smoke_thresholds(threshold_overrides)
    threshold_required = mode == "smoke"

    doc = workflow_module.doctor_status(
        days=int(window_days),
        source="SAM.gov",
        scan_limit=int(scan_limit),
        max_keywords_per_event=int(max_keywords_per_event),
        database_url=database_url,
    )
    db_ok = ((doc.get("db") or {}).get("status") == "ok")
    counts = doc.get("counts") or {}
    kw = doc.get("keywords") or {}
    entities_diag = doc.get("entities") or {}
    lane_counts = ((doc.get("correlations") or {}).get("by_lane")) or {}
    sam_ctx = doc.get("sam_context") or {}
    coverage_by_field_pct = sam_ctx.get("coverage_by_field_pct") or {}
    snapshot_items = _safe_int((workflow_res.get("snapshot") or {}).get("items"))

    events_window = _safe_int(counts.get("events_window"))
    events_with_keywords = _safe_int(kw.get("events_with_keywords"))
    keywords_scanned_events = _safe_int(kw.get("scanned_events"))
    events_with_entity = _safe_int(counts.get("events_with_entity_window"))
    same_keyword_lane = _safe_int(lane_counts.get("same_keyword"))
    kw_pair_lane = _safe_int(lane_counts.get("kw_pair"))
    same_sam_naics_lane = _safe_int(lane_counts.get("same_sam_naics"))
    keyword_signal_total = same_keyword_lane + kw_pair_lane

    events_with_research_context = _safe_int(sam_ctx.get("events_with_research_context"))
    research_context_coverage_pct = _safe_float(sam_ctx.get("research_context_coverage_pct"))
    events_with_core_procurement_context = _safe_int(sam_ctx.get("events_with_core_procurement_context"))
    core_procurement_context_coverage_pct = _safe_float(sam_ctx.get("core_procurement_context_coverage_pct"))
    avg_context_fields_per_event = _safe_float(sam_ctx.get("avg_context_fields_per_event"))

    sam_notice_type_coverage_pct = _safe_float(coverage_by_field_pct.get("sam_notice_type"))
    sam_solicitation_number_coverage_pct = _safe_float(coverage_by_field_pct.get("sam_solicitation_number"))
    sam_naics_code_coverage_pct = _safe_float(coverage_by_field_pct.get("sam_naics_code"))

    raw_keywords_coverage = kw.get("coverage_pct")
    if raw_keywords_coverage is None:
        keywords_coverage_pct = (
            round((events_with_keywords / keywords_scanned_events) * 100.0, 1) if keywords_scanned_events else 0.0
        )
    else:
        keywords_coverage_pct = _safe_float(raw_keywords_coverage)
    entity_coverage_pct = round((events_with_entity / events_window) * 100.0, 1) if events_window else 0.0

    smoke_tune_cmd = (
        f"ss workflow samgov-smoke --days {int(window_days)} --pages {max(int(pages), 2)} "
        f"--limit {max(_safe_int(max_records, 50), 50)} --window-days {int(window_days)} --json"
    )
    larger_validate_cmd = (
        f"ss workflow samgov-validate --days {max(int(window_days), 30)} --pages {max(int(pages), 5)} "
        f"--limit {max(_safe_int(max_records, 250), 250)} --window-days {int(window_days)} --json"
    )
    doctor_cmd = f'ss doctor status --source "SAM.gov" --days {int(window_days)} --json'
    rebuild_keywords_cmd = (
        f'ss correlate rebuild-keyword-pairs --window-days {int(window_days)} --source "SAM.gov" '
        f"--min-events {int(min_events_keywords)} --max-events {int(max_events_keywords)}"
    )
    rebuild_entities_cmd = f'ss entities link --source "SAM.gov" --days {int(window_days)}'
    rebuild_naics_cmd = (
        f'ss correlate rebuild-sam-naics --window-days {int(window_days)} --source "SAM.gov" '
        f"--min-events {int(min_events_keywords)} --max-events {int(max_events_keywords)}"
    )
    rerun_snapshot_cmd = f'ss leads snapshot --source "SAM.gov" --min-score {int(min_score)} --limit {int(snapshot_limit)}'

    checks: list[dict[str, Any]] = []
    checks.append(
        {
            "name": "doctor_db_ok",
            "required": True,
            "ok": bool(db_ok),
            "status": "pass" if db_ok else "fail",
            "observed": (doc.get("db") or {}).get("status"),
            "actual": (doc.get("db") or {}).get("status"),
            "expected": "ok",
            "why": "SAM.gov workflow diagnostics require DB health to read quality signals.",
            "hint": doctor_cmd,
        }
    )

    if workflow_error:
        checks.append(
            {
                "name": "workflow_execution",
                "required": True,
                "ok": False,
                "status": "fail",
                "observed": workflow_error,
                "actual": workflow_error,
                "expected": "workflow executes without exception",
                "why": "The workflow failed before all artifact stages completed.",
                "hint": smoke_tune_cmd if mode == "smoke" else larger_validate_cmd,
            }
        )

    if skip_ingest:
        checks.append(
            {
                "name": "ingest_nonzero",
                "required": False,
                "ok": True,
                "status": "info",
                "observed": "skipped",
                "actual": "skipped",
                "expected": "skip_ingest=True",
                "why": "Ingest was intentionally skipped for offline replay.",
                "hint": smoke_tune_cmd,
            }
        )
    else:
        ingest_ok = bool(ingest_nonzero) and status != "skipped"
        checks.append(
            {
                "name": "ingest_nonzero",
                "required": True,
                "ok": ingest_ok,
                "status": "pass" if ingest_ok else "fail",
                "observed": {
                    "status": (ingest or {}).get("status"),
                    "fetched": _safe_int((ingest or {}).get("fetched")),
                    "inserted": _safe_int((ingest or {}).get("inserted")),
                    "normalized": _safe_int((ingest or {}).get("normalized")),
                },
                "actual": {
                    "status": (ingest or {}).get("status"),
                    "fetched": _safe_int((ingest or {}).get("fetched")),
                    "inserted": _safe_int((ingest or {}).get("inserted")),
                    "normalized": _safe_int((ingest or {}).get("normalized")),
                },
                "expected": "fetched>0 OR inserted>0 OR normalized>0",
                "why": "Fresh SAM.gov ingest keeps validation checks tied to current market movement.",
                "hint": smoke_tune_cmd if mode == "smoke" else larger_validate_cmd,
            }
        )

    ingest_request_diag = (ingest.get("request_diagnostics") if isinstance(ingest, dict) else {}) or {}
    rate_limit_retries = _safe_int(ingest_request_diag.get("rate_limit_retries"))
    retry_attempts_total = _safe_int(ingest_request_diag.get("retry_attempts_total"))
    checks.append(
        {
            "name": "ingest_retry_pressure",
            "required": False,
            "ok": rate_limit_retries == 0,
            "status": "pass" if rate_limit_retries == 0 else "info",
            "observed": {
                "retry_attempts_total": retry_attempts_total,
                "rate_limit_retries": rate_limit_retries,
                "retry_sleep_seconds_total": _safe_float(ingest_request_diag.get("retry_sleep_seconds_total")),
            },
            "actual": {
                "retry_attempts_total": retry_attempts_total,
                "rate_limit_retries": rate_limit_retries,
                "retry_sleep_seconds_total": _safe_float(ingest_request_diag.get("retry_sleep_seconds_total")),
            },
            "expected": "rate_limit_retries == 0 (best case)",
            "why": "Retries/429s can make larger SAM windows slow while still producing usable output.",
            "hint": "Tune SAM_API_TIMEOUT_SECONDS, SAM_API_MAX_RETRIES, and SAM_API_BACKOFF_BASE.",
        }
    )

    threshold_specs: list[dict[str, Any]] = [
        {
            "name": "events_window_threshold",
            "observed": events_window,
            "threshold": thresholds["events_window_min"],
            "why": "Too few SAM.gov events in-window weakens research confidence and lane stability.",
            "hint": smoke_tune_cmd if mode == "smoke" else larger_validate_cmd,
        },
        {
            "name": "events_with_keywords_coverage_threshold",
            "observed": keywords_coverage_pct,
            "threshold": thresholds["events_with_keywords_coverage_pct_min"],
            "unit": "%",
            "actual": {
                "events_with_keywords": events_with_keywords,
                "sample_scanned_events": keywords_scanned_events,
                "coverage_pct": keywords_coverage_pct,
            },
            "why": "Low keyword coverage reduces thematic signal quality for SAM.gov research pivots.",
            "hint": doctor_cmd,
        },
        {
            "name": "events_with_entity_coverage_threshold",
            "observed": entity_coverage_pct,
            "threshold": thresholds["events_with_entity_coverage_pct_min"],
            "unit": "%",
            "actual": {
                "events_with_entity_window": events_with_entity,
                "events_window": events_window,
                "coverage_pct": entity_coverage_pct,
            },
            "why": "Low entity linkage coverage weakens recipient-level SAM.gov targeting and triage.",
            "hint": rebuild_entities_cmd,
        },
        {
            "name": "keyword_or_kw_pair_signal_threshold",
            "observed": keyword_signal_total,
            "threshold": thresholds["keyword_signal_total_min"],
            "actual": {
                "same_keyword": same_keyword_lane,
                "kw_pair": kw_pair_lane,
                "signal_total": keyword_signal_total,
            },
            "why": "Weak keyword lanes reduce confidence that related SAM.gov opportunities are clustering.",
            "hint": rebuild_keywords_cmd,
        },
        {
            "name": "sam_research_context_events_threshold",
            "observed": events_with_research_context,
            "threshold": thresholds["events_with_research_context_min"],
            "why": "Research-context event depth supports fast analyst pivots inside SAM.gov notices.",
            "hint": doctor_cmd,
        },
        {
            "name": "sam_research_context_coverage_threshold",
            "observed": research_context_coverage_pct,
            "threshold": thresholds["research_context_coverage_pct_min"],
            "unit": "%",
            "why": "Low SAM.gov research-context coverage limits usefulness of downstream lead prioritization.",
            "hint": doctor_cmd,
        },
        {
            "name": "sam_core_procurement_context_events_threshold",
            "observed": events_with_core_procurement_context,
            "threshold": thresholds["events_with_core_procurement_context_min"],
            "why": "Core procurement context counts drive high-signal filtering for SAM.gov opportunities.",
            "hint": doctor_cmd,
        },
        {
            "name": "sam_core_procurement_context_coverage_threshold",
            "observed": core_procurement_context_coverage_pct,
            "threshold": thresholds["core_procurement_context_coverage_pct_min"],
            "unit": "%",
            "why": "Core procurement context coverage indicates whether notices are usable for operator triage.",
            "hint": doctor_cmd,
        },
        {
            "name": "sam_avg_context_fields_threshold",
            "observed": avg_context_fields_per_event,
            "threshold": thresholds["avg_context_fields_per_event_min"],
            "why": "Average SAM.gov context depth tracks how actionable each event is for research.",
            "hint": doctor_cmd,
        },
        {
            "name": "sam_notice_type_coverage_threshold",
            "observed": sam_notice_type_coverage_pct,
            "threshold": thresholds["sam_notice_type_coverage_pct_min"],
            "unit": "%",
            "why": "Notice type coverage is required for reliable procurement-stage interpretation.",
            "hint": doctor_cmd,
        },
        {
            "name": "sam_solicitation_number_coverage_threshold",
            "observed": sam_solicitation_number_coverage_pct,
            "threshold": thresholds["sam_solicitation_number_coverage_pct_min"],
            "unit": "%",
            "why": "Solicitation number coverage is required for stable dedupe and follow-up targeting.",
            "hint": doctor_cmd,
        },
        {
            "name": "sam_naics_coverage_threshold",
            "observed": sam_naics_code_coverage_pct,
            "threshold": thresholds["sam_naics_code_coverage_pct_min"],
            "unit": "%",
            "why": "NAICS coverage is required for industry scoping and same_sam_naics lane trust.",
            "hint": doctor_cmd,
        },
        {
            "name": "same_sam_naics_lane_threshold",
            "observed": same_sam_naics_lane,
            "threshold": thresholds["same_sam_naics_lane_min"],
            "actual": {"same_sam_naics": same_sam_naics_lane},
            "why": "The same_sam_naics lane validates industry-based clustering that analysts rely on.",
            "hint": rebuild_naics_cmd,
        },
        {
            "name": "snapshot_items_threshold",
            "observed": snapshot_items,
            "threshold": thresholds["snapshot_items_min"],
            "why": "Lead snapshots must contain actionable SAM.gov rows for operator review.",
            "hint": rerun_snapshot_cmd,
        },
    ]

    for spec in threshold_specs:
        checks.append(
            workflow_module._threshold_check(
                name=spec["name"],
                observed=spec["observed"],
                threshold=spec["threshold"],
                required=threshold_required,
                unit=str(spec.get("unit") or ""),
                why=str(spec["why"]),
                hint=str(spec["hint"]),
                actual=spec.get("actual"),
            )
        )

    if mode == "larger":
        checks.append(
            workflow_module._threshold_check(
                name="larger_run_window_signal",
                observed=events_window,
                threshold=max(10.0, thresholds["events_window_min"]),
                required=False,
                why="Large-window validation expects more than trivial volume but can still be sparse-valid.",
                hint=larger_validate_cmd,
            )
        )

    failed_required_checks = [c for c in checks if bool(c.get("required", True)) and not bool(c.get("ok"))]
    warning_checks = [c for c in checks if not bool(c.get("required", True)) and not bool(c.get("ok"))]
    smoke_passed = len(failed_required_checks) == 0

    quality = _classify_sam_quality(
        failed_required_checks=failed_required_checks,
        warning_checks=warning_checks,
        events_window=events_window,
        events_with_keywords=events_with_keywords,
        events_with_entity=events_with_entity,
        snapshot_items=snapshot_items,
        ingest_nonzero=bool(ingest_nonzero),
        ingest_request_diag=ingest_request_diag,
    )

    baseline = {
        "captured_at": now.isoformat(),
        "source": "SAM.gov",
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": list(compare_scoring_versions or []),
        "window_days": int(window_days),
        "counts": {
            "events_window": _safe_int(counts.get("events_window")),
            "events_with_entity_window": _safe_int(counts.get("events_with_entity_window")),
            "lead_snapshots_total": _safe_int(counts.get("lead_snapshots_total")),
        },
        "entity_coverage": {
            "window_linked_coverage_pct": entities_diag.get("window_linked_coverage_pct"),
            "sample_scanned_events": _safe_int(entities_diag.get("sample_scanned_events")),
            "sample_events_with_identity_signal": _safe_int(entities_diag.get("sample_events_with_identity_signal")),
            "sample_events_with_identity_signal_linked": _safe_int(entities_diag.get("sample_events_with_identity_signal_linked")),
            "sample_identity_signal_coverage_pct": entities_diag.get("sample_identity_signal_coverage_pct"),
            "sample_events_with_name": _safe_int(entities_diag.get("sample_events_with_name")),
            "sample_events_with_name_linked": _safe_int(entities_diag.get("sample_events_with_name_linked")),
            "sample_name_coverage_pct": entities_diag.get("sample_name_coverage_pct"),
        },
        "keyword_coverage": {
            "scanned_events": _safe_int(kw.get("scanned_events")),
            "events_with_keywords": _safe_int(kw.get("events_with_keywords")),
            "coverage_pct": kw.get("coverage_pct"),
            "unique_keywords": _safe_int(kw.get("unique_keywords")),
        },
        "correlations_by_lane": {
            "same_entity": _safe_int(lane_counts.get("same_entity")),
            "same_uei": _safe_int(lane_counts.get("same_uei")),
            "same_keyword": _safe_int(lane_counts.get("same_keyword")),
            "kw_pair": _safe_int(lane_counts.get("kw_pair")),
            "same_sam_naics": _safe_int(lane_counts.get("same_sam_naics")),
        },
        "sam_context": {
            "scanned_events": _safe_int(sam_ctx.get("scanned_events")),
            "events_with_research_context": _safe_int(sam_ctx.get("events_with_research_context")),
            "research_context_coverage_pct": sam_ctx.get("research_context_coverage_pct"),
            "avg_context_fields_per_event": sam_ctx.get("avg_context_fields_per_event"),
            "events_with_core_procurement_context": _safe_int(sam_ctx.get("events_with_core_procurement_context")),
            "core_procurement_context_coverage_pct": sam_ctx.get("core_procurement_context_coverage_pct"),
            "coverage_by_field_pct": sam_ctx.get("coverage_by_field_pct") or {},
            "top_notice_types": sam_ctx.get("top_notice_types") or [],
            "top_naics_codes": sam_ctx.get("top_naics_codes") or [],
            "top_set_aside_codes": sam_ctx.get("top_set_aside_codes") or [],
        },
        "ingest_request_diagnostics": ingest_request_diag,
        "snapshot_items": snapshot_items,
    }

    results_dir = bundle_dir / SAM_BUNDLE_RESULTS_DIR
    workflow_json = results_dir / "workflow_result.json"
    doctor_json = results_dir / "doctor_status.json"
    summary_json = results_dir / "workflow_summary.json"

    workflow_module._write_json(workflow_json, {"generated_at": now.isoformat(), "result": workflow_res})
    workflow_module._write_json(doctor_json, {"generated_at": now.isoformat(), "result": doc})

    if failed_required_checks:
        workflow_status = "failed" if bool(require_nonzero) else "warning"
    elif warning_checks:
        workflow_status = "warning"
    else:
        workflow_status = "ok"

    summary_payload = {
        "generated_at": now.isoformat(),
        "source": "SAM.gov",
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "bundle_version": SAM_BUNDLE_VERSION,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": list(compare_scoring_versions or []),
        "status": workflow_status,
        "smoke_passed": smoke_passed,
        "partially_useful": bool(quality.get("partially_useful")),
        "quality": quality,
        "require_nonzero": bool(require_nonzero),
        "thresholds": thresholds,
        "failed_required_checks": failed_required_checks,
        "warning_checks": warning_checks,
        "checks": checks,
        "baseline": baseline,
    }

    artifacts = {
        "bundle_manifest_json": bundle_dir / "bundle_manifest.json",
        "workflow_result_json": workflow_json,
        "workflow_summary_json": summary_json,
        "smoke_summary_json": summary_json,
        "doctor_status_json": doctor_json,
        "exports": (workflow_res.get("exports") if isinstance(workflow_res, dict) else None),
    }

    summary_payload["artifacts"] = {
        "workflow_result_json": workflow_json,
        "doctor_status_json": doctor_json,
        "smoke_summary_json": summary_json,
        "bundle_manifest_json": artifacts.get("bundle_manifest_json"),
        "report_html": bundle_dir / "report" / "bundle_report.html",
        "exports": artifacts.get("exports"),
    }
    workflow_module._write_json(summary_json, summary_payload)

    report_summary = {
        "status": workflow_status,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": ",".join(compare_scoring_versions or []),
        "quality": quality.get("quality"),
        "smoke_passed": smoke_passed,
        "partially_useful": bool(quality.get("partially_useful")),
        "events_window": events_window,
        "events_with_keywords": events_with_keywords,
        "events_with_entity_window": events_with_entity,
        "same_keyword": same_keyword_lane,
        "kw_pair": kw_pair_lane,
        "same_sam_naics": same_sam_naics_lane,
        "snapshot_items": snapshot_items,
        "rate_limit_retries": rate_limit_retries,
        "retry_attempts_total": retry_attempts_total,
    }

    report_html = render_sam_bundle_report(
        bundle_dir=bundle_dir,
        title="SAM.gov Workflow Bundle Report",
        status=workflow_status,
        workflow_type=workflow_type,
        validation_mode=mode,
        scoring_version=str(scoring_version),
        compare_scoring_versions=list(compare_scoring_versions or []),
        checks=checks,
        failed_required_checks=failed_required_checks,
        warning_checks=warning_checks,
        summary=report_summary,
        artifacts=artifacts,
    )
    artifacts["report_html"] = report_html

    manifest_payload = {
        "bundle_version": SAM_BUNDLE_VERSION,
        "source": "SAM.gov",
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": list(compare_scoring_versions or []),
        "generated_at": now.isoformat(),
        "status": workflow_status,
        "quality": quality,
        "summary_counts": {
            "events_window": events_window,
            "events_with_keywords": events_with_keywords,
            "events_with_entity_window": events_with_entity,
            "same_keyword": same_keyword_lane,
            "kw_pair": kw_pair_lane,
            "same_sam_naics": same_sam_naics_lane,
            "snapshot_items": snapshot_items,
        },
        "check_summary": {
            "total": len(checks),
            "failed_required": len(failed_required_checks),
            "warnings": len(warning_checks),
        },
        "run_parameters": {
            "ingest_days": int(ingest_days),
            "pages": int(pages),
            "page_size": int(page_size),
            "max_records": max_records,
            "start_page": int(start_page),
            "keywords": list(keywords or []),
            "window_days": int(window_days),
            "scan_limit": int(scan_limit),
            "scoring_version": str(scoring_version),
            "compare_scoring_versions": list(compare_scoring_versions or []),
        },
        "ingest_diagnostics": ingest_request_diag,
        "generated_files": flatten_bundle_files(artifacts=artifacts, bundle_dir=bundle_dir),
    }
    manifest_json = write_bundle_manifest(bundle_dir=bundle_dir, payload=manifest_payload)
    artifacts["bundle_manifest_json"] = manifest_json

    return {
        "status": workflow_status,
        "smoke_passed": bool(smoke_passed),
        "partially_useful": bool(quality.get("partially_useful")),
        "quality": quality,
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": list(compare_scoring_versions or []),
        "bundle_version": SAM_BUNDLE_VERSION,
        "bundle_dir": bundle_dir,
        "workflow": workflow_res,
        "doctor": doc,
        "checks": checks,
        "failed_required_checks": failed_required_checks,
        "warning_checks": warning_checks,
        "thresholds": thresholds,
        "baseline": baseline,
        "artifacts": artifacts,
    }


def run_samgov_validation_workflow_hardened(
    *,
    ingest_days: int = 30,
    pages: int = 5,
    page_size: int = 100,
    max_records: Optional[int] = 250,
    start_page: int = 1,
    keywords: Optional[list[str]] = None,
    api_key: Optional[str] = None,
    ontology_path: Path = Path("examples/ontology_sam_procurement_starter.json"),
    ontology_days: int = 30,
    entity_days: int = 30,
    entity_batch: int = 500,
    window_days: int = 30,
    min_events_entity: int = 2,
    min_events_keywords: int = 2,
    max_events_keywords: int = 200,
    max_keywords_per_event: int = 10,
    min_score: int = 1,
    snapshot_limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    compare_scoring_versions: Optional[list[str]] = None,
    notes: Optional[str] = "samgov larger-run validation",
    bundle_root: Optional[Path] = None,
    database_url: Optional[str] = None,
    require_nonzero: bool = True,
    skip_ingest: bool = False,
    threshold_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return run_samgov_smoke_workflow_hardened(
        ingest_days=int(ingest_days),
        pages=int(pages),
        page_size=int(page_size),
        max_records=max_records,
        start_page=int(start_page),
        keywords=keywords,
        api_key=api_key,
        ontology_path=Path(ontology_path),
        ontology_days=int(ontology_days),
        entity_days=int(entity_days),
        entity_batch=int(entity_batch),
        window_days=int(window_days),
        min_events_entity=int(min_events_entity),
        min_events_keywords=int(min_events_keywords),
        max_events_keywords=int(max_events_keywords),
        max_keywords_per_event=int(max_keywords_per_event),
        min_score=int(min_score),
        snapshot_limit=int(snapshot_limit),
        scan_limit=int(scan_limit),
        scoring_version=str(scoring_version),
        compare_scoring_versions=list(compare_scoring_versions or []),
        notes=notes,
        bundle_root=(Path(bundle_root).expanduser() if bundle_root else EXPORTS_DIR / "validation" / "samgov"),
        database_url=database_url,
        require_nonzero=bool(require_nonzero),
        skip_ingest=bool(skip_ingest),
        threshold_overrides=threshold_overrides,
        validation_mode="larger",
        workflow_type="samgov-validation",
    )


__all__ = [
    "run_samgov_smoke_workflow_hardened",
    "run_samgov_validation_workflow_hardened",
]
