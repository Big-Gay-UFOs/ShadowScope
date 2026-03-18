from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.ingest import (
    format_sam_posted_window_cli_args,
    resolve_sam_posted_window,
    serialize_sam_posted_window,
)
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


def _iso_or_none(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _snapshot_window_args(
    *,
    date_from: Any = None,
    date_to: Any = None,
    occurred_after: Any = None,
    occurred_before: Any = None,
    created_after: Any = None,
    created_before: Any = None,
    since_days: Optional[int] = None,
) -> list[str]:
    parts: list[str] = []
    if date_from is not None:
        parts.append(f"--date-from {_iso_or_none(date_from)}")
    if date_to is not None:
        parts.append(f"--date-to {_iso_or_none(date_to)}")
    if occurred_after is not None:
        parts.append(f"--occurred-after {_iso_or_none(occurred_after)}")
    if occurred_before is not None:
        parts.append(f"--occurred-before {_iso_or_none(occurred_before)}")
    if created_after is not None:
        parts.append(f"--created-after {_iso_or_none(created_after)}")
    if created_before is not None:
        parts.append(f"--created-before {_iso_or_none(created_before)}")
    if since_days is not None:
        parts.append(f"--since-days {int(since_days)}")
    return parts


def _ordered_failure_categories(checks: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in checks:
        category = str(item.get("category") or "").strip()
        if not category or category in seen:
            continue
        seen.add(category)
        ordered.append(category)
    return ordered


def _summarize_check_groups(checks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    from backend.services import workflow as workflow_module

    ordered_categories = [
        "pipeline_health",
        "source_coverage_context_health",
        "lead_signal_quality",
    ]
    groups: dict[str, dict[str, Any]] = {}
    for category in ordered_categories:
        groups[category] = {
            "category": category,
            "category_label": workflow_module.SAM_VALIDATION_CATEGORY_LABELS.get(
                category,
                category.replace("_", " "),
            ),
            "checks": [],
        }

    for item in checks:
        category = str(item.get("category") or "pipeline_health")
        group = groups.setdefault(
            category,
            {
                "category": category,
                "category_label": workflow_module.SAM_VALIDATION_CATEGORY_LABELS.get(
                    category,
                    category.replace("_", " "),
                ),
                "checks": [],
            },
        )
        group["checks"].append(item)

    summary: dict[str, dict[str, Any]] = {}
    for category in ordered_categories + sorted([k for k in groups.keys() if k not in ordered_categories]):
        group = groups.get(category)
        if not group:
            continue
        category_checks = list(group.get("checks") or [])
        if not category_checks and category not in ordered_categories:
            continue
        failed_required = [item for item in category_checks if bool(item.get("required")) and not bool(item.get("passed"))]
        failed_advisory = [
            item for item in category_checks if not bool(item.get("required")) and not bool(item.get("passed"))
        ]
        summary[category] = {
            "category": category,
            "category_label": group.get("category_label"),
            "total": len(category_checks),
            "passed": len([item for item in category_checks if bool(item.get("passed"))]),
            "failed": len([item for item in category_checks if not bool(item.get("passed"))]),
            "required_total": len([item for item in category_checks if bool(item.get("required"))]),
            "advisory_total": len([item for item in category_checks if not bool(item.get("required"))]),
            "failed_required": len(failed_required),
            "failed_advisory": len(failed_advisory),
            "checks": category_checks,
        }
    return summary


def _summarize_policy_groups(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    from backend.services import workflow as workflow_module

    ordered_categories = [
        "pipeline_health",
        "source_coverage_context_health",
        "lead_signal_quality",
    ]
    groups: dict[str, dict[str, Any]] = {}
    for category in ordered_categories:
        groups[category] = {
            "category_label": workflow_module.SAM_VALIDATION_CATEGORY_LABELS.get(
                category,
                category.replace("_", " "),
            ),
            "required_checks": [],
            "advisory_checks": [],
        }

    for item in items:
        category = str(item.get("category") or "pipeline_health")
        group = groups.setdefault(
            category,
            {
                "category_label": workflow_module.SAM_VALIDATION_CATEGORY_LABELS.get(
                    category,
                    category.replace("_", " "),
                ),
                "required_checks": [],
                "advisory_checks": [],
            },
        )
        target = "required_checks" if bool(item.get("required")) else "advisory_checks"
        group[target].append(item.get("name"))

    summary: dict[str, dict[str, Any]] = {}
    for category in ordered_categories + sorted([k for k in groups.keys() if k not in ordered_categories]):
        group = groups.get(category)
        if not group:
            continue
        required_checks = [str(item) for item in group.get("required_checks") or [] if str(item).strip()]
        advisory_checks = [str(item) for item in group.get("advisory_checks") or [] if str(item).strip()]
        if not required_checks and not advisory_checks and category not in ordered_categories:
            continue
        summary[category] = {
            "category_label": group.get("category_label"),
            "required_checks": required_checks,
            "advisory_checks": advisory_checks,
        }
    return summary


def _build_quality_gate_policy(
    *,
    validation_mode: str,
    checks: list[dict[str, Any]],
    policy_overrides: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    from backend.services import workflow as workflow_module

    declared_checks = workflow_module._list_sam_validation_policy_checks(validation_mode=validation_mode)
    effective_groups = _summarize_check_groups(checks)
    return {
        "validation_mode": validation_mode,
        "required_checks": [item.get("name") for item in declared_checks if bool(item.get("required"))],
        "advisory_checks": [item.get("name") for item in declared_checks if not bool(item.get("required"))],
        "by_category": _summarize_policy_groups(declared_checks),
        "effective_required_checks": [item.get("name") for item in checks if bool(item.get("required"))],
        "effective_advisory_checks": [item.get("name") for item in checks if not bool(item.get("required"))],
        "effective_by_category": {
            category: {
                "category_label": group.get("category_label"),
                "required_checks": [item.get("name") for item in group.get("checks", []) if bool(item.get("required"))],
                "advisory_checks": [
                    item.get("name") for item in group.get("checks", []) if not bool(item.get("required"))
                ],
            }
            for category, group in effective_groups.items()
        },
        "policy_overrides": list(policy_overrides or []),
    }


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
    required_failure_categories = _ordered_failure_categories(failed_required_checks)
    advisory_failure_categories = _ordered_failure_categories(warning_checks)

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
    if required_failure_categories:
        operator_messages.append(
            "Required quality gates failed in: " + ", ".join(required_failure_categories) + "."
        )
    elif advisory_failure_categories:
        operator_messages.append(
            "Advisory quality misses were observed in: " + ", ".join(advisory_failure_categories) + "."
        )
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
        "required_failure_categories": required_failure_categories,
        "advisory_failure_categories": advisory_failure_categories,
        "failure_categories": required_failure_categories + [
            category for category in advisory_failure_categories if category not in required_failure_categories
        ],
        "operator_messages": operator_messages,
    }


def run_samgov_smoke_workflow_hardened(
    *,
    ingest_days: Optional[int] = None,
    posted_from: Optional[date] = None,
    posted_to: Optional[date] = None,
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
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    occurred_after: Optional[datetime] = None,
    occurred_before: Optional[datetime] = None,
    created_after: Optional[datetime] = None,
    created_before: Optional[datetime] = None,
    since_days: Optional[int] = None,
    min_score: int = 1,
    snapshot_limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = "v2",
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
    requested_window = resolve_sam_posted_window(days=ingest_days, posted_from=posted_from, posted_to=posted_to)

    default_root = EXPORTS_DIR / "smoke" / "samgov"
    if mode == "larger":
        default_root = EXPORTS_DIR / "validation" / "samgov"
    root = Path(bundle_root).expanduser() if bundle_root else default_root
    bundle_dir = root / stamp
    bundle_dir.mkdir(parents=True, exist_ok=True)

    workflow_error: Optional[str] = None
    try:
        workflow_res = workflow_module.run_samgov_workflow(
            ingest_days=int(ingest_days) if ingest_days is not None else None,
            posted_from=posted_from,
            posted_to=posted_to,
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
            date_from=date_from,
            date_to=date_to,
            occurred_after=occurred_after,
            occurred_before=occurred_before,
            created_after=created_after,
            created_before=created_before,
            since_days=since_days,
            min_score=int(min_score),
            snapshot_limit=int(snapshot_limit),
            scan_limit=int(scan_limit),
            scoring_version=str(scoring_version),
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
    workflow_run_metadata = workflow_res.get("run_metadata") if isinstance(workflow_res.get("run_metadata"), dict) else {}
    effective_window = serialize_sam_posted_window(requested_window)
    if isinstance(ingest, dict) and isinstance(ingest.get("date_window"), dict):
        effective_window = ingest.get("date_window")  # type: ignore[assignment]
    elif isinstance(workflow_run_metadata, dict) and workflow_run_metadata.get("effective_posted_from") and workflow_run_metadata.get("effective_posted_to"):
        effective_window = {
            "mode": workflow_run_metadata.get("posted_window_mode"),
            "requested_days": workflow_run_metadata.get("ingest_days"),
            "effective_days": workflow_run_metadata.get("ingest_days"),
            "posted_from": workflow_run_metadata.get("effective_posted_from"),
            "posted_to": workflow_run_metadata.get("effective_posted_to"),
            "calendar_span_days": workflow_run_metadata.get("calendar_span_days"),
        }
    ingest_nonzero = (
        _safe_int((ingest or {}).get("fetched")) > 0
        or _safe_int((ingest or {}).get("inserted")) > 0
        or _safe_int((ingest or {}).get("normalized")) > 0
    )

    thresholds = workflow_module._resolve_sam_validation_thresholds(
        threshold_overrides,
        validation_mode=mode,
    )

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
    ingest_window_args = " ".join(format_sam_posted_window_cli_args(effective_window))

    smoke_tune_cmd = (
        f"ss workflow samgov-smoke {ingest_window_args} --pages {max(int(pages), 2)} "
        f"--limit {max(_safe_int(max_records, 50), 50)} --window-days {int(window_days)} --json"
    )
    larger_validate_cmd = (
        f"ss workflow samgov-validate {ingest_window_args} --pages {max(int(pages), 5)} "
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
    rerun_snapshot_parts = [
        'ss leads snapshot --source "SAM.gov"',
        f"--min-score {int(min_score)}",
        f"--limit {int(snapshot_limit)}",
    ]
    rerun_snapshot_parts.extend(
        _snapshot_window_args(
            date_from=date_from,
            date_to=date_to,
            occurred_after=occurred_after,
            occurred_before=occurred_before,
            created_after=created_after,
            created_before=created_before,
            since_days=since_days,
        )
    )
    rerun_snapshot_cmd = " ".join(rerun_snapshot_parts)

    checks: list[dict[str, Any]] = []
    policy_overrides: list[dict[str, Any]] = []
    checks.append(
        workflow_module._serialize_check(
            name="doctor_db_ok",
            ok=bool(db_ok),
            observed=(doc.get("db") or {}).get("status"),
            actual=(doc.get("db") or {}).get("status"),
            threshold="ok",
            expected="ok",
            why="SAM.gov workflow diagnostics require DB health to read quality signals.",
            hint=doctor_cmd,
            kind="health",
            validation_mode=mode,
        )
    )

    checks.append(
        workflow_module._serialize_check(
            name="workflow_execution",
            ok=workflow_error is None,
            observed="ok" if workflow_error is None else workflow_error,
            actual="ok" if workflow_error is None else workflow_error,
            threshold="workflow executes without exception",
            expected="workflow executes without exception",
            why="The workflow must complete all stages to produce trustworthy SAM.gov validation artifacts.",
            hint=smoke_tune_cmd if mode == "smoke" else larger_validate_cmd,
            kind="health",
            validation_mode=mode,
        )
    )

    if skip_ingest:
        policy_overrides.append(
            {
                "name": "ingest_nonzero",
                "declared_policy_level": "required",
                "effective_policy_level": "advisory",
                "reason": "skip_ingest=True reuses existing local SAM.gov data instead of requiring fresh ingest volume.",
            }
        )
        checks.append(
            workflow_module._serialize_check(
                name="ingest_nonzero",
                ok=True,
                observed="skipped",
                actual="skipped",
                threshold="skip_ingest=True",
                expected="skip_ingest=True",
                why="Ingest was intentionally skipped for offline replay.",
                hint=smoke_tune_cmd,
                kind="health",
                validation_mode=mode,
                required=False,
                severity="info",
                status="info",
            )
        )
    else:
        ingest_ok = bool(ingest_nonzero) and status != "skipped"
        checks.append(
            workflow_module._serialize_check(
                name="ingest_nonzero",
                ok=bool(ingest_ok),
                observed={
                    "status": (ingest or {}).get("status"),
                    "fetched": _safe_int((ingest or {}).get("fetched")),
                    "inserted": _safe_int((ingest or {}).get("inserted")),
                    "normalized": _safe_int((ingest or {}).get("normalized")),
                },
                actual={
                    "status": (ingest or {}).get("status"),
                    "fetched": _safe_int((ingest or {}).get("fetched")),
                    "inserted": _safe_int((ingest or {}).get("inserted")),
                    "normalized": _safe_int((ingest or {}).get("normalized")),
                },
                threshold="fetched>0 OR inserted>0 OR normalized>0",
                expected="fetched>0 OR inserted>0 OR normalized>0",
                why="Fresh SAM.gov ingest keeps validation checks tied to current market movement.",
                hint=smoke_tune_cmd if mode == "smoke" else larger_validate_cmd,
                kind="health",
                validation_mode=mode,
            )
        )

    ingest_request_diag = (ingest.get("request_diagnostics") if isinstance(ingest, dict) else {}) or {}
    rate_limit_retries = _safe_int(ingest_request_diag.get("rate_limit_retries"))
    retry_attempts_total = _safe_int(ingest_request_diag.get("retry_attempts_total"))
    checks.append(
        workflow_module._serialize_check(
            name="ingest_retry_pressure",
            ok=rate_limit_retries == 0,
            observed={
                "retry_attempts_total": retry_attempts_total,
                "rate_limit_retries": rate_limit_retries,
                "retry_sleep_seconds_total": _safe_float(ingest_request_diag.get("retry_sleep_seconds_total")),
            },
            actual={
                "retry_attempts_total": retry_attempts_total,
                "rate_limit_retries": rate_limit_retries,
                "retry_sleep_seconds_total": _safe_float(ingest_request_diag.get("retry_sleep_seconds_total")),
            },
            threshold="rate_limit_retries == 0 (best case)",
            expected="rate_limit_retries == 0 (best case)",
            why="Retries/429s can make larger SAM windows slow while still producing usable output.",
            hint="Tune SAM_API_TIMEOUT_SECONDS, SAM_API_MAX_RETRIES, and SAM_API_BACKOFF_BASE.",
            kind="health",
            validation_mode=mode,
            status="pass" if rate_limit_retries == 0 else "info",
        )
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
                unit=str(spec.get("unit") or ""),
                why=str(spec["why"]),
                hint=str(spec["hint"]),
                actual=spec.get("actual"),
                validation_mode=mode,
            )
        )

    if mode == "larger":
        checks.append(
            workflow_module._threshold_check(
                name="larger_run_window_signal",
                observed=events_window,
                threshold=max(10.0, thresholds["events_window_min"]),
                why="Large-window validation expects more than trivial volume but can still be sparse-valid.",
                hint=larger_validate_cmd,
                validation_mode=mode,
            )
        )

    failed_required_checks = [c for c in checks if bool(c.get("required", True)) and not bool(c.get("ok"))]
    warning_checks = [c for c in checks if not bool(c.get("required", True)) and not bool(c.get("ok"))]
    check_groups = _summarize_check_groups(checks)
    quality_gate_policy = _build_quality_gate_policy(
        validation_mode=mode,
        checks=checks,
        policy_overrides=policy_overrides,
    )
    required_checks_passed = len(failed_required_checks) == 0
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
    run_metadata = dict(workflow_run_metadata or {})
    run_metadata.update(
        {
            "source": "SAM.gov",
            "workflow_type": workflow_type,
            "run_timestamp": now.isoformat(),
            "ingest_days": effective_window.get("effective_days"),
            "posted_window_mode": effective_window.get("mode"),
            "effective_posted_from": effective_window.get("posted_from"),
            "effective_posted_to": effective_window.get("posted_to"),
            "calendar_span_days": effective_window.get("calendar_span_days"),
            "pages": int(pages),
            "page_size": int(page_size),
            "max_records": max_records,
            "start_page": int(start_page),
            "window_days": int(window_days),
            "keywords": list(keywords or []),
        }
    )

    baseline = {
        "captured_at": now.isoformat(),
        "source": "SAM.gov",
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "window_days": int(window_days),
        "ingest_window": effective_window,
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
        workflow_status = "failed"
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
        "status": workflow_status,
        "smoke_passed": smoke_passed,
        "required_checks_passed": required_checks_passed,
        "partially_useful": bool(quality.get("partially_useful")),
        "quality": quality,
        "require_nonzero": bool(require_nonzero),
        "thresholds": thresholds,
        "quality_gate_policy": quality_gate_policy,
        "run_metadata": run_metadata,
        "failed_required_checks": failed_required_checks,
        "failed_advisory_checks": warning_checks,
        "warning_checks": warning_checks,
        "checks": checks,
        "check_groups": check_groups,
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
        "quality": quality.get("quality"),
        "smoke_passed": smoke_passed,
        "required_checks_passed": required_checks_passed,
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
        "posted_window_mode": effective_window.get("mode"),
        "effective_posted_from": effective_window.get("posted_from"),
        "effective_posted_to": effective_window.get("posted_to"),
    }

    report_html = render_sam_bundle_report(
        bundle_dir=bundle_dir,
        title="SAM.gov Workflow Bundle Report",
        status=workflow_status,
        workflow_type=workflow_type,
        validation_mode=mode,
        checks=checks,
        check_groups=check_groups,
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
            "passed": len([item for item in checks if bool(item.get("passed"))]),
            "required_total": len([item for item in checks if bool(item.get("required"))]),
            "advisory_total": len([item for item in checks if not bool(item.get("required"))]),
            "failed_required": len(failed_required_checks),
            "failed_advisory": len(warning_checks),
            "warnings": len(warning_checks),
            "required_failure_categories": quality.get("required_failure_categories") or [],
            "advisory_failure_categories": quality.get("advisory_failure_categories") or [],
            "by_category": {
                category: {
                    "category_label": group.get("category_label"),
                    "total": group.get("total"),
                    "required_total": group.get("required_total"),
                    "advisory_total": group.get("advisory_total"),
                    "failed_required": group.get("failed_required"),
                    "failed_advisory": group.get("failed_advisory"),
                }
                for category, group in check_groups.items()
            },
        },
        "quality_gate_policy": quality_gate_policy,
        "run_parameters": {
            "ingest_days": effective_window.get("effective_days"),
            "posted_window_mode": effective_window.get("mode"),
            "effective_posted_from": effective_window.get("posted_from"),
            "effective_posted_to": effective_window.get("posted_to"),
            "calendar_span_days": effective_window.get("calendar_span_days"),
            "pages": int(pages),
            "page_size": int(page_size),
            "max_records": max_records,
            "start_page": int(start_page),
            "keywords": list(keywords or []),
            "window_days": int(window_days),
            "scan_limit": int(scan_limit),
            "date_from": _iso_or_none(date_from),
            "date_to": _iso_or_none(date_to),
            "occurred_after": _iso_or_none(occurred_after),
            "occurred_before": _iso_or_none(occurred_before),
            "created_after": _iso_or_none(created_after),
            "created_before": _iso_or_none(created_before),
            "since_days": int(since_days) if since_days is not None else None,
        },
        "ingest_diagnostics": ingest_request_diag,
        "generated_files": flatten_bundle_files(artifacts=artifacts, bundle_dir=bundle_dir),
    }
    manifest_json = write_bundle_manifest(bundle_dir=bundle_dir, payload=manifest_payload)
    artifacts["bundle_manifest_json"] = manifest_json

    return {
        "status": workflow_status,
        "smoke_passed": bool(smoke_passed),
        "required_checks_passed": bool(required_checks_passed),
        "partially_useful": bool(quality.get("partially_useful")),
        "quality": quality,
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "bundle_version": SAM_BUNDLE_VERSION,
        "bundle_dir": bundle_dir,
        "run_metadata": run_metadata,
        "workflow": workflow_res,
        "doctor": doc,
        "checks": checks,
        "check_groups": check_groups,
        "quality_gate_policy": quality_gate_policy,
        "failed_required_checks": failed_required_checks,
        "failed_advisory_checks": warning_checks,
        "warning_checks": warning_checks,
        "thresholds": thresholds,
        "baseline": baseline,
        "artifacts": artifacts,
    }


def run_samgov_validation_workflow_hardened(
    *,
    ingest_days: Optional[int] = None,
    posted_from: Optional[date] = None,
    posted_to: Optional[date] = None,
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
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    occurred_after: Optional[datetime] = None,
    occurred_before: Optional[datetime] = None,
    created_after: Optional[datetime] = None,
    created_before: Optional[datetime] = None,
    since_days: Optional[int] = None,
    min_score: int = 1,
    snapshot_limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = "v2",
    notes: Optional[str] = "samgov larger-run validation",
    bundle_root: Optional[Path] = None,
    database_url: Optional[str] = None,
    require_nonzero: bool = True,
    skip_ingest: bool = False,
    threshold_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return run_samgov_smoke_workflow_hardened(
        ingest_days=int(ingest_days) if ingest_days is not None else None,
        posted_from=posted_from,
        posted_to=posted_to,
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
        date_from=date_from,
        date_to=date_to,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        created_after=created_after,
        created_before=created_before,
        since_days=since_days,
        min_score=int(min_score),
        snapshot_limit=int(snapshot_limit),
        scan_limit=int(scan_limit),
        scoring_version=str(scoring_version),
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
