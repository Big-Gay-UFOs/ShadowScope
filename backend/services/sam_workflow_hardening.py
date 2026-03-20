from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.ingest import (
    format_sam_posted_window_cli_args,
    resolve_sam_posted_window,
    serialize_sam_posted_window,
)
from backend.services.leads import DEFAULT_SCORING_VERSION
from backend.services.bundle import (
    SAM_BUNDLE_RESULTS_DIR,
    SAM_BUNDLE_VERSION,
    flatten_bundle_files,
    normalize_sam_exports,
    render_sam_bundle_report,
    write_bundle_manifest,
)
from backend.services.evidence_package import (
    DEFAULT_BUNDLE_DOSSIER_TOP_N,
    export_top_lead_evidence_packages,
)
from backend.services.foia_review_board import (
    FOIA_LEAD_DOSSIER_INDEX_CSV_PATH,
    FOIA_LEAD_DOSSIER_INDEX_JSON_PATH,
    FOIA_LEAD_REVIEW_BOARD_HTML_PATH,
    FOIA_LEAD_REVIEW_BOARD_MD_PATH,
    build_foia_lead_review_diagnostics,
    render_foia_lead_review_board_from_bundle,
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


def _dedupe_text(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _artifact_exists(value: Any) -> bool:
    try:
        return Path(value).expanduser().exists()
    except Exception:
        return False


def _load_json_payload(path_value: Any) -> dict[str, Any]:
    try:
        path = Path(path_value).expanduser()
    except Exception:
        return {}
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _path_or_none(path_value: Any) -> Optional[Path]:
    try:
        return Path(path_value).expanduser()
    except Exception:
        return None


def _lead_dossier_artifacts(
    *,
    bundle_dir: Path,
    review_board_artifacts: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Path]]:
    sources = review_board_artifacts if isinstance(review_board_artifacts, dict) else {}
    index_json = _path_or_none(sources.get("dossier_index_json"))
    index_csv = _path_or_none(sources.get("dossier_index_csv"))

    if index_json is None:
        candidate = bundle_dir / FOIA_LEAD_DOSSIER_INDEX_JSON_PATH
        if candidate.exists():
            index_json = candidate
    if index_csv is None:
        candidate = bundle_dir / FOIA_LEAD_DOSSIER_INDEX_CSV_PATH
        if candidate.exists():
            index_csv = candidate

    artifacts: dict[str, Path] = {}
    if isinstance(index_json, Path) and index_json.exists():
        artifacts["index_json"] = index_json
    if isinstance(index_csv, Path) and index_csv.exists():
        artifacts["index_csv"] = index_csv
    return artifacts or None


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_snapshot_export_items(artifacts: dict[str, Any]) -> list[dict[str, Any]]:
    exports = artifacts.get("exports") if isinstance(artifacts.get("exports"), dict) else {}
    lead = exports.get("lead_snapshot") if isinstance(exports.get("lead_snapshot"), dict) else {}
    json_path = lead.get("json")
    if not json_path:
        return []
    path = Path(json_path).expanduser()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    items = payload.get("items") if isinstance(payload, dict) else []
    return [dict(item) for item in items if isinstance(item, dict)]


def _effective_window_bounds(window: dict[str, Any]) -> tuple[Optional[datetime], Optional[datetime]]:
    payload = serialize_sam_posted_window(window)
    posted_from = str(payload.get("posted_from") or "").strip()
    posted_to = str(payload.get("posted_to") or "").strip()
    if not posted_from or not posted_to:
        return None, None
    try:
        start_day = date.fromisoformat(posted_from)
        end_day = date.fromisoformat(posted_to)
    except ValueError:
        return None, None
    start_dt = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_day, datetime.max.time(), tzinfo=timezone.utc)
    return start_dt, end_dt


def _snapshot_window_diagnostics(
    *,
    items: list[dict[str, Any]],
    effective_window: dict[str, Any],
) -> dict[str, Any]:
    start_dt, end_dt = _effective_window_bounds(effective_window)
    occurred_values = [
        _parse_iso_datetime(item.get("occurred_at")) or _parse_iso_datetime(item.get("created_at"))
        for item in items
    ]
    occurred_values = [value for value in occurred_values if value is not None]
    snapshot_event_min = min(occurred_values).isoformat() if occurred_values else None
    snapshot_event_max = max(occurred_values).isoformat() if occurred_values else None

    outside_items: list[dict[str, Any]] = []
    if start_dt is not None and end_dt is not None:
        for item in items:
            occurred_at = _parse_iso_datetime(item.get("occurred_at")) or _parse_iso_datetime(item.get("created_at"))
            if occurred_at is None or occurred_at < start_dt or occurred_at > end_dt:
                outside_items.append(item)

    top10 = items[:10]
    top10_outside = [
        item
        for item in top10
        if item in outside_items
    ]
    return {
        "snapshot_event_min": snapshot_event_min,
        "snapshot_event_max": snapshot_event_max,
        "outside_window_count": len(outside_items),
        "outside_window_event_ids": [
            _safe_int(item.get("event_id"), default=0)
            for item in outside_items
            if _safe_int(item.get("event_id"), default=0) > 0
        ][:20],
        "top10_outside_window_count": len(top10_outside),
        "top10_outside_window_event_ids": [
            _safe_int(item.get("event_id"), default=0)
            for item in top10_outside
            if _safe_int(item.get("event_id"), default=0) > 0
        ][:10],
        "top10_inside_window": len(top10_outside) == 0,
    }


def _family_distribution_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(items, start=1):
        family = str(item.get("lead_family") or "unassigned")
        bucket = counts.setdefault(
            family,
            {
                "lead_family": family,
                "label": str(item.get("lead_family_label") or family.replace("_", " ")),
                "count": 0,
                "sample_event_ids": [],
            },
        )
        bucket["count"] += 1
        event_id = _safe_int(item.get("event_id"), default=0)
        if event_id > 0 and event_id not in bucket["sample_event_ids"] and len(bucket["sample_event_ids"]) < 5:
            bucket["sample_event_ids"].append(event_id)

    ordered = sorted(
        counts.values(),
        key=lambda item: (-(item.get("count") or 0), str(item.get("lead_family") or "")),
    )

    def _share(rows: list[dict[str, Any]], family: str) -> float:
        if not rows:
            return 0.0
        family_count = len([item for item in rows if str(item.get("lead_family") or "unassigned") == family])
        return round(float(family_count) / float(len(rows)), 4)

    top10 = items[:10]
    top50 = items[:50]
    dominant = ordered[0] if ordered else None
    dominant_family = None if dominant is None else str(dominant.get("lead_family") or "unassigned")
    top10_share = _share(top10, dominant_family) if dominant_family else 0.0
    top50_share = _share(top50, dominant_family) if dominant_family else 0.0
    family_collapse_warning = bool(dominant_family) and (
        top50_share > 0.5 or (dominant_family == "vendor_network_contract_lineage" and top10_share >= 0.5)
    )
    return {
        "top_family_counts": ordered[:10],
        "dominant_family": dominant_family,
        "dominant_family_label": None if dominant is None else dominant.get("label"),
        "dominant_family_share_top10": top10_share,
        "dominant_family_share_top50": top50_share,
        "family_share_top10": {
            str(item.get("lead_family")): _share(top10, str(item.get("lead_family")))
            for item in ordered[:10]
        },
        "family_share_top50": {
            str(item.get("lead_family")): _share(top50, str(item.get("lead_family")))
            for item in ordered[:10]
        },
        "family_collapse_warning": family_collapse_warning,
    }


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
        "mission_quality",
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
        "mission_quality",
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


def _requested_window_differs_from_effective(
    requested_window: dict[str, Any],
    effective_window: dict[str, Any],
) -> bool:
    if not requested_window or not effective_window:
        return False

    requested_mode = str(requested_window.get("mode") or "").strip()
    effective_mode = str(effective_window.get("mode") or "").strip()
    if requested_mode and effective_mode and requested_mode != effective_mode:
        return True

    for key in ("posted_from", "posted_to"):
        requested_value = requested_window.get(key)
        effective_value = effective_window.get(key)
        if requested_value and effective_value and requested_value != effective_value:
            return True

    requested_days = requested_window.get("requested_days")
    if requested_days is None:
        requested_days = requested_window.get("effective_days")
    effective_days = effective_window.get("effective_days")
    if requested_days is not None and effective_days is not None:
        try:
            if int(requested_days) != int(effective_days):
                return True
        except Exception:
            if requested_days != effective_days:
                return True

    return False


def _build_comparison_summary(
    *,
    compare_scoring_versions: list[str],
    workflow_exports: dict[str, Any],
    requested_window: dict[str, Any],
    effective_window: dict[str, Any],
) -> dict[str, Any]:
    requested_versions = list(compare_scoring_versions or [])
    requested = bool(requested_versions)
    comparison_export = (
        workflow_exports.get("scoring_comparison")
        if isinstance(workflow_exports.get("scoring_comparison"), dict)
        else {}
    )
    comparison_json = _load_json_payload(comparison_export.get("json"))
    available = bool(
        requested
        and (
            _artifact_exists(comparison_export.get("json"))
            or _artifact_exists(comparison_export.get("csv"))
        )
    )
    count_value = comparison_export.get("count")
    if count_value is None:
        count_value = comparison_json.get("count")
    comparison_count: Optional[int]
    if count_value is None:
        comparison_count = None
    else:
        comparison_count = _safe_int(count_value, default=0)

    empty = bool(requested and available and comparison_count == 0)
    reason_codes: list[str] = []
    operator_messages: list[str] = []

    if requested and not available:
        reason_codes.append("comparison_not_available")
        operator_messages.append(
            "Scoring comparison was requested, but no comparison artifact is available in this bundle."
        )
    if empty:
        reason_codes.append("comparison_requested_but_empty")
        operator_messages.append(
            "Scoring comparison was requested, but the comparison artifact contains zero comparable rows."
        )
    if requested and _requested_window_differs_from_effective(requested_window, effective_window):
        reason_codes.append("requested_window_differs_from_effective_window")
        operator_messages.append(
            "Requested comparison window differs from the effective ingest window. Review the effective window shown in the bundle before interpreting deltas."
        )

    state_counts = comparison_json.get("state_counts") if isinstance(comparison_json.get("state_counts"), dict) else {}
    baseline_version = str(
        comparison_export.get("baseline_version")
        or comparison_json.get("baseline_version")
        or (requested_versions[0] if len(requested_versions) >= 1 else "")
    )
    target_version = str(
        comparison_export.get("target_version")
        or comparison_json.get("target_version")
        or (requested_versions[1] if len(requested_versions) >= 2 else "")
    )

    return {
        "requested": requested,
        "available": available,
        "empty": empty,
        "policy_level": "advisory" if requested else None,
        "requested_versions": requested_versions,
        "baseline_version": baseline_version or None,
        "target_version": target_version or None,
        "count": comparison_count,
        "state_counts": state_counts,
        "requested_window": requested_window,
        "effective_window": effective_window,
        "reason_codes": _dedupe_text(reason_codes),
        "operator_messages": _dedupe_text(operator_messages),
    }


def _build_comparison_checks(
    *,
    workflow_module: Any,
    comparison_summary: dict[str, Any],
    validation_mode: str,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    if not bool(comparison_summary.get("requested")):
        return checks

    requested_versions = list(comparison_summary.get("requested_versions") or [])
    compare_flag = ",".join([str(item) for item in requested_versions if str(item).strip()]) or "<baseline,target>"
    rerun_hint = f"Rerun with --compare-scoring-versions {compare_flag} and inspect exports/lead_scoring_comparison.json."

    if not bool(comparison_summary.get("available")):
        checks.append(
            workflow_module._serialize_check(
                name="scoring_comparison_available",
                ok=False,
                observed="missing",
                actual="missing",
                threshold="comparison artifact available",
                expected="comparison artifact available",
                why="Requested scoring comparison must be present so operators can audit historical/control deltas.",
                hint=rerun_hint,
                kind="artifact",
                validation_mode=validation_mode,
                required=False,
                severity="warning",
                category="lead_signal_quality",
                status="info",
            )
        )
    elif bool(comparison_summary.get("empty")):
        checks.append(
            workflow_module._serialize_check(
                name="scoring_comparison_non_empty",
                ok=False,
                observed=comparison_summary.get("count"),
                actual={
                    "count": comparison_summary.get("count"),
                    "state_counts": comparison_summary.get("state_counts") or {},
                },
                threshold="count > 0",
                expected="count > 0",
                why="Requested scoring comparison should produce at least one comparable lead row for operator review.",
                hint=rerun_hint,
                kind="artifact",
                validation_mode=validation_mode,
                required=False,
                severity="warning",
                category="lead_signal_quality",
                status="info",
            )
        )

    if "requested_window_differs_from_effective_window" in list(comparison_summary.get("reason_codes") or []):
        checks.append(
            workflow_module._serialize_check(
                name="comparison_effective_window_matches_request",
                ok=False,
                observed={
                    "requested_window": comparison_summary.get("requested_window") or {},
                    "effective_window": comparison_summary.get("effective_window") or {},
                },
                actual={
                    "requested_window": comparison_summary.get("requested_window") or {},
                    "effective_window": comparison_summary.get("effective_window") or {},
                },
                threshold="requested_window == effective_window",
                expected="requested_window == effective_window",
                why="Comparison interpretation depends on the effective ingest window matching the requested window.",
                hint="Use the effective window shown in the bundle when interpreting comparison output, or rerun with explicit posted dates.",
                kind="artifact",
                validation_mode=validation_mode,
                required=False,
                severity="warning",
                category="source_coverage_context_health",
                status="info",
            )
        )

    return checks


def _build_mission_quality_checks(
    *,
    workflow_module: Any,
    mission_quality: dict[str, Any],
    thresholds: dict[str, float],
    validation_mode: str,
    scoring_version_cmd: str,
    review_board_hint: str,
    dossier_hint: str,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    family_diversity = mission_quality.get("family_diversity") if isinstance(mission_quality.get("family_diversity"), dict) else {}
    score_spread = mission_quality.get("score_spread") if isinstance(mission_quality.get("score_spread"), dict) else {}
    foia_draftability = (
        mission_quality.get("foia_draftability") if isinstance(mission_quality.get("foia_draftability"), dict) else {}
    )

    scoring_version = str(mission_quality.get("scoring_version") or "").strip().lower()
    row_scoring_versions = [
        str(item).strip().lower()
        for item in (mission_quality.get("row_scoring_versions") or [])
        if str(item).strip()
    ]
    scoring_version_ok = bool(scoring_version == "v3" and (not row_scoring_versions or set(row_scoring_versions) == {"v3"}))
    checks.append(
        workflow_module._serialize_check(
            name="scoring_version_is_v3",
            ok=scoring_version_ok,
            observed={
                "lead_snapshot_scoring_version": mission_quality.get("scoring_version"),
                "row_scoring_versions": row_scoring_versions,
            },
            actual={
                "lead_snapshot_scoring_version": mission_quality.get("scoring_version"),
                "row_scoring_versions": row_scoring_versions,
            },
            threshold="v3",
            expected="v3",
            why="Larger-run mission review should gate on the current v3 ranked surface, not an older scoring contract.",
            hint=scoring_version_cmd,
            kind="artifact",
            validation_mode=validation_mode,
        )
    )

    checks.append(
        workflow_module._threshold_check(
            name="top_leads_core_field_coverage_threshold",
            observed=mission_quality.get("core_field_coverage_pct"),
            threshold=thresholds["top_leads_core_field_coverage_pct_min"],
            unit="%",
            why="Top-ranked FOIA leads need enough identifiers, office handles, vendor context, and record hooks to be reviewable.",
            hint=review_board_hint,
            actual={
                "considered_top_leads": mission_quality.get("considered_top_leads"),
                "core_field_counts": mission_quality.get("core_field_counts") or {},
                "rows_with_three_plus_core_fields": mission_quality.get("rows_with_three_plus_core_fields"),
                "rows_with_three_plus_core_fields_pct": mission_quality.get("rows_with_three_plus_core_fields_pct"),
            },
            validation_mode=validation_mode,
        )
    )

    checks.append(
        workflow_module._threshold_check(
            name="top_leads_family_diversity_threshold",
            observed=family_diversity.get("unique_primary_families"),
            threshold=thresholds["top_leads_family_diversity_min"],
            why="A family-collapsed top rank makes the surface weaker for FOIA target generation across programs, contractors, and facilities.",
            hint=review_board_hint,
            actual={
                "considered_top_leads": mission_quality.get("considered_top_leads"),
                "primary_family_counts": family_diversity.get("primary_family_counts") or {},
                "top_family": family_diversity.get("top_family"),
                "top_family_share_pct": family_diversity.get("top_family_share_pct"),
                "unassigned_count": family_diversity.get("unassigned_count"),
            },
            validation_mode=validation_mode,
        )
    )

    checks.append(
        workflow_module._threshold_check(
            name="nonstarter_pack_presence_threshold",
            observed=mission_quality.get("nonstarter_pack_presence_pct"),
            threshold=thresholds["nonstarter_pack_presence_pct_min"],
            unit="%",
            why="Useful FOIA leads should be driven by proxy or companion-pack evidence, not only starter ontology support.",
            hint=review_board_hint,
            actual={
                "considered_top_leads": mission_quality.get("considered_top_leads"),
                "nonstarter_pack_count": mission_quality.get("nonstarter_pack_count"),
                "top_non_starter_rules": mission_quality.get("top_non_starter_rules") or [],
            },
            validation_mode=validation_mode,
        )
    )

    checks.append(
        workflow_module._threshold_check(
            name="starter_only_pair_dominance_threshold",
            observed=mission_quality.get("starter_only_pair_share_pct"),
            threshold=thresholds["starter_only_pair_dominance_pct_max"],
            comparator="<=",
            unit="%",
            why="Starter-only kw_pair support should not dominate the top-ranked review surface.",
            hint=review_board_hint,
            actual={
                "considered_top_leads": mission_quality.get("considered_top_leads"),
                "starter_only_pair_count": mission_quality.get("starter_only_pair_count"),
                "starter_only_pair_share_pct": mission_quality.get("starter_only_pair_share_pct"),
            },
            validation_mode=validation_mode,
        )
    )

    checks.append(
        workflow_module._threshold_check(
            name="score_spread_threshold",
            observed=score_spread.get("spread"),
            threshold=thresholds["score_spread_min"],
            why="The top-ranked lead surface should not be so compressed that version drift or rank collapse is hidden from operators.",
            hint=review_board_hint,
            actual=score_spread,
            validation_mode=validation_mode,
        )
    )

    checks.append(
        workflow_module._threshold_check(
            name="routine_noise_share_threshold",
            observed=mission_quality.get("routine_noise_share_pct"),
            threshold=thresholds["routine_noise_share_pct_max"],
            comparator="<=",
            unit="%",
            why="Routine-noise suppressors should not make up a large share of the top-ranked FOIA review surface.",
            hint=review_board_hint,
            actual={
                "considered_top_leads": mission_quality.get("considered_top_leads"),
                "routine_noise_count": mission_quality.get("routine_noise_count"),
                "routine_noise_share_pct": mission_quality.get("routine_noise_share_pct"),
            },
            validation_mode=validation_mode,
        )
    )

    if bool(mission_quality.get("dossier_export_enabled")):
        checks.append(
            workflow_module._threshold_check(
                name="dossier_linkage_threshold",
                observed=mission_quality.get("dossier_linkage_pct"),
                threshold=thresholds["dossier_linkage_pct_min"],
                unit="%",
                why="When reviewer dossiers are exported, top leads should link cleanly into those artifact files.",
                hint=dossier_hint,
                actual={
                    "expected_count": mission_quality.get("dossier_expected_count"),
                    "linked_count": mission_quality.get("dossier_linked_count"),
                    "linkage_pct": mission_quality.get("dossier_linkage_pct"),
                },
                validation_mode=validation_mode,
            )
        )

    checks.append(
        workflow_module._threshold_check(
            name="foia_draftability_threshold",
            observed=foia_draftability.get("draftable_share_pct"),
            threshold=thresholds["foia_draftability_pct_min"],
            unit="%",
            why="Larger-run validation should fail when too few top-ranked leads are draftable into concrete FOIA follow-up targets.",
            hint=review_board_hint,
            actual={
                "considered_top_leads": mission_quality.get("considered_top_leads"),
                "draftable_count": foia_draftability.get("draftable_count"),
                "draftable_share_pct": foia_draftability.get("draftable_share_pct"),
                "levels": foia_draftability.get("levels") or {},
            },
            validation_mode=validation_mode,
        )
    )

    return checks


def _build_sam_run_status(
    *,
    failed_required_checks: list[dict[str, Any]],
    warning_checks: list[dict[str, Any]],
    comparison_summary: dict[str, Any],
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
    failed_required_names = {
        str(item.get("name") or "").strip()
        for item in failed_required_checks
        if str(item.get("name") or "").strip()
    }
    comparison_reason_codes = list(comparison_summary.get("reason_codes") or [])
    comparison_messages = list(comparison_summary.get("operator_messages") or [])

    has_required_failures = bool(failed_required_checks)
    has_advisory_failures = bool(warning_checks) or bool(comparison_reason_codes)
    has_usable_artifacts = bool(
        snapshot_items > 0
        or (
            events_window > 0
            and (
                events_with_keywords > 0
                or events_with_entity > 0
            )
        )
    )
    pipeline_blocked = bool(failed_required_names & {"doctor_db_ok", "workflow_execution"})

    if rate_limit_retries > 0 and has_usable_artifacts and not has_required_failures:
        quality = "rate_limited"
    elif not has_usable_artifacts:
        quality = "degraded" if pipeline_blocked else "sparse"
    elif has_required_failures or has_advisory_failures:
        quality = "degraded"
    else:
        quality = "healthy"

    partially_useful = bool(
        has_usable_artifacts
        and not has_required_failures
        and (has_advisory_failures or quality in {"degraded", "rate_limited"})
    )

    if has_required_failures:
        workflow_status = "failed"
    elif has_advisory_failures:
        workflow_status = "warning"
    else:
        workflow_status = "ok"

    reason_codes: list[str] = []
    reason_codes.extend(
        [f"required_check_failed:{item.get('name')}" for item in failed_required_checks if item.get("name")]
    )
    reason_codes.extend(
        [f"advisory_check_failed:{item.get('name')}" for item in warning_checks if item.get("name")]
    )
    reason_codes.extend(comparison_reason_codes)

    if workflow_status != "ok" or quality != "healthy":
        if quality == "degraded":
            reason_codes.append("quality_degraded")
        elif quality == "sparse":
            reason_codes.append("quality_sparse")
        elif quality == "rate_limited":
            reason_codes.append("quality_rate_limited")

    if workflow_status != "ok" and not reason_codes:
        reason_codes.append(f"workflow_status:{workflow_status}")
    if "mission_quality" in required_failure_categories:
        reason_codes.append("mission_quality_failed")
    elif "mission_quality" in advisory_failure_categories:
        reason_codes.append("mission_quality_warning")

    operator_messages: list[str] = []
    if required_failure_categories:
        operator_messages.append(
            "Required quality gates failed in: " + ", ".join(required_failure_categories) + "."
        )
    elif advisory_failure_categories:
        operator_messages.append(
            "Advisory quality misses were observed in: " + ", ".join(advisory_failure_categories) + "."
        )
    if warning_checks and not required_failure_categories and advisory_failure_categories:
        operator_messages.append(
            "Required gates passed, but advisory misses prevent treating this run as fully healthy."
        )
    if "mission_quality" in required_failure_categories:
        operator_messages.append(
            "Mission-quality review gates failed: the ranked FOIA lead surface is not strong enough yet even though ingest or structural coverage may have succeeded."
        )
    elif "mission_quality" in advisory_failure_categories:
        operator_messages.append(
            "Mission-quality weaknesses were detected on the ranked FOIA lead surface; review the top leads before treating this run as mission-healthy."
        )
    if rate_limit_retries > 0:
        operator_messages.append(
            "SAM.gov retries/429s were observed. Consider tuning SAM_API_TIMEOUT_SECONDS, "
            "SAM_API_MAX_RETRIES, and SAM_API_BACKOFF_BASE for larger windows."
        )
    if quality == "sparse":
        operator_messages.append(
            "Run did not produce enough usable SAM.gov artifacts to treat the output as fully reviewable."
        )
        if events_window == 0:
            operator_messages.append(
                "No SAM.gov events were found in the diagnostic window. Try a wider --days/--pages window before drawing conclusions."
            )
    if partially_useful:
        operator_messages.append(
            "Run produced usable artifacts, but nonfatal weaknesses prevent treating it as cleanly healthy."
        )
    if not ingest_nonzero and events_window == 0:
        reason_codes.append("empty_window_or_no_fresh_ingest")
    if rate_limit_retries > 0 and retry_attempts_total > 0:
        reason_codes.append("rate_limit_retries_observed")

    operator_messages.extend(comparison_messages)

    return {
        "workflow_status": workflow_status,
        "quality": quality,
        "has_required_failures": has_required_failures,
        "has_advisory_failures": has_advisory_failures,
        "has_usable_artifacts": has_usable_artifacts,
        "partially_useful": partially_useful,
        "rate_limit_retries": rate_limit_retries,
        "retry_attempts_total": retry_attempts_total,
        "reason_codes": _dedupe_text(reason_codes),
        "required_failure_categories": required_failure_categories,
        "advisory_failure_categories": advisory_failure_categories,
        "failure_categories": required_failure_categories + [
            category for category in advisory_failure_categories if category not in required_failure_categories
        ],
        "comparison_requested": bool(comparison_summary.get("requested")),
        "comparison_available": bool(comparison_summary.get("available")),
        "comparison_empty": bool(comparison_summary.get("empty")),
        "comparison_reason_codes": comparison_reason_codes,
        "comparison_operator_messages": comparison_messages,
        "comparison": comparison_summary,
        "operator_messages": _dedupe_text(operator_messages),
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
    scoring_version: str = DEFAULT_SCORING_VERSION,
    compare_scoring_versions: Optional[list[str]] = None,
    notes: Optional[str] = None,
    bundle_root: Optional[Path] = None,
    lead_dossier_top_n: int = DEFAULT_BUNDLE_DOSSIER_TOP_N,
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
    scoring_version_cmd = (
        f"{smoke_tune_cmd if mode == 'smoke' else larger_validate_cmd} --scoring-version v3"
    )
    review_board_hint = (
        f"Inspect {(bundle_dir / FOIA_LEAD_REVIEW_BOARD_HTML_PATH)} and {(bundle_dir / 'exports' / 'lead_snapshot.json')}."
    )
    dossier_hint = f"Inspect {(bundle_dir / 'report' / 'lead_dossiers')} and rerender the reviewer board if dossier files are missing."
    snapshot_export_items = _load_snapshot_export_items({"exports": workflow_res.get("exports")})
    snapshot_window_summary = _snapshot_window_diagnostics(
        items=snapshot_export_items,
        effective_window=effective_window,
    )
    family_distribution = _family_distribution_summary(snapshot_export_items)

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

    if snapshot_export_items or snapshot_items == 0:
        checks.append(
            workflow_module._serialize_check(
                name="snapshot_window_integrity",
                ok=snapshot_window_summary.get("outside_window_count", 0) == 0,
                observed={
                    "snapshot_event_min": snapshot_window_summary.get("snapshot_event_min"),
                    "snapshot_event_max": snapshot_window_summary.get("snapshot_event_max"),
                    "outside_window_count": snapshot_window_summary.get("outside_window_count"),
                    "top10_outside_window_count": snapshot_window_summary.get("top10_outside_window_count"),
                },
                actual={
                    "outside_window_event_ids": snapshot_window_summary.get("outside_window_event_ids") or [],
                    "top10_outside_window_event_ids": snapshot_window_summary.get("top10_outside_window_event_ids") or [],
                },
                threshold="outside_window_count == 0",
                expected="outside_window_count == 0",
                why="Historical SAM bundles must not leak lead rows from outside the effective postedDate window.",
                hint=rerun_snapshot_cmd,
                kind="validation",
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

    comparison_summary = _build_comparison_summary(
        compare_scoring_versions=list(compare_scoring_versions or []),
        workflow_exports=(
            workflow_res.get("exports")
            if isinstance(workflow_res, dict) and isinstance(workflow_res.get("exports"), dict)
            else {}
        ),
        requested_window=serialize_sam_posted_window(requested_window),
        effective_window=effective_window,
    )
    checks.extend(
        _build_comparison_checks(
            workflow_module=workflow_module,
            comparison_summary=comparison_summary,
            validation_mode=mode,
        )
    )

    lead_export = (
        workflow_res.get("exports", {}).get("lead_snapshot")
        if isinstance(workflow_res, dict) and isinstance(workflow_res.get("exports"), dict)
        else {}
    )
    lead_export = dict(lead_export or {})
    lead_snapshot_payload = _load_json_payload(lead_export.get("json"))
    review_summary_payload = _load_json_payload(lead_export.get("review_summary_json"))
    resolved_lead_dossier_top_n = max(int(lead_dossier_top_n), 0)
    snapshot_payload = workflow_res.get("snapshot") if isinstance(workflow_res.get("snapshot"), dict) else {}
    lead_snapshot_id = _safe_int(snapshot_payload.get("snapshot_id"))
    if lead_snapshot_id <= 0:
        lead_snapshot_id = _safe_int(((lead_snapshot_payload.get("snapshot") or {}).get("id")))
    dossier_exports = export_top_lead_evidence_packages(
        lead_snapshot=lead_snapshot_payload,
        bundle_dir=bundle_dir,
        top_n=resolved_lead_dossier_top_n,
        snapshot_id=lead_snapshot_id or None,
        database_url=database_url,
    )
    mission_quality_diagnostics = build_foia_lead_review_diagnostics(
        lead_snapshot=lead_snapshot_payload,
        review_summary=review_summary_payload,
        bundle_dir=bundle_dir,
        mission_top_n=10,
        dossier_top_n=resolved_lead_dossier_top_n,
        dossier_export_enabled=resolved_lead_dossier_top_n > 0,
    )
    mission_quality_summary = mission_quality_diagnostics["mission_quality"]
    checks.extend(
        _build_mission_quality_checks(
            workflow_module=workflow_module,
            mission_quality=mission_quality_summary,
            thresholds=thresholds,
            validation_mode=mode,
            scoring_version_cmd=scoring_version_cmd,
            review_board_hint=review_board_hint,
            dossier_hint=dossier_hint,
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

    status_summary = _build_sam_run_status(
        failed_required_checks=failed_required_checks,
        warning_checks=warning_checks,
        comparison_summary=comparison_summary,
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
            "scoring_version": str(scoring_version),
            "compare_scoring_versions": list(compare_scoring_versions or []),
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
            "lead_dossier_top_n": resolved_lead_dossier_top_n,
        }
    )

    baseline = {
        "captured_at": now.isoformat(),
        "source": "SAM.gov",
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": list(compare_scoring_versions or []),
        "window_days": int(window_days),
        "requested_window": serialize_sam_posted_window(requested_window),
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
        "lead_dossiers": {
            "top_n": resolved_lead_dossier_top_n,
            "snapshot_id": dossier_exports.get("snapshot_id"),
            "evidence_packages_written": dossier_exports.get("count"),
        },
        "mission_quality": mission_quality_summary,
        "snapshot_window_summary": snapshot_window_summary,
        "family_distribution_summary": family_distribution,
    }

    results_dir = bundle_dir / SAM_BUNDLE_RESULTS_DIR
    workflow_json = results_dir / "workflow_result.json"
    doctor_json = results_dir / "doctor_status.json"
    summary_json = results_dir / "workflow_summary.json"

    workflow_module._write_json(workflow_json, {"generated_at": now.isoformat(), "result": workflow_res})
    workflow_module._write_json(doctor_json, {"generated_at": now.isoformat(), "result": doc})

    workflow_status = str(status_summary.get("workflow_status") or "warning")

    summary_payload = {
        "generated_at": now.isoformat(),
        "source": "SAM.gov",
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "bundle_version": SAM_BUNDLE_VERSION,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": list(compare_scoring_versions or []),
        "workflow_status": workflow_status,
        "status": workflow_status,
        "quality": status_summary.get("quality"),
        "has_required_failures": bool(status_summary.get("has_required_failures")),
        "has_advisory_failures": bool(status_summary.get("has_advisory_failures")),
        "has_usable_artifacts": bool(status_summary.get("has_usable_artifacts")),
        "partially_useful": bool(status_summary.get("partially_useful")),
        "comparison_requested": bool(status_summary.get("comparison_requested")),
        "comparison_available": bool(status_summary.get("comparison_available")),
        "comparison_empty": bool(status_summary.get("comparison_empty")),
        "reason_codes": list(status_summary.get("reason_codes") or []),
        "operator_messages": list(status_summary.get("operator_messages") or []),
        "required_failure_categories": list(status_summary.get("required_failure_categories") or []),
        "advisory_failure_categories": list(status_summary.get("advisory_failure_categories") or []),
        "failure_categories": list(status_summary.get("failure_categories") or []),
        "comparison_reason_codes": list(status_summary.get("comparison_reason_codes") or []),
        "comparison_operator_messages": list(status_summary.get("comparison_operator_messages") or []),
        "comparison": status_summary.get("comparison") or {},
        "smoke_passed": smoke_passed,
        "required_checks_passed": required_checks_passed,
        "require_nonzero": bool(require_nonzero),
        "thresholds": thresholds,
        "quality_gate_policy": quality_gate_policy,
        "run_metadata": run_metadata,
        "lead_dossier_top_n": resolved_lead_dossier_top_n,
        "lead_dossiers": {
            "top_n": resolved_lead_dossier_top_n,
            "snapshot_id": dossier_exports.get("snapshot_id"),
            "evidence_packages_written": dossier_exports.get("count"),
        },
        "failed_required_checks": failed_required_checks,
        "failed_advisory_checks": warning_checks,
        "warning_checks": warning_checks,
        "checks": checks,
        "check_groups": check_groups,
        "mission_quality": mission_quality_summary,
        "baseline": baseline,
        "requested_window": serialize_sam_posted_window(requested_window),
        "effective_window": effective_window,
        "snapshot_event_min": snapshot_window_summary.get("snapshot_event_min"),
        "snapshot_event_max": snapshot_window_summary.get("snapshot_event_max"),
        "outside_window_count": snapshot_window_summary.get("outside_window_count"),
        "family_distribution_summary": family_distribution,
    }

    artifacts = {
        "bundle_manifest_json": bundle_dir / "bundle_manifest.json",
        "workflow_result_json": workflow_json,
        "workflow_summary_json": summary_json,
        "smoke_summary_json": summary_json,
        "doctor_status_json": doctor_json,
        "foia_lead_review_board_html": bundle_dir / FOIA_LEAD_REVIEW_BOARD_HTML_PATH,
        "foia_lead_review_board_md": bundle_dir / FOIA_LEAD_REVIEW_BOARD_MD_PATH,
        "lead_dossiers": None,
        "exports": (workflow_res.get("exports") if isinstance(workflow_res, dict) else None),
    }

    summary_payload["artifacts"] = {
        "workflow_result_json": workflow_json,
        "doctor_status_json": doctor_json,
        "smoke_summary_json": summary_json,
        "bundle_manifest_json": artifacts.get("bundle_manifest_json"),
        "report_html": bundle_dir / "report" / "bundle_report.html",
        "foia_lead_review_board_html": artifacts.get("foia_lead_review_board_html"),
        "foia_lead_review_board_md": artifacts.get("foia_lead_review_board_md"),
        "lead_dossiers": artifacts.get("lead_dossiers"),
        "exports": artifacts.get("exports"),
    }
    workflow_module._write_json(summary_json, summary_payload)

    review_board_artifacts = render_foia_lead_review_board_from_bundle(bundle_dir)
    artifacts["foia_lead_review_board_html"] = review_board_artifacts["html"]
    artifacts["foia_lead_review_board_md"] = review_board_artifacts["markdown"]
    artifacts["lead_dossiers"] = _lead_dossier_artifacts(
        bundle_dir=bundle_dir,
        review_board_artifacts=review_board_artifacts,
    )

    report_html = render_sam_bundle_report(
        bundle_dir=bundle_dir,
        title="SAM.gov Workflow Bundle Report",
        workflow_summary=summary_payload,
        artifacts=artifacts,
    )
    artifacts["report_html"] = report_html
    summary_payload["artifacts"]["report_html"] = report_html
    summary_payload["artifacts"]["foia_lead_review_board_html"] = artifacts["foia_lead_review_board_html"]
    summary_payload["artifacts"]["foia_lead_review_board_md"] = artifacts["foia_lead_review_board_md"]
    summary_payload["artifacts"]["lead_dossiers"] = artifacts.get("lead_dossiers")
    workflow_module._write_json(summary_json, summary_payload)

    lead_dossiers_payload = _load_json_payload((artifacts.get("lead_dossiers") or {}).get("index_json"))

    manifest_payload = {
        "bundle_version": SAM_BUNDLE_VERSION,
        "source": "SAM.gov",
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": list(compare_scoring_versions or []),
        "lead_dossier_top_n": resolved_lead_dossier_top_n,
        "generated_at": now.isoformat(),
        "workflow_status": workflow_status,
        "status": workflow_status,
        "quality": status_summary.get("quality"),
        "has_required_failures": bool(status_summary.get("has_required_failures")),
        "has_advisory_failures": bool(status_summary.get("has_advisory_failures")),
        "has_usable_artifacts": bool(status_summary.get("has_usable_artifacts")),
        "partially_useful": bool(status_summary.get("partially_useful")),
        "comparison_requested": bool(status_summary.get("comparison_requested")),
        "comparison_available": bool(status_summary.get("comparison_available")),
        "comparison_empty": bool(status_summary.get("comparison_empty")),
        "reason_codes": list(status_summary.get("reason_codes") or []),
        "operator_messages": list(status_summary.get("operator_messages") or []),
        "required_failure_categories": list(status_summary.get("required_failure_categories") or []),
        "advisory_failure_categories": list(status_summary.get("advisory_failure_categories") or []),
        "failure_categories": list(status_summary.get("failure_categories") or []),
        "comparison_reason_codes": list(status_summary.get("comparison_reason_codes") or []),
        "comparison_operator_messages": list(status_summary.get("comparison_operator_messages") or []),
        "comparison": status_summary.get("comparison") or {},
        "summary_counts": {
            "events_window": events_window,
            "events_with_keywords": events_with_keywords,
            "events_with_entity_window": events_with_entity,
            "same_keyword": same_keyword_lane,
            "kw_pair": kw_pair_lane,
            "same_sam_naics": same_sam_naics_lane,
            "snapshot_items": snapshot_items,
            "outside_window_count": snapshot_window_summary.get("outside_window_count"),
        },
        "check_summary": {
            "total": len(checks),
            "passed": len([item for item in checks if bool(item.get("passed"))]),
            "required_total": len([item for item in checks if bool(item.get("required"))]),
            "advisory_total": len([item for item in checks if not bool(item.get("required"))]),
            "failed_required": len(failed_required_checks),
            "failed_advisory": len(warning_checks),
            "warnings": len(warning_checks),
            "required_failure_categories": status_summary.get("required_failure_categories") or [],
            "advisory_failure_categories": status_summary.get("advisory_failure_categories") or [],
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
        "mission_quality": mission_quality_summary,
        "lead_dossiers": lead_dossiers_payload
        or {
            "top_n": resolved_lead_dossier_top_n,
            "snapshot_id": dossier_exports.get("snapshot_id"),
            "evidence_packages_written": dossier_exports.get("count"),
        },
        "quality_gate_policy": quality_gate_policy,
        "run_parameters": {
            "requested_window": serialize_sam_posted_window(requested_window),
            "effective_window": effective_window,
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
            "scoring_version": str(scoring_version),
            "compare_scoring_versions": list(compare_scoring_versions or []),
            "lead_dossier_top_n": resolved_lead_dossier_top_n,
            "date_from": _iso_or_none(date_from),
            "date_to": _iso_or_none(date_to),
            "occurred_after": _iso_or_none(occurred_after),
            "occurred_before": _iso_or_none(occurred_before),
            "created_after": _iso_or_none(created_after),
            "created_before": _iso_or_none(created_before),
            "since_days": int(since_days) if since_days is not None else None,
            "ontology_path": str(Path(ontology_path)),
        },
        "snapshot_event_span": {
            "snapshot_event_min": snapshot_window_summary.get("snapshot_event_min"),
            "snapshot_event_max": snapshot_window_summary.get("snapshot_event_max"),
            "outside_window_count": snapshot_window_summary.get("outside_window_count"),
            "outside_window_event_ids": snapshot_window_summary.get("outside_window_event_ids") or [],
        },
        "family_distribution_summary": family_distribution,
        "ingest_diagnostics": ingest_request_diag,
        "generated_files": flatten_bundle_files(artifacts=artifacts, bundle_dir=bundle_dir),
    }
    manifest_json = write_bundle_manifest(bundle_dir=bundle_dir, payload=manifest_payload)
    artifacts["bundle_manifest_json"] = manifest_json

    return {
        "workflow_status": workflow_status,
        "status": workflow_status,
        "quality": status_summary.get("quality"),
        "has_required_failures": bool(status_summary.get("has_required_failures")),
        "has_advisory_failures": bool(status_summary.get("has_advisory_failures")),
        "has_usable_artifacts": bool(status_summary.get("has_usable_artifacts")),
        "partially_useful": bool(status_summary.get("partially_useful")),
        "comparison_requested": bool(status_summary.get("comparison_requested")),
        "comparison_available": bool(status_summary.get("comparison_available")),
        "comparison_empty": bool(status_summary.get("comparison_empty")),
        "reason_codes": list(status_summary.get("reason_codes") or []),
        "operator_messages": list(status_summary.get("operator_messages") or []),
        "required_failure_categories": list(status_summary.get("required_failure_categories") or []),
        "advisory_failure_categories": list(status_summary.get("advisory_failure_categories") or []),
        "comparison_reason_codes": list(status_summary.get("comparison_reason_codes") or []),
        "comparison_operator_messages": list(status_summary.get("comparison_operator_messages") or []),
        "comparison": status_summary.get("comparison") or {},
        "smoke_passed": bool(smoke_passed),
        "required_checks_passed": bool(required_checks_passed),
        "workflow_type": workflow_type,
        "validation_mode": mode,
        "scoring_version": str(scoring_version),
        "compare_scoring_versions": list(compare_scoring_versions or []),
        "lead_dossier_top_n": resolved_lead_dossier_top_n,
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
        "mission_quality": mission_quality_summary,
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
    scoring_version: str = DEFAULT_SCORING_VERSION,
    compare_scoring_versions: Optional[list[str]] = None,
    notes: Optional[str] = "samgov larger-run validation",
    bundle_root: Optional[Path] = None,
    lead_dossier_top_n: int = DEFAULT_BUNDLE_DOSSIER_TOP_N,
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
        compare_scoring_versions=list(compare_scoring_versions or []),
        notes=notes,
        bundle_root=(Path(bundle_root).expanduser() if bundle_root else EXPORTS_DIR / "validation" / "samgov"),
        lead_dossier_top_n=int(lead_dossier_top_n),
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
