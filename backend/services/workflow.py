from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from backend.correlate import correlate
from backend.services.doctor import doctor_status  # noqa: F401
from backend.services.entities import link_entities_from_events
from backend.services.export import export_events
from backend.services.export_correlations import export_kw_pairs
from backend.services.export_entities import export_entities_bundle
from backend.services.export_leads import export_lead_snapshot, export_scoring_comparison
from backend.services.ingest import (
    append_sam_posted_window_note,
    ingest_sam_opportunities,
    ingest_usaspending,
    resolve_sam_posted_window,
    serialize_sam_posted_window,
)
from backend.services.leads import DEFAULT_SCORING_VERSION, create_lead_snapshot
from backend.services.tagging import apply_ontology_to_events

_IngestFn = Callable[..., dict[str, Any]]


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


DEFAULT_SAM_SMOKE_THRESHOLDS: dict[str, float] = {
    # Keep this low enough for deterministic fixture tests, but above trivial non-zero.
    "events_window_min": 3.0,
    "events_with_keywords_coverage_pct_min": 60.0,
    "events_with_entity_coverage_pct_min": 60.0,
    "keyword_signal_total_min": 3.0,
    "events_with_research_context_min": 2.0,
    "research_context_coverage_pct_min": 60.0,
    "events_with_core_procurement_context_min": 2.0,
    "core_procurement_context_coverage_pct_min": 60.0,
    "avg_context_fields_per_event_min": 2.5,
    "sam_notice_type_coverage_pct_min": 70.0,
    "sam_solicitation_number_coverage_pct_min": 70.0,
    "sam_naics_code_coverage_pct_min": 60.0,
    "same_sam_naics_lane_min": 1.0,
    "snapshot_items_min": 1.0,
    "top_leads_core_field_coverage_pct_min": 70.0,
    "top_leads_family_diversity_min": 3.0,
    "nonstarter_pack_presence_pct_min": 60.0,
    "starter_only_pair_dominance_pct_max": 35.0,
    "score_spread_min": 5.0,
    "routine_noise_share_pct_max": 35.0,
    "dossier_linkage_pct_min": 100.0,
    "foia_draftability_pct_min": 40.0,
}

DEFAULT_SAM_LARGER_THRESHOLDS: dict[str, float] = dict(DEFAULT_SAM_SMOKE_THRESHOLDS)

SAM_VALIDATION_CATEGORY_LABELS: dict[str, str] = {
    "pipeline_health": "Pipeline health",
    "source_coverage_context_health": "Source coverage/context health",
    "lead_signal_quality": "Lead-signal quality",
    "mission_quality": "Mission quality",
}

SAM_VALIDATION_CHECK_POLICIES: dict[str, dict[str, dict[str, Any]]] = {
    "smoke": {
        "doctor_db_ok": {
            "category": "pipeline_health",
            "severity": "critical",
            "required": True,
        },
        "workflow_execution": {
            "category": "pipeline_health",
            "severity": "critical",
            "required": True,
        },
        "ingest_nonzero": {
            "category": "pipeline_health",
            "severity": "critical",
            "required": True,
        },
        "ingest_retry_pressure": {
            "category": "pipeline_health",
            "severity": "warning",
            "required": False,
        },
        "events_window_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "events_with_keywords_coverage_threshold": {
            "category": "lead_signal_quality",
            "severity": "error",
            "required": True,
        },
        "events_with_entity_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "keyword_or_kw_pair_signal_threshold": {
            "category": "lead_signal_quality",
            "severity": "error",
            "required": True,
        },
        "sam_research_context_events_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_research_context_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_core_procurement_context_events_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_core_procurement_context_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_avg_context_fields_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_notice_type_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_solicitation_number_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_naics_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "same_sam_naics_lane_threshold": {
            "category": "lead_signal_quality",
            "severity": "error",
            "required": True,
        },
        "snapshot_items_threshold": {
            "category": "lead_signal_quality",
            "severity": "error",
            "required": True,
        },
        "scoring_comparison_available": {
            "category": "lead_signal_quality",
            "severity": "warning",
            "required": False,
        },
        "scoring_comparison_non_empty": {
            "category": "lead_signal_quality",
            "severity": "warning",
            "required": False,
        },
        "comparison_effective_window_matches_request": {
            "category": "source_coverage_context_health",
            "severity": "warning",
            "required": False,
        },
        "scoring_version_is_v3": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
        "top_leads_core_field_coverage_threshold": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
        "top_leads_family_diversity_threshold": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
        "nonstarter_pack_presence_threshold": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
        "starter_only_pair_dominance_threshold": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
        "score_spread_threshold": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
        "routine_noise_share_threshold": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
        "dossier_linkage_threshold": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
        "foia_draftability_threshold": {
            "category": "mission_quality",
            "severity": "warning",
            "required": False,
        },
    },
    "larger": {
        "doctor_db_ok": {
            "category": "pipeline_health",
            "severity": "critical",
            "required": True,
        },
        "workflow_execution": {
            "category": "pipeline_health",
            "severity": "critical",
            "required": True,
        },
        "ingest_nonzero": {
            "category": "pipeline_health",
            "severity": "critical",
            "required": True,
        },
        "ingest_retry_pressure": {
            "category": "pipeline_health",
            "severity": "warning",
            "required": False,
        },
        "events_window_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "events_with_keywords_coverage_threshold": {
            "category": "lead_signal_quality",
            "severity": "warning",
            "required": False,
        },
        "events_with_entity_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "keyword_or_kw_pair_signal_threshold": {
            "category": "lead_signal_quality",
            "severity": "error",
            "required": True,
        },
        "sam_research_context_events_threshold": {
            "category": "source_coverage_context_health",
            "severity": "warning",
            "required": False,
        },
        "sam_research_context_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_core_procurement_context_events_threshold": {
            "category": "source_coverage_context_health",
            "severity": "warning",
            "required": False,
        },
        "sam_core_procurement_context_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "sam_avg_context_fields_threshold": {
            "category": "source_coverage_context_health",
            "severity": "warning",
            "required": False,
        },
        "sam_notice_type_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "warning",
            "required": False,
        },
        "sam_solicitation_number_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "warning",
            "required": False,
        },
        "sam_naics_coverage_threshold": {
            "category": "source_coverage_context_health",
            "severity": "error",
            "required": True,
        },
        "same_sam_naics_lane_threshold": {
            "category": "lead_signal_quality",
            "severity": "warning",
            "required": False,
        },
        "snapshot_items_threshold": {
            "category": "lead_signal_quality",
            "severity": "error",
            "required": True,
        },
        "larger_run_window_signal": {
            "category": "source_coverage_context_health",
            "severity": "warning",
            "required": False,
        },
        "scoring_comparison_available": {
            "category": "lead_signal_quality",
            "severity": "warning",
            "required": False,
        },
        "scoring_comparison_non_empty": {
            "category": "lead_signal_quality",
            "severity": "warning",
            "required": False,
        },
        "comparison_effective_window_matches_request": {
            "category": "source_coverage_context_health",
            "severity": "warning",
            "required": False,
        },
        "scoring_version_is_v3": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
        "top_leads_core_field_coverage_threshold": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
        "top_leads_family_diversity_threshold": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
        "nonstarter_pack_presence_threshold": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
        "starter_only_pair_dominance_threshold": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
        "score_spread_threshold": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
        "routine_noise_share_threshold": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
        "dossier_linkage_threshold": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
        "foia_draftability_threshold": {
            "category": "mission_quality",
            "severity": "error",
            "required": True,
        },
    },
}


def _normalize_validation_mode(value: str) -> str:
    normalized = str(value or "smoke").strip().lower()
    return normalized if normalized in {"smoke", "larger"} else "smoke"


def _resolve_sam_validation_thresholds(
    overrides: Optional[dict[str, Any]],
    *,
    validation_mode: str = "smoke",
) -> dict[str, float]:
    mode = _normalize_validation_mode(validation_mode)
    base = DEFAULT_SAM_SMOKE_THRESHOLDS if mode == "smoke" else DEFAULT_SAM_LARGER_THRESHOLDS
    resolved = dict(base)
    for key, value in (overrides or {}).items():
        if key not in resolved:
            continue
        parsed = _safe_float(value, default=resolved[key])
        if key.endswith("_pct_min") or key.endswith("_pct_max"):
            parsed = max(0.0, min(100.0, parsed))
        else:
            parsed = max(0.0, parsed)
        resolved[key] = parsed
    return resolved


def _resolve_sam_larger_thresholds(overrides: Optional[dict[str, Any]]) -> dict[str, float]:
    return _resolve_sam_validation_thresholds(overrides, validation_mode="larger")


def _resolve_sam_smoke_thresholds(overrides: Optional[dict[str, Any]]) -> dict[str, float]:
    return _resolve_sam_validation_thresholds(overrides, validation_mode="smoke")


def _format_threshold_value(value: float) -> str:
    if abs(value - int(value)) < 1e-9:
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _get_sam_validation_check_policy(
    name: str,
    *,
    validation_mode: str = "smoke",
) -> dict[str, Any]:
    mode = _normalize_validation_mode(validation_mode)
    policy = SAM_VALIDATION_CHECK_POLICIES.get(mode, {})
    fallback_policy = SAM_VALIDATION_CHECK_POLICIES["smoke"]
    resolved = dict(fallback_policy.get(name) or {})
    resolved.update(policy.get(name) or {})
    category = str(resolved.get("category") or "pipeline_health")
    return {
        "category": category,
        "category_label": SAM_VALIDATION_CATEGORY_LABELS.get(category, category.replace("_", " ")),
        "severity": str(resolved.get("severity") or "warning"),
        "required": bool(resolved.get("required", True)),
    }


def _list_sam_validation_policy_checks(*, validation_mode: str = "smoke") -> list[dict[str, Any]]:
    mode = _normalize_validation_mode(validation_mode)
    fallback_policy = SAM_VALIDATION_CHECK_POLICIES["smoke"]
    mode_policy = SAM_VALIDATION_CHECK_POLICIES.get(mode, {})
    ordered_names = list(dict.fromkeys(list(fallback_policy.keys()) + list(mode_policy.keys())))
    checks: list[dict[str, Any]] = []
    for name in ordered_names:
        policy = _get_sam_validation_check_policy(name, validation_mode=mode)
        checks.append(
            {
                "name": name,
                "category": policy["category"],
                "category_label": policy["category_label"],
                "severity": policy["severity"],
                "required": policy["required"],
                "policy_level": "required" if bool(policy["required"]) else "advisory",
            }
        )
    return checks


def _serialize_check(
    *,
    name: str,
    ok: bool,
    observed: Any,
    threshold: Any,
    expected: str,
    why: str,
    hint: str,
    actual: Any = None,
    comparator: Optional[str] = None,
    unit: str = "",
    kind: str = "threshold",
    validation_mode: str = "smoke",
    required: Optional[bool] = None,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
) -> dict[str, Any]:
    policy = _get_sam_validation_check_policy(name, validation_mode=validation_mode)
    category_name = str(category or policy["category"])
    category_label = SAM_VALIDATION_CATEGORY_LABELS.get(category_name, category_name.replace("_", " "))
    required_flag = policy["required"] if required is None else bool(required)
    severity_name = str(severity or policy["severity"])
    passed = bool(ok)
    legacy_status = status or ("pass" if passed else ("fail" if required_flag else "info"))
    return {
        "name": name,
        "kind": kind,
        "category": category_name,
        "category_label": category_label,
        "severity": severity_name,
        "required": required_flag,
        "policy_level": "required" if required_flag else "advisory",
        "ok": passed,
        "passed": passed,
        "result": "pass" if passed else "fail",
        "status": legacy_status,
        "observed": observed,
        "actual": observed if actual is None else actual,
        "threshold": threshold,
        "expected": expected,
        "comparator": comparator,
        "unit": unit,
        "why": why,
        "hint": hint,
    }


def _threshold_check(
    *,
    name: str,
    observed: Any,
    threshold: float,
    comparator: str = ">=",
    required: Optional[bool] = None,
    unit: str = "",
    why: str,
    hint: str,
    actual: Any = None,
    validation_mode: str = "smoke",
    severity: Optional[str] = None,
    category: Optional[str] = None,
) -> dict[str, Any]:
    observed_num = _safe_float(observed, default=0.0)
    comparators = {
        ">=": observed_num >= threshold,
        "<=": observed_num <= threshold,
        ">": observed_num > threshold,
        "<": observed_num < threshold,
        "==": abs(observed_num - threshold) < 1e-9,
    }
    ok = comparators.get(comparator, False)
    expected = f"{comparator} {_format_threshold_value(threshold)}{unit}"
    return _serialize_check(
        name=name,
        ok=bool(ok),
        observed=observed,
        threshold=threshold,
        expected=expected,
        why=why,
        hint=hint,
        actual=actual,
        comparator=comparator,
        unit=unit,
        kind="threshold",
        validation_mode=validation_mode,
        required=required,
        severity=severity,
        category=category,
    )

def _normalize_for_json(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {k: _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_for_json(v) for v in value]
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_normalize_for_json(payload), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _make_output_resolver(output: Optional[Path]) -> Callable[[str, Optional[int]], Optional[Path]]:
    out_path = Path(output).expanduser() if output else None
    out_is_file = False

    if out_path is not None:
        try:
            if out_path.exists() and out_path.is_dir():
                out_is_file = False
            elif out_path.suffix:
                out_is_file = True
        except Exception:
            if out_path.suffix:
                out_is_file = True

    export_dir: Optional[Path] = None
    base: Optional[str] = None
    ts: Optional[str] = None

    if out_is_file and out_path is not None:
        export_dir = out_path.parent if out_path.parent else Path(".")
        export_dir.mkdir(parents=True, exist_ok=True)
        base = out_path.stem or "run"
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def _out(kind: str, snapshot_id: Optional[int] = None) -> Optional[Path]:
        if out_path is None:
            return None
        if not out_is_file:
            return out_path
        assert export_dir is not None and base is not None and ts is not None
        if kind == "lead_snapshot" and snapshot_id is not None:
            return export_dir / f"{base}_lead_snapshot_{int(snapshot_id)}_{ts}.csv"
        return export_dir / f"{base}_{kind}_{ts}.csv"

    return _out


def _build_sam_workflow_run_metadata(
    *,
    date_window: dict[str, object],
    pages: int,
    page_size: int,
    max_records: Optional[int],
    start_page: int,
    window_days: int,
    keywords: Optional[list[str]],
    ontology_path: Path,
) -> dict[str, Any]:
    payload = serialize_sam_posted_window(date_window)
    return {
        "source": "SAM.gov",
        "ingest_days": payload.get("effective_days"),
        "posted_window_mode": payload.get("mode"),
        "effective_posted_from": payload.get("posted_from"),
        "effective_posted_to": payload.get("posted_to"),
        "calendar_span_days": payload.get("calendar_span_days"),
        "pages": int(pages),
        "page_size": int(page_size),
        "max_records": max_records,
        "start_page": int(start_page),
        "window_days": int(window_days),
        "keywords": list(keywords or []),
        "ontology_path": str(Path(ontology_path)),
    }


def _run_source_workflow(
    *,
    source: str,
    ingest_fn: _IngestFn,
    ingest_kwargs: dict[str, Any],
    ontology_path: Path,
    ontology_days: int,
    analysis_run_id: Optional[int],
    entity_days: int,
    entity_batch: int,
    window_days: int,
    min_events_entity: int,
    min_events_keywords: int,
    max_events_keywords: int,
    max_keywords_per_event: int,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    occurred_after: Optional[datetime],
    occurred_before: Optional[datetime],
    created_after: Optional[datetime],
    created_before: Optional[datetime],
    since_days: Optional[int],
    min_score: int,
    snapshot_limit: int,
    scan_limit: int,
    scoring_version: str,
    compare_scoring_versions: Optional[list[str]],
    notes: Optional[str],
    notes_resolver: Optional[Callable[[dict[str, Any], Optional[str]], Optional[str]]],
    output: Optional[Path],
    export_events_flag: bool,
    kw_pairs_limit: int,
    kw_pairs_min_event_count: int,
    database_url: Optional[str],
    skip_ingest: bool,
    skip_ontology: bool,
    skip_entities: bool,
    skip_correlations: bool,
    skip_snapshot: bool,
    skip_exports: bool,
    abort_on_ingest_skip: bool = False,
) -> dict[str, Any]:
    res: dict[str, Any] = {"source": source}

    if not skip_ingest:
        ing = ingest_fn(database_url=database_url, **ingest_kwargs)
        res["ingest"] = ing
        if abort_on_ingest_skip and isinstance(ing, dict) and ing.get("status") == "skipped":
            res["status"] = "skipped"
            return res

    arid = analysis_run_id
    if not skip_ontology:
        ont = apply_ontology_to_events(
            ontology_path=Path(ontology_path),
            days=int(ontology_days),
            source=source,
            batch=500,
            dry_run=False,
            database_url=database_url,
        )
        res["ontology_apply"] = ont
        if arid is None:
            arid = ont.get("analysis_run_id")

    if not skip_entities:
        ent = link_entities_from_events(
            source=source,
            days=int(entity_days),
            batch=int(entity_batch),
            dry_run=False,
            database_url=database_url,
        )
        res["entities_link"] = ent

    if not skip_correlations:
        corr: dict[str, Any] = {}
        corr["same_entity"] = correlate.rebuild_entity_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_entity),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_uei"] = correlate.rebuild_uei_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_entity),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_award_id"] = correlate.rebuild_award_id_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_entity),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_contract_id"] = correlate.rebuild_contract_id_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_entity),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_doc_id"] = correlate.rebuild_doc_id_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_entity),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_agency"] = correlate.rebuild_agency_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_keywords),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_psc"] = correlate.rebuild_psc_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_keywords),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_naics"] = correlate.rebuild_naics_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_keywords),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_place_region"] = correlate.rebuild_place_region_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_keywords),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_keyword"] = correlate.rebuild_keyword_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_keywords),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["kw_pair"] = correlate.rebuild_keyword_pair_correlations(
            window_days=int(window_days),
            source=source,
            min_events=int(min_events_keywords),
            max_events=int(max_events_keywords),
            max_keywords_per_event=int(max_keywords_per_event),
            dry_run=False,
            database_url=database_url,
        )
        if source == "SAM.gov":
            corr["same_sam_naics"] = correlate.rebuild_sam_naics_correlations(
                window_days=int(window_days),
                source=source,
                min_events=int(min_events_keywords),
                max_events=int(max_events_keywords),
                dry_run=False,
                database_url=database_url,
            )
        res["correlations"] = corr

    snapshot_id: Optional[int] = None
    if not skip_snapshot:
        snapshot_notes = notes_resolver(res, notes) if notes_resolver is not None else notes
        snap = create_lead_snapshot(
            analysis_run_id=arid,
            source=source,
            date_from=date_from,
            date_to=date_to,
            occurred_after=occurred_after,
            occurred_before=occurred_before,
            created_after=created_after,
            created_before=created_before,
            since_days=since_days,
            min_score=int(min_score),
            limit=int(snapshot_limit),
            scan_limit=int(scan_limit),
            scoring_version=str(scoring_version),
            notes=snapshot_notes,
            database_url=database_url,
        )
        snap["notes"] = snapshot_notes
        res["snapshot"] = snap
        snapshot_id = _safe_int(snap.get("snapshot_id"), default=0) or None

    if not skip_exports:
        exports: dict[str, Any] = {}
        out = _make_output_resolver(output)

        if snapshot_id is not None:
            exports["lead_snapshot"] = export_lead_snapshot(
                snapshot_id=int(snapshot_id),
                database_url=database_url,
                output=out("lead_snapshot", snapshot_id),
            )
            if compare_scoring_versions:
                exports["scoring_comparison"] = export_scoring_comparison(
                    versions=list(compare_scoring_versions),
                    source=source,
                    min_score=int(min_score),
                    limit=int(snapshot_limit),
                    scan_limit=int(scan_limit),
                    database_url=database_url,
                    output=out("scoring_comparison"),
                )

        exports["kw_pairs"] = export_kw_pairs(
            database_url=database_url,
            output=out("kw_pairs"),
            limit=int(kw_pairs_limit),
            min_event_count=int(kw_pairs_min_event_count),
        )

        exports["entities"] = export_entities_bundle(
            database_url=database_url,
            output=out("entities"),
        )

        if export_events_flag:
            exports["events"] = export_events(
                database_url=database_url,
                output=out("events"),
            )

        res["exports"] = exports

    return res


def run_usaspending_workflow(
    *,
    ingest_days: int = 30,
    pages: int = 1,
    page_size: int = 100,
    max_records: Optional[int] = None,
    start_page: int = 1,
    recipient_search_text: Optional[list[str]] = None,
    keywords: Optional[list[str]] = None,
    ontology_path: Path = Path("examples/ontology_usaspending_starter.json"),
    ontology_days: int = 30,
    analysis_run_id: Optional[int] = None,
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
    output: Optional[Path] = None,
    export_events_flag: bool = False,
    kw_pairs_limit: int = 200,
    kw_pairs_min_event_count: int = 2,
    database_url: Optional[str] = None,
    skip_ingest: bool = False,
    skip_ontology: bool = False,
    skip_entities: bool = False,
    skip_correlations: bool = False,
    skip_snapshot: bool = False,
    skip_exports: bool = False,
) -> dict[str, Any]:
    """One-command USAspending workflow wrapper."""
    return _run_source_workflow(
        source="USAspending",
        ingest_fn=ingest_usaspending,
        ingest_kwargs={
            "days": int(ingest_days),
            "pages": int(pages),
            "page_size": int(page_size),
            "max_records": max_records,
            "start_page": int(start_page),
            "recipient_search_text": recipient_search_text,
            "keywords": keywords,
        },
        ontology_path=Path(ontology_path),
        ontology_days=int(ontology_days),
        analysis_run_id=analysis_run_id,
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
        compare_scoring_versions=None,
        notes=notes,
        notes_resolver=None,
        output=Path(output).expanduser() if output else None,
        export_events_flag=bool(export_events_flag),
        kw_pairs_limit=int(kw_pairs_limit),
        kw_pairs_min_event_count=int(kw_pairs_min_event_count),
        database_url=database_url,
        skip_ingest=bool(skip_ingest),
        skip_ontology=bool(skip_ontology),
        skip_entities=bool(skip_entities),
        skip_correlations=bool(skip_correlations),
        skip_snapshot=bool(skip_snapshot),
        skip_exports=bool(skip_exports),
    )


def run_samgov_workflow(
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
    analysis_run_id: Optional[int] = None,
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
    output: Optional[Path] = None,
    export_events_flag: bool = True,
    kw_pairs_limit: int = 200,
    kw_pairs_min_event_count: int = 2,
    database_url: Optional[str] = None,
    skip_ingest: bool = False,
    skip_ontology: bool = False,
    skip_entities: bool = False,
    skip_correlations: bool = False,
    skip_snapshot: bool = False,
    skip_exports: bool = False,
    abort_on_ingest_skip: bool = True,
) -> dict[str, Any]:
    """One-command SAM.gov workflow wrapper."""
    requested_window = resolve_sam_posted_window(days=ingest_days, posted_from=posted_from, posted_to=posted_to)

    def _resolve_snapshot_notes(res: dict[str, Any], base_notes: Optional[str]) -> Optional[str]:
        ingest_window = None
        ingest_payload = res.get("ingest")
        if isinstance(ingest_payload, dict) and isinstance(ingest_payload.get("date_window"), dict):
            ingest_window = ingest_payload.get("date_window")
        return append_sam_posted_window_note(base_notes, window=ingest_window or requested_window)

    res = _run_source_workflow(
        source="SAM.gov",
        ingest_fn=ingest_sam_opportunities,
        ingest_kwargs={
            "api_key": api_key,
            "days": int(ingest_days) if ingest_days is not None else None,
            "posted_from": posted_from,
            "posted_to": posted_to,
            "pages": int(pages),
            "page_size": int(page_size),
            "max_records": max_records,
            "start_page": int(start_page),
            "keywords": keywords,
        },
        ontology_path=Path(ontology_path),
        ontology_days=int(ontology_days),
        analysis_run_id=analysis_run_id,
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
        notes_resolver=_resolve_snapshot_notes,
        output=Path(output).expanduser() if output else None,
        export_events_flag=bool(export_events_flag),
        kw_pairs_limit=int(kw_pairs_limit),
        kw_pairs_min_event_count=int(kw_pairs_min_event_count),
        database_url=database_url,
        skip_ingest=bool(skip_ingest),
        skip_ontology=bool(skip_ontology),
        skip_entities=bool(skip_entities),
        skip_correlations=bool(skip_correlations),
        skip_snapshot=bool(skip_snapshot),
        skip_exports=bool(skip_exports),
        abort_on_ingest_skip=bool(abort_on_ingest_skip),
    )
    effective_window = requested_window
    ingest_payload = res.get("ingest")
    if isinstance(ingest_payload, dict) and isinstance(ingest_payload.get("date_window"), dict):
        effective_window = ingest_payload.get("date_window")  # type: ignore[assignment]
    res["run_metadata"] = _build_sam_workflow_run_metadata(
        date_window=effective_window,
        pages=int(pages),
        page_size=int(page_size),
        max_records=max_records,
        start_page=int(start_page),
        window_days=int(window_days),
        keywords=keywords,
        ontology_path=Path(ontology_path),
    )
    return res


# Hardened SAM workflow wrappers (bundle normalization + larger-run validation mode)
def run_samgov_smoke_workflow(
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
    database_url: Optional[str] = None,
    require_nonzero: bool = True,
    skip_ingest: bool = False,
    threshold_overrides: Optional[dict[str, Any]] = None,
    validation_mode: str = "smoke",
    workflow_type: str = "samgov-smoke",
) -> dict[str, Any]:
    from backend.services.sam_workflow_hardening import run_samgov_smoke_workflow_hardened

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
        bundle_root=(Path(bundle_root).expanduser() if bundle_root else None),
        database_url=database_url,
        require_nonzero=bool(require_nonzero),
        skip_ingest=bool(skip_ingest),
        threshold_overrides=threshold_overrides,
        validation_mode=str(validation_mode),
        workflow_type=str(workflow_type),
    )


def run_samgov_validation_workflow(
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
    database_url: Optional[str] = None,
    require_nonzero: bool = True,
    skip_ingest: bool = False,
    threshold_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    from backend.services.sam_workflow_hardening import run_samgov_validation_workflow_hardened

    return run_samgov_validation_workflow_hardened(
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
        bundle_root=(Path(bundle_root).expanduser() if bundle_root else None),
        database_url=database_url,
        require_nonzero=bool(require_nonzero),
        skip_ingest=bool(skip_ingest),
        threshold_overrides=threshold_overrides,
    )


__all__ = [
    "run_usaspending_workflow",
    "run_samgov_workflow",
    "run_samgov_smoke_workflow",
    "run_samgov_validation_workflow",
]
