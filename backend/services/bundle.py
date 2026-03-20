from __future__ import annotations

import html
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.services.foia_review_board import (
    FOIA_LEAD_DOSSIER_INDEX_CSV_PATH,
    FOIA_LEAD_DOSSIER_INDEX_JSON_PATH,
    FOIA_LEAD_REVIEW_BOARD_HTML_PATH,
    FOIA_LEAD_REVIEW_BOARD_MD_PATH,
)

SAM_BUNDLE_VERSION = "samgov.bundle.v2"
SAM_BUNDLE_MANIFEST_NAME = "bundle_manifest.json"
SAM_BUNDLE_RESULTS_DIR = Path("results")
SAM_BUNDLE_EXPORTS_DIR = Path("exports")
SAM_BUNDLE_REPORT_PATH = Path("report") / "bundle_report.html"


def _as_path(value: Any) -> Optional[Path]:
    if isinstance(value, Path):
        return value
    if isinstance(value, str) and value.strip():
        return Path(value)
    return None


def _rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path)


def _move_file(src: Path, dest: Path) -> None:
    src = src.expanduser()
    dest = dest.expanduser()
    if not src.exists():
        return
    if src.resolve() == dest.resolve():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()
    try:
        src.replace(dest)
    except Exception:
        shutil.move(str(src), str(dest))


def normalize_sam_exports(
    *,
    workflow_exports: dict[str, Any] | None,
    bundle_dir: Path,
) -> dict[str, Any]:
    """Normalize workflow export paths into stable bundle-relative filenames."""
    exports = dict(workflow_exports or {})
    bundle_dir = bundle_dir.expanduser()
    export_dir = bundle_dir / SAM_BUNDLE_EXPORTS_DIR
    export_dir.mkdir(parents=True, exist_ok=True)

    lead = dict(exports.get("lead_snapshot") or {})
    lead_csv_src = _as_path(lead.get("csv"))
    lead_json_src = _as_path(lead.get("json"))
    lead_review_summary_src = _as_path(lead.get("review_summary_json"))
    lead_csv_dst = export_dir / "lead_snapshot.csv"
    lead_json_dst = export_dir / "lead_snapshot.json"
    lead_review_summary_dst = export_dir / "review_summary.json"
    if lead_csv_src:
        _move_file(lead_csv_src, lead_csv_dst)
        lead["csv"] = lead_csv_dst
    if lead_json_src:
        _move_file(lead_json_src, lead_json_dst)
        lead["json"] = lead_json_dst
    if lead_review_summary_src:
        _move_file(lead_review_summary_src, lead_review_summary_dst)
        lead["review_summary_json"] = lead_review_summary_dst
    if lead:
        exports["lead_snapshot"] = lead

    comparison = dict(exports.get("scoring_comparison") or {})
    comparison_csv_src = _as_path(comparison.get("csv"))
    comparison_json_src = _as_path(comparison.get("json"))
    comparison_csv_dst = export_dir / "lead_scoring_comparison.csv"
    comparison_json_dst = export_dir / "lead_scoring_comparison.json"
    if comparison_csv_src:
        _move_file(comparison_csv_src, comparison_csv_dst)
        comparison["csv"] = comparison_csv_dst
    if comparison_json_src:
        _move_file(comparison_json_src, comparison_json_dst)
        comparison["json"] = comparison_json_dst
    if comparison:
        exports["scoring_comparison"] = comparison

    kw = dict(exports.get("kw_pairs") or {})
    kw_csv_src = _as_path(kw.get("csv"))
    kw_json_src = _as_path(kw.get("json"))
    kw_csv_dst = export_dir / "keyword_pairs.csv"
    kw_json_dst = export_dir / "keyword_pairs.json"
    if kw_csv_src:
        _move_file(kw_csv_src, kw_csv_dst)
        kw["csv"] = kw_csv_dst
    if kw_json_src:
        _move_file(kw_json_src, kw_json_dst)
        kw["json"] = kw_json_dst
    if kw:
        exports["kw_pairs"] = kw

    entities = dict(exports.get("entities") or {})
    entities_csv_src = _as_path(entities.get("entities_csv"))
    entities_json_src = _as_path(entities.get("entities_json"))
    event_entities_csv_src = _as_path(entities.get("event_entities_csv"))
    event_entities_json_src = _as_path(entities.get("event_entities_json"))
    entities_csv_dst = export_dir / "entities.csv"
    entities_json_dst = export_dir / "entities.json"
    event_entities_csv_dst = export_dir / "event_entities.csv"
    event_entities_json_dst = export_dir / "event_entities.json"

    if entities_csv_src:
        _move_file(entities_csv_src, entities_csv_dst)
        entities["entities_csv"] = entities_csv_dst
    if entities_json_src:
        _move_file(entities_json_src, entities_json_dst)
        entities["entities_json"] = entities_json_dst
    if event_entities_csv_src:
        _move_file(event_entities_csv_src, event_entities_csv_dst)
        entities["event_entities_csv"] = event_entities_csv_dst
    if event_entities_json_src:
        _move_file(event_entities_json_src, event_entities_json_dst)
        entities["event_entities_json"] = event_entities_json_dst
    if entities:
        exports["entities"] = entities

    events = dict(exports.get("events") or {})
    events_csv_src = _as_path(events.get("csv"))
    events_jsonl_src = _as_path(events.get("jsonl"))
    events_csv_dst = export_dir / "events.csv"
    events_jsonl_dst = export_dir / "events.jsonl"
    if events_csv_src:
        _move_file(events_csv_src, events_csv_dst)
        events["csv"] = events_csv_dst
    if events_jsonl_src:
        _move_file(events_jsonl_src, events_jsonl_dst)
        events["jsonl"] = events_jsonl_dst
    if events:
        exports["events"] = events

    return exports


def flatten_bundle_files(*, artifacts: dict[str, Any], bundle_dir: Path) -> dict[str, str]:
    """Collect a normalized file map for manifest consumers."""
    files: dict[str, str] = {}

    def _add(name: str, value: Any) -> None:
        path = _as_path(value)
        if path is None:
            return
        files[name] = _rel(path, bundle_dir)

    for key in (
        "bundle_manifest_json",
        "workflow_result_json",
        "workflow_summary_json",
        "smoke_summary_json",
        "doctor_status_json",
        "report_html",
        "foia_lead_review_board_html",
        "foia_lead_review_board_md",
        "evaluation_summary_json",
        "scoring_comparison_json",
        "review_board_md",
        "evaluation_report_md",
        "dossiers_dir",
        "dossiers_index_json",
        "bundle_adjudications_csv",
        "bundle_metrics_json",
        "export_lead_adjudications_csv",
        "export_lead_adjudication_metrics_json",
    ):
        _add(key, artifacts.get(key))

    exports = artifacts.get("exports") if isinstance(artifacts.get("exports"), dict) else {}
    if isinstance(exports, dict):
        lead = exports.get("lead_snapshot") if isinstance(exports.get("lead_snapshot"), dict) else {}
        if isinstance(lead, dict):
            _add("export_lead_snapshot_csv", lead.get("csv"))
            _add("export_lead_snapshot_json", lead.get("json"))
            _add("export_lead_review_summary_json", lead.get("review_summary_json"))
        comparison = exports.get("scoring_comparison") if isinstance(exports.get("scoring_comparison"), dict) else {}
        if isinstance(comparison, dict):
            _add("export_scoring_comparison_csv", comparison.get("csv"))
            _add("export_scoring_comparison_json", comparison.get("json"))
        kw = exports.get("kw_pairs") if isinstance(exports.get("kw_pairs"), dict) else {}
        if isinstance(kw, dict):
            _add("export_keyword_pairs_csv", kw.get("csv"))
            _add("export_keyword_pairs_json", kw.get("json"))
        entities = exports.get("entities") if isinstance(exports.get("entities"), dict) else {}
        if isinstance(entities, dict):
            _add("export_entities_csv", entities.get("entities_csv"))
            _add("export_entities_json", entities.get("entities_json"))
            _add("export_event_entities_csv", entities.get("event_entities_csv"))
            _add("export_event_entities_json", entities.get("event_entities_json"))
        events = exports.get("events") if isinstance(exports.get("events"), dict) else {}
        if isinstance(events, dict):
            _add("export_events_csv", events.get("csv"))
            _add("export_events_jsonl", events.get("jsonl"))
        adjudications = exports.get("adjudications") if isinstance(exports.get("adjudications"), dict) else {}
        if isinstance(adjudications, dict):
            _add("export_lead_adjudications_csv", adjudications.get("csv"))
        adjudication_metrics = (
            exports.get("adjudication_metrics") if isinstance(exports.get("adjudication_metrics"), dict) else {}
        )
        if isinstance(adjudication_metrics, dict):
            _add("export_lead_adjudication_metrics_json", adjudication_metrics.get("json"))

    lead_dossiers = artifacts.get("lead_dossiers") if isinstance(artifacts.get("lead_dossiers"), dict) else {}
    if isinstance(lead_dossiers, dict):
        _add("lead_dossier_index_json", lead_dossiers.get("index_json"))
        _add("lead_dossier_index_csv", lead_dossiers.get("index_csv"))

    return files


def write_bundle_manifest(*, bundle_dir: Path, payload: dict[str, Any]) -> Path:
    manifest_path = bundle_dir / SAM_BUNDLE_MANIFEST_NAME
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return manifest_path


def _quality_name(summary: dict[str, Any], manifest: Optional[dict[str, Any]] = None) -> Optional[str]:
    value = summary.get("quality")
    if isinstance(value, dict):
        value = value.get("quality")
    if value is not None:
        return str(value)
    manifest_value = (manifest or {}).get("quality")
    if isinstance(manifest_value, dict):
        manifest_value = manifest_value.get("quality")
    return str(manifest_value) if manifest_value is not None else None


def _summary_flag(
    summary: dict[str, Any],
    key: str,
    *,
    manifest: Optional[dict[str, Any]] = None,
    legacy_nested_key: Optional[str] = None,
) -> Optional[bool]:
    if key in summary:
        return bool(summary.get(key))
    if legacy_nested_key:
        quality_value = summary.get("quality")
        if isinstance(quality_value, dict) and legacy_nested_key in quality_value:
            return bool(quality_value.get(legacy_nested_key))
    if manifest and key in manifest:
        return bool(manifest.get(key))
    return None


def _summary_list(
    summary: dict[str, Any],
    key: str,
    *,
    manifest: Optional[dict[str, Any]] = None,
    legacy_nested_key: Optional[str] = None,
) -> list[str]:
    value = summary.get(key)
    if value is None and legacy_nested_key:
        quality_value = summary.get("quality")
        if isinstance(quality_value, dict):
            value = quality_value.get(legacy_nested_key)
    if value is None and manifest is not None:
        value = manifest.get(key)
    if value is None and legacy_nested_key and manifest is not None:
        manifest_quality = manifest.get("quality")
        if isinstance(manifest_quality, dict):
            value = manifest_quality.get(legacy_nested_key)
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return []


def _display_value(value: Any) -> str:
    if value is None:
        return "Unavailable"
    if isinstance(value, list):
        return ", ".join([str(item) for item in value if str(item).strip()]) or "Unavailable"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _comparison_window_text(value: Any) -> str:
    if not isinstance(value, dict):
        return _display_value(value)
    posted_from = str(value.get("posted_from") or "").strip()
    posted_to = str(value.get("posted_to") or "").strip()
    mode = str(value.get("mode") or "").strip()
    requested_days = value.get("requested_days")
    effective_days = value.get("effective_days")
    calendar_span_days = value.get("calendar_span_days")

    if posted_from and posted_to:
        headline = f"{posted_from}..{posted_to}"
    elif effective_days is not None:
        headline = f"last {effective_days} days"
    elif requested_days is not None:
        headline = f"requested {requested_days} days"
    else:
        headline = "Unavailable"

    parts = [headline]
    if mode:
        parts.append(f"mode={mode}")
    if requested_days is not None and requested_days != effective_days:
        parts.append(f"requested_days={requested_days}")
    if effective_days is not None:
        parts.append(f"effective_days={effective_days}")
    if calendar_span_days is not None:
        parts.append(f"span={calendar_span_days} days")
    return " | ".join(parts)


def _summary_baseline(summary: dict[str, Any]) -> dict[str, Any]:
    return summary.get("baseline") if isinstance(summary.get("baseline"), dict) else {}


def _summary_comparison(summary: dict[str, Any], manifest: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    comparison = summary.get("comparison") if isinstance(summary.get("comparison"), dict) else {}
    if comparison:
        return comparison
    manifest_comparison = (manifest or {}).get("comparison")
    if isinstance(manifest_comparison, dict):
        return manifest_comparison
    return {}


def _summary_mission_quality(summary: dict[str, Any], manifest: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    mission_quality = summary.get("mission_quality") if isinstance(summary.get("mission_quality"), dict) else {}
    if mission_quality:
        return mission_quality
    manifest_mission_quality = (manifest or {}).get("mission_quality")
    if isinstance(manifest_mission_quality, dict):
        return manifest_mission_quality
    return {}


def render_sam_bundle_report(
    *,
    bundle_dir: Path,
    title: str,
    workflow_summary: dict[str, Any],
    artifacts: dict[str, Any],
) -> Path:
    report_path = bundle_dir / SAM_BUNDLE_REPORT_PATH
    report_path.parent.mkdir(parents=True, exist_ok=True)

    summary = dict(workflow_summary or {})
    workflow_status = str(summary.get("workflow_status") or summary.get("status") or "warning")
    workflow_type = str(summary.get("workflow_type") or "samgov-smoke")
    validation_mode = str(summary.get("validation_mode") or "smoke")
    scoring_version = str(summary.get("scoring_version") or "Unavailable")
    compare_scoring_versions = list(summary.get("compare_scoring_versions") or [])
    check_groups = summary.get("check_groups") if isinstance(summary.get("check_groups"), dict) else {}
    failed_required_checks = list(summary.get("failed_required_checks") or [])
    warning_checks = list(summary.get("warning_checks") or summary.get("failed_advisory_checks") or [])
    baseline = _summary_baseline(summary)
    counts = baseline.get("counts") if isinstance(baseline.get("counts"), dict) else {}
    keyword_coverage = (
        baseline.get("keyword_coverage") if isinstance(baseline.get("keyword_coverage"), dict) else {}
    )
    correlations_by_lane = (
        baseline.get("correlations_by_lane") if isinstance(baseline.get("correlations_by_lane"), dict) else {}
    )
    comparison = _summary_comparison(summary)
    mission_quality = _summary_mission_quality(summary)
    quality = _quality_name(summary)
    reason_codes = _summary_list(summary, "reason_codes")
    operator_messages = _summary_list(summary, "operator_messages")
    comparison_reason_codes = _summary_list(summary, "comparison_reason_codes")
    comparison_messages = _summary_list(summary, "comparison_operator_messages")

    def _row(key: str, value: Any) -> str:
        return f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(_display_value(value))}</td></tr>"

    summary_rows = "".join(
        [
            _row("workflow_status", workflow_status),
            _row("quality", quality),
            _row("required_checks_passed", summary.get("required_checks_passed")),
            _row(
                "has_required_failures",
                _summary_flag(summary, "has_required_failures"),
            ),
            _row(
                "has_advisory_failures",
                _summary_flag(summary, "has_advisory_failures"),
            ),
            _row(
                "has_usable_artifacts",
                _summary_flag(summary, "has_usable_artifacts"),
            ),
            _row(
                "partially_useful",
                _summary_flag(summary, "partially_useful", legacy_nested_key="partially_useful"),
            ),
            _row(
                "comparison_requested",
                _summary_flag(summary, "comparison_requested"),
            ),
            _row(
                "comparison_available",
                _summary_flag(summary, "comparison_available"),
            ),
            _row(
                "comparison_empty",
                _summary_flag(summary, "comparison_empty"),
            ),
            _row("reason_codes", reason_codes),
            _row("required_failure_categories", _summary_list(summary, "required_failure_categories", legacy_nested_key="required_failure_categories")),
            _row("advisory_failure_categories", _summary_list(summary, "advisory_failure_categories", legacy_nested_key="advisory_failure_categories")),
            _row("events_window", counts.get("events_window")),
            _row("events_with_keywords", keyword_coverage.get("events_with_keywords")),
            _row("events_with_entity_window", counts.get("events_with_entity_window")),
            _row("same_keyword", correlations_by_lane.get("same_keyword")),
            _row("kw_pair", correlations_by_lane.get("kw_pair")),
            _row("same_sam_naics", correlations_by_lane.get("same_sam_naics")),
            _row("snapshot_items", baseline.get("snapshot_items")),
        ]
    )

    category_rows: list[str] = []
    for category, group in check_groups.items():
        category_rows.append(
            "<tr>"
            f"<td>{html.escape(str(group.get('category_label') or category))}</td>"
            f"<td>{html.escape(str(group.get('required_total') or 0))}</td>"
            f"<td>{html.escape(str(group.get('advisory_total') or 0))}</td>"
            f"<td>{html.escape(str(group.get('failed_required') or 0))}</td>"
            f"<td>{html.escape(str(group.get('failed_advisory') or 0))}</td>"
            "</tr>"
        )
    category_table = "".join(category_rows) or "<tr><td colspan='5'>No category data found.</td></tr>"

    def _render_check_table(items: list[dict[str, Any]]) -> str:
        rows: list[str] = []
        for chk in items:
            rows.append(
                "<tr>"
                f"<td>{html.escape(str(chk.get('result')))}</td>"
                f"<td>{html.escape(str(chk.get('severity')))}</td>"
                f"<td>{html.escape(str(chk.get('policy_level')))}</td>"
                f"<td>{html.escape(str(chk.get('name')))}</td>"
                f"<td>{html.escape(str(chk.get('observed', chk.get('actual'))))}</td>"
                f"<td>{html.escape(str(chk.get('expected', chk.get('threshold'))))}</td>"
                f"<td>{html.escape(str(chk.get('hint') or ''))}</td>"
                "</tr>"
            )
        return "".join(rows) or "<tr><td colspan='7'>No checks found.</td></tr>"

    check_sections: list[str] = []
    for category, group in check_groups.items():
        label = str(group.get("category_label") or category)
        check_sections.append(
            f"<h3>{html.escape(label)}</h3>"
            "<table>"
            "<thead><tr><th>Result</th><th>Severity</th><th>Policy</th><th>Name</th><th>Observed</th><th>Threshold</th><th>Next</th></tr></thead>"
            f"<tbody>{_render_check_table(list(group.get('checks') or []))}</tbody>"
            "</table>"
        )
    checks_markup = "".join(check_sections) or (
        "<table><tbody><tr><td colspan='7'>No checks found.</td></tr></tbody></table>"
    )

    message_rows = "".join([f"<li>{html.escape(message)}</li>" for message in operator_messages])
    if not message_rows:
        message_rows = "<li>No operator messages recorded.</li>"
    comparison_rows = "".join(
        [
            _row("requested_versions", comparison.get("requested_versions") or compare_scoring_versions),
            _row("requested_window", _comparison_window_text(comparison.get("requested_window"))),
            _row("effective_window", _comparison_window_text(comparison.get("effective_window"))),
            _row("baseline_version", comparison.get("baseline_version")),
            _row("target_version", comparison.get("target_version")),
            _row("count", comparison.get("count")),
            _row("state_counts", comparison.get("state_counts")),
            _row("reason_codes", comparison_reason_codes),
        ]
    )
    comparison_messages_markup = "".join([f"<li>{html.escape(message)}</li>" for message in comparison_messages])
    if not comparison_messages_markup:
        comparison_messages_markup = "<li>No comparison messages recorded.</li>"

    mission_quality_rows = ""
    if mission_quality:
        family_diversity = (
            mission_quality.get("family_diversity") if isinstance(mission_quality.get("family_diversity"), dict) else {}
        )
        score_spread = mission_quality.get("score_spread") if isinstance(mission_quality.get("score_spread"), dict) else {}
        foia_draftability = (
            mission_quality.get("foia_draftability") if isinstance(mission_quality.get("foia_draftability"), dict) else {}
        )
        mission_quality_rows = "".join(
            [
                _row("mission_top_n", mission_quality.get("mission_top_n")),
                _row("considered_top_leads", mission_quality.get("considered_top_leads")),
                _row("lead_snapshot_scoring_version", mission_quality.get("scoring_version")),
                _row("row_scoring_versions", mission_quality.get("row_scoring_versions")),
                _row("core_field_coverage_pct", mission_quality.get("core_field_coverage_pct")),
                _row("family_diversity", family_diversity.get("unique_primary_families")),
                _row("top_family_share_pct", family_diversity.get("top_family_share_pct")),
                _row("nonstarter_pack_presence_pct", mission_quality.get("nonstarter_pack_presence_pct")),
                _row("starter_only_pair_share_pct", mission_quality.get("starter_only_pair_share_pct")),
                _row("routine_noise_share_pct", mission_quality.get("routine_noise_share_pct")),
                _row("score_spread", score_spread.get("spread")),
                _row("score_spread_summary", score_spread.get("summary")),
                _row("foia_draftability_pct", foia_draftability.get("draftable_share_pct")),
                _row("foia_draftability_levels", foia_draftability.get("levels")),
                _row("dossier_linkage_pct", mission_quality.get("dossier_linkage_pct")),
                _row("mission_verdict", (mission_quality.get("verdict") or {}).get("detail") if isinstance(mission_quality.get("verdict"), dict) else mission_quality.get("verdict")),
            ]
        )
    mission_quality_markup = (
        "<h2>Mission Quality</h2>"
        "<div class=\"meta\">These metrics are computed from the exported ranked lead artifacts, not from ingest-only counts.</div>"
        f"<table><tbody>{mission_quality_rows}</tbody></table>"
        if mission_quality_rows
        else ""
    )

    file_rows: list[str] = []
    files = flatten_bundle_files(artifacts=artifacts, bundle_dir=bundle_dir)
    for file_id, rel_path in sorted(files.items()):
        file_rows.append(
            "<tr>"
            f"<td>{html.escape(file_id)}</td>"
            f"<td><a href=\"../{html.escape(rel_path)}\">{html.escape(rel_path)}</a></td>"
            "</tr>"
        )
    files_table = "".join(file_rows) or "<tr><td colspan='2'>No files recorded.</td></tr>"
    evaluation_markup = _render_evaluation_section(bundle_dir=bundle_dir, artifacts=artifacts)
    review_board_html = _as_path(artifacts.get("foia_lead_review_board_html")) or (bundle_dir / FOIA_LEAD_REVIEW_BOARD_HTML_PATH)
    review_board_md = _as_path(artifacts.get("foia_lead_review_board_md")) or (bundle_dir / FOIA_LEAD_REVIEW_BOARD_MD_PATH)
    lead_dossiers = artifacts.get("lead_dossiers") if isinstance(artifacts.get("lead_dossiers"), dict) else {}
    dossier_index_json = _as_path((lead_dossiers or {}).get("index_json")) or (bundle_dir / FOIA_LEAD_DOSSIER_INDEX_JSON_PATH)
    dossier_index_csv = _as_path((lead_dossiers or {}).get("index_csv")) or (bundle_dir / FOIA_LEAD_DOSSIER_INDEX_CSV_PATH)
    dossier_links: list[str] = []
    if dossier_index_json.exists():
        dossier_links.append(f"<a href=\"../{html.escape(_rel(dossier_index_json, bundle_dir))}\">lead_dossiers/dossier_index.json</a>")
    if dossier_index_csv.exists():
        dossier_links.append(f"<a href=\"../{html.escape(_rel(dossier_index_csv, bundle_dir))}\">lead_dossiers/dossier_index.csv</a>")
    review_board_markup = (
        "<h2>Reviewer Surface</h2>"
        "<div class=\"meta\">Open the reviewer-first FOIA Lead Review Board for ranked lead evaluation, noise patterns, and next-record targeting.</div>"
        f"<p><a href=\"{html.escape(review_board_html.name)}\">foia_lead_review_board.html</a>"
        f" | <a href=\"{html.escape(review_board_md.name)}\">foia_lead_review_board.md</a>"
        + (f" | {' | '.join(dossier_links)}" if dossier_links else "")
        + "</p>"
    )

    generated_at = str(summary.get("generated_at") or datetime.now(timezone.utc).isoformat())
    comparison_text = ",".join(compare_scoring_versions) if compare_scoring_versions else "none"
    html_payload = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 20px; color: #111; }}
    h1, h2, h3 {{ margin-bottom: 0.3rem; }}
    .meta {{ color: #333; margin-bottom: 1rem; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-weight: 600; background: #e8eef7; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 1.2rem; }}
    th, td {{ border: 1px solid #d8dce3; padding: 6px 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f5f7fa; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <div class=\"meta\">
    <span class=\"badge\">workflow_status={html.escape(workflow_status)}</span>
    workflow_type={html.escape(workflow_type)} |
    validation_mode={html.escape(validation_mode)} |
    scoring_version={html.escape(scoring_version)} |
    compare_scoring_versions={html.escape(comparison_text)} |
    generated_at={html.escape(generated_at)}
  </div>

  <h2>Summary</h2>
  <table><tbody>{summary_rows}</tbody></table>
  <h2>Operator Messages</h2>
  <ul>{message_rows}</ul>
  <h2>Comparison</h2>
  <table><tbody>{comparison_rows}</tbody></table>
  <ul>{comparison_messages_markup}</ul>
  {mission_quality_markup}
  {review_board_markup}

  <h2>Checks</h2>
  <div class=\"meta\">failed_required={len(failed_required_checks)} failed_advisory={len(warning_checks)}</div>
  <table>
    <thead><tr><th>Category</th><th>Required</th><th>Advisory</th><th>Failed Required</th><th>Failed Advisory</th></tr></thead>
    <tbody>{category_table}</tbody>
  </table>
  {checks_markup}
  {evaluation_markup}

  <h2>Bundle Files</h2>
  <table>
    <thead><tr><th>ID</th><th>Path</th></tr></thead>
    <tbody>{files_table}</tbody>
  </table>
</body>
</html>
"""
    report_path.write_text(html_payload, encoding="utf-8")
    return report_path


def _format_metric(value: Any) -> str:
    if value is None:
        return "Unavailable"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _kv_row(key: str, value: Any) -> str:
    return f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _render_evaluation_section(*, bundle_dir: Path, artifacts: dict[str, Any]) -> str:
    from backend.services.adjudication import load_bundle_adjudication_state

    state = load_bundle_adjudication_state(bundle_dir=bundle_dir, artifact_payload=artifacts)
    adjudications_csv = state.get("adjudications_csv")
    metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else {}
    if adjudications_csv is None and not metrics:
        return ""

    summary = metrics.get("summary") if isinstance(metrics.get("summary"), dict) else {}
    precision_rows: list[str] = []
    for entry in (summary.get("precision_at_k") or {}).values():
        if not isinstance(entry, dict):
            continue
        precision_rows.append(
            "<tr>"
            f"<td>{html.escape(_format_metric(entry.get('k')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('precision_pct')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('reviewed_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('decisive_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('keep_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('reject_count')))}</td>"
            "</tr>"
        )
    precision_table = "".join(precision_rows) or "<tr><td colspan='6'>No precision metrics available.</td></tr>"

    version_rows: list[str] = []
    for entry in metrics.get("by_scoring_version") or []:
        if not isinstance(entry, dict):
            continue
        version_rows.append(
            "<tr>"
            f"<td>{html.escape(_format_metric(entry.get('scoring_version')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('row_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('keep_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('reject_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('acceptance_rate_pct')))}</td>"
            "</tr>"
        )
    version_table = "".join(version_rows) or "<tr><td colspan='5'>No scoring-version metrics available.</td></tr>"

    family_rows: list[str] = []
    for entry in metrics.get("by_lead_family") or []:
        if not isinstance(entry, dict):
            continue
        family_rows.append(
            "<tr>"
            f"<td>{html.escape(_format_metric(entry.get('lead_family')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('row_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('keep_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('reject_count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('acceptance_rate_pct')))}</td>"
            "</tr>"
        )
    family_table = "".join(family_rows) or "<tr><td colspan='5'>No lead-family metrics available.</td></tr>"

    rejection_rows: list[str] = []
    for entry in metrics.get("rejection_reasons") or []:
        if not isinstance(entry, dict):
            continue
        rejection_rows.append(
            "<tr>"
            f"<td>{html.escape(_format_metric(entry.get('reason_code')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('count')))}</td>"
            f"<td>{html.escape(_format_metric(entry.get('share_of_rejects_pct')))}</td>"
            "</tr>"
        )
    rejection_table = "".join(rejection_rows) or "<tr><td colspan='3'>No rejection reasons recorded.</td></tr>"
    if isinstance(adjudications_csv, Path):
        try:
            adjudications_label: Any = adjudications_csv.relative_to(bundle_dir)
        except ValueError:
            adjudications_label = adjudications_csv
    else:
        adjudications_label = "Unavailable"

    summary_rows = "".join(
        [
            _kv_row("Adjudications CSV", adjudications_label),
            _kv_row("Reviewed Rows", summary.get("reviewed_count")),
            _kv_row("Decisive Rows", summary.get("decisive_count")),
            _kv_row("Acceptance Rate (%)", _format_metric(summary.get("acceptance_rate_pct"))),
            _kv_row("FOIA Ready Yes", summary.get("foia_ready_yes_count")),
        ]
    )
    if not metrics:
        return (
            "<h2>Evaluation</h2>"
            "<div class=\"meta\">Adjudications were found, but no metrics JSON is present yet.</div>"
            f"<table><tbody>{summary_rows}</tbody></table>"
        )

    return (
        "<h2>Evaluation</h2>"
        "<div class=\"meta\">Reviewer adjudications are local bundle artifacts and do not call external services.</div>"
        f"<table><tbody>{summary_rows}</tbody></table>"
        "<h3>Precision @ k</h3>"
        "<table><thead><tr><th>k</th><th>Precision (%)</th><th>Reviewed</th><th>Decisive</th><th>Keep</th><th>Reject</th></tr></thead>"
        f"<tbody>{precision_table}</tbody></table>"
        "<h3>By Scoring Version</h3>"
        "<table><thead><tr><th>Scoring Version</th><th>Rows</th><th>Keep</th><th>Reject</th><th>Acceptance (%)</th></tr></thead>"
        f"<tbody>{version_table}</tbody></table>"
        "<h3>By Lead Family</h3>"
        "<table><thead><tr><th>Lead Family</th><th>Rows</th><th>Keep</th><th>Reject</th><th>Acceptance (%)</th></tr></thead>"
        f"<tbody>{family_table}</tbody></table>"
        "<h3>Rejection Reasons</h3>"
        "<table><thead><tr><th>Reason Code</th><th>Count</th><th>Share of Rejects (%)</th></tr></thead>"
        f"<tbody>{rejection_table}</tbody></table>"
    )


def render_sam_bundle_report_from_bundle(bundle_dir: Path) -> Path:
    root = Path(bundle_dir).expanduser()
    manifest_path = root / SAM_BUNDLE_MANIFEST_NAME
    summary_path = root / SAM_BUNDLE_RESULTS_DIR / "workflow_summary.json"
    manifest = _load_json_file(manifest_path)
    summary = _load_json_file(summary_path)
    if not manifest and not summary:
        raise ValueError(f"Bundle manifest/summary not found under {root}")

    artifacts = summary.get("artifacts") if isinstance(summary.get("artifacts"), dict) else {}
    if not isinstance(artifacts, dict):
        artifacts = {}
    artifacts.setdefault("bundle_manifest_json", manifest_path)
    generated_files = manifest.get("generated_files") if isinstance(manifest.get("generated_files"), dict) else {}
    for file_id, rel_path in generated_files.items():
        if file_id in artifacts:
            continue
        artifacts[str(file_id)] = root / str(rel_path)

    return render_sam_bundle_report(
        bundle_dir=root,
        title="SAM.gov Workflow Bundle Report",
        workflow_summary=summary or manifest,
        artifacts=artifacts,
    )


def inspect_bundle(path: Path) -> dict[str, Any]:
    root = path.expanduser().resolve()
    manifest_path = root / SAM_BUNDLE_MANIFEST_NAME
    payload: dict[str, Any] = {
        "bundle_dir": root,
        "bundle_manifest_json": manifest_path,
        "exists": root.exists(),
    }

    if not root.exists():
        payload["status"] = "missing_bundle_dir"
        payload["bundle_status"] = "missing_bundle_dir"
        payload["errors"] = [f"Bundle directory not found: {root}"]
        return payload

    if not manifest_path.exists():
        payload["status"] = "missing_manifest"
        payload["bundle_status"] = "missing_manifest"
        payload["errors"] = [f"Manifest not found: {manifest_path}"]
        legacy_files = {
            "workflow_result_json": root / "workflow_result.json",
            "doctor_status_json": root / "doctor_status.json",
            "smoke_summary_json": root / "smoke_summary.json",
        }
        payload["legacy_files"] = {k: {"path": v, "exists": v.exists()} for k, v in legacy_files.items()}
        return payload

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        payload["status"] = "invalid_manifest_json"
        payload["bundle_status"] = "invalid_manifest_json"
        payload["errors"] = [str(exc)]
        return payload

    files = manifest.get("generated_files") if isinstance(manifest.get("generated_files"), dict) else {}
    missing_files: list[dict[str, str]] = []
    file_status: dict[str, dict[str, Any]] = {}
    for file_id, rel_path in sorted(files.items()):
        abs_path = (root / str(rel_path)).resolve()
        exists = abs_path.exists()
        file_status[str(file_id)] = {"path": abs_path, "exists": exists}
        if not exists:
            missing_files.append({"id": str(file_id), "path": str(abs_path)})

    summary = _load_json_file(root / SAM_BUNDLE_RESULTS_DIR / "workflow_summary.json")
    quality_name = _quality_name(summary, manifest)
    check_summary = manifest.get("check_summary") if isinstance(manifest.get("check_summary"), dict) else {}
    integrity_status = "ok" if not missing_files else "missing_files"
    payload.update(
        {
            "status": integrity_status,
            "bundle_status": integrity_status,
            "bundle_integrity_status": integrity_status,
            "workflow_status": summary.get("workflow_status") or summary.get("status") or manifest.get("workflow_status") or manifest.get("status"),
            "quality": quality_name,
            "workflow_quality": quality_name,
            "has_required_failures": _summary_flag(summary, "has_required_failures", manifest=manifest),
            "has_advisory_failures": _summary_flag(summary, "has_advisory_failures", manifest=manifest),
            "has_usable_artifacts": _summary_flag(summary, "has_usable_artifacts", manifest=manifest),
            "partially_useful": _summary_flag(summary, "partially_useful", manifest=manifest, legacy_nested_key="partially_useful"),
            "comparison_requested": _summary_flag(summary, "comparison_requested", manifest=manifest),
            "comparison_available": _summary_flag(summary, "comparison_available", manifest=manifest),
            "comparison_empty": _summary_flag(summary, "comparison_empty", manifest=manifest),
            "reason_codes": _summary_list(summary, "reason_codes", manifest=manifest),
            "operator_messages": _summary_list(summary, "operator_messages", manifest=manifest),
            "required_failure_categories": _summary_list(
                summary,
                "required_failure_categories",
                manifest=manifest,
                legacy_nested_key="required_failure_categories",
            ),
            "advisory_failure_categories": _summary_list(
                summary,
                "advisory_failure_categories",
                manifest=manifest,
                legacy_nested_key="advisory_failure_categories",
            ),
            "comparison_reason_codes": _summary_list(summary, "comparison_reason_codes", manifest=manifest),
            "comparison_operator_messages": _summary_list(summary, "comparison_operator_messages", manifest=manifest),
            "comparison": _summary_comparison(summary, manifest),
            "check_summary": check_summary,
            "manifest": manifest,
            "workflow_summary": summary,
            "generated_files": file_status,
            "missing_files": missing_files,
        }
    )
    return payload


__all__ = [
    "SAM_BUNDLE_EXPORTS_DIR",
    "SAM_BUNDLE_MANIFEST_NAME",
    "SAM_BUNDLE_REPORT_PATH",
    "SAM_BUNDLE_RESULTS_DIR",
    "SAM_BUNDLE_VERSION",
    "flatten_bundle_files",
    "inspect_bundle",
    "normalize_sam_exports",
    "render_sam_bundle_report_from_bundle",
    "render_sam_bundle_report",
    "write_bundle_manifest",
]
