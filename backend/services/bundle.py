from __future__ import annotations

import html
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SAM_BUNDLE_VERSION = "samgov.bundle.v1"
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
    lead_csv_dst = export_dir / "lead_snapshot.csv"
    lead_json_dst = export_dir / "lead_snapshot.json"
    if lead_csv_src:
        _move_file(lead_csv_src, lead_csv_dst)
        lead["csv"] = lead_csv_dst
    if lead_json_src:
        _move_file(lead_json_src, lead_json_dst)
        lead["json"] = lead_json_dst
    if lead:
        exports["lead_snapshot"] = lead

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
    ):
        _add(key, artifacts.get(key))

    exports = artifacts.get("exports") if isinstance(artifacts.get("exports"), dict) else {}
    if isinstance(exports, dict):
        lead = exports.get("lead_snapshot") if isinstance(exports.get("lead_snapshot"), dict) else {}
        if isinstance(lead, dict):
            _add("export_lead_snapshot_csv", lead.get("csv"))
            _add("export_lead_snapshot_json", lead.get("json"))
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

    return files


def write_bundle_manifest(*, bundle_dir: Path, payload: dict[str, Any]) -> Path:
    manifest_path = bundle_dir / SAM_BUNDLE_MANIFEST_NAME
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return manifest_path


def render_sam_bundle_report(
    *,
    bundle_dir: Path,
    title: str,
    status: str,
    workflow_type: str,
    validation_mode: str,
    checks: list[dict[str, Any]],
    failed_required_checks: list[dict[str, Any]],
    warning_checks: list[dict[str, Any]],
    summary: dict[str, Any],
    artifacts: dict[str, Any],
) -> Path:
    report_path = bundle_dir / SAM_BUNDLE_REPORT_PATH
    report_path.parent.mkdir(parents=True, exist_ok=True)

    def _row(key: str, value: Any) -> str:
        return f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"

    summary_rows = "".join([_row(k, v) for k, v in summary.items()])

    checks_rows: list[str] = []
    for chk in checks:
        checks_rows.append(
            "<tr>"
            f"<td>{html.escape(str(chk.get('status')))}</td>"
            f"<td>{html.escape(str(chk.get('name')))}</td>"
            f"<td>{html.escape(str(chk.get('observed', chk.get('actual'))))}</td>"
            f"<td>{html.escape(str(chk.get('expected')))}</td>"
            f"<td>{html.escape(str(chk.get('hint') or ''))}</td>"
            "</tr>"
        )
    checks_table = "".join(checks_rows) or "<tr><td colspan='5'>No checks found.</td></tr>"

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

    generated_at = datetime.now(timezone.utc).isoformat()
    html_payload = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 20px; color: #111; }}
    h1, h2 {{ margin-bottom: 0.3rem; }}
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
    <span class=\"badge\">status={html.escape(status)}</span>
    workflow_type={html.escape(workflow_type)} |
    validation_mode={html.escape(validation_mode)} |
    generated_at={html.escape(generated_at)}
  </div>

  <h2>Summary</h2>
  <table><tbody>{summary_rows}</tbody></table>

  <h2>Checks</h2>
  <div class=\"meta\">failed_required={len(failed_required_checks)} warning_checks={len(warning_checks)}</div>
  <table>
    <thead><tr><th>Status</th><th>Name</th><th>Observed</th><th>Expected</th><th>Next</th></tr></thead>
    <tbody>{checks_table}</tbody>
  </table>

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
        payload["errors"] = [f"Bundle directory not found: {root}"]
        return payload

    if not manifest_path.exists():
        payload["status"] = "missing_manifest"
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

    payload.update(
        {
            "status": "ok" if not missing_files else "missing_files",
            "manifest": manifest,
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
    "render_sam_bundle_report",
    "write_bundle_manifest",
]
