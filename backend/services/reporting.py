from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from backend.runtime import EXPORTS_DIR


_REPORTABLE_EXTENSIONS = {".csv", ".html", ".json", ".jsonl"}
_STATUS_BADGE_CLASS = {"PASS": "pass", "FAIL": "fail", "WARNING": "warn"}
_STAMPED_BUNDLE_DIR_RE = re.compile(r"^\d{8}_\d{6}$")


def resolve_bundle_directory(bundle_path: Path | str) -> Path:
    raw = Path(bundle_path).expanduser()
    if raw.is_dir():
        return raw
    return raw.parent if raw.suffix else raw


def find_latest_sam_smoke_bundle(bundle_root: Optional[Path | str] = None) -> Optional[Path]:
    root = Path(bundle_root).expanduser() if bundle_root else (EXPORTS_DIR / "smoke" / "samgov")
    if not root.exists() or not root.is_dir():
        return None
    dirs = [p for p in root.iterdir() if _is_stamped_bundle_dir(p)]
    if not dirs:
        return None
    dirs.sort(key=lambda p: p.name, reverse=True)
    return dirs[0]


def load_sam_bundle_payload(bundle_dir: Path | str) -> dict[str, Any]:
    bundle = resolve_bundle_directory(bundle_dir)
    workflow_doc = _load_json_payload(bundle / "workflow_result.json")
    doctor_doc = _load_json_payload(bundle / "doctor_status.json")
    smoke_doc = _load_json_payload(bundle / "smoke_summary.json")

    workflow = workflow_doc.get("result") if isinstance(workflow_doc.get("result"), dict) else {}
    doctor = doctor_doc.get("result") if isinstance(doctor_doc.get("result"), dict) else {}
    smoke = smoke_doc if isinstance(smoke_doc, dict) else {}

    run_metadata = smoke.get("run_metadata") if isinstance(smoke.get("run_metadata"), dict) else {}
    artifacts = smoke.get("artifacts") if isinstance(smoke.get("artifacts"), dict) else {}

    generated_at = (
        smoke.get("generated_at")
        or workflow_doc.get("generated_at")
        or doctor_doc.get("generated_at")
        or datetime.now(timezone.utc).isoformat()
    )
    workflow_type = str(run_metadata.get("workflow_type") or _infer_workflow_type(smoke))
    source = str(run_metadata.get("source") or workflow.get("source") or smoke.get("source") or "SAM.gov")

    return {
        "bundle_dir": bundle,
        "generated_at": generated_at,
        "workflow_type": workflow_type,
        "source": source,
        "run_metadata": run_metadata,
        "workflow_result": workflow,
        "doctor_status": doctor,
        "smoke_summary": smoke,
        "artifacts": artifacts,
    }


def generate_sam_report(
    *,
    bundle_dir: Path | str,
    workflow_type: str,
    source: str = "SAM.gov",
    generated_at: Optional[str] = None,
    run_metadata: Optional[dict[str, Any]] = None,
    workflow_result: Optional[dict[str, Any]] = None,
    doctor_status_result: Optional[dict[str, Any]] = None,
    smoke_summary: Optional[dict[str, Any]] = None,
    artifacts: Optional[dict[str, Any]] = None,
    report_filename: str = "report.html",
) -> Path:
    bundle = resolve_bundle_directory(bundle_dir)
    bundle.mkdir(parents=True, exist_ok=True)
    report_path = bundle / report_filename

    metadata = run_metadata if isinstance(run_metadata, dict) else {}
    workflow = workflow_result if isinstance(workflow_result, dict) else {}
    doctor = doctor_status_result if isinstance(doctor_status_result, dict) else {}
    smoke = smoke_summary if isinstance(smoke_summary, dict) else {}
    artifact_payload = artifacts if isinstance(artifacts, dict) else {}
    generated = generated_at or datetime.now(timezone.utc).isoformat()

    status_text = _resolve_report_status(smoke=smoke, workflow=workflow)
    status_class = _STATUS_BADGE_CLASS.get(status_text, "warn")
    ingest = workflow.get("ingest") if isinstance(workflow.get("ingest"), dict) else {}
    ontology = workflow.get("ontology_apply") if isinstance(workflow.get("ontology_apply"), dict) else {}
    entities = workflow.get("entities_link") if isinstance(workflow.get("entities_link"), dict) else {}
    correlations = workflow.get("correlations") if isinstance(workflow.get("correlations"), dict) else {}
    snapshot = workflow.get("snapshot") if isinstance(workflow.get("snapshot"), dict) else {}

    lead_items = _load_top_lead_rows(workflow=workflow, bundle_dir=bundle)
    entity_items = _load_top_entities(workflow=workflow, bundle_dir=bundle)

    html_doc = _render_report_html(
        bundle_dir=bundle,
        source=source,
        workflow_type=workflow_type,
        generated_at=generated,
        status_text=status_text,
        status_class=status_class,
        run_metadata=metadata,
        workflow=workflow,
        smoke=smoke,
        doctor=doctor,
        ingest=ingest,
        ontology=ontology,
        entities=entities,
        correlations=correlations,
        snapshot=snapshot,
        lead_items=lead_items,
        entity_items=entity_items,
        artifacts=artifact_payload,
        report_path=report_path,
    )

    report_path.write_text(html_doc, encoding="utf-8")
    return report_path


def generate_sam_report_from_bundle(
    bundle_dir: Path | str,
    *,
    workflow_type: Optional[str] = None,
    source: str = "SAM.gov",
) -> dict[str, Any]:
    payload = load_sam_bundle_payload(bundle_dir)
    bundle = payload["bundle_dir"]
    resolved_type = str(workflow_type or payload.get("workflow_type") or "samgov-smoke")
    resolved_source = str(payload.get("source") or source)

    report_path = generate_sam_report(
        bundle_dir=bundle,
        workflow_type=resolved_type,
        source=resolved_source,
        generated_at=payload.get("generated_at"),
        run_metadata=payload.get("run_metadata"),
        workflow_result=payload.get("workflow_result"),
        doctor_status_result=payload.get("doctor_status"),
        smoke_summary=payload.get("smoke_summary"),
        artifacts=payload.get("artifacts"),
    )
    status_text = _resolve_report_status(
        smoke=(payload.get("smoke_summary") or {}),
        workflow=(payload.get("workflow_result") or {}),
    )
    return {
        "status": status_text,
        "workflow_type": resolved_type,
        "bundle_dir": bundle,
        "report_html": report_path,
    }


def _render_report_html(
    *,
    bundle_dir: Path,
    source: str,
    workflow_type: str,
    generated_at: str,
    status_text: str,
    status_class: str,
    run_metadata: dict[str, Any],
    workflow: dict[str, Any],
    smoke: dict[str, Any],
    doctor: dict[str, Any],
    ingest: dict[str, Any],
    ontology: dict[str, Any],
    entities: dict[str, Any],
    correlations: dict[str, Any],
    snapshot: dict[str, Any],
    lead_items: list[dict[str, Any]],
    entity_items: list[dict[str, Any]],
    artifacts: dict[str, Any],
    report_path: Path,
) -> str:
    run_meta_rows = [
        ("Source", source),
        ("Workflow Type", workflow_type),
        ("Run Timestamp", generated_at),
        ("Ingest", _summary_label(ingest, keys=("fetched", "inserted", "normalized"))),
        ("Ontology", _summary_label(ontology, keys=("updated", "unchanged", "scanned"))),
        ("Entities", _summary_label(entities, keys=("linked", "entities_created", "scanned"))),
        ("Correlations", _summary_corr_label(correlations)),
        ("Snapshot", _summary_label(snapshot, keys=("items", "scanned", "snapshot_id"))),
    ]

    ingest_rows = [
        ("Status", ingest.get("status")),
        ("Run ID", ingest.get("run_id")),
        ("Fetched", ingest.get("fetched")),
        ("Inserted", ingest.get("inserted")),
        ("Normalized", ingest.get("normalized")),
        ("Date Window (days)", run_metadata.get("ingest_days")),
        ("Pages", run_metadata.get("pages")),
        ("Page Size", run_metadata.get("page_size")),
        ("Limit", run_metadata.get("max_records")),
        ("Start Page", run_metadata.get("start_page")),
        ("Snapshot Dir", ingest.get("snapshot_dir")),
    ]

    doctor_rows = _doctor_summary_rows(doctor)
    top_keywords = _table_rows_from_list(
        rows=((doctor.get("keywords") or {}).get("top_keywords") or []),
        expected=("keyword", "count"),
    )
    correlation_lanes = _correlation_lanes_rows(doctor)
    top_leads = _table_rows_from_list(
        rows=lead_items,
        expected=("rank", "score", "doc_id", "entity_id", "source_url", "why_summary"),
    )
    top_entities = _table_rows_from_list(
        rows=entity_items,
        expected=("entity_id", "name", "uei", "cage", "type"),
    )

    hint_rows = [{"hint": h} for h in (doctor.get("hints") or []) if str(h).strip()]
    last_runs_rows = _last_runs_rows((doctor.get("last_runs") if isinstance(doctor, dict) else None) or {})
    artifact_rows = _artifact_rows(
        bundle_dir=bundle_dir,
        report_path=report_path,
        artifact_payload=artifacts,
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SAM.gov Workflow Report</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --fg: #1a2230;
      --card: #ffffff;
      --muted: #5c6677;
      --line: #d8dee9;
      --pass: #0f7b3e;
      --fail: #ad1a24;
      --warn: #9a6700;
      --accent: #1d4ed8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 24px;
      background: linear-gradient(180deg, #eef2f9 0%, #f8fafc 100%);
      color: var(--fg);
      font-family: "Segoe UI", Tahoma, Arial, sans-serif;
      line-height: 1.4;
    }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    .header {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 18px 20px;
      margin-bottom: 16px;
    }}
    .title {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin: 0 0 6px 0;
    }}
    .badge {{
      font-weight: 700;
      border-radius: 999px;
      padding: 6px 14px;
      color: #fff;
      letter-spacing: 0.4px;
    }}
    .badge.pass {{ background: var(--pass); }}
    .badge.fail {{ background: var(--fail); }}
    .badge.warn {{ background: var(--warn); }}
    .meta {{ color: var(--muted); font-size: 14px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
      gap: 12px;
    }}
    .section {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 14px;
    }}
    h1 {{ font-size: 24px; margin: 0; }}
    h2 {{ font-size: 17px; margin: 0 0 8px 0; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      font-size: 13px;
    }}
    th, td {{
      border: 1px solid var(--line);
      padding: 6px 8px;
      vertical-align: top;
      word-break: break-word;
      text-align: left;
    }}
    th {{
      background: #f1f5fb;
      font-weight: 600;
    }}
    .small {{ font-size: 12px; color: var(--muted); }}
    .missing {{
      display: inline-block;
      color: var(--muted);
      font-style: italic;
    }}
    @media (max-width: 700px) {{
      body {{ padding: 12px; }}
      .section {{ padding: 10px; }}
      h1 {{ font-size: 20px; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="title">
        <h1>SAM.gov Workflow Report</h1>
        <span class="badge {status_class}">{_esc(status_text)}</span>
      </div>
      <div class="meta">Bundle: {_esc(str(bundle_dir.resolve()))}</div>
    </div>

    <div class="grid">
      <section class="section">
        <h2>Run Metadata</h2>
        {_render_kv_table(run_meta_rows)}
      </section>
      <section class="section">
        <h2>Ingest Summary</h2>
        {_render_kv_table(ingest_rows)}
      </section>
      <section class="section">
        <h2>Doctor Summary</h2>
        {_render_kv_table(doctor_rows)}
      </section>
      <section class="section">
        <h2>Last Run References</h2>
        {_render_table(last_runs_rows, fallback="No run references available.")}
      </section>
      <section class="section">
        <h2>Top Keywords</h2>
        {_render_table(top_keywords, fallback="Top keyword data unavailable.")}
      </section>
      <section class="section">
        <h2>Correlation Lanes</h2>
        {_render_table(correlation_lanes, fallback="Correlation lane summary unavailable.")}
      </section>
      <section class="section">
        <h2>Top Leads</h2>
        {_render_table(top_leads, fallback="Lead snapshot export unavailable.")}
      </section>
      <section class="section">
        <h2>Top Entities</h2>
        {_render_table(top_entities, fallback="Entity export unavailable.")}
      </section>
      <section class="section">
        <h2>Hints</h2>
        {_render_table(hint_rows, fallback="No hints reported.")}
      </section>
      <section class="section">
        <h2>Artifacts</h2>
        {_render_table(artifact_rows, fallback="No artifacts found for this bundle.")}
      </section>
    </div>

    <p class="small">Generated at {_esc(generated_at)} (UTC).</p>
  </div>
</body>
</html>
"""


def _resolve_report_status(*, smoke: dict[str, Any], workflow: dict[str, Any]) -> str:
    smoke_passed = smoke.get("smoke_passed")
    if smoke_passed is True:
        return "PASS"
    if smoke_passed is False:
        return "FAIL"

    status = str(workflow.get("status") or "").strip().lower()
    if status in {"ok", "success", "passed"}:
        return "PASS"
    if status in {"failed", "fail", "error"}:
        return "FAIL"
    return "WARNING"


def _doctor_summary_rows(doctor: dict[str, Any]) -> list[tuple[str, Any]]:
    db = doctor.get("db") if isinstance(doctor.get("db"), dict) else {}
    counts = doctor.get("counts") if isinstance(doctor.get("counts"), dict) else {}
    entities = doctor.get("entities") if isinstance(doctor.get("entities"), dict) else {}
    keywords = doctor.get("keywords") if isinstance(doctor.get("keywords"), dict) else {}
    corr = doctor.get("correlations") if isinstance(doctor.get("correlations"), dict) else {}
    lanes = corr.get("by_lane") if isinstance(corr.get("by_lane"), dict) else {}
    window = doctor.get("window") if isinstance(doctor.get("window"), dict) else {}

    lane_summary = ", ".join([f"{k}={lanes[k]}" for k in sorted(lanes.keys())]) if lanes else None
    return [
        ("DB Status", db.get("status")),
        ("DB URL", db.get("url")),
        ("Window (days)", window.get("days")),
        ("Window Since", window.get("since")),
        ("Events Total", counts.get("events_total")),
        ("Events Window", counts.get("events_window")),
        ("Events With Entity", counts.get("events_with_entity_window")),
        ("Entity Coverage %", entities.get("window_linked_coverage_pct")),
        ("Keyword Coverage %", keywords.get("coverage_pct")),
        ("Unique Keywords", keywords.get("unique_keywords")),
        ("Correlation Lanes", lane_summary),
    ]


def _correlation_lanes_rows(doctor: dict[str, Any]) -> list[dict[str, Any]]:
    corr = doctor.get("correlations") if isinstance(doctor.get("correlations"), dict) else {}
    lanes = corr.get("by_lane") if isinstance(corr.get("by_lane"), dict) else {}
    out: list[dict[str, Any]] = []
    for lane in sorted(lanes.keys()):
        out.append({"lane": lane, "count": lanes.get(lane)})
    return out


def _load_top_lead_rows(*, workflow: dict[str, Any], bundle_dir: Path, limit: int = 10) -> list[dict[str, Any]]:
    exports = workflow.get("exports") if isinstance(workflow.get("exports"), dict) else {}
    lead = exports.get("lead_snapshot") if isinstance(exports.get("lead_snapshot"), dict) else {}
    lead_json = _resolve_path(bundle_dir, lead.get("json"))
    if lead_json is None or not lead_json.exists():
        lead_json = _find_first(bundle_dir, "lead_snapshot", ".json")
    if lead_json is None or not lead_json.exists():
        return []

    payload = _load_json_payload(lead_json)
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    if not rows:
        return []

    out: list[dict[str, Any]] = []
    for item in rows[: int(limit)]:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "rank": item.get("rank"),
                "score": item.get("score"),
                "doc_id": item.get("doc_id"),
                "entity_id": item.get("entity_id"),
                "source_url": item.get("source_url"),
                "why_summary": item.get("why_summary"),
            }
        )
    return out


def _load_top_entities(*, workflow: dict[str, Any], bundle_dir: Path, limit: int = 10) -> list[dict[str, Any]]:
    exports = workflow.get("exports") if isinstance(workflow.get("exports"), dict) else {}
    entities = exports.get("entities") if isinstance(exports.get("entities"), dict) else {}
    entities_json = _resolve_path(bundle_dir, entities.get("entities_json"))
    if entities_json is None or not entities_json.exists():
        entities_json = _find_first(bundle_dir, "entities", ".json", exclude_prefix="event_")
    if entities_json is None or not entities_json.exists():
        return []

    payload = _load_json_payload(entities_json)
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    out: list[dict[str, Any]] = []
    for item in rows[: int(limit)]:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "entity_id": item.get("entity_id"),
                "name": item.get("name"),
                "uei": item.get("uei"),
                "cage": item.get("cage"),
                "type": item.get("type"),
            }
        )
    return out


def _artifact_rows(
    *,
    bundle_dir: Path,
    report_path: Path,
    artifact_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for label, path in _iter_payload_artifact_paths(artifact_payload):
        if path is None:
            continue
        abs_path = _resolve_path(bundle_dir, path)
        if abs_path is None:
            continue
        key = abs_path.resolve().as_posix() if abs_path.exists() else abs_path.as_posix()
        if key in seen:
            continue
        seen.add(key)
        rows.append(_artifact_row(label, abs_path, bundle_dir, report_path))

    for path in sorted([p for p in bundle_dir.rglob("*") if p.is_file()], key=lambda p: p.relative_to(bundle_dir).as_posix()):
        if path.suffix.lower() not in _REPORTABLE_EXTENSIONS:
            continue
        key = path.resolve().as_posix()
        if key in seen:
            continue
        seen.add(key)
        label = path.relative_to(bundle_dir).as_posix()
        rows.append(_artifact_row(label, path, bundle_dir, report_path))

    rows.sort(key=lambda r: str(r.get("label", "")).lower())
    return rows


def _artifact_row(label: str, path: Path, bundle_dir: Path, report_path: Path) -> dict[str, Any]:
    rel = _relative_href(report_path.parent, path)
    rel_from_bundle = path.relative_to(bundle_dir).as_posix() if path.is_relative_to(bundle_dir) else str(path)
    return {"label": label, "file": _link(rel_from_bundle, rel), "exists": "yes" if path.exists() else "no"}


def _iter_payload_artifact_paths(payload: Any, prefix: str = "") -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    if isinstance(payload, dict):
        for key in sorted(payload.keys()):
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            out.extend(_iter_payload_artifact_paths(payload.get(key), child_prefix))
        return out
    if isinstance(payload, list):
        for idx, item in enumerate(payload):
            child_prefix = f"{prefix}[{idx}]"
            out.extend(_iter_payload_artifact_paths(item, child_prefix))
        return out
    if isinstance(payload, (Path, str)):
        s = str(payload).strip()
        if not s:
            return out
        if _looks_like_path(s):
            out.append((prefix or "artifact", payload))
    return out


def _last_runs_rows(last_runs: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("ingest", "ontology_apply", "lead_snapshot"):
        item = last_runs.get(key)
        if not isinstance(item, dict):
            continue
        summary = ", ".join(
            [
                f"{k}={item.get(k)}"
                for k in ("id", "status", "source", "fetched", "inserted", "normalized", "updated", "unchanged", "items", "ended_at", "created_at")
                if item.get(k) is not None
            ]
        )
        rows.append({"run_type": key, "summary": summary or "Unavailable"})
    return rows


def _table_rows_from_list(*, rows: list[Any], expected: tuple[str, ...]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = {k: row.get(k) for k in expected}
        out.append(item)
    return out


def _render_kv_table(rows: list[tuple[str, Any]]) -> str:
    clean = [{"field": k, "value": v} for k, v in rows if k]
    return _render_table(clean, fallback="Section unavailable.")


def _render_table(rows: list[dict[str, Any]], fallback: str) -> str:
    if not rows:
        return f"<span class=\"missing\">{_esc(fallback)}</span>"

    columns = list(rows[0].keys())
    head = "".join([f"<th>{_esc(c)}</th>" for c in columns])
    body_parts: list[str] = []
    for row in rows:
        tds: list[str] = []
        for col in columns:
            value = row.get(col)
            if isinstance(value, str) and value.startswith("<a "):
                cell = value
            else:
                cell = _esc("Unavailable" if value in (None, "", []) else value)
            tds.append(f"<td>{cell}</td>")
        body_parts.append(f"<tr>{''.join(tds)}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_parts)}</tbody></table>"


def _summary_label(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    if not payload:
        return "Unavailable"
    parts = [f"{k}={payload.get(k)}" for k in keys if payload.get(k) is not None]
    if not parts and payload.get("status") is not None:
        parts.append(f"status={payload.get('status')}")
    return ", ".join(parts) if parts else "Unavailable"


def _summary_corr_label(correlations: dict[str, Any]) -> str:
    if not correlations:
        return "Unavailable"
    lane_parts: list[str] = []
    for lane in sorted(correlations.keys()):
        lane_payload = correlations.get(lane)
        if not isinstance(lane_payload, dict):
            continue
        created = lane_payload.get("correlations_created")
        updated = lane_payload.get("correlations_updated")
        links = lane_payload.get("links_created")
        bits = [f"created={created}", f"updated={updated}", f"links={links}"]
        lane_parts.append(f"{lane}[{', '.join(bits)}]")
    return "; ".join(lane_parts) if lane_parts else "Unavailable"


def _relative_href(base_dir: Path, target: Path) -> str:
    try:
        rel = target.relative_to(base_dir)
    except ValueError:
        rel = Path(target)
    return quote(rel.as_posix(), safe="/")


def _link(label: Any, href: str) -> str:
    return f"<a href=\"{_esc(href)}\">{_esc(label)}</a>"


def _looks_like_path(value: str) -> bool:
    if not value:
        return False
    pathy = any(ch in value for ch in ("/", "\\"))
    ext = Path(value).suffix.lower() in _REPORTABLE_EXTENSIONS
    return pathy or ext


def _resolve_path(base: Path, value: Any) -> Optional[Path]:
    if value is None:
        return None
    raw = Path(value).expanduser() if isinstance(value, (str, Path)) else None
    if raw is None:
        return None
    if raw.is_absolute():
        return raw

    # Most artifacts are bundle-relative paths.
    candidate = base / raw
    if candidate.exists():
        return candidate

    # Some persisted payloads already include the bundle-dir prefix (for example:
    # "<bundle_name>/workflow_result.json"), so resolve from parent only in that case.
    if raw.parts and raw.parts[0] == base.name:
        return base.parent / raw

    return candidate


def _find_first(bundle_dir: Path, contains: str, suffix: str, exclude_prefix: str = "") -> Optional[Path]:
    candidates: list[Path] = []
    for path in bundle_dir.rglob(f"*{suffix}"):
        name = path.name.lower()
        if contains.lower() not in name:
            continue
        if exclude_prefix and name.startswith(exclude_prefix.lower()):
            continue
        candidates.append(path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.relative_to(bundle_dir).as_posix())
    return candidates[0]


def _load_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _infer_workflow_type(smoke_summary: dict[str, Any]) -> str:
    if smoke_summary:
        return "samgov-smoke"
    return "samgov"


def _is_stamped_bundle_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    return bool(_STAMPED_BUNDLE_DIR_RE.fullmatch(path.name))


def _esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


__all__ = [
    "find_latest_sam_smoke_bundle",
    "generate_sam_report",
    "generate_sam_report_from_bundle",
    "load_sam_bundle_payload",
    "resolve_bundle_directory",
]

