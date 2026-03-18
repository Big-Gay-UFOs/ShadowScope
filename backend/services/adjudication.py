from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.export_leads import build_lead_snapshot_export

ADJUDICATION_SCHEMA_VERSION = "lead_adjudication.v1"
DEFAULT_PRECISION_AT_K: tuple[int, ...] = (5, 10, 25, 50)
BUNDLE_ADJUDICATIONS_CSV = Path("exports") / "lead_adjudications.csv"
BUNDLE_ADJUDICATION_METRICS_JSON = Path("exports") / "lead_adjudication_metrics.json"

_ALLOWED_DECISIONS = {"keep", "reject", "unclear"}
_TRUTHY = {"1", "true", "yes", "y"}
_FALSY = {"0", "false", "no", "n"}
_TEMPLATE_COLUMNS = [
    "snapshot_id",
    "snapshot_item_id",
    "snapshot_created_at",
    "snapshot_source",
    "snapshot_scoring_version",
    "scoring_version",
    "rank",
    "score",
    "event_id",
    "event_hash",
    "source",
    "doc_id",
    "source_url",
    "occurred_at",
    "created_at",
    "lead_family",
    "lead_family_label",
    "why_summary",
    "decision",
    "reason_code",
    "reviewer_notes",
    "foia_ready",
    "lead_family_override",
]
_NORMALIZED_COLUMNS = _TEMPLATE_COLUMNS + ["source_file"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> Optional[int]:
    text = _text(value)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _safe_float(value: Any) -> Optional[float]:
    text = _text(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _round_metric(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 4)


def _pct(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return _round_metric((float(numerator) / float(denominator)) * 100.0)


def _normalize_decision(value: Any, *, source: str) -> Optional[str]:
    text = _text(value)
    if text is None:
        return None
    normalized = text.lower()
    if normalized not in _ALLOWED_DECISIONS:
        allowed = ", ".join(sorted(_ALLOWED_DECISIONS))
        raise ValueError(f"{source}: decision must be one of {allowed}")
    return normalized


def _normalize_foia_ready(value: Any, *, source: str) -> Optional[str]:
    text = _text(value)
    if text is None:
        return None
    normalized = text.lower()
    if normalized in _TRUTHY:
        return "yes"
    if normalized in _FALSY:
        return "no"
    raise ValueError(f"{source}: foia_ready must be yes/no")


def _write_csv(path: Path, rows: list[dict[str, Any]], *, fieldnames: Optional[Sequence[str]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = list(fieldnames) if fieldnames is not None else (list(rows[0].keys()) if rows else list(_NORMALIZED_COLUMNS))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _resolve_file_output(output: Optional[Path], *, default_filename: str, default_dir: Optional[Path] = None) -> Path:
    base_dir = Path(default_dir).expanduser() if default_dir else EXPORTS_DIR
    base_dir.mkdir(parents=True, exist_ok=True)
    if output is None:
        return base_dir / default_filename

    resolved = Path(output).expanduser()
    if resolved.suffix:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        return resolved
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved / default_filename


def _ordered_ks(values: Optional[Sequence[int]]) -> list[int]:
    raw_values = list(values) if values is not None else list(DEFAULT_PRECISION_AT_K)
    ordered: list[int] = []
    seen: set[int] = set()
    for value in raw_values:
        try:
            parsed = int(value)
        except Exception:
            continue
        if parsed <= 0 or parsed in seen:
            continue
        seen.add(parsed)
        ordered.append(parsed)
    return ordered or list(DEFAULT_PRECISION_AT_K)


def _snapshot_group_key(row: dict[str, Any]) -> str:
    snapshot_id = row.get("snapshot_id")
    if snapshot_id is not None:
        return f"snapshot:{snapshot_id}"
    snapshot_item_id = row.get("snapshot_item_id")
    if snapshot_item_id is not None:
        return f"item:{snapshot_item_id}"
    event_id = row.get("event_id")
    event_hash = row.get("event_hash") or ""
    return f"event:{event_id}:{event_hash}"


def _row_identity_key(row: dict[str, Any]) -> str:
    snapshot_item_id = row.get("snapshot_item_id")
    if snapshot_item_id is not None:
        return f"snapshot_item:{snapshot_item_id}"
    snapshot_id = row.get("snapshot_id")
    event_id = row.get("event_id")
    event_hash = row.get("event_hash") or ""
    return f"snapshot_event:{snapshot_id}:{event_id}:{event_hash}"


def _effective_lead_family(row: dict[str, Any]) -> str:
    return (
        _text(row.get("lead_family_override"))
        or _text(row.get("lead_family"))
        or "unassigned"
    )


def _row_sort_key(row: dict[str, Any]) -> tuple[int, int, int]:
    rank = row.get("rank")
    event_id = row.get("event_id")
    snapshot_item_id = row.get("snapshot_item_id")
    return (
        int(rank) if isinstance(rank, int) else 10**9,
        int(event_id) if isinstance(event_id, int) else 10**9,
        int(snapshot_item_id) if isinstance(snapshot_item_id, int) else 10**9,
    )


def _normalize_template_row(
    row: dict[str, Any],
    *,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        "snapshot_id": row.get("snapshot_id"),
        "snapshot_item_id": row.get("snapshot_item_id"),
        "snapshot_created_at": snapshot.get("created_at"),
        "snapshot_source": snapshot.get("source"),
        "snapshot_scoring_version": snapshot.get("scoring_version"),
        "scoring_version": row.get("scoring_version"),
        "rank": row.get("rank"),
        "score": row.get("score"),
        "event_id": row.get("event_id"),
        "event_hash": row.get("event_hash"),
        "source": row.get("source"),
        "doc_id": row.get("doc_id"),
        "source_url": row.get("source_url"),
        "occurred_at": row.get("occurred_at"),
        "created_at": row.get("created_at"),
        "lead_family": row.get("lead_family"),
        "lead_family_label": row.get("lead_family_label"),
        "why_summary": row.get("why_summary"),
        "decision": "",
        "reason_code": "",
        "reviewer_notes": "",
        "foia_ready": "",
        "lead_family_override": "",
    }
    return normalized


def export_lead_adjudication_template(
    *,
    snapshot_id: int,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
    bundle_dir: Optional[Path] = None,
) -> dict[str, Any]:
    ensure_runtime_directories()
    export_data = build_lead_snapshot_export(snapshot_id=int(snapshot_id), database_url=database_url)
    snapshot = export_data.get("snapshot") or {}
    rows = [_normalize_template_row(row, snapshot=snapshot) for row in export_data.get("items") or []]

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    default_name = f"lead_adjudication_template_{int(snapshot_id)}_{stamp}.csv"
    bundle_csv: Optional[Path] = None
    if bundle_dir is not None:
        bundle_csv = Path(bundle_dir).expanduser() / BUNDLE_ADJUDICATIONS_CSV
    csv_path = (
        bundle_csv
        if output is None and bundle_csv is not None
        else _resolve_file_output(
            output,
            default_filename=default_name,
            default_dir=EXPORTS_DIR,
        )
    )
    _write_csv(csv_path, rows, fieldnames=_TEMPLATE_COLUMNS)

    if bundle_csv is not None and bundle_csv.resolve() != csv_path.resolve():
        _write_csv(bundle_csv, rows, fieldnames=_TEMPLATE_COLUMNS)

    return {
        "schema_version": ADJUDICATION_SCHEMA_VERSION,
        "csv": csv_path,
        "bundle_csv": bundle_csv,
        "count": len(rows),
        "snapshot": snapshot,
    }


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSONL adjudication row") from exc
            if not isinstance(payload, dict):
                raise ValueError(f"{path}:{line_number}: adjudication row must be a JSON object")
            rows.append(payload)
    return rows


def _expand_sources(paths: Sequence[Path | str]) -> list[Path]:
    resolved: list[Path] = []
    seen: set[str] = set()
    for value in paths:
        path = Path(value).expanduser()
        if path.is_dir():
            candidates = sorted(
                [candidate for candidate in path.rglob("*") if candidate.is_file() and candidate.suffix.lower() in {".csv", ".jsonl"}],
                key=lambda item: item.as_posix().lower(),
            )
        else:
            candidates = [path]
        for candidate in candidates:
            key = candidate.resolve().as_posix() if candidate.exists() else candidate.expanduser().as_posix()
            if key in seen:
                continue
            seen.add(key)
            resolved.append(candidate)
    return resolved


def _normalize_adjudication_row(raw: dict[str, Any], *, source_path: Path, index: int) -> dict[str, Any]:
    source_label = f"{source_path}:{index}"
    decision = _normalize_decision(raw.get("decision"), source=source_label)
    normalized = {
        "snapshot_id": _safe_int(raw.get("snapshot_id")),
        "snapshot_item_id": _safe_int(raw.get("snapshot_item_id")),
        "snapshot_created_at": _text(raw.get("snapshot_created_at")),
        "snapshot_source": _text(raw.get("snapshot_source")),
        "snapshot_scoring_version": _text(raw.get("snapshot_scoring_version")),
        "rank": _safe_int(raw.get("rank")),
        "score": _safe_float(raw.get("score")),
        "event_id": _safe_int(raw.get("event_id")),
        "event_hash": _text(raw.get("event_hash")),
        "source": _text(raw.get("source")),
        "doc_id": _text(raw.get("doc_id")),
        "source_url": _text(raw.get("source_url")),
        "occurred_at": _text(raw.get("occurred_at")),
        "created_at": _text(raw.get("created_at")),
        "lead_family": _text(raw.get("lead_family")),
        "lead_family_label": _text(raw.get("lead_family_label")),
        "why_summary": _text(raw.get("why_summary")),
        "decision": decision,
        "reason_code": _text(raw.get("reason_code")),
        "reviewer_notes": _text(raw.get("reviewer_notes")),
        "foia_ready": _normalize_foia_ready(raw.get("foia_ready"), source=source_label),
        "lead_family_override": _text(raw.get("lead_family_override")),
        "source_file": str(source_path),
    }
    normalized["scoring_version"] = (
        _text(raw.get("scoring_version"))
        or normalized.get("snapshot_scoring_version")
    )
    normalized["effective_lead_family"] = _effective_lead_family(normalized)
    if normalized.get("rank") is None:
        raise ValueError(f"{source_label}: rank is required")
    if normalized.get("snapshot_item_id") is None and (
        normalized.get("snapshot_id") is None or normalized.get("event_id") is None
    ):
        raise ValueError(f"{source_label}: snapshot_item_id or snapshot_id/event_id is required")
    return normalized


def load_lead_adjudications(*, paths: Sequence[Path | str]) -> dict[str, Any]:
    sources = _expand_sources(paths)
    if not sources:
        raise ValueError("No adjudication files found.")

    rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for source in sources:
        if not source.exists():
            raise ValueError(f"Adjudication file not found: {source}")
        suffix = source.suffix.lower()
        if suffix == ".csv":
            raw_rows = _load_csv_rows(source)
        elif suffix == ".jsonl":
            raw_rows = _load_jsonl_rows(source)
        else:
            raise ValueError(f"Unsupported adjudication format: {source}")
        for index, raw in enumerate(raw_rows, start=2 if suffix == ".csv" else 1):
            row = _normalize_adjudication_row(raw, source_path=source, index=index)
            key = _row_identity_key(row)
            if key in seen_keys:
                raise ValueError(f"Duplicate adjudication row key detected: {key}")
            seen_keys.add(key)
            rows.append(row)

    rows.sort(key=lambda item: (_snapshot_group_key(item),) + _row_sort_key(item))
    return {
        "schema_version": ADJUDICATION_SCHEMA_VERSION,
        "source_files": [str(path) for path in sources],
        "count": len(rows),
        "rows": rows,
    }


def _summarize_rows(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    reviewed = 0
    keep = 0
    reject = 0
    unclear = 0
    foia_ready_yes = 0
    foia_ready_no = 0

    snapshot_ids = {
        int(snapshot_id)
        for snapshot_id in [row.get("snapshot_id") for row in rows]
        if isinstance(snapshot_id, int)
    }

    for row in rows:
        decision = row.get("decision")
        if decision is not None:
            reviewed += 1
        if decision == "keep":
            keep += 1
        elif decision == "reject":
            reject += 1
        elif decision == "unclear":
            unclear += 1

        foia_ready = row.get("foia_ready")
        if foia_ready == "yes":
            foia_ready_yes += 1
        elif foia_ready == "no":
            foia_ready_no += 1

    decisive = keep + reject
    unreviewed = total - reviewed

    return {
        "snapshot_count": len(snapshot_ids),
        "row_count": total,
        "reviewed_count": reviewed,
        "decisive_count": decisive,
        "keep_count": keep,
        "reject_count": reject,
        "unclear_count": unclear,
        "unreviewed_count": unreviewed,
        "review_coverage_pct": _pct(reviewed, total),
        "decisive_coverage_pct": _pct(decisive, total),
        "acceptance_rate": _round_metric((float(keep) / float(decisive)) if decisive > 0 else None),
        "acceptance_rate_pct": _pct(keep, decisive),
        "foia_ready_yes_count": foia_ready_yes,
        "foia_ready_no_count": foia_ready_no,
        "foia_ready_unspecified_count": total - foia_ready_yes - foia_ready_no,
        "foia_ready_yes_keep_count": len(
            [row for row in rows if row.get("decision") == "keep" and row.get("foia_ready") == "yes"]
        ),
    }


def _precision_at_k(rows: Sequence[dict[str, Any]], ks: Sequence[int]) -> dict[str, dict[str, Any]]:
    by_snapshot: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_snapshot.setdefault(_snapshot_group_key(row), []).append(row)

    results: dict[str, dict[str, Any]] = {}
    for k in _ordered_ks(ks):
        keep = 0
        reject = 0
        unclear = 0
        reviewed = 0
        snapshots_considered = 0
        rows_considered = 0
        for snapshot_rows in by_snapshot.values():
            ranked_rows = sorted(snapshot_rows, key=_row_sort_key)
            top_rows = ranked_rows[:k]
            if not top_rows:
                continue
            snapshots_considered += 1
            rows_considered += len(top_rows)
            for row in top_rows:
                decision = row.get("decision")
                if decision is not None:
                    reviewed += 1
                if decision == "keep":
                    keep += 1
                elif decision == "reject":
                    reject += 1
                elif decision == "unclear":
                    unclear += 1

        decisive = keep + reject
        results[str(k)] = {
            "k": int(k),
            "snapshot_count": snapshots_considered,
            "rows_considered": rows_considered,
            "reviewed_count": reviewed,
            "decisive_count": decisive,
            "keep_count": keep,
            "reject_count": reject,
            "unclear_count": unclear,
            "precision": _round_metric((float(keep) / float(decisive)) if decisive > 0 else None),
            "precision_pct": _pct(keep, decisive),
            "review_coverage_pct": _pct(reviewed, rows_considered),
            "unreviewed_count": max(0, rows_considered - reviewed),
            "non_decisive_count": max(0, rows_considered - decisive),
        }
    return results


def _rejection_reason_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    total_rejects = 0
    for row in rows:
        if row.get("decision") != "reject":
            continue
        total_rejects += 1
        reason = _text(row.get("reason_code")) or "unspecified"
        counts[reason] = counts.get(reason, 0) + 1

    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [
        {
            "reason_code": reason,
            "count": count,
            "share_of_rejects_pct": _pct(count, total_rejects),
        }
        for reason, count in ordered
    ]


def _group_rows(
    rows: Sequence[dict[str, Any]],
    *,
    key_name: str,
    key_getter: Any,
    include_precision: bool = False,
    ks: Optional[Sequence[int]] = None,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = key_getter(row)
        grouped.setdefault(key, []).append(row)

    out: list[dict[str, Any]] = []
    for key, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), str(item[0]))):
        summary = _summarize_rows(group_rows)
        payload = {key_name: key, **summary}
        if include_precision:
            payload["precision_at_k"] = _precision_at_k(group_rows, ks or DEFAULT_PRECISION_AT_K)
        out.append(payload)
    return out


def compute_lead_adjudication_metrics(
    *,
    rows: Sequence[dict[str, Any]],
    precision_at_k: Optional[Sequence[int]] = None,
) -> dict[str, Any]:
    ks = _ordered_ks(precision_at_k)
    rows_list = list(rows)
    payload = {
        "schema_version": ADJUDICATION_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "summary": {
            **_summarize_rows(rows_list),
            "precision_at_k": _precision_at_k(rows_list, ks),
        },
        "rejection_reasons": _rejection_reason_rows(rows_list),
        "by_lead_family": _group_rows(
            rows_list,
            key_name="lead_family",
            key_getter=lambda row: str(row.get("effective_lead_family") or "unassigned"),
            include_precision=False,
        ),
        "by_scoring_version": _group_rows(
            rows_list,
            key_name="scoring_version",
            key_getter=lambda row: str(row.get("scoring_version") or "unknown"),
            include_precision=True,
            ks=ks,
        ),
        "by_snapshot": _group_rows(
            rows_list,
            key_name="snapshot_id",
            key_getter=lambda row: str(row.get("snapshot_id") if row.get("snapshot_id") is not None else "unknown"),
            include_precision=True,
            ks=ks,
        ),
    }
    return payload


def _bundle_state_from_artifacts(bundle_dir: Path, artifact_payload: Optional[dict[str, Any]]) -> dict[str, Any]:
    candidates: list[tuple[str, Optional[Path]]] = []
    bundle_dir = bundle_dir.expanduser()

    artifact_payload = artifact_payload if isinstance(artifact_payload, dict) else {}
    exports = artifact_payload.get("exports") if isinstance(artifact_payload.get("exports"), dict) else {}
    adjudications_export = exports.get("adjudications") if isinstance(exports.get("adjudications"), dict) else {}
    metrics_export = exports.get("adjudication_metrics") if isinstance(exports.get("adjudication_metrics"), dict) else {}

    def _candidate(value: Any) -> Optional[Path]:
        if isinstance(value, Path):
            return value
        if isinstance(value, str) and value.strip():
            return Path(value)
        return None

    candidates.extend(
        [
            ("adjudications_csv", _candidate(artifact_payload.get("export_lead_adjudications_csv"))),
            ("adjudications_csv", _candidate(adjudications_export.get("csv"))),
            ("adjudications_csv", BUNDLE_ADJUDICATIONS_CSV),
            ("metrics_json", _candidate(artifact_payload.get("export_lead_adjudication_metrics_json"))),
            ("metrics_json", _candidate(metrics_export.get("json"))),
            ("metrics_json", BUNDLE_ADJUDICATION_METRICS_JSON),
        ]
    )

    state: dict[str, Any] = {"adjudications_csv": None, "metrics_json": None, "metrics": {}}
    for key, candidate in candidates:
        if candidate is None:
            continue
        resolved = candidate if candidate.is_absolute() else (bundle_dir / candidate)
        if key == "adjudications_csv" and state[key] is None and resolved.exists():
            state[key] = resolved
        if key == "metrics_json" and state[key] is None and resolved.exists():
            state[key] = resolved

    metrics_path = state.get("metrics_json")
    if isinstance(metrics_path, Path) and metrics_path.exists():
        try:
            payload = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            state["metrics"] = payload
    return state


def load_bundle_adjudication_state(
    *,
    bundle_dir: Path,
    artifact_payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return _bundle_state_from_artifacts(Path(bundle_dir), artifact_payload)


def _relative_to_bundle(path: Path, bundle_dir: Path) -> str:
    try:
        return str(path.resolve().relative_to(bundle_dir.resolve()))
    except Exception:
        return str(path)


def _update_bundle_manifest(
    *,
    bundle_dir: Path,
    adjudications_csv: Optional[Path],
    metrics_json: Optional[Path],
) -> Optional[Path]:
    from backend.services.bundle import SAM_BUNDLE_MANIFEST_NAME

    manifest_path = Path(bundle_dir).expanduser() / SAM_BUNDLE_MANIFEST_NAME
    if not manifest_path.exists():
        return None

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        manifest = {}

    generated_files = manifest.get("generated_files") if isinstance(manifest.get("generated_files"), dict) else {}
    if adjudications_csv is not None:
        generated_files["export_lead_adjudications_csv"] = _relative_to_bundle(adjudications_csv, Path(bundle_dir))
    if metrics_json is not None:
        generated_files["export_lead_adjudication_metrics_json"] = _relative_to_bundle(metrics_json, Path(bundle_dir))
    manifest["generated_files"] = generated_files
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return manifest_path


def _refresh_bundle_reports(bundle_dir: Path) -> dict[str, Any]:
    artifacts: dict[str, Any] = {}
    errors: list[str] = []

    try:
        from backend.services.bundle import render_sam_bundle_report_from_bundle

        artifacts["bundle_report_html"] = render_sam_bundle_report_from_bundle(bundle_dir)
    except Exception as exc:
        errors.append(f"bundle_report_refresh_failed: {exc}")

    try:
        from backend.services.reporting import generate_sam_report_from_bundle

        report = generate_sam_report_from_bundle(bundle_dir)
        artifacts["report_html"] = report.get("report_html")
    except Exception as exc:
        errors.append(f"report_refresh_failed: {exc}")

    if errors:
        artifacts["refresh_errors"] = errors
    return artifacts


def evaluate_lead_adjudications(
    *,
    adjudications: Sequence[Path | str],
    precision_at_k: Optional[Sequence[int]] = None,
    output: Optional[Path] = None,
    bundle_dir: Optional[Path] = None,
) -> dict[str, Any]:
    ensure_runtime_directories()
    loaded = load_lead_adjudications(paths=adjudications)
    rows = loaded.get("rows") or []
    metrics = compute_lead_adjudication_metrics(rows=rows, precision_at_k=precision_at_k)
    metrics["source_files"] = loaded.get("source_files") or []

    artifacts: dict[str, Any] = {}

    if output is not None:
        output_path = Path(output).expanduser()
        metrics_json_path = _resolve_file_output(
            output_path,
            default_filename="lead_adjudication_metrics.json",
            default_dir=EXPORTS_DIR,
        )
        _write_json(metrics_json_path, metrics)
        artifacts["metrics_json"] = metrics_json_path

        if not output_path.suffix:
            normalized_csv_path = output_path / "lead_adjudications.csv"
            _write_csv(
                normalized_csv_path,
                [{key: row.get(key) for key in _NORMALIZED_COLUMNS} for row in rows],
                fieldnames=_NORMALIZED_COLUMNS,
            )
            artifacts["normalized_adjudications_csv"] = normalized_csv_path

    if bundle_dir is not None:
        bundle_root = Path(bundle_dir).expanduser()
        if not bundle_root.exists() or not bundle_root.is_dir():
            raise ValueError(f"Bundle directory not found: {bundle_root}")

        bundle_csv = bundle_root / BUNDLE_ADJUDICATIONS_CSV
        bundle_metrics_json = bundle_root / BUNDLE_ADJUDICATION_METRICS_JSON
        _write_csv(
            bundle_csv,
            [{key: row.get(key) for key in _NORMALIZED_COLUMNS} for row in rows],
            fieldnames=_NORMALIZED_COLUMNS,
        )
        _write_json(bundle_metrics_json, metrics)
        artifacts["bundle_adjudications_csv"] = bundle_csv
        artifacts["bundle_metrics_json"] = bundle_metrics_json
        manifest_path = _update_bundle_manifest(
            bundle_dir=bundle_root,
            adjudications_csv=bundle_csv,
            metrics_json=bundle_metrics_json,
        )
        if manifest_path is not None:
            artifacts["bundle_manifest_json"] = manifest_path
        artifacts.update(_refresh_bundle_reports(bundle_root))

    return {
        **metrics,
        "artifacts": artifacts,
    }


__all__ = [
    "ADJUDICATION_SCHEMA_VERSION",
    "BUNDLE_ADJUDICATIONS_CSV",
    "BUNDLE_ADJUDICATION_METRICS_JSON",
    "DEFAULT_PRECISION_AT_K",
    "compute_lead_adjudication_metrics",
    "evaluate_lead_adjudications",
    "export_lead_adjudication_template",
    "load_bundle_adjudication_state",
    "load_lead_adjudications",
]
