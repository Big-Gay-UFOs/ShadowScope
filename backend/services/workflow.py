from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from backend.correlate import correlate
from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.doctor import doctor_status
from backend.services.entities import link_entities_from_events
from backend.services.export import export_events
from backend.services.export_correlations import export_kw_pairs
from backend.services.export_entities import export_entities_bundle
from backend.services.export_leads import export_lead_snapshot
from backend.services.ingest import ingest_sam_opportunities, ingest_usaspending
from backend.services.leads import create_lead_snapshot
from backend.services.tagging import apply_ontology_to_events

_IngestFn = Callable[..., dict[str, Any]]


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


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
    min_score: int,
    snapshot_limit: int,
    scan_limit: int,
    scoring_version: str,
    notes: Optional[str],
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
        res["correlations"] = corr

    snapshot_id: Optional[int] = None
    if not skip_snapshot:
        snap = create_lead_snapshot(
            analysis_run_id=arid,
            source=source,
            min_score=int(min_score),
            limit=int(snapshot_limit),
            scan_limit=int(scan_limit),
            scoring_version=str(scoring_version),
            notes=notes,
            database_url=database_url,
        )
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
        min_score=int(min_score),
        snapshot_limit=int(snapshot_limit),
        scan_limit=int(scan_limit),
        scoring_version=str(scoring_version),
        notes=notes,
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
    ingest_days: int = 30,
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
    min_score: int = 1,
    snapshot_limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = "v2",
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
    return _run_source_workflow(
        source="SAM.gov",
        ingest_fn=ingest_sam_opportunities,
        ingest_kwargs={
            "api_key": api_key,
            "days": int(ingest_days),
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
        min_score=int(min_score),
        snapshot_limit=int(snapshot_limit),
        scan_limit=int(scan_limit),
        scoring_version=str(scoring_version),
        notes=notes,
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


def run_samgov_smoke_workflow(
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
    scoring_version: str = "v2",
    notes: Optional[str] = None,
    bundle_root: Optional[Path] = None,
    database_url: Optional[str] = None,
    require_nonzero: bool = True,
    skip_ingest: bool = False,
) -> dict[str, Any]:
    """Run SAM.gov end-to-end workflow and persist a smoke artifact bundle."""
    ensure_runtime_directories()
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    root = Path(bundle_root).expanduser() if bundle_root else (EXPORTS_DIR / "smoke" / "samgov")
    bundle_dir = root / stamp
    bundle_dir.mkdir(parents=True, exist_ok=True)

    workflow_res = run_samgov_workflow(
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
        notes=notes,
        output=bundle_dir / "exports" / "samgov_smoke.csv",
        export_events_flag=True,
        database_url=database_url,
        skip_ingest=bool(skip_ingest),
        abort_on_ingest_skip=True,
    )

    status = workflow_res.get("status")
    ingest = workflow_res.get("ingest") if isinstance(workflow_res, dict) else {}
    ingest_nonzero = (
        _safe_int((ingest or {}).get("fetched")) > 0
        or _safe_int((ingest or {}).get("inserted")) > 0
        or _safe_int((ingest or {}).get("normalized")) > 0
    )

    doc = doctor_status(
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
    snapshot_items = _safe_int((workflow_res.get("snapshot") or {}).get("items"))

    checks: list[dict[str, Any]] = []
    checks.append(
        {
            "name": "doctor_db_ok",
            "required": True,
            "ok": bool(db_ok),
            "actual": (doc.get("db") or {}).get("status"),
            "expected": "ok",
        }
    )

    if skip_ingest:
        checks.append(
            {
                "name": "ingest_nonzero",
                "required": False,
                "ok": True,
                "actual": "skipped",
                "expected": "skip_ingest=True",
            }
        )
    else:
        checks.append(
            {
                "name": "ingest_nonzero",
                "required": True,
                "ok": bool(ingest_nonzero) and status != "skipped",
                "actual": {
                    "status": (ingest or {}).get("status"),
                    "fetched": _safe_int((ingest or {}).get("fetched")),
                    "inserted": _safe_int((ingest or {}).get("inserted")),
                    "normalized": _safe_int((ingest or {}).get("normalized")),
                },
                "expected": "fetched>0 OR inserted>0 OR normalized>0",
            }
        )

    checks.extend(
        [
            {
                "name": "events_window_nonzero",
                "required": True,
                "ok": _safe_int(counts.get("events_window")) > 0,
                "actual": _safe_int(counts.get("events_window")),
                "expected": "> 0",
            },
            {
                "name": "events_with_keywords_nonzero",
                "required": True,
                "ok": _safe_int(kw.get("events_with_keywords")) > 0,
                "actual": _safe_int(kw.get("events_with_keywords")),
                "expected": "> 0",
            },
            {
                "name": "events_with_entity_nonzero",
                "required": True,
                "ok": _safe_int(counts.get("events_with_entity_window")) > 0,
                "actual": _safe_int(counts.get("events_with_entity_window")),
                "expected": "> 0",
            },
            {
                "name": "keyword_or_kw_pair_lane_nonzero",
                "required": True,
                "ok": (
                    _safe_int(lane_counts.get("same_keyword")) > 0
                    or _safe_int(lane_counts.get("kw_pair")) > 0
                ),
                "actual": {
                    "same_keyword": _safe_int(lane_counts.get("same_keyword")),
                    "kw_pair": _safe_int(lane_counts.get("kw_pair")),
                },
                "expected": "same_keyword>0 OR kw_pair>0",
            },
            {
                "name": "snapshot_items_nonzero",
                "required": True,
                "ok": snapshot_items > 0,
                "actual": snapshot_items,
                "expected": "> 0",
            },
        ]
    )

    smoke_passed = all(bool(c.get("ok")) for c in checks if bool(c.get("required", True)))

    baseline = {
        "captured_at": now.isoformat(),
        "source": "SAM.gov",
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
        },
        "snapshot_items": snapshot_items,
    }

    workflow_json = bundle_dir / "workflow_result.json"
    doctor_json = bundle_dir / "doctor_status.json"
    summary_json = bundle_dir / "smoke_summary.json"

    _write_json(workflow_json, {"generated_at": now.isoformat(), "result": workflow_res})
    _write_json(doctor_json, {"generated_at": now.isoformat(), "result": doc})
    _write_json(
        summary_json,
        {
            "generated_at": now.isoformat(),
            "source": "SAM.gov",
            "smoke_passed": smoke_passed,
            "require_nonzero": bool(require_nonzero),
            "checks": checks,
            "baseline": baseline,
            "artifacts": {
                "workflow_result_json": workflow_json,
                "doctor_status_json": doctor_json,
                "exports": (workflow_res.get("exports") if isinstance(workflow_res, dict) else None),
            },
        },
    )

    smoke_status = "failed" if (require_nonzero and not smoke_passed) else "ok"

    return {
        "status": smoke_status,
        "smoke_passed": bool(smoke_passed),
        "bundle_dir": bundle_dir,
        "workflow": workflow_res,
        "doctor": doc,
        "checks": checks,
        "baseline": baseline,
        "artifacts": {
            "workflow_result_json": workflow_json,
            "doctor_status_json": doctor_json,
            "smoke_summary_json": summary_json,
            "exports": (workflow_res.get("exports") if isinstance(workflow_res, dict) else None),
        },
    }


__all__ = [
    "run_usaspending_workflow",
    "run_samgov_workflow",
    "run_samgov_smoke_workflow",
]
