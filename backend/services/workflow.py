from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.correlate import correlate
from backend.services.entities import link_entities_from_events
from backend.services.export import export_events
from backend.services.export_correlations import export_kw_pairs
from backend.services.export_entities import export_entities_bundle
from backend.services.export_leads import export_lead_snapshot
from backend.services.ingest import ingest_usaspending
from backend.services.leads import create_lead_snapshot
from backend.services.tagging import apply_ontology_to_events


def run_usaspending_workflow(
    *,
    # Ingest
    ingest_days: int = 30,
    pages: int = 1,
    page_size: int = 100,
    max_records: Optional[int] = None,
    start_page: int = 1,
    recipient_search_text: Optional[list[str]] = None,
    keywords: Optional[list[str]] = None,
    # Ontology
    ontology_path: Path = Path("ontology.json"),
    ontology_days: int = 30,
    analysis_run_id: Optional[int] = None,
    # Entities
    entity_days: int = 30,
    entity_batch: int = 500,
    # Correlations
    window_days: int = 30,
    min_events_entity: int = 2,
    min_events_keywords: int = 3,
    max_events_keywords: int = 200,
    max_keywords_per_event: int = 10,
    # Snapshot
    min_score: int = 1,
    snapshot_limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = "v2",
    notes: Optional[str] = None,
    # Exports
    output: Optional[Path] = None,
    export_events_flag: bool = False,
    kw_pairs_limit: int = 200,
    kw_pairs_min_event_count: int = 2,
    # DB
    database_url: Optional[str] = None,
    # Skips
    skip_ingest: bool = False,
    skip_ontology: bool = False,
    skip_entities: bool = False,
    skip_correlations: bool = False,
    skip_snapshot: bool = False,
    skip_exports: bool = False,
) -> dict[str, Any]:
    """One-command USAspending workflow wrapper.

    Order:
      ingest -> ontology -> entities -> correlations -> snapshot -> exports

    NOTE: If output is a file path (has a suffix), we generate per-artifact
    file names with a shared prefix + timestamp to avoid clobbering.
    """
    res: dict[str, Any] = {"source": "USAspending"}

    if not skip_ingest:
        ing = ingest_usaspending(
            days=int(ingest_days),
            pages=int(pages),
            page_size=int(page_size),
            max_records=max_records,
            start_page=int(start_page),
            recipient_search_text=recipient_search_text,
            keywords=keywords,
            database_url=database_url,
        )
        res["ingest"] = ing

    arid = analysis_run_id
    if not skip_ontology:
        ont = apply_ontology_to_events(
            ontology_path=Path(ontology_path),
            days=int(ontology_days),
            source="USAspending",
            batch=500,
            dry_run=False,
            database_url=database_url,
        )
        res["ontology_apply"] = ont
        if arid is None:
            arid = ont.get("analysis_run_id")

    if not skip_entities:
        ent = link_entities_from_events(
            source="USAspending",
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
            source="USAspending",
            min_events=int(min_events_entity),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_uei"] = correlate.rebuild_uei_correlations(
            window_days=int(window_days),
            source="USAspending",
            min_events=int(min_events_entity),
            dry_run=False,
            database_url=database_url,
        )
        corr["same_keyword"] = correlate.rebuild_keyword_correlations(
            window_days=int(window_days),
            source="USAspending",
            min_events=int(min_events_keywords),
            max_events=int(max_events_keywords),
            dry_run=False,
            database_url=database_url,
        )
        corr["kw_pair"] = correlate.rebuild_keyword_pair_correlations(
            window_days=int(window_days),
            source="USAspending",
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
            source="USAspending",
            min_score=int(min_score),
            limit=int(snapshot_limit),
            scan_limit=int(scan_limit),
            scoring_version=str(scoring_version),
            notes=notes,
            database_url=database_url,
        )
        res["snapshot"] = snap
        snapshot_id = int(snap.get("snapshot_id")) if snap.get("snapshot_id") is not None else None

    if not skip_exports:
        exports: dict[str, Any] = {}

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

        def _out(kind: str) -> Optional[Path]:
            if out_path is None:
                return None
            if not out_is_file:
                return out_path
            assert export_dir is not None and base is not None and ts is not None
            if kind == "lead_snapshot":
                assert snapshot_id is not None
                return export_dir / f"{base}_lead_snapshot_{int(snapshot_id)}_{ts}.csv"
            return export_dir / f"{base}_{kind}_{ts}.csv"

        if snapshot_id is not None:
            exports["lead_snapshot"] = export_lead_snapshot(
                snapshot_id=int(snapshot_id),
                database_url=database_url,
                output=_out("lead_snapshot"),
            )

        exports["kw_pairs"] = export_kw_pairs(
            database_url=database_url,
            output=_out("kw_pairs"),
            limit=int(kw_pairs_limit),
            min_event_count=int(kw_pairs_min_event_count),
        )

        exports["entities"] = export_entities_bundle(
            database_url=database_url,
            output=_out("entities"),
        )

        if export_events_flag:
            exports["events"] = export_events(
                database_url=database_url,
                output=_out("events"),
            )

        res["exports"] = exports

    return res


__all__ = ["run_usaspending_workflow"]
