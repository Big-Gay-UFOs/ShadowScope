"""Export utilities for lead snapshots and lead deltas."""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import select

from backend.db.models import Event, LeadSnapshot, LeadSnapshotItem, get_session_factory
from backend.runtime import EXPORTS_DIR, ensure_runtime_directories
from backend.services.deltas import compute_lead_deltas
from backend.services.explainability import (
    build_event_context_payload,
    enrich_lead_score_details,
    load_event_correlation_evidence,
    load_event_linked_source_summary,
)
from backend.services.lead_families import (
    classify_lead_families,
    lead_matches_family,
    summarize_lead_family_distribution,
    summarize_lead_family_groups,
)
from backend.services.leads import compare_lead_scoring_versions
from backend.services.review_contract import (
    CANONICAL_RANKED_LEAD_REVIEW_FIELDS,
    RANKED_LEAD_REVIEW_CONTRACT_VERSION,
    candidate_join_text as review_candidate_join_text,
    correlation_text as review_correlation_text,
    family_assignment_text as review_family_assignment_text,
    linked_source_text as review_linked_source_text,
    list_text as review_list_text,
    review_completeness_counts,
    review_effective_window,
    review_row_csv_safe,
    serialize_ranked_lead_review_row,
    signal_text as review_signal_text,
    suppressor_text as review_suppressor_text,
    top_clauses_text as review_top_clauses_text,
    why_summary as review_why_summary,
)


def _score_part(details: dict[str, Any], key: str, default: Any = 0) -> Any:
    v = details.get(key, default)
    return default if v is None else v


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _list_text(values: list[Any], *, limit: int = 5) -> str:
    return review_list_text(values, limit=limit)


def _top_clauses_text(details: dict[str, Any], limit: int = 5) -> str:
    return review_top_clauses_text(details, limit=limit)


def _correlation_text(correlation: dict[str, Any]) -> str:
    return review_correlation_text(correlation)


def _signal_text(signal: dict[str, Any]) -> str:
    return review_signal_text(signal)


def _suppressor_text(signal: dict[str, Any]) -> str:
    return review_suppressor_text(signal)


def _family_assignment_text(assignment: dict[str, Any]) -> str:
    return review_family_assignment_text(assignment)


def _family_selection_text(selection: dict[str, Any]) -> str:
    primary = str(selection.get("primary_family") or "").strip()
    runner_up = str(selection.get("runner_up_family") or "").strip()
    primary_score = selection.get("primary_selection_score")
    runner_up_score = selection.get("runner_up_selection_score")
    margin = selection.get("selection_margin")
    bits: list[str] = []
    if primary:
        if primary_score is not None:
            bits.append(f"primary={primary}({primary_score})")
        else:
            bits.append(f"primary={primary}")
    if runner_up:
        if runner_up_score is not None:
            bits.append(f"runner_up={runner_up}({runner_up_score})")
        else:
            bits.append(f"runner_up={runner_up}")
    if margin is not None:
        bits.append(f"margin={margin}")
    return " | ".join(bits) if bits else "lead_family_selection"


def _candidate_join_text(entry: dict[str, Any]) -> str:
    return review_candidate_join_text(entry)


def _linked_source_text(entry: dict[str, Any]) -> str:
    return review_linked_source_text(entry)


def _why_summary(details: dict[str, Any]) -> str:
    return review_why_summary(details)


def _flatten_details(prefix: str, details: dict[str, Any]) -> dict[str, Any]:
    contributing_lanes = details.get("contributing_lanes") or []
    contributing_correlations = details.get("contributing_correlations") or []
    matched_rules = details.get("matched_ontology_rules") or []
    matched_clauses = details.get("matched_ontology_clauses") or []
    top_positive_signals = details.get("top_positive_signals") or []
    top_suppressors = details.get("top_suppressors") or []
    corroboration_sources = details.get("corroboration_sources") or []
    corroboration_summary = details.get("corroboration_summary") or {}
    candidate_join_evidence = corroboration_summary.get("candidate_join_evidence") or []
    linked_source_summary = corroboration_summary.get("linked_source_summary") or []
    correlation_types_hit = corroboration_summary.get("correlation_types_hit") or []
    family_assignments = details.get("lead_family_assignments") or []
    family_selection = details.get("lead_family_selection") or {}
    family_context = details.get("lead_family_context") or {}
    secondary_lead_families = details.get("secondary_lead_families") or []
    subscore_math = details.get("subscore_math") or {}
    return {
        f"{prefix}_scoring_version": details.get("scoring_version"),
        f"{prefix}_lead_family": details.get("lead_family"),
        f"{prefix}_lead_family_label": details.get("lead_family_label"),
        f"{prefix}_secondary_lead_families_text": _list_text([str(v) for v in secondary_lead_families], limit=10),
        f"{prefix}_secondary_lead_families_json": _json_text(secondary_lead_families),
        f"{prefix}_lead_family_assignments_text": _list_text([_family_assignment_text(item) for item in family_assignments], limit=5),
        f"{prefix}_lead_family_assignments_json": _json_text(family_assignments),
        f"{prefix}_lead_family_selection_text": _family_selection_text(family_selection),
        f"{prefix}_lead_family_selection_json": _json_text(family_selection),
        f"{prefix}_lead_family_context_json": _json_text(family_context),
        f"{prefix}_clause_score": _score_part(details, "clause_score", 0),
        f"{prefix}_clause_score_raw": _score_part(details, "clause_score_raw", None),
        f"{prefix}_keyword_score": _score_part(details, "keyword_score", 0),
        f"{prefix}_entity_bonus": _score_part(details, "entity_bonus", 0),
        f"{prefix}_pair_bonus": _score_part(details, "pair_bonus", 0),
        f"{prefix}_pair_bonus_applied": _score_part(details, "pair_bonus_applied", _score_part(details, "pair_bonus", 0)),
        f"{prefix}_pair_count": _score_part(details, "pair_count", 0),
        f"{prefix}_pair_strength": _score_part(details, "pair_strength", 0.0),
        f"{prefix}_noise_penalty": _score_part(details, "noise_penalty", 0),
        f"{prefix}_noise_penalty_applied": _score_part(details, "noise_penalty_applied", _score_part(details, "noise_penalty", 0)),
        f"{prefix}_proxy_relevance_score": _score_part(details, "proxy_relevance_score", 0),
        f"{prefix}_investigability_score": _score_part(details, "investigability_score", 0),
        f"{prefix}_corroboration_score": _score_part(details, "corroboration_score", 0),
        f"{prefix}_structural_context_score": _score_part(details, "structural_context_score", 0),
        f"{prefix}_total_score": _score_part(details, "total_score", 0),
        f"{prefix}_candidate_corroboration_count": _score_part(details, "candidate_corroboration_count", 0),
        f"{prefix}_pair_or_candidate_corroboration": bool(details.get("pair_or_candidate_corroboration")),
        f"{prefix}_top_rank_gate_penalty": _score_part(details, "top_rank_gate_penalty", 0),
        f"{prefix}_ranking_tier": details.get("ranking_tier"),
        f"{prefix}_classification_tags_text": _list_text([str(v) for v in (details.get("classification_tags") or [])], limit=10),
        f"{prefix}_classification_tags_json": _json_text(details.get("classification_tags") or []),
        f"{prefix}_classification_matches_json": _json_text(details.get("classification_matches") or []),
        f"{prefix}_routine_noise_tags_text": _list_text([str(v) for v in (details.get("routine_noise_tags") or [])], limit=10),
        f"{prefix}_routine_noise_tags_json": _json_text(details.get("routine_noise_tags") or []),
        f"{prefix}_contributing_lanes_text": _list_text([str(v) for v in contributing_lanes], limit=20),
        f"{prefix}_contributing_lanes_json": _json_text(contributing_lanes),
        f"{prefix}_contributing_correlations_text": _list_text([_correlation_text(c) for c in contributing_correlations], limit=5),
        f"{prefix}_contributing_correlations_json": _json_text(contributing_correlations),
        f"{prefix}_matched_ontology_rules_text": _list_text([str(v) for v in matched_rules], limit=10),
        f"{prefix}_matched_ontology_rules_json": _json_text(matched_rules),
        f"{prefix}_matched_ontology_clauses_json": _json_text(matched_clauses),
        f"{prefix}_top_positive_signals_text": _list_text([_signal_text(signal) for signal in top_positive_signals], limit=5),
        f"{prefix}_top_positive_signals_json": _json_text(top_positive_signals),
        f"{prefix}_top_suppressors_text": _list_text([_suppressor_text(signal) for signal in top_suppressors], limit=5),
        f"{prefix}_top_suppressors_json": _json_text(top_suppressors),
        f"{prefix}_corroboration_sources_text": _list_text([_signal_text(signal) for signal in corroboration_sources], limit=5),
        f"{prefix}_corroboration_sources_json": _json_text(corroboration_sources),
        f"{prefix}_correlation_types_hit_text": _list_text([str(v) for v in correlation_types_hit], limit=10),
        f"{prefix}_correlation_types_hit_json": _json_text(correlation_types_hit),
        f"{prefix}_candidate_join_evidence_text": _list_text([_candidate_join_text(item) for item in candidate_join_evidence], limit=5),
        f"{prefix}_candidate_join_evidence_json": _json_text(candidate_join_evidence),
        f"{prefix}_linked_source_summary_text": _list_text([_linked_source_text(item) for item in linked_source_summary], limit=5),
        f"{prefix}_linked_source_summary_json": _json_text(linked_source_summary),
        f"{prefix}_corroboration_summary_json": _json_text(corroboration_summary),
        f"{prefix}_subscore_math_json": _json_text(subscore_math),
        f"{prefix}_why_summary": _why_summary(details),
        f"{prefix}_score_details_json": _json_text(details or {}),
    }


def _load_event_context(
    database_url: Optional[str],
    event_ids: list[int],
) -> tuple[dict[int, Event], dict[int, list[dict[str, Any]]], dict[int, dict[str, Any]]]:
    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        events_by_id: dict[int, Event] = {}
        if event_ids:
            rows = db.execute(select(Event).where(Event.id.in_(event_ids))).scalars().all()
            events_by_id = {int(event.id): event for event in rows}
        correlations_by_event = load_event_correlation_evidence(db, event_ids=event_ids)
        linked_source_context = load_event_linked_source_summary(db, event_ids=event_ids)
    return events_by_id, correlations_by_event, linked_source_context


def _legacy_review_export_fields(
    *,
    snapshot: LeadSnapshot,
    details: dict[str, Any],
    review_row: dict[str, Any],
) -> dict[str, Any]:
    pair_correlations = [
        correlation
        for correlation in (details.get("contributing_correlations") or [])
        if str(correlation.get("lane") or "") == "kw_pair"
    ]
    top_pairs = pair_correlations[:5]
    top_pairs_text = "; ".join(
        [
            f"{pair.get('pair_label') or pair.get('pair_label_raw') or pair.get('correlation_key')}(n={pair.get('event_count')})"
            for pair in top_pairs
        ]
    )
    matched_rules = details.get("matched_ontology_rules") or []
    contributing_lanes = details.get("contributing_lanes") or []
    contributing_correlations = details.get("contributing_correlations") or []
    top_positive_signals = details.get("top_positive_signals") or []
    top_suppressors = details.get("top_suppressors") or []
    corroboration_sources = details.get("corroboration_sources") or []
    corroboration_summary = details.get("corroboration_summary") or {}
    candidate_join_evidence = corroboration_summary.get("candidate_join_evidence") or []
    linked_source_summary = corroboration_summary.get("linked_source_summary") or []
    correlation_types_hit = corroboration_summary.get("correlation_types_hit") or []
    lead_family_assignments = details.get("lead_family_assignments") or []
    lead_family_selection = details.get("lead_family_selection") or {}
    lead_family_context = details.get("lead_family_context") or {}
    return {
        "snapshot_scoring_version": getattr(snapshot, "scoring_version", None),
        "secondary_lead_families_text": _list_text(
            [str(value) for value in (review_row.get("secondary_lead_families") or [])],
            limit=10,
        ),
        "secondary_lead_families_json": _json_text(review_row.get("secondary_lead_families") or []),
        "lead_family_assignments_text": _list_text(
            [_family_assignment_text(item) for item in lead_family_assignments],
            limit=5,
        ),
        "lead_family_assignments_json": _json_text(lead_family_assignments),
        "lead_family_selection_text": _family_selection_text(lead_family_selection),
        "lead_family_selection_json": _json_text(lead_family_selection),
        "lead_family_context_json": _json_text(lead_family_context),
        "clause_score": _score_part(details, "clause_score", 0),
        "clause_score_raw": _score_part(details, "clause_score_raw", None),
        "keyword_score": _score_part(details, "keyword_score", 0),
        "entity_bonus": _score_part(details, "entity_bonus", 0),
        "pair_bonus": _score_part(details, "pair_bonus", 0),
        "pair_bonus_applied": _score_part(details, "pair_bonus_applied", _score_part(details, "pair_bonus", 0)),
        "pair_count": _score_part(details, "pair_count", 0),
        "pair_strength": _score_part(details, "pair_strength", 0.0),
        "has_noise": bool(_score_part(details, "has_noise", False)),
        "noise_penalty": _score_part(details, "noise_penalty", 0),
        "noise_penalty_applied": _score_part(
            details,
            "noise_penalty_applied",
            _score_part(details, "noise_penalty", 0),
        ),
        "proxy_relevance_score": _score_part(details, "proxy_relevance_score", 0),
        "investigability_score": _score_part(details, "investigability_score", 0),
        "corroboration_score": _score_part(details, "corroboration_score", 0),
        "structural_context_score": _score_part(details, "structural_context_score", 0),
        "total_score": _score_part(details, "total_score", 0),
        "contributing_lanes_text": _list_text([str(value) for value in contributing_lanes], limit=20),
        "contributing_lanes_json": _json_text(contributing_lanes),
        "contributing_correlations_text": _list_text(
            [_correlation_text(correlation) for correlation in contributing_correlations],
            limit=5,
        ),
        "contributing_correlations_json": _json_text(contributing_correlations),
        "matched_ontology_rules_text": _list_text([str(value) for value in matched_rules], limit=10),
        "matched_ontology_rules_json": _json_text(matched_rules),
        "matched_ontology_clauses_json": _json_text(details.get("matched_ontology_clauses") or []),
        "top_positive_signals_text": _list_text(
            [_signal_text(signal) for signal in top_positive_signals],
            limit=5,
        ),
        "top_positive_signals_json": _json_text(top_positive_signals),
        "top_suppressors_text": _list_text(
            [_suppressor_text(signal) for signal in top_suppressors],
            limit=5,
        ),
        "top_suppressors_json": _json_text(top_suppressors),
        "corroboration_sources_text": _list_text(
            [_signal_text(signal) for signal in corroboration_sources],
            limit=5,
        ),
        "corroboration_sources_json": _json_text(corroboration_sources),
        "correlation_types_hit_text": _list_text([str(value) for value in correlation_types_hit], limit=10),
        "correlation_types_hit_json": _json_text(correlation_types_hit),
        "candidate_join_evidence_text": _list_text(
            [_candidate_join_text(item) for item in candidate_join_evidence],
            limit=5,
        ),
        "candidate_join_evidence_json": _json_text(candidate_join_evidence),
        "linked_source_summary_text": _list_text(
            [_linked_source_text(item) for item in linked_source_summary],
            limit=5,
        ),
        "linked_source_summary_json": _json_text(linked_source_summary),
        "corroboration_summary_json": _json_text(corroboration_summary),
        "subscore_math_json": _json_text(details.get("subscore_math") or {}),
        "top_clauses_text": _top_clauses_text(details, limit=5),
        "top_kw_pairs_text": top_pairs_text,
        "top_kw_pairs_json": _json_text(top_pairs),
        "why_summary": review_row.get("why_summary"),
        "score_details_json": _json_text(details or {}),
    }


def _build_review_summary_payload(
    *,
    snapshot: dict[str, Any],
    rows: list[dict[str, Any]],
    csv_path: Path,
    json_path: Path,
    review_summary_path: Path,
    lead_family_filter: Optional[str],
    family_distribution: dict[str, Any],
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_contract_version": RANKED_LEAD_REVIEW_CONTRACT_VERSION,
        "canonical_review_fields": list(CANONICAL_RANKED_LEAD_REVIEW_FIELDS),
        "snapshot_id": snapshot.get("id"),
        "snapshot_source": snapshot.get("source"),
        "snapshot_created_at": snapshot.get("created_at"),
        "scoring_version": snapshot.get("scoring_version"),
        "lead_family_filter": lead_family_filter,
        "row_count": len(rows),
        "effective_window": review_effective_window(rows),
        "completeness_counts": review_completeness_counts(rows),
        "family_distribution": family_distribution,
        "review_artifact_filenames": {
            "lead_snapshot_csv": csv_path.name,
            "lead_snapshot_json": json_path.name,
            "review_summary_json": review_summary_path.name,
        },
        "evidence_package_availability": {
            "available": bool(snapshot.get("id")) and bool(rows),
            "package_type": "lead_evidence_package",
            "snapshot_id": snapshot.get("id"),
            "row_count": len(rows),
            "selectors": ["rank", "event_id"],
        },
    }


def build_lead_snapshot_export(
    *,
    snapshot_id: int,
    database_url: Optional[str] = None,
    lead_family: Optional[str] = None,
) -> Dict[str, Any]:
    """Collect normalized lead snapshot rows and payload metadata."""
    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)

    with SessionFactory() as db:
        snap = db.execute(select(LeadSnapshot).where(LeadSnapshot.id == int(snapshot_id))).scalar_one_or_none()
        if snap is None:
            raise ValueError(f"lead_snapshot {snapshot_id} not found")

        items = (
            db.execute(
                select(LeadSnapshotItem)
                .where(LeadSnapshotItem.snapshot_id == int(snapshot_id))
                .order_by(LeadSnapshotItem.rank.asc())
            )
            .scalars()
            .all()
        )

    event_ids = [int(i.event_id) for i in items]
    events_by_id, correlations_by_event, linked_source_context = _load_event_context(database_url, event_ids)

    rows_out: list[dict[str, Any]] = []
    csv_rows: list[dict[str, Any]] = []
    for it in items:
        event = events_by_id.get(int(it.event_id))
        details = it.score_details if isinstance(it.score_details, dict) else {}
        details = enrich_lead_score_details(
            clauses=None if event is None else event.clauses,
            base_details=details,
            correlations=correlations_by_event.get(int(it.event_id), []),
            event_context=build_event_context_payload(event),
        )
        context = linked_source_context.get(int(it.event_id), {})
        details = classify_lead_families(
            details=details,
            linked_source_summary=context.get("linked_source_summary"),
            linked_records_by_correlation=context.get("linked_records_by_correlation"),
        )
        if lead_family and not lead_matches_family(details, lead_family):
            continue

        review_row = serialize_ranked_lead_review_row(
            snapshot=snap,
            item=it,
            event=event,
            details=details,
        )
        export_row = {
            **review_row,
            **_legacy_review_export_fields(snapshot=snap, details=details, review_row=review_row),
        }
        rows_out.append(export_row)
        csv_rows.append(review_row_csv_safe(export_row))

    max_items = getattr(snap, "max_items", None)
    if max_items is None:
        max_items = getattr(snap, "limit", 0)

    snapshot_payload = {
        "id": int(snapshot_id),
        "created_at": snap.created_at.isoformat() if snap.created_at else None,
        "analysis_run_id": getattr(snap, "analysis_run_id", None),
        "source": getattr(snap, "source", None),
        "min_score": int(getattr(snap, "min_score", 0)),
        "max_items": int(max_items or 0),
        "scoring_version": getattr(snap, "scoring_version", None),
        "notes": getattr(snap, "notes", None),
    }
    family_groups = summarize_lead_family_groups(rows_out, lead_family_filter=lead_family)
    family_distribution = summarize_lead_family_distribution(rows_out, lead_family_filter=lead_family)
    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "review_contract_version": RANKED_LEAD_REVIEW_CONTRACT_VERSION,
        "canonical_review_fields": list(CANONICAL_RANKED_LEAD_REVIEW_FIELDS),
        "scoring_version": snapshot_payload.get("scoring_version"),
        "snapshot": snapshot_payload,
        "lead_family_filter": lead_family,
        "family_groups": family_groups,
        "family_distribution": family_distribution,
        "count": len(rows_out),
        "items": rows_out,
    }

    return {
        "snapshot": snapshot_payload,
        "scoring_version": snapshot_payload.get("scoring_version"),
        "lead_family_filter": lead_family,
        "family_groups": family_groups,
        "family_distribution": family_distribution,
        "count": len(rows_out),
        "items": rows_out,
        "csv_rows": csv_rows,
        "payload": payload,
    }


def export_lead_snapshot(
    *,
    snapshot_id: int,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
    lead_family: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Export a lead snapshot (lead_snapshots + lead_snapshot_items + event metadata)
    to CSV + JSON.
    """
    export_data = build_lead_snapshot_export(
        snapshot_id=int(snapshot_id),
        database_url=database_url,
        lead_family=lead_family,
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_name = f"lead_snapshot_{int(snapshot_id)}_{ts}"
    export_dir = EXPORTS_DIR

    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent if output.parent else Path(".")
            export_dir.mkdir(parents=True, exist_ok=True)
            base_name = output.stem or base_name
        else:
            export_dir = output
            export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / f"{base_name}.csv"
    json_path = export_dir / f"{base_name}.json"
    review_summary_path = export_dir / f"{base_name}_review_summary.json"

    _write_csv(csv_path, export_data["csv_rows"])
    json_path.write_text(json.dumps(export_data["payload"], ensure_ascii=False, indent=2), encoding="utf-8")
    review_summary_payload = _build_review_summary_payload(
        snapshot=export_data["snapshot"],
        rows=export_data["items"],
        csv_path=csv_path,
        json_path=json_path,
        review_summary_path=review_summary_path,
        lead_family_filter=lead_family,
        family_distribution=export_data["family_distribution"],
    )
    review_summary_path.write_text(
        json.dumps(review_summary_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "csv": csv_path,
        "json": json_path,
        "review_summary_json": review_summary_path,
        "count": int(export_data["count"]),
        "snapshot_id": int(snapshot_id),
    }


def export_scoring_comparison(
    *,
    versions: list[str],
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    min_score: int = 1,
    limit: int = 200,
    scan_limit: int = 5000,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> Dict[str, Any]:
    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)

    with SessionFactory() as db:
        comparison = compare_lead_scoring_versions(
            db,
            versions=versions,
            source=source,
            exclude_source=exclude_source,
            min_score=min_score,
            limit=limit,
            scan_limit=scan_limit,
        )

    baseline_version = str(comparison.get("baseline_version") or "baseline")
    target_version = str(comparison.get("target_version") or "target")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_name = f"lead_scoring_comparison_{baseline_version}_vs_{target_version}_{ts}"
    export_dir = EXPORTS_DIR

    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent if output.parent else Path(".")
            export_dir.mkdir(parents=True, exist_ok=True)
            base_name = output.stem or base_name
        else:
            export_dir = output
            export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / f"{base_name}.csv"
    json_path = export_dir / f"{base_name}.json"

    rows: list[dict[str, Any]] = []
    for item in comparison.get("items", []):
        rows.append(
            {
                "event_id": item.get("event_id"),
                "event_hash": item.get("event_hash"),
                "doc_id": item.get("doc_id"),
                "source": item.get("source"),
                "source_url": item.get("source_url"),
                "snippet": item.get("snippet"),
                "lead_family": item.get("lead_family"),
                "baseline_version": baseline_version,
                "target_version": target_version,
                f"{baseline_version}_rank": item.get(f"{baseline_version}_rank"),
                f"{baseline_version}_score": item.get(f"{baseline_version}_score"),
                f"{target_version}_rank": item.get(f"{target_version}_rank"),
                f"{target_version}_score": item.get(f"{target_version}_score"),
                "delta_rank": item.get("delta_rank"),
                "delta_score": item.get("delta_score"),
                "comparison_state": item.get("comparison_state"),
                "explanation_delta": item.get("explanation_delta"),
                "baseline_score_details_json": json.dumps(item.get("baseline_score_details") or {}, ensure_ascii=False),
                "target_score_details_json": json.dumps(item.get("target_score_details") or {}, ensure_ascii=False),
            }
        )

    _write_csv(csv_path, rows)
    json_path.write_text(
        json.dumps({"exported_at": datetime.now(timezone.utc).isoformat(), **comparison}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "csv": csv_path,
        "json": json_path,
        "count": len(rows),
        "baseline_version": baseline_version,
        "target_version": target_version,
        "versions": comparison.get("versions") or [baseline_version, target_version],
    }


def export_lead_deltas(
    *,
    from_snapshot_id: int,
    to_snapshot_id: int,
    database_url: Optional[str] = None,
    output: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Export lead deltas between two snapshots to CSV + JSON.
    """
    ensure_runtime_directories()
    deltas = compute_lead_deltas(
        from_snapshot_id=int(from_snapshot_id),
        to_snapshot_id=int(to_snapshot_id),
        database_url=database_url,
    )

    event_ids: list[int] = []
    for item in deltas.get("new", []):
        event_ids.append(int(item.get("event_id") or 0))
    for item in deltas.get("removed", []):
        event_ids.append(int(item.get("event_id") or 0))
    for item in deltas.get("changed", []):
        event_ids.append(int(item.get("event_id") or 0))
    event_ids = [event_id for event_id in sorted(set(event_ids)) if event_id > 0]
    events_by_id, correlations_by_event, linked_source_context = _load_event_context(database_url, event_ids)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_name = f"lead_deltas_{int(from_snapshot_id)}_{int(to_snapshot_id)}_{ts}"
    export_dir = EXPORTS_DIR

    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent if output.parent else Path(".")
            export_dir.mkdir(parents=True, exist_ok=True)
            base_name = output.stem or base_name
        else:
            export_dir = output
            export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / f"{base_name}.csv"
    json_path = export_dir / f"{base_name}.json"

    def _event_cols(ev: Optional[dict]) -> dict:
        if not ev:
            return {
                "source": None,
                "doc_id": None,
                "source_url": None,
                "occurred_at": None,
                "created_at": None,
                "snippet": None,
                "place_text": None,
            }
        return {
            "source": ev.get("source"),
            "doc_id": ev.get("doc_id"),
            "source_url": ev.get("source_url"),
            "occurred_at": ev.get("occurred_at"),
            "created_at": ev.get("created_at"),
            "snippet": ev.get("snippet"),
            "place_text": ev.get("place_text"),
        }

    def _enrich(event_id: Any, details: Any) -> dict[str, Any]:
        event = events_by_id.get(int(event_id or 0))
        base_details = details if isinstance(details, dict) else {}
        enriched = enrich_lead_score_details(
            clauses=None if event is None else event.clauses,
            base_details=base_details,
            correlations=correlations_by_event.get(int(event_id or 0), []),
            event_context=build_event_context_payload(event),
        )
        context = linked_source_context.get(int(event_id or 0), {})
        return classify_lead_families(
            details=enriched,
            linked_source_summary=context.get("linked_source_summary"),
            linked_records_by_correlation=context.get("linked_records_by_correlation"),
        )

    rows: list[dict[str, Any]] = []

    for item in deltas.get("new", []):
        event = item.get("event")
        to_details = _enrich(item.get("event_id"), item.get("score_details"))
        rows.append(
            {
                "change_type": "new",
                "event_hash": item.get("event_hash"),
                "event_id": item.get("event_id"),
                "from_rank": None,
                "from_score": None,
                "to_rank": item.get("rank"),
                "to_score": item.get("score"),
                "delta_rank": None,
                "delta_score": None,
                **_flatten_details("from", {}),
                **_flatten_details("to", to_details),
                **_event_cols(event),
            }
        )

    for item in deltas.get("removed", []):
        event = item.get("event")
        from_details = _enrich(item.get("event_id"), item.get("score_details"))
        rows.append(
            {
                "change_type": "removed",
                "event_hash": item.get("event_hash"),
                "event_id": item.get("event_id"),
                "from_rank": item.get("rank"),
                "from_score": item.get("score"),
                "to_rank": None,
                "to_score": None,
                "delta_rank": None,
                "delta_score": None,
                **_flatten_details("from", from_details),
                **_flatten_details("to", {}),
                **_event_cols(event),
            }
        )

    for item in deltas.get("changed", []):
        event = item.get("event")
        frm = item.get("from") or {}
        to = item.get("to") or {}
        delta = item.get("delta") or {}
        from_details = _enrich(item.get("event_id"), frm.get("score_details"))
        to_details = _enrich(item.get("event_id"), to.get("score_details"))
        rows.append(
            {
                "change_type": "changed",
                "event_hash": item.get("event_hash"),
                "event_id": item.get("event_id"),
                "from_rank": frm.get("rank"),
                "from_score": frm.get("score"),
                "to_rank": to.get("rank"),
                "to_score": to.get("score"),
                "delta_rank": delta.get("rank"),
                "delta_score": delta.get("score"),
                **_flatten_details("from", from_details),
                **_flatten_details("to", to_details),
                **_event_cols(event),
            }
        )

    _write_csv(csv_path, rows)

    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "from_snapshot_id": int(from_snapshot_id),
        "to_snapshot_id": int(to_snapshot_id),
        **deltas,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"csv": csv_path, "json": json_path, "count": len(rows), "from_snapshot_id": int(from_snapshot_id), "to_snapshot_id": int(to_snapshot_id)}


def _write_csv(path: Path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


__all__ = ["build_lead_snapshot_export", "export_lead_snapshot", "export_lead_deltas", "export_scoring_comparison"]
