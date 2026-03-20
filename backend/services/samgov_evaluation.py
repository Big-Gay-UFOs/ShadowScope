from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select

from backend.db.models import Event, get_session_factory
from backend.runtime import EXPORTS_DIR
from backend.services import workflow as workflow_module
from backend.services.bundle import (
    SAM_BUNDLE_MANIFEST_NAME,
    SAM_BUNDLE_RESULTS_DIR,
    flatten_bundle_files,
    render_sam_bundle_report_from_bundle,
    write_bundle_manifest,
)
from backend.services.export_leads import build_lead_snapshot_export
from backend.services.explainability import LANE_PRIORITY, load_event_linked_source_summary
from backend.services.ingest import format_sam_posted_window_cli_args
from backend.services.leads import compute_leads
from backend.services.sam_workflow_hardening import (
    _build_quality_gate_policy,
    _build_sam_run_status,
    _safe_float,
    _safe_int,
    _summarize_check_groups,
    run_samgov_smoke_workflow_hardened,
)


_ROUTINE_FAMILIES = {
    "commodity_supply_chain",
    "facility_maintenance_upgrade",
    "site_security_access_control",
    "industrial_equipment_support",
    "aviation_spares_lineage",
}
_SUPPORT_RECORD_LIMIT = 8
_DOSSIER_COUNT = 10
_COMPARISON_LIMIT = 25


def _parse_datetime(value: Any) -> Optional[datetime]:
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


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def _json_field(value: Any, *, default: Any) -> Any:
    if isinstance(value, default.__class__):
        return value
    if not isinstance(value, str) or not value.strip():
        return default
    try:
        parsed = json.loads(value)
    except Exception:
        return default
    return parsed if isinstance(parsed, default.__class__) else default


def _lane_rank(value: Any) -> int:
    lane = str(value or "").strip()
    try:
        return LANE_PRIORITY.index(lane)
    except ValueError:
        return len(LANE_PRIORITY)


def _event_payload(event: Optional[Event], row: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    row = row or {}
    if event is None:
        return {
            "event_id": _safe_int(row.get("event_id"), default=0) or None,
            "source": row.get("source"),
            "doc_id": row.get("doc_id"),
            "source_url": row.get("source_url"),
            "occurred_at": row.get("occurred_at"),
            "created_at": row.get("created_at"),
            "snippet": row.get("snippet"),
            "recipient_name": None,
            "recipient_uei": None,
            "awarding_agency_name": None,
            "naics_code": None,
            "psc_code": None,
            "place_text": row.get("place_text"),
        }
    return {
        "event_id": int(event.id),
        "source": event.source,
        "doc_id": event.doc_id,
        "source_url": event.source_url,
        "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
        "created_at": event.created_at.isoformat() if event.created_at else None,
        "snippet": event.snippet,
        "recipient_name": event.recipient_name,
        "recipient_uei": event.recipient_uei,
        "awarding_agency_name": event.awarding_agency_name,
        "naics_code": event.naics_code,
        "psc_code": event.psc_code,
        "place_text": event.place_text,
    }


def _row_details(row: dict[str, Any]) -> dict[str, Any]:
    return _json_field(row.get("score_details_json"), default={})


def _row_proxy_or_pairbacked(row: dict[str, Any], details: Optional[dict[str, Any]] = None) -> bool:
    payload = details or _row_details(row)
    return bool(
        _safe_int(row.get("proxy_relevance_score"), default=_safe_int(payload.get("proxy_relevance_score"))) > 0
        or bool(row.get("pair_or_candidate_corroboration"))
        or bool(payload.get("pair_or_candidate_corroboration"))
        or _safe_int(payload.get("candidate_corroboration_count"), default=0) > 0
    )


def _row_is_routine(row: dict[str, Any], details: Optional[dict[str, Any]] = None) -> bool:
    payload = details or _row_details(row)
    family = str(row.get("lead_family") or payload.get("lead_family") or "").strip()
    routine_tags = [
        str(tag).strip()
        for tag in list(row.get("routine_noise_tags") or []) + list(payload.get("routine_noise_tags") or [])
        if str(tag or "").strip()
    ]
    ranking_tier = str(row.get("ranking_tier") or payload.get("ranking_tier") or "").strip().lower()
    return bool(family in _ROUTINE_FAMILIES or routine_tags or ranking_tier == "context_only")


def _foia_rationale(row: dict[str, Any], details: dict[str, Any]) -> str:
    proxy = _safe_int(row.get("proxy_relevance_score"), default=_safe_int(details.get("proxy_relevance_score")))
    corroboration = _safe_int(row.get("corroboration_score"), default=_safe_int(details.get("corroboration_score")))
    structural = _safe_int(row.get("structural_context_score"), default=_safe_int(details.get("structural_context_score")))
    pair_backed = _row_proxy_or_pairbacked(row, details=details)
    if proxy > 0 and pair_backed:
        return "Proxy-relevant text is reinforced by pair or candidate corroboration and traceable procurement handles."
    if proxy >= 6:
        return "Direct proxy language and reviewable identifiers make this a viable FOIA follow-up candidate."
    if proxy > 0:
        return "Some proxy-relevant language is present, so this merits reviewer triage before follow-up."
    if pair_backed and corroboration > 0:
        return "Cross-record corroboration elevates this lead even though direct proxy text is limited."
    if structural > 0:
        return "Structural procurement context is present, but it should be treated as supporting context rather than primary FOIA fuel."
    return "Current evidence is thin and is better treated as background triage than a strong FOIA target."


def _noise_rationale(row: dict[str, Any], details: dict[str, Any]) -> str:
    family = str(row.get("lead_family") or details.get("lead_family") or "").strip()
    routine_tags = [
        str(tag).replace("_", " ")
        for tag in list(row.get("routine_noise_tags") or []) + list(details.get("routine_noise_tags") or [])
        if str(tag or "").strip()
    ]
    ranking_tier = str(row.get("ranking_tier") or details.get("ranking_tier") or "").strip().lower()
    structural = _safe_int(row.get("structural_context_score"), default=_safe_int(details.get("structural_context_score")))
    proxy = _safe_int(row.get("proxy_relevance_score"), default=_safe_int(details.get("proxy_relevance_score")))
    noise_penalty = _safe_int(row.get("noise_penalty_applied"), default=_safe_int(details.get("noise_penalty_applied")))
    if family in _ROUTINE_FAMILIES:
        return f"Lead-family classification points to routine {family.replace('_', ' ')} activity rather than a proxy-heavy target."
    if routine_tags:
        return "Routine-noise cues dominate this row: " + ", ".join(routine_tags[:3]) + "."
    if ranking_tier == "context_only" or (proxy <= 0 and structural > 0):
        return "This row is driven mostly by starter structural context, so it should not outrank proxy-backed leads."
    if noise_penalty > 0:
        return "Noise suppressors materially reduce confidence, so treat this as possible background procurement activity."
    return "No dominant routine-noise pattern surfaced in the current evidence."


def _markdown_cell(value: Any) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return ""
    return text.replace("|", "\\|")


def _evaluation_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    top10 = rows[:10]
    top50 = rows[:50]
    proxy_or_pairbacked = 0
    non_routine = 0
    context_only = 0
    pair_backed = 0
    for row in top10:
        details = _row_details(row)
        if _row_proxy_or_pairbacked(row, details=details):
            proxy_or_pairbacked += 1
        if not _row_is_routine(row, details=details):
            non_routine += 1
        if str(row.get("ranking_tier") or details.get("ranking_tier") or "").strip().lower() == "context_only":
            context_only += 1
        if bool(row.get("pair_or_candidate_corroboration")) or bool(details.get("pair_or_candidate_corroboration")):
            pair_backed += 1

    family_counts: dict[str, int] = {}
    for row in top50:
        family = str(row.get("lead_family") or "unassigned")
        family_counts[family] = family_counts.get(family, 0) + 1

    dominant_family = None
    dominant_share = 0.0
    if family_counts and top50:
        dominant_family = sorted(family_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
        dominant_share = round(float(family_counts[dominant_family]) / float(len(top50)), 4)

    return {
        "row_count": len(rows),
        "top10_count": len(top10),
        "top50_count": len(top50),
        "top10_proxy_or_pairbacked_count": proxy_or_pairbacked,
        "top10_non_routine_count": non_routine,
        "top10_context_only_count": context_only,
        "top10_pair_backed_count": pair_backed,
        "dominant_family_top50": dominant_family,
        "dominant_family_share_top50": dominant_share,
    }


def _comparison_rows(
    *,
    v3_rows: list[dict[str, Any]],
    v2_ranked: list[tuple[int, Event, dict[str, Any]]],
) -> dict[str, Any]:
    v3_map = {
        _safe_int(row.get("event_id"), default=0): {
            "rank": _safe_int(row.get("rank"), default=0),
            "score": _safe_int(row.get("score"), default=0),
            "lead_family": row.get("lead_family"),
            "doc_id": row.get("doc_id"),
            "source_url": row.get("source_url"),
            "occurred_at": row.get("occurred_at"),
        }
        for row in v3_rows[:_COMPARISON_LIMIT]
        if _safe_int(row.get("event_id"), default=0) > 0
    }
    v2_map = {
        int(event.id): {
            "rank": index,
            "score": int(score),
            "lead_family": details.get("lead_family"),
            "doc_id": event.doc_id,
            "source_url": event.source_url,
            "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
        }
        for index, (score, event, details) in enumerate(v2_ranked[:_COMPARISON_LIMIT], start=1)
    }

    event_ids = set(v3_map) | set(v2_map)
    ordered_ids = sorted(
        event_ids,
        key=lambda event_id: (
            min(
                _safe_int((v3_map.get(event_id) or {}).get("rank"), default=10**6),
                _safe_int((v2_map.get(event_id) or {}).get("rank"), default=10**6),
            ),
            event_id,
        ),
    )
    comparison_rows: list[dict[str, Any]] = []
    for event_id in ordered_ids[:_COMPARISON_LIMIT]:
        v2_item = v2_map.get(event_id) or {}
        v3_item = v3_map.get(event_id) or {}
        v2_rank = _safe_int(v2_item.get("rank"), default=0) or None
        v3_rank = _safe_int(v3_item.get("rank"), default=0) or None
        rank_delta = None
        if v2_rank is not None and v3_rank is not None:
            rank_delta = int(v2_rank) - int(v3_rank)
        comparison_rows.append(
            {
                "event_id": event_id,
                "doc_id": v3_item.get("doc_id") or v2_item.get("doc_id"),
                "occurred_at": v3_item.get("occurred_at") or v2_item.get("occurred_at"),
                "source_url": v3_item.get("source_url") or v2_item.get("source_url"),
                "lead_family_v2": v2_item.get("lead_family"),
                "lead_family_v3": v3_item.get("lead_family"),
                "rank_v2": v2_rank,
                "rank_v3": v3_rank,
                "score_v2": v2_item.get("score"),
                "score_v3": v3_item.get("score"),
                "rank_delta_v3_minus_v2": rank_delta,
            }
        )

    top10_v2 = {event.id for _, event, _ in v2_ranked[:10]}
    top10_v3 = {
        _safe_int(row.get("event_id"), default=0)
        for row in v3_rows[:10]
        if _safe_int(row.get("event_id"), default=0) > 0
    }
    promoted = [row for row in comparison_rows if row.get("event_id") in top10_v3 - top10_v2][:10]
    demoted = [row for row in comparison_rows if row.get("event_id") in top10_v2 - top10_v3][:10]
    return {
        "summary": {
            "top10_overlap_count": len(top10_v2 & top10_v3),
            "top10_promoted_in_v3_count": len(top10_v3 - top10_v2),
            "top10_demoted_from_v3_count": len(top10_v2 - top10_v3),
            "v2_row_count": len(v2_ranked),
            "v3_row_count": len(v3_rows),
        },
        "top10_promoted_in_v3": promoted,
        "top10_demoted_from_v3": demoted,
        "rows": comparison_rows,
    }


def _support_records_for_dossier(
    *,
    focal_event_id: int,
    linked_context: dict[str, Any],
    events_by_id: dict[int, Event],
) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    seen: set[int] = set()
    for correlation_id, records in (linked_context.get("linked_records_by_correlation") or {}).items():
        for record in records or []:
            event_id = _safe_int(record.get("event_id"), default=0)
            if event_id <= 0 or event_id == focal_event_id or event_id in seen:
                continue
            seen.add(event_id)
            event = events_by_id.get(event_id)
            flattened.append(
                {
                    "event_id": event_id,
                    "correlation_id": _safe_int(correlation_id, default=0) or None,
                    "lane": record.get("lane"),
                    "source": record.get("source"),
                    "doc_id": record.get("doc_id"),
                    "award_id": record.get("award_id"),
                    "solicitation_number": record.get("solicitation_number"),
                    "source_url": record.get("source_url"),
                    "recipient_name": record.get("recipient_name"),
                    "recipient_uei": record.get("recipient_uei"),
                    "agency": record.get("agency"),
                    "place_region": record.get("place_region"),
                    "occurred_at": event.occurred_at.isoformat() if event and event.occurred_at else None,
                    "created_at": event.created_at.isoformat() if event and event.created_at else None,
                    "snippet": event.snippet if event else None,
                }
            )
    flattened.sort(
        key=lambda item: (
            _lane_rank(item.get("lane")),
            str(item.get("source") or ""),
            str(item.get("doc_id") or ""),
            _safe_int(item.get("event_id"), default=0),
        )
    )
    return flattened[:_SUPPORT_RECORD_LIMIT]


def _build_dossiers(
    *,
    bundle_dir: Path,
    rows: list[dict[str, Any]],
    database_url: Optional[str],
) -> dict[str, Any]:
    dossiers_dir = bundle_dir / "exports" / "dossiers"
    dossiers_dir.mkdir(parents=True, exist_ok=True)
    top_rows = rows[:_DOSSIER_COUNT]
    top_event_ids = [
        _safe_int(row.get("event_id"), default=0)
        for row in top_rows
        if _safe_int(row.get("event_id"), default=0) > 0
    ]

    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        linked_context = load_event_linked_source_summary(db, event_ids=top_event_ids) if top_event_ids else {}
        linked_event_ids: set[int] = set(top_event_ids)
        for payload in linked_context.values():
            for records in (payload.get("linked_records_by_correlation") or {}).values():
                for record in records or []:
                    event_id = _safe_int(record.get("event_id"), default=0)
                    if event_id > 0:
                        linked_event_ids.add(event_id)
        events = (
            db.execute(select(Event).where(Event.id.in_(sorted(linked_event_ids)))).scalars().all()
            if linked_event_ids
            else []
        )
    events_by_id = {int(event.id): event for event in events}

    index_rows: list[dict[str, Any]] = []
    for row in top_rows:
        event_id = _safe_int(row.get("event_id"), default=0)
        details = _row_details(row)
        focal_event = events_by_id.get(event_id)
        context = linked_context.get(event_id, {})
        dossier = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_id": _safe_int(row.get("snapshot_id"), default=0) or None,
            "snapshot_item_id": _safe_int(row.get("snapshot_item_id"), default=0) or None,
            "rank": _safe_int(row.get("rank"), default=0) or None,
            "score": _safe_int(row.get("score"), default=0),
            "lead_family": row.get("lead_family"),
            "lead_family_label": row.get("lead_family_label"),
            "ranking_tier": row.get("ranking_tier") or details.get("ranking_tier"),
            "foia_rationale": _foia_rationale(row, details),
            "noise_rationale": _noise_rationale(row, details),
            "lead": _event_payload(focal_event, row=row),
            "matched_ontology_rules": _json_field(row.get("matched_ontology_rules_json"), default=[]),
            "contributing_correlations": _json_field(row.get("contributing_correlations_json"), default=[])[:5],
            "candidate_join_evidence": _json_field(row.get("candidate_join_evidence_json"), default=[])[:5],
            "linked_source_summary": _json_field(row.get("linked_source_summary_json"), default=[])[:5],
            "supporting_records": [
                {
                    "role": "focal_lead",
                    **_event_payload(focal_event, row=row),
                }
            ]
            + _support_records_for_dossier(
                focal_event_id=event_id,
                linked_context=context,
                events_by_id=events_by_id,
            ),
        }
        dossier_path = dossiers_dir / f"{_safe_int(row.get('rank'), default=0):03d}_{event_id}.json"
        _write_json(dossier_path, dossier)
        index_rows.append(
            {
                "rank": dossier.get("rank"),
                "event_id": event_id,
                "doc_id": row.get("doc_id"),
                "lead_family": row.get("lead_family"),
                "score": dossier.get("score"),
                "foia_rationale": dossier.get("foia_rationale"),
                "noise_rationale": dossier.get("noise_rationale"),
                "file": str(dossier_path.relative_to(bundle_dir)),
            }
        )

    index_path = dossiers_dir / "index.json"
    _write_json(
        index_path,
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(index_rows),
            "items": index_rows,
        },
    )
    return {
        "dossiers_dir": dossiers_dir,
        "dossiers_index_json": index_path,
        "dossier_count": len(index_rows),
    }


def _build_review_board(*, rows: list[dict[str, Any]], effective_window: dict[str, Any]) -> str:
    lines = [
        "# FOIA Lead Review Board",
        "",
        f"- Window: {effective_window.get('posted_from')} -> {effective_window.get('posted_to')}",
        f"- Ranked rows: {len(rows)}",
        "",
        "| Rank | Score | Family | Event | Date | Proxy | Corroboration | FOIA Rationale | Noise Rationale |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows[:25]:
        details = _row_details(row)
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_cell(row.get("rank")),
                    _markdown_cell(row.get("score")),
                    _markdown_cell(row.get("lead_family_label") or row.get("lead_family")),
                    _markdown_cell(row.get("doc_id") or row.get("event_id")),
                    _markdown_cell(row.get("occurred_at") or row.get("created_at")),
                    _markdown_cell(row.get("proxy_relevance_score")),
                    _markdown_cell(row.get("corroboration_score")),
                    _markdown_cell(_foia_rationale(row, details)),
                    _markdown_cell(_noise_rationale(row, details)),
                ]
            )
            + " |"
        )
    if not rows:
        lines.append("|  |  |  | No ranked leads |  |  |  | Sparse or empty window; no top board rows were generated. |  |")
    lines.append("")
    return "\n".join(lines)


def _build_evaluation_report(
    *,
    evaluation_summary: dict[str, Any],
    comparison_payload: dict[str, Any],
) -> str:
    metrics = evaluation_summary.get("signal_metrics") or {}
    artifact_summary = evaluation_summary.get("artifact_completeness") or {}
    family_summary = evaluation_summary.get("family_distribution_summary") or {}
    lines = [
        "# SAM.gov Evaluation Report",
        "",
        f"- Status: {evaluation_summary.get('status')}",
        f"- Scoring version: {evaluation_summary.get('scoring_version')}",
        f"- Window: {evaluation_summary.get('effective_window', {}).get('posted_from')} -> {evaluation_summary.get('effective_window', {}).get('posted_to')}",
        f"- Top-10 proxy or pair-backed count: {metrics.get('top10_proxy_or_pairbacked_count')}",
        f"- Top-10 non-routine count: {metrics.get('top10_non_routine_count')}",
        f"- Top-10 context-only count: {metrics.get('top10_context_only_count')}",
        f"- Dominant family share of top-50: {family_summary.get('dominant_family_share_top50')}",
        f"- Outside-window count: {evaluation_summary.get('outside_window_count')}",
        f"- Artifact completeness: {'complete' if artifact_summary.get('complete') else 'incomplete'}",
        "",
        "## Comparison",
        "",
        f"- Top-10 overlap between v2 and v3: {(comparison_payload.get('summary') or {}).get('top10_overlap_count')}",
        f"- Leads promoted into v3 top-10: {(comparison_payload.get('summary') or {}).get('top10_promoted_in_v3_count')}",
        f"- Leads demoted from the old v2 top-10: {(comparison_payload.get('summary') or {}).get('top10_demoted_from_v3_count')}",
        "",
        "## Notes",
        "",
        "- Proxy-relevance and corroboration are treated as the primary ranking fuel; structural starter context is support-only.",
        "- Candidate joins remain reviewer hypotheses and are not asserted as confirmed identities.",
        "- Sparse windows are reported honestly; empty top ranks are preferable to leaking out-of-window leads.",
        "",
    ]
    return "\n".join(lines)


def _artifact_completeness(
    *,
    artifact_paths: dict[str, Any],
    dossier_count_expected: int,
) -> dict[str, Any]:
    required = {
        "evaluation_summary_json": artifact_paths.get("evaluation_summary_json"),
        "scoring_comparison_json": artifact_paths.get("scoring_comparison_json"),
        "review_board_md": artifact_paths.get("review_board_md"),
        "evaluation_report_md": artifact_paths.get("evaluation_report_md"),
        "dossiers_dir": artifact_paths.get("dossiers_dir"),
        "dossiers_index_json": artifact_paths.get("dossiers_index_json"),
    }
    present: list[str] = []
    missing: list[str] = []
    for key, value in required.items():
        path = Path(value) if value is not None else None
        exists = path.exists() if path is not None else False
        if key == "dossiers_dir" and path is not None:
            exists = path.exists() and path.is_dir()
        if exists:
            present.append(key)
        else:
            missing.append(key)

    actual_dossier_count = 0
    dossiers_dir = artifact_paths.get("dossiers_dir")
    if dossiers_dir is not None:
        actual_dossier_count = len(
            [path for path in Path(dossiers_dir).glob("*.json") if path.name.lower() != "index.json"]
        )
    dossiers_ok = actual_dossier_count == int(dossier_count_expected)
    if not dossiers_ok and "dossiers_dir" not in missing:
        missing.append("dossier_payload_count")
    return {
        "required_artifacts": {key: str(value) if value is not None else None for key, value in required.items()},
        "present": sorted(set(present)),
        "missing": sorted(set(missing)),
        "dossier_count_expected": int(dossier_count_expected),
        "dossier_count_actual": int(actual_dossier_count),
        "complete": not missing and dossiers_ok,
    }


def _evaluation_checks(
    *,
    signal_metrics: dict[str, Any],
    family_distribution_summary: dict[str, Any],
    artifact_completeness: dict[str, Any],
    effective_window: dict[str, Any],
) -> list[dict[str, Any]]:
    ingest_window_args = " ".join(format_sam_posted_window_cli_args(effective_window))
    if not ingest_window_args:
        ingest_window_args = "--days 30"
    rerun_cmd = f"ss workflow samgov-evaluate {ingest_window_args} --json"
    top10_count = _safe_int(signal_metrics.get("top10_count"), default=0)
    top50_count = _safe_int(signal_metrics.get("top50_count"), default=0)
    proxy_target = min(5, top10_count)
    non_routine_target = (top10_count // 2) + (1 if top10_count % 2 else 0)
    dominant_family = str(family_distribution_summary.get("dominant_family") or "").strip()
    dominant_share = _safe_float(family_distribution_summary.get("dominant_family_share_top50"), default=0.0)
    family_required = bool(top50_count >= 10 and dominant_family == "vendor_network_contract_lineage")
    family_ok = top50_count < 10 or dominant_share <= 0.5
    return [
        workflow_module._serialize_check(
            name="evaluation_artifact_completeness",
            ok=bool(artifact_completeness.get("complete")),
            observed={
                "present": artifact_completeness.get("present"),
                "missing": artifact_completeness.get("missing"),
                "dossier_count_expected": artifact_completeness.get("dossier_count_expected"),
                "dossier_count_actual": artifact_completeness.get("dossier_count_actual"),
            },
            actual=artifact_completeness,
            threshold="all evaluation artifacts present",
            expected="all evaluation artifacts present",
            why="The evaluation bundle is only reviewable when the summary, comparison, board, report, and dossier artifacts are all present.",
            hint=rerun_cmd,
            kind="validation",
            validation_mode="larger",
            required=True,
            severity="critical",
            category="pipeline_health",
        ),
        workflow_module._serialize_check(
            name="evaluation_top10_non_routine_majority",
            ok=_safe_int(signal_metrics.get("top10_non_routine_count"), default=0) >= non_routine_target,
            observed=_safe_int(signal_metrics.get("top10_non_routine_count"), default=0),
            actual=signal_metrics,
            threshold=f">= {non_routine_target}",
            expected=f">= {non_routine_target}",
            why="Top-ranked evaluation rows should be mostly non-routine so reviewers are not led by commodity or sustainment noise.",
            hint=rerun_cmd,
            kind="threshold",
            validation_mode="larger",
            required=False,
            severity="warning",
            category="lead_signal_quality",
        ),
        workflow_module._serialize_check(
            name="evaluation_top10_proxy_or_pairbacked_threshold",
            ok=_safe_int(signal_metrics.get("top10_proxy_or_pairbacked_count"), default=0) >= proxy_target,
            observed=_safe_int(signal_metrics.get("top10_proxy_or_pairbacked_count"), default=0),
            actual=signal_metrics,
            threshold=f">= {proxy_target}",
            expected=f">= {proxy_target}",
            why="Recent top leads should show direct proxy relevance or pair-backed corroboration before they occupy the review tier.",
            hint=rerun_cmd,
            kind="threshold",
            validation_mode="larger",
            required=False,
            severity="warning",
            category="lead_signal_quality",
        ),
        workflow_module._serialize_check(
            name="evaluation_family_collapse_threshold",
            ok=family_ok,
            observed={
                "dominant_family": dominant_family or None,
                "dominant_family_share_top50": dominant_share,
                "top50_count": top50_count,
            },
            actual=family_distribution_summary,
            threshold="dominant_family_share_top50 <= 0.5 when top50_count >= 10",
            expected="dominant_family_share_top50 <= 0.5 when top50_count >= 10",
            why="Family collapse hides ranking quality problems, especially when the fallback vendor-network family dominates the review set.",
            hint=rerun_cmd,
            kind="validation",
            validation_mode="larger",
            required=family_required,
            severity="error" if family_required else "warning",
            category="lead_signal_quality",
        ),
    ]


def _build_evaluation_artifacts(
    *,
    bundle_dir: Path,
    workflow_res: dict[str, Any],
    run_metadata: dict[str, Any],
    family_distribution_summary: dict[str, Any],
    outside_window_count: int,
    database_url: Optional[str],
) -> dict[str, Any]:
    snapshot_payload = workflow_res.get("snapshot") if isinstance(workflow_res.get("snapshot"), dict) else {}
    snapshot_id = _safe_int(snapshot_payload.get("snapshot_id"), default=0)
    snapshot_export = (
        build_lead_snapshot_export(snapshot_id=snapshot_id, database_url=database_url)
        if snapshot_id > 0
        else {"snapshot": {}, "items": [], "count": 0}
    )
    rows = list(snapshot_export.get("items") or [])
    signal_metrics = _evaluation_metrics(rows)
    effective_window = (
        run_metadata.get("effective_window") if isinstance(run_metadata.get("effective_window"), dict) else {}
    )
    snapshot_window = (
        run_metadata.get("snapshot_window") if isinstance(run_metadata.get("snapshot_window"), dict) else {}
    )

    filter_kwargs = {
        "source": "SAM.gov",
        "min_score": _safe_int(snapshot_payload.get("min_score"), default=1),
        "limit": _safe_int(
            snapshot_payload.get("max_items"),
            default=_safe_int(snapshot_payload.get("limit"), default=len(rows) or 200),
        ),
        "scan_limit": _safe_int(run_metadata.get("scan_limit"), default=5000),
        "scoring_version": "v2",
        "date_from": _parse_datetime(snapshot_window.get("date_from")),
        "date_to": _parse_datetime(snapshot_window.get("date_to")),
        "occurred_after": _parse_datetime(snapshot_window.get("occurred_after")),
        "occurred_before": _parse_datetime(snapshot_window.get("occurred_before")),
        "created_after": _parse_datetime(snapshot_window.get("created_after")),
        "created_before": _parse_datetime(snapshot_window.get("created_before")),
        "since_days": snapshot_window.get("since_days"),
    }
    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        v2_ranked, _ = compute_leads(db, **filter_kwargs)

    comparison_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "effective_window": effective_window,
        "snapshot_window": snapshot_window,
        **_comparison_rows(v3_rows=rows, v2_ranked=v2_ranked),
    }

    dossiers = _build_dossiers(bundle_dir=bundle_dir, rows=rows, database_url=database_url)
    review_board_path = _write_text(
        bundle_dir / "report" / "FOIA_LEAD_REVIEW_BOARD.md",
        _build_review_board(rows=rows, effective_window=effective_window),
    )

    artifact_paths = {
        "evaluation_summary_json": bundle_dir / SAM_BUNDLE_RESULTS_DIR / "evaluation_summary.json",
        "scoring_comparison_json": bundle_dir / SAM_BUNDLE_RESULTS_DIR / "scoring_comparison_v2_v3.json",
        "review_board_md": review_board_path,
        "evaluation_report_md": bundle_dir / "report" / "evaluation_report.md",
        "dossiers_dir": dossiers.get("dossiers_dir"),
        "dossiers_index_json": dossiers.get("dossiers_index_json"),
    }
    artifact_completeness = _artifact_completeness(
        artifact_paths=artifact_paths,
        dossier_count_expected=_safe_int(dossiers.get("dossier_count"), default=0),
    )
    evaluation_summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workflow_type": "samgov-evaluate",
        "scoring_version": str(snapshot_payload.get("scoring_version") or "v3"),
        "snapshot_id": snapshot_id or None,
        "snapshot_count": _safe_int(snapshot_export.get("count"), default=0),
        "requested_window": run_metadata.get("requested_window"),
        "effective_window": effective_window,
        "snapshot_window": snapshot_window,
        "snapshot_event_min": run_metadata.get("snapshot_event_min"),
        "snapshot_event_max": run_metadata.get("snapshot_event_max"),
        "outside_window_count": int(outside_window_count),
        "signal_metrics": signal_metrics,
        "family_distribution_summary": family_distribution_summary,
        "artifact_completeness": artifact_completeness,
        "status": "ok" if artifact_completeness.get("complete") else "failed",
    }

    _write_json(artifact_paths["scoring_comparison_json"], comparison_payload)
    _write_text(
        artifact_paths["evaluation_report_md"],
        _build_evaluation_report(
            evaluation_summary=evaluation_summary,
            comparison_payload=comparison_payload,
        ),
    )
    _write_json(artifact_paths["evaluation_summary_json"], evaluation_summary)

    artifact_completeness = _artifact_completeness(
        artifact_paths=artifact_paths,
        dossier_count_expected=_safe_int(dossiers.get("dossier_count"), default=0),
    )
    evaluation_summary["artifact_completeness"] = artifact_completeness
    evaluation_summary["status"] = "ok" if artifact_completeness.get("complete") else "failed"
    _write_json(artifact_paths["evaluation_summary_json"], evaluation_summary)

    return {
        "evaluation_summary": evaluation_summary,
        "comparison_payload": comparison_payload,
        **artifact_paths,
        **dossiers,
    }


def run_samgov_evaluation_workflow(
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
    scoring_version: str = "v3",
    notes: Optional[str] = "samgov FOIA lead evaluation",
    bundle_root: Optional[Path] = None,
    database_url: Optional[str] = None,
    require_nonzero: bool = True,
    skip_ingest: bool = False,
    threshold_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    base_result = run_samgov_smoke_workflow_hardened(
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
        bundle_root=(Path(bundle_root).expanduser() if bundle_root else EXPORTS_DIR / "evaluation" / "samgov"),
        database_url=database_url,
        require_nonzero=bool(require_nonzero),
        skip_ingest=bool(skip_ingest),
        threshold_overrides=threshold_overrides,
        validation_mode="larger",
        workflow_type="samgov-evaluate",
    )

    bundle_dir = Path(base_result.get("bundle_dir")).expanduser()
    summary_path = Path(
        (base_result.get("artifacts") or {}).get("smoke_summary_json")
        or (bundle_dir / "results" / "workflow_summary.json")
    )
    manifest_path = Path(
        (base_result.get("artifacts") or {}).get("bundle_manifest_json")
        or (bundle_dir / SAM_BUNDLE_MANIFEST_NAME)
    )
    summary_payload = _load_json(summary_path)
    manifest_payload = _load_json(manifest_path)

    workflow_res = base_result.get("workflow") if isinstance(base_result.get("workflow"), dict) else {}
    run_metadata = base_result.get("run_metadata") if isinstance(base_result.get("run_metadata"), dict) else {}
    family_distribution_summary = (
        summary_payload.get("family_distribution_summary")
        if isinstance(summary_payload.get("family_distribution_summary"), dict)
        else (
            manifest_payload.get("family_distribution_summary")
            if isinstance(manifest_payload.get("family_distribution_summary"), dict)
            else {}
        )
    )
    outside_window_count = _safe_int(
        summary_payload.get("outside_window_count"),
        default=_safe_int((manifest_payload.get("summary_counts") or {}).get("outside_window_count"), default=0),
    )

    evaluation_artifacts = _build_evaluation_artifacts(
        bundle_dir=bundle_dir,
        workflow_res=workflow_res,
        run_metadata=run_metadata,
        family_distribution_summary=family_distribution_summary,
        outside_window_count=outside_window_count,
        database_url=database_url,
    )

    artifacts = dict(base_result.get("artifacts") or {})
    artifacts.update(
        {
            "evaluation_summary_json": evaluation_artifacts.get("evaluation_summary_json"),
            "scoring_comparison_json": evaluation_artifacts.get("scoring_comparison_json"),
            "review_board_md": evaluation_artifacts.get("review_board_md"),
            "evaluation_report_md": evaluation_artifacts.get("evaluation_report_md"),
            "dossiers_dir": evaluation_artifacts.get("dossiers_dir"),
            "dossiers_index_json": evaluation_artifacts.get("dossiers_index_json"),
        }
    )

    evaluation_summary = evaluation_artifacts.get("evaluation_summary") or {}
    evaluation_summary["snapshot_event_min"] = summary_payload.get("snapshot_event_min")
    evaluation_summary["snapshot_event_max"] = summary_payload.get("snapshot_event_max")
    workflow_module._write_json(
        Path(evaluation_artifacts.get("evaluation_summary_json")),
        evaluation_summary,
    )
    evaluation_checks = _evaluation_checks(
        signal_metrics=evaluation_summary.get("signal_metrics") or {},
        family_distribution_summary=family_distribution_summary,
        artifact_completeness=evaluation_summary.get("artifact_completeness") or {},
        effective_window=run_metadata.get("effective_window") if isinstance(run_metadata.get("effective_window"), dict) else {},
    )

    checks = list(summary_payload.get("checks") or [])
    checks.extend(evaluation_checks)
    failed_required_checks = [item for item in checks if bool(item.get("required")) and not bool(item.get("passed"))]
    warning_checks = [item for item in checks if not bool(item.get("required")) and not bool(item.get("passed"))]
    check_groups = _summarize_check_groups(checks)
    policy_overrides = list((summary_payload.get("quality_gate_policy") or {}).get("policy_overrides") or [])
    quality_gate_policy = _build_quality_gate_policy(
        validation_mode="larger",
        checks=checks,
        policy_overrides=policy_overrides,
    )

    ingest_payload = workflow_res.get("ingest") if isinstance(workflow_res.get("ingest"), dict) else {}
    counts = (summary_payload.get("baseline") or {}).get("counts") or {}
    keyword_coverage = (summary_payload.get("baseline") or {}).get("keyword_coverage") or {}
    status_summary = _build_sam_run_status(
        failed_required_checks=failed_required_checks,
        warning_checks=warning_checks,
        comparison_summary=(
            summary_payload.get("comparison") if isinstance(summary_payload.get("comparison"), dict) else {}
        ),
        events_window=_safe_int(counts.get("events_window"), default=0),
        events_with_keywords=_safe_int(keyword_coverage.get("events_with_keywords"), default=0),
        events_with_entity=_safe_int(counts.get("events_with_entity_window"), default=0),
        snapshot_items=_safe_int((workflow_res.get("snapshot") or {}).get("items"), default=0),
        ingest_nonzero=bool(
            _safe_int(ingest_payload.get("fetched"), default=0) > 0
            or _safe_int(ingest_payload.get("inserted"), default=0) > 0
            or _safe_int(ingest_payload.get("normalized"), default=0) > 0
            or skip_ingest
        ),
        ingest_request_diag=(ingest_payload.get("request_diagnostics") if isinstance(ingest_payload, dict) else {}) or {},
    )
    workflow_status = str(status_summary.get("workflow_status") or "warning")

    summary_payload.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "workflow_type": "samgov-evaluate",
            "workflow_status": workflow_status,
            "status": workflow_status,
            "smoke_passed": len(failed_required_checks) == 0,
            "required_checks_passed": len(failed_required_checks) == 0,
            "partially_useful": bool(status_summary.get("partially_useful")),
            "quality": status_summary.get("quality"),
            "has_required_failures": bool(status_summary.get("has_required_failures")),
            "has_advisory_failures": bool(status_summary.get("has_advisory_failures")),
            "has_usable_artifacts": bool(status_summary.get("has_usable_artifacts")),
            "reason_codes": list(status_summary.get("reason_codes") or []),
            "operator_messages": list(status_summary.get("operator_messages") or []),
            "required_failure_categories": list(status_summary.get("required_failure_categories") or []),
            "advisory_failure_categories": list(status_summary.get("advisory_failure_categories") or []),
            "failure_categories": list(status_summary.get("failure_categories") or []),
            "comparison_requested": bool(status_summary.get("comparison_requested")),
            "comparison_available": bool(status_summary.get("comparison_available")),
            "comparison_empty": bool(status_summary.get("comparison_empty")),
            "comparison_reason_codes": list(status_summary.get("comparison_reason_codes") or []),
            "comparison_operator_messages": list(status_summary.get("comparison_operator_messages") or []),
            "comparison": status_summary.get("comparison") or {},
            "checks": checks,
            "check_groups": check_groups,
            "failed_required_checks": failed_required_checks,
            "failed_advisory_checks": warning_checks,
            "warning_checks": warning_checks,
            "quality_gate_policy": quality_gate_policy,
            "artifacts": {
                **(summary_payload.get("artifacts") if isinstance(summary_payload.get("artifacts"), dict) else {}),
                **artifacts,
            },
            "evaluation_summary": evaluation_summary,
            "artifact_completeness_summary": evaluation_summary.get("artifact_completeness") or {},
            "family_distribution_summary": family_distribution_summary,
        }
    )
    summary_payload["run_metadata"] = {
        **(summary_payload.get("run_metadata") if isinstance(summary_payload.get("run_metadata"), dict) else {}),
        "workflow_type": "samgov-evaluate",
        "scan_limit": int(scan_limit),
    }
    workflow_module._write_json(summary_path, summary_payload)

    manifest_payload.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "workflow_type": "samgov-evaluate",
            "workflow_status": workflow_status,
            "status": workflow_status,
            "quality": status_summary.get("quality"),
            "has_required_failures": bool(status_summary.get("has_required_failures")),
            "has_advisory_failures": bool(status_summary.get("has_advisory_failures")),
            "has_usable_artifacts": bool(status_summary.get("has_usable_artifacts")),
            "partially_useful": bool(status_summary.get("partially_useful")),
            "reason_codes": list(status_summary.get("reason_codes") or []),
            "operator_messages": list(status_summary.get("operator_messages") or []),
            "required_failure_categories": list(status_summary.get("required_failure_categories") or []),
            "advisory_failure_categories": list(status_summary.get("advisory_failure_categories") or []),
            "failure_categories": list(status_summary.get("failure_categories") or []),
            "comparison_requested": bool(status_summary.get("comparison_requested")),
            "comparison_available": bool(status_summary.get("comparison_available")),
            "comparison_empty": bool(status_summary.get("comparison_empty")),
            "comparison_reason_codes": list(status_summary.get("comparison_reason_codes") or []),
            "comparison_operator_messages": list(status_summary.get("comparison_operator_messages") or []),
            "comparison": status_summary.get("comparison") or {},
            "quality_gate_policy": quality_gate_policy,
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
            "run_parameters": {
                **(manifest_payload.get("run_parameters") if isinstance(manifest_payload.get("run_parameters"), dict) else {}),
                "scan_limit": int(scan_limit),
            },
            "summary_counts": {
                **(manifest_payload.get("summary_counts") if isinstance(manifest_payload.get("summary_counts"), dict) else {}),
                "top10_proxy_or_pairbacked_count": _safe_int((evaluation_summary.get("signal_metrics") or {}).get("top10_proxy_or_pairbacked_count"), default=0),
                "top10_non_routine_count": _safe_int((evaluation_summary.get("signal_metrics") or {}).get("top10_non_routine_count"), default=0),
                "dossier_count": _safe_int(evaluation_artifacts.get("dossier_count"), default=0),
            },
            "artifact_completeness_summary": evaluation_summary.get("artifact_completeness") or {},
            "evaluation_summary": evaluation_summary,
            "family_distribution_summary": family_distribution_summary,
            "generated_files": flatten_bundle_files(artifacts=artifacts, bundle_dir=bundle_dir),
        }
    )
    write_bundle_manifest(bundle_dir=bundle_dir, payload=manifest_payload)
    bundle_report_html = render_sam_bundle_report_from_bundle(bundle_dir)
    artifacts["report_html"] = bundle_report_html
    summary_payload["artifacts"]["report_html"] = bundle_report_html
    workflow_module._write_json(summary_path, summary_payload)
    manifest_payload["generated_files"] = flatten_bundle_files(artifacts=artifacts, bundle_dir=bundle_dir)
    write_bundle_manifest(bundle_dir=bundle_dir, payload=manifest_payload)

    return {
        **base_result,
        "status": workflow_status,
        "workflow_type": "samgov-evaluate",
        "required_checks_passed": len(failed_required_checks) == 0,
        "smoke_passed": len(failed_required_checks) == 0,
        "partially_useful": bool(status_summary.get("partially_useful")),
        "quality": status_summary.get("quality"),
        "reason_codes": list(status_summary.get("reason_codes") or []),
        "operator_messages": list(status_summary.get("operator_messages") or []),
        "required_failure_categories": list(status_summary.get("required_failure_categories") or []),
        "advisory_failure_categories": list(status_summary.get("advisory_failure_categories") or []),
        "checks": checks,
        "check_groups": check_groups,
        "quality_gate_policy": quality_gate_policy,
        "failed_required_checks": failed_required_checks,
        "failed_advisory_checks": warning_checks,
        "warning_checks": warning_checks,
        "artifacts": artifacts,
        "evaluation_summary": evaluation_summary,
    }


__all__ = ["run_samgov_evaluation_workflow"]
