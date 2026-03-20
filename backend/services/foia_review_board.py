from __future__ import annotations

import html
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Optional
from urllib.parse import quote

from backend.services.adjudication import load_bundle_adjudication_state
from backend.services.lead_families import (
    lead_family_label,
    summarize_lead_family_distribution,
    summarize_lead_family_groups,
)
from backend.services.review_contract import (
    candidate_join_text,
    linked_source_text,
    signal_text,
    suppressor_text,
)

FOIA_LEAD_REVIEW_BOARD_HTML_PATH = Path("report") / "foia_lead_review_board.html"
FOIA_LEAD_REVIEW_BOARD_MD_PATH = Path("report") / "foia_lead_review_board.md"
FOIA_LEAD_DOSSIER_DIR = Path("report") / "lead_dossiers"

_TOP_LEAD_TABLE_LIMIT = 15
_TOP_LEAD_DETAIL_LIMIT = 10
_TOP_LEAD_DOSSIER_LIMIT = 15
_MISSION_QUALITY_TOP_LEAD_LIMIT = 10
_ROUTINE_NOISE_PREFIXES = ("operational_noise_terms:", "sam_proxy_noise_expansion:")
_ROUTINE_NOISE_TOKENS = ("routine", "admin", "commodity", "janitorial", "license renewal", "maintenance")
_STARTER_PACK_PREFIXES = ("sam_procurement_starter",)
_ONTOLOGY_PROFILE_BY_FILENAME = {
    "ontology_sam_procurement_starter.json": "starter",
    "ontology_sam_dod_foia_companion.json": "dod_foia",
    "ontology_sam_procurement_plus_dod_foia.json": "starter_plus_dod_foia",
    "ontology_sam_hidden_program_proxy_companion.json": "hidden_program_proxy",
    "ontology_sam_hidden_program_proxy_exploratory.json": "hidden_program_proxy_exploratory",
    "ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json": "starter_plus_dod_foia_hidden_program_proxy",
    "ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json": "starter_plus_dod_foia_hidden_program_proxy_exploratory",
}


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


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _norm_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_first(root: Path, contains: str, suffix: str) -> Optional[Path]:
    if not root.exists():
        return None
    candidates = [path for path in root.rglob(f"*{suffix}") if contains.lower() in path.name.lower()]
    if not candidates:
        return None
    candidates.sort(key=lambda item: item.as_posix().lower())
    return candidates[0]


def _relative_href(base_dir: Path, target: Path) -> str:
    try:
        rel = target.relative_to(base_dir)
    except ValueError:
        rel = Path(target)
    return quote(rel.as_posix(), safe="/")


def _bundle_relative(path: Path, bundle_dir: Path) -> str:
    try:
        return path.resolve().relative_to(bundle_dir.resolve()).as_posix()
    except Exception:
        return str(path)


def _esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _link(label: Any, href: str) -> str:
    return f"<a href=\"{_esc(href)}\">{_esc(label)}</a>"


def _md_link(label: Any, href: str) -> str:
    return f"[{label}]({href})"


def _truncate(value: Any, limit: int = 180) -> str:
    text = _norm_text(value)
    if len(text) <= int(limit):
        return text
    return text[: max(int(limit) - 3, 0)].rstrip() + "..."


def _iso_text(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return "Unavailable"
    return text.replace("T", " ").replace("+00:00", "Z")


def _first_present(*values: Any) -> str | None:
    for value in values:
        text = _norm_text(value)
        if text:
            return text
    return None


def _format_pct(value: Optional[float]) -> str:
    if value is None:
        return "Unavailable"
    return f"{float(value):.1f}%"


def _load_bundle_inputs(bundle_dir: Path) -> dict[str, Any]:
    summary = _load_json(bundle_dir / "results" / "workflow_summary.json")
    workflow_doc = _load_json(bundle_dir / "results" / "workflow_result.json")
    doctor_doc = _load_json(bundle_dir / "results" / "doctor_status.json")
    manifest = _load_json(bundle_dir / "bundle_manifest.json")

    workflow = workflow_doc.get("result") if isinstance(workflow_doc.get("result"), dict) else workflow_doc
    doctor = doctor_doc.get("result") if isinstance(doctor_doc.get("result"), dict) else doctor_doc

    lead_snapshot = _load_json(bundle_dir / "exports" / "lead_snapshot.json")
    if not lead_snapshot:
        lead_path = _find_first(bundle_dir / "exports", "lead_snapshot", ".json")
        if lead_path is not None:
            lead_snapshot = _load_json(lead_path)

    review_summary = _load_json(bundle_dir / "exports" / "review_summary.json")
    if not review_summary:
        review_path = _find_first(bundle_dir / "exports", "review_summary", ".json")
        if review_path is not None:
            review_summary = _load_json(review_path)

    artifacts = summary.get("artifacts") if isinstance(summary.get("artifacts"), dict) else {}
    adjudication_state = load_bundle_adjudication_state(bundle_dir=bundle_dir, artifact_payload=artifacts)
    metrics = adjudication_state.get("metrics") if isinstance(adjudication_state.get("metrics"), dict) else {}
    return {
        "summary": summary,
        "workflow": workflow if isinstance(workflow, dict) else {},
        "doctor": doctor if isinstance(doctor, dict) else {},
        "manifest": manifest,
        "lead_snapshot": lead_snapshot,
        "review_summary": review_summary,
        "adjudication_state": adjudication_state,
        "adjudication_metrics": metrics,
    }


def _merged_run_metadata(summary: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    manifest_params = manifest.get("run_parameters") if isinstance(manifest.get("run_parameters"), dict) else {}
    summary_params = summary.get("run_metadata") if isinstance(summary.get("run_metadata"), dict) else {}
    if isinstance(manifest_params, dict):
        merged.update(manifest_params)
    if isinstance(summary_params, dict):
        merged.update(summary_params)
    return merged


def _ontology_profile_label(run_metadata: dict[str, Any]) -> str:
    explicit = _norm_text(run_metadata.get("ontology_profile"))
    if explicit:
        return explicit
    path_text = _norm_text(run_metadata.get("ontology_path"))
    if not path_text:
        return "Unavailable"
    path = Path(path_text)
    profile = _ONTOLOGY_PROFILE_BY_FILENAME.get(path.name)
    if profile:
        return f"{profile} ({path.as_posix()})"
    return path.as_posix()


def _requested_window_text(run_metadata: dict[str, Any]) -> str:
    posted_from = _norm_text(run_metadata.get("effective_posted_from"))
    posted_to = _norm_text(run_metadata.get("effective_posted_to"))
    if posted_from and posted_to:
        return f"SAM postedDate {posted_from}..{posted_to}"
    ingest_days = run_metadata.get("ingest_days")
    if ingest_days is not None:
        return f"Rolling ingest lookback: last {_safe_int(ingest_days)} days"
    return "Unavailable"


def _effective_window_text(review_summary: dict[str, Any]) -> str:
    effective = review_summary.get("effective_window") if isinstance(review_summary.get("effective_window"), dict) else {}
    earliest = _norm_text(effective.get("earliest"))
    latest = _norm_text(effective.get("latest"))
    span_days = effective.get("span_days")
    if earliest and latest:
        return f"{earliest} .. {latest} (span={span_days} days)"
    return "Unavailable"


def _agency_office_label(row: dict[str, Any]) -> str:
    contracting = _first_present(row.get("contracting_office_name"), row.get("contracting_office_code"))
    awarding = _first_present(row.get("awarding_agency_name"), row.get("awarding_agency_code"))
    funding = _first_present(row.get("funding_agency_name"), row.get("funding_agency_code"))
    if contracting and awarding:
        return f"{contracting} | {awarding}"
    return contracting or awarding or funding or "Unavailable"


def _identifiers(row: dict[str, Any]) -> list[str]:
    fields = [
        ("doc", row.get("doc_id")),
        ("sol", row.get("solicitation_number")),
        ("notice", row.get("notice_id")),
        ("award", row.get("award_id")),
        ("piid", row.get("piid")),
        ("uei", row.get("recipient_uei")),
    ]
    out: list[str] = []
    for label, value in fields:
        text = _norm_text(value)
        if text:
            out.append(f"{label}={text}")
    return out


def _matched_rule_keys(row: dict[str, Any]) -> list[str]:
    details = _norm_dict(row.get("score_details"))
    rule_keys = [str(item) for item in _norm_list(details.get("matched_ontology_rules")) if _norm_text(item)]
    if rule_keys:
        return rule_keys

    clauses = _norm_list(details.get("matched_ontology_clauses"))
    out: list[str] = []
    for clause in clauses:
        if not isinstance(clause, dict):
            continue
        pack = _norm_text(clause.get("pack"))
        rule = _norm_text(clause.get("rule"))
        if not pack:
            continue
        out.append(f"{pack}:{rule}" if rule else pack)
    return out


def _non_starter_rule_keys(row: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in _matched_rule_keys(row):
        pack = key.split(":", 1)[0].strip().lower()
        if not pack.startswith(_STARTER_PACK_PREFIXES):
            out.append(key)
    return out


def _positive_signal_texts(row: dict[str, Any]) -> list[str]:
    items = row.get("top_positive_signals")
    if not isinstance(items, list):
        items = _norm_list(_norm_dict(row.get("score_details")).get("top_positive_signals"))
    return [signal_text(item) for item in items if isinstance(item, dict)]


def _suppressor_texts(row: dict[str, Any]) -> list[str]:
    items = row.get("top_suppressors")
    if not isinstance(items, list):
        items = _norm_list(_norm_dict(row.get("score_details")).get("top_suppressors"))
    return [suppressor_text(item) for item in items if isinstance(item, dict)]


def _corroboration_lanes(row: dict[str, Any]) -> list[str]:
    items = row.get("contributing_lanes")
    if not isinstance(items, list):
        items = _norm_list(_norm_dict(row.get("score_details")).get("contributing_lanes"))
    return [str(item) for item in items if _norm_text(item)]


def _candidate_join_items(row: dict[str, Any]) -> list[dict[str, Any]]:
    items = row.get("candidate_join_evidence")
    if isinstance(items, list):
        return [dict(item) for item in items if isinstance(item, dict)]
    items = _norm_list(_norm_dict(row.get("corroboration_summary")).get("candidate_join_evidence"))
    return [dict(item) for item in items if isinstance(item, dict)]


def _linked_source_items(row: dict[str, Any]) -> list[dict[str, Any]]:
    items = row.get("linked_source_summary")
    if isinstance(items, list):
        return [dict(item) for item in items if isinstance(item, dict)]
    items = _norm_list(_norm_dict(row.get("corroboration_summary")).get("linked_source_summary"))
    return [dict(item) for item in items if isinstance(item, dict)]


def _is_routine_noise_label(label: str) -> bool:
    lowered = label.lower()
    if lowered.startswith(_ROUTINE_NOISE_PREFIXES):
        return True
    return any(token in lowered for token in _ROUTINE_NOISE_TOKENS)


def _candidate_evidence_summary(row: dict[str, Any]) -> str:
    joins = _candidate_join_items(row)
    linked_sources = _linked_source_items(row)
    parts: list[str] = []
    if joins:
        parts.append(f"{len(joins)} candidate join(s): {candidate_join_text(joins[0])}")
    if linked_sources:
        parts.append(f"linked source: {linked_source_text(linked_sources[0])}")
    if not parts:
        return "No bundle-level cross-source corroboration recorded."
    return " | ".join(parts)


def _next_records_target(row: dict[str, Any]) -> str:
    office = _agency_office_label(row)
    solicitation = _first_present(row.get("solicitation_number"), row.get("notice_id"), row.get("document_id"))
    award = _first_present(row.get("award_id"), row.get("piid"), row.get("generated_unique_award_id"))
    candidate_join = _candidate_join_items(row)

    if solicitation:
        return f"Solicitation file, amendments, Q&A, and attachments from {office} for {solicitation}."
    if award:
        return f"Award file, modifications, SOW/PWS, and funding trail from {office} for {award}."
    if candidate_join:
        matched = _norm_dict(candidate_join[0].get("matched_values"))
        handle = _first_present(
            matched.get("award_id"),
            matched.get("piid"),
            matched.get("solicitation_number"),
            matched.get("notice_id"),
        )
        if handle:
            return f"Cross-source lineage records tied to {handle}, including predecessor award and incumbent trail."
    if _norm_text(row.get("source_url")) and office != "Unavailable":
        return f"Office correspondence and procurement planning records for {office} around {_iso_text(row.get('occurred_at') or row.get('created_at'))}."
    return "Partial record hooks only; identify a better office or contract handle before drafting."


def _noise_assessment(row: dict[str, Any]) -> dict[str, Any]:
    details = _norm_dict(row.get("score_details"))
    suppressors = _suppressor_texts(row)
    lanes = _corroboration_lanes(row)
    non_pair_lanes = [lane for lane in lanes if lane != "kw_pair"]
    pair_count = _safe_int(details.get("pair_count"))
    has_foia_handles = bool(row.get("has_foia_handles"))
    has_agency_target = bool(row.get("has_agency_target"))
    lead_family = _norm_text(row.get("lead_family"))
    corroboration_score = _safe_int(details.get("corroboration_score"))
    candidate_joins = _candidate_join_items(row)
    linked_sources = _linked_source_items(row)
    non_starter_rules = _non_starter_rule_keys(row)

    reasons: list[str] = []
    if suppressors:
        reasons.append("suppressors hit: " + "; ".join(suppressors[:2]))
    if pair_count > 0 and not non_pair_lanes and not candidate_joins and not linked_sources:
        reasons.append("pair support without independent corroboration lanes")
    if pair_count > 0 and not non_starter_rules:
        reasons.append("starter-only pair support")
    if not has_foia_handles:
        reasons.append("missing FOIA handles")
    elif not has_agency_target:
        reasons.append("missing clear office target")
    if not lead_family or corroboration_score < 3:
        reasons.append("weak family/corroboration fit")

    if not reasons:
        summary = "No dominant noise flag from current bundle evidence."
        level = "low"
    else:
        summary = "; ".join(reasons[:3])
        level = "high" if len(reasons) >= 3 or (suppressors and not has_foia_handles) else "medium"
    return {
        "level": level,
        "summary": summary,
        "reasons": reasons,
        "routine_noise": any(_is_routine_noise_label(label) for label in suppressors),
    }


def _draftability(row: dict[str, Any], next_target: str) -> dict[str, Any]:
    has_handles = bool(row.get("has_foia_handles"))
    has_agency = bool(row.get("has_agency_target"))
    has_vendor = bool(row.get("has_vendor_context"))
    has_classification = bool(row.get("has_classification_context"))
    has_candidate = bool(_candidate_join_items(row))
    has_target = not next_target.startswith("Partial record hooks only")

    if has_handles and has_agency and has_target and (has_vendor or has_classification or has_candidate):
        level = "strong"
        summary = "Strong: identifiers + office handle + record hook"
    elif has_handles and has_target and (has_agency or has_vendor or has_classification):
        level = "moderate"
        summary = "Moderate: usable handles, but some context gaps remain"
    elif has_handles or has_agency:
        level = "weak"
        summary = "Weak: partial handles only"
    else:
        level = "blocked"
        summary = "Blocked: missing traceable FOIA hooks"
    return {"level": level, "summary": summary}


def _subscore_rows(row: dict[str, Any]) -> list[tuple[str, Any]]:
    details = _norm_dict(row.get("score_details"))
    if str(details.get("scoring_version") or "").strip().lower() == "v3":
        return [
            ("proxy_relevance", details.get("proxy_relevance_score")),
            ("investigability", details.get("investigability_score")),
            ("corroboration", details.get("corroboration_score")),
            ("structural_context", details.get("structural_context_score")),
            ("noise_penalty", f"-{_safe_int(details.get('noise_penalty_applied', details.get('noise_penalty')))}"),
            ("total", details.get("total_score", row.get("score"))),
        ]
    return [
        ("clauses", details.get("clause_score")),
        ("keywords", details.get("keyword_score")),
        ("entity_bonus", details.get("entity_bonus")),
        ("pair_bonus", details.get("pair_bonus_applied", details.get("pair_bonus"))),
        ("noise_penalty", f"-{_safe_int(details.get('noise_penalty_applied', details.get('noise_penalty')))}"),
        ("total", row.get("score")),
    ]


def _vendor_block(row: dict[str, Any]) -> list[str]:
    fields = [
        ("vendor", row.get("vendor_name") or row.get("recipient_name")),
        ("uei", row.get("vendor_uei") or row.get("recipient_uei")),
        ("cage", row.get("vendor_cage_code") or row.get("recipient_cage_code")),
        ("entity_id", row.get("entity_id")),
    ]
    out = [f"{label}: {value}" for label, value in fields if _norm_text(value)]
    return out or ["No vendor/entity block available."]


def _place_time_block(row: dict[str, Any]) -> list[str]:
    fields = [
        ("place", row.get("place_text")),
        ("region", row.get("place_region")),
        ("occurred_at", row.get("occurred_at")),
        ("created_at", row.get("created_at")),
    ]
    out = [f"{label}: {_iso_text(value) if label.endswith('_at') else value}" for label, value in fields if _norm_text(value)]
    return out or ["No place/time anchors available."]


def _derive_lead_row(row: dict[str, Any]) -> dict[str, Any]:
    next_target = _next_records_target(row)
    noise = _noise_assessment(row)
    draftability = _draftability(row, next_target)
    lead_family = _norm_text(row.get("lead_family")) or "unassigned"
    family_label = lead_family_label(lead_family) or "Unassigned"
    return {
        "row": row,
        "rank": _safe_int(row.get("rank")),
        "score": _safe_int(row.get("score")),
        "lead_family": lead_family,
        "lead_family_label": family_label,
        "snippet": _truncate(row.get("snippet") or row.get("why_summary"), limit=180),
        "agency_label": _agency_office_label(row),
        "identifiers_text": "; ".join(_identifiers(row)) or "Unavailable",
        "why_interesting": _truncate(row.get("why_summary"), limit=220) or "Unavailable",
        "why_likely_noise": noise,
        "foia_draftability": draftability,
        "subscores": _subscore_rows(row),
        "positive_signals": _positive_signal_texts(row) or ["No positive signals serialized."],
        "suppressors": _suppressor_texts(row) or ["No suppressor hits serialized."],
        "corroboration_lanes": _corroboration_lanes(row) or ["No corroboration lanes serialized."],
        "candidate_evidence_summary": _candidate_evidence_summary(row),
        "vendor_block": _vendor_block(row),
        "place_time_block": _place_time_block(row),
        "next_records_target": next_target,
        "routine_noise": bool(noise.get("routine_noise")),
        "starter_only_pair": _safe_int(_norm_dict(row.get("score_details")).get("pair_count")) > 0 and not _non_starter_rule_keys(row),
        "non_starter_rules": _non_starter_rule_keys(row),
        "dossier_path": None,
    }


def _score_spread_summary(rows: list[dict[str, Any]]) -> str:
    return _score_spread_metrics(rows).get("summary") or "No scores available."


def _score_spread_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [_safe_int(item.get("score")) for item in rows if item.get("score") is not None]
    if not scores:
        return {
            "max_score": None,
            "min_score": None,
            "spread": 0,
            "median_score": None,
            "unique_score_count": 0,
            "repeated_peak": 0,
            "compression": "unavailable",
            "summary": "No scores available.",
        }
    top_scores = scores[: min(len(scores), 10)]
    spread = max(top_scores) - min(top_scores)
    repeated_peak = max(Counter(top_scores).values()) if top_scores else 0
    if spread <= 2 or repeated_peak >= max(2, len(top_scores) // 2):
        compression = "highly compressed"
    elif spread <= 5:
        compression = "moderately compressed"
    else:
        compression = "well separated"
    return {
        "max_score": max(top_scores),
        "min_score": min(top_scores),
        "spread": int(spread),
        "median_score": float(median(scores)),
        "unique_score_count": len(set(top_scores)),
        "repeated_peak": int(repeated_peak),
        "compression": compression,
        "summary": (
            f"Top {len(top_scores)} scores span {max(top_scores)}..{min(top_scores)} "
            f"(spread={spread}); median={median(scores):.1f}; ranking is {compression}."
        ),
    }


def _top_non_starter_rules(rows: list[dict[str, Any]], limit: int = 8) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in rows:
        for key in _non_starter_rule_keys(row):
            counter[key] += 1
    return [{"rule": rule, "hits": count} for rule, count in counter.most_common(int(limit))]


def _overall_verdict(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "level": "empty",
            "title": "No reviewable FOIA leads exported.",
            "detail": "The bundle does not contain ranked lead rows, so there is nothing to evaluate on the reviewer board.",
        }

    promising = [
        row
        for row in rows
        if row["foia_draftability"]["level"] in {"strong", "moderate"}
        and row["why_likely_noise"]["level"] != "high"
        and (row["lead_family"] != "unassigned" or row["non_starter_rules"] or _candidate_join_items(row["row"]))
    ]
    routine_noise_share = sum(1 for row in rows if row["routine_noise"]) / float(len(rows))
    starter_only_share = sum(1 for row in rows if row["starter_only_pair"]) / float(len(rows))

    if len(promising) >= 3:
        return {
            "level": "promising",
            "title": "Promising candidate-grade FOIA leads are present.",
            "detail": f"{len(promising)} ranked leads have draftable handles with limited visible noise; keep them candidate-grade and review evidence before escalating.",
        }
    if promising:
        return {
            "level": "mixed",
            "title": "Mixed run: a few draftable leads, but skepticism is still warranted.",
            "detail": f"{len(promising)} lead(s) look usable, while routine noise or weak corroboration remains visible in the rest of the board.",
        }
    if routine_noise_share >= 0.5 or starter_only_share >= 0.5:
        return {
            "level": "noisy",
            "title": "Run looks noisy or starter-pair dominated.",
            "detail": "Most ranked leads are driven by suppressors, starter-only pair support, or missing FOIA hooks rather than durable corroboration.",
        }
    return {
        "level": "thin",
        "title": "Run is thin and under-corroborated.",
        "detail": "The board shows some signal, but current leads need stronger corroboration or better record handles before they are draftable.",
    }


def build_foia_lead_review_diagnostics(
    *,
    lead_snapshot: dict[str, Any],
    review_summary: Optional[dict[str, Any]] = None,
    bundle_dir: Optional[Path] = None,
    mission_top_n: int = _MISSION_QUALITY_TOP_LEAD_LIMIT,
    dossier_export_enabled: bool = False,
) -> dict[str, Any]:
    review_summary = _norm_dict(review_summary)
    rows_raw = [dict(item) for item in _norm_list(lead_snapshot.get("items")) if isinstance(item, dict)]
    rows_raw.sort(
        key=lambda item: (
            _safe_int(item.get("rank"), default=10**9),
            _safe_int(item.get("event_id"), default=10**9),
        )
    )

    derived_rows = [_derive_lead_row(row) for row in rows_raw]
    dossiers: dict[int, Path] = {}
    dossier_export_enabled = bool(dossier_export_enabled and bundle_dir is not None)
    if dossier_export_enabled and bundle_dir is not None:
        dossiers = _write_dossiers(bundle_dir, derived_rows)
        for item in derived_rows:
            dossier_path = dossiers.get(item["rank"])
            if dossier_path is not None:
                item["dossier_path"] = dossier_path

    top_n = max(int(mission_top_n), 0)
    top_rows_raw = rows_raw[:top_n]
    top_rows = derived_rows[:top_n]

    family_groups = summarize_lead_family_groups(rows_raw)
    family_distribution = summarize_lead_family_distribution(rows_raw)
    top_non_starter_rules = _top_non_starter_rules(top_rows_raw)
    score_spread = _score_spread_metrics(top_rows_raw)
    verdict = _overall_verdict(top_rows or derived_rows)

    core_fields = (
        "has_core_identifiers",
        "has_agency_target",
        "has_vendor_context",
        "has_classification_context",
        "has_foia_handles",
    )
    core_field_counts = {
        field: sum(1 for row in top_rows_raw if bool(row.get(field)))
        for field in core_fields
    }
    total_core_slots = len(top_rows_raw) * len(core_fields)
    core_field_coverage_pct = (
        round((100.0 * sum(core_field_counts.values()) / float(total_core_slots)), 1)
        if total_core_slots
        else 0.0
    )
    rows_with_three_plus_core_fields = sum(
        1
        for row in top_rows_raw
        if sum(1 for field in core_fields if bool(row.get(field))) >= 3
    )
    rows_with_three_plus_core_fields_pct = (
        round((100.0 * rows_with_three_plus_core_fields / float(len(top_rows_raw))), 1)
        if top_rows_raw
        else 0.0
    )

    assigned_family_counts: Counter[str] = Counter()
    unassigned_count = 0
    for item in top_rows:
        family = _norm_text(item.get("lead_family"))
        if family and family != "unassigned":
            assigned_family_counts[family] += 1
        else:
            unassigned_count += 1
    top_family, top_family_count = (assigned_family_counts.most_common(1)[0] if assigned_family_counts else (None, 0))
    top_family_share_pct = (
        round((100.0 * top_family_count / float(len(top_rows))), 1)
        if top_rows
        else 0.0
    )

    nonstarter_pack_count = sum(1 for item in top_rows if item["non_starter_rules"])
    nonstarter_pack_presence_pct = (
        round((100.0 * nonstarter_pack_count / float(len(top_rows))), 1)
        if top_rows
        else 0.0
    )
    starter_only_pair_count = sum(1 for item in top_rows if item["starter_only_pair"])
    starter_only_pair_share_pct = (
        round((100.0 * starter_only_pair_count / float(len(top_rows))), 1)
        if top_rows
        else 0.0
    )
    routine_noise_count = sum(1 for item in top_rows if item["routine_noise"])
    routine_noise_share_pct = (
        round((100.0 * routine_noise_count / float(len(top_rows))), 1)
        if top_rows
        else 0.0
    )

    draftability_counts: Counter[str] = Counter(
        str(item["foia_draftability"]["level"])
        for item in top_rows
        if isinstance(item.get("foia_draftability"), dict)
    )
    draftable_count = sum(
        count for level, count in draftability_counts.items() if str(level) in {"strong", "moderate"}
    )
    foia_draftability_pct = (
        round((100.0 * draftable_count / float(len(top_rows))), 1)
        if top_rows
        else 0.0
    )

    dossier_expected_count = len(top_rows) if dossier_export_enabled else None
    dossier_linked_count = None
    dossier_linkage_pct = None
    if dossier_export_enabled:
        dossier_linked_count = sum(
            1
            for item in top_rows
            if isinstance(item.get("dossier_path"), Path) and Path(item["dossier_path"]).exists()
        )
        dossier_linkage_pct = (
            round((100.0 * dossier_linked_count / float(len(top_rows))), 1)
            if top_rows
            else 0.0
        )

    row_scoring_versions = sorted(
        {
            str(
                row.get("scoring_version")
                or _norm_dict(row.get("score_details")).get("scoring_version")
                or ""
            ).strip().lower()
            for row in top_rows_raw
        }
        - {""}
    )
    scoring_version = _first_present(
        lead_snapshot.get("scoring_version"),
        review_summary.get("scoring_version"),
        row_scoring_versions[0] if len(row_scoring_versions) == 1 else None,
    )

    mission_quality = {
        "artifact_available": bool(rows_raw),
        "mission_top_n": top_n,
        "considered_top_leads": len(top_rows),
        "scoring_version": scoring_version,
        "row_scoring_versions": row_scoring_versions,
        "core_field_coverage_pct": core_field_coverage_pct,
        "core_field_counts": core_field_counts,
        "rows_with_three_plus_core_fields": rows_with_three_plus_core_fields,
        "rows_with_three_plus_core_fields_pct": rows_with_three_plus_core_fields_pct,
        "family_diversity": {
            "unique_primary_families": len(assigned_family_counts),
            "primary_family_counts": dict(assigned_family_counts),
            "top_family": top_family,
            "top_family_share_pct": top_family_share_pct,
            "unassigned_count": unassigned_count,
        },
        "nonstarter_pack_count": nonstarter_pack_count,
        "nonstarter_pack_presence_pct": nonstarter_pack_presence_pct,
        "starter_only_pair_count": starter_only_pair_count,
        "starter_only_pair_share_pct": starter_only_pair_share_pct,
        "routine_noise_count": routine_noise_count,
        "routine_noise_share_pct": routine_noise_share_pct,
        "score_spread": score_spread,
        "foia_draftability": {
            "draftable_count": draftable_count,
            "draftable_share_pct": foia_draftability_pct,
            "levels": {str(level): int(count) for level, count in sorted(draftability_counts.items())},
        },
        "dossier_export_enabled": dossier_export_enabled,
        "dossier_expected_count": dossier_expected_count,
        "dossier_linked_count": dossier_linked_count,
        "dossier_linkage_pct": dossier_linkage_pct,
        "top_non_starter_rules": top_non_starter_rules,
        "verdict": verdict,
    }

    return {
        "rows_raw": rows_raw,
        "derived_rows": derived_rows,
        "family_groups": family_groups,
        "family_distribution": family_distribution,
        "top_non_starter_rules": top_non_starter_rules,
        "verdict": verdict,
        "mission_quality": mission_quality,
    }


def _table_html(headers: list[str], rows: list[list[Any]], *, fallback: str, class_name: str = "") -> str:
    if not rows:
        return f"<div class=\"empty\">{_esc(fallback)}</div>"
    head = "".join([f"<th>{_esc(header)}</th>" for header in headers])
    body_rows: list[str] = []
    for row in rows:
        cells: list[str] = []
        for value in row:
            if isinstance(value, str) and value.startswith("<a "):
                cells.append(f"<td>{value}</td>")
            else:
                cells.append(f"<td>{_esc(value if value not in (None, '', []) else 'Unavailable')}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")
    return f"<table class=\"{_esc(class_name)}\"><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def _md_table(headers: list[str], rows: list[list[Any]], *, fallback: str) -> str:
    if not rows:
        return fallback
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        values = [str(value if value not in (None, "", []) else "Unavailable").replace("\n", "<br>") for value in row]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _write_dossiers(bundle_dir: Path, rows: list[dict[str, Any]]) -> dict[int, Path]:
    dossier_dir = bundle_dir / FOIA_LEAD_DOSSIER_DIR
    dossier_dir.mkdir(parents=True, exist_ok=True)
    written: dict[int, Path] = {}
    for item in rows[: _TOP_LEAD_DOSSIER_LIMIT]:
        row = item["row"]
        rank = max(item["rank"], 0)
        event_id = _safe_int(row.get("event_id"))
        filename = f"lead_{rank:03d}_event_{event_id or 0}.md"
        path = dossier_dir / filename
        identifiers = _identifiers(row)
        content = [
            f"# Lead #{item['rank']}: {item['lead_family_label']}",
            "",
            f"- Score: {item['score']}",
            f"- FOIA draftability: {item['foia_draftability']['summary']}",
            f"- Why likely noise: {item['why_likely_noise']['summary']}",
            f"- Agency/office: {item['agency_label']}",
            f"- Source: {_first_present(row.get('source'), 'Unavailable')}",
            "",
            "## Snippet",
            "",
            row.get("snippet") or "Unavailable",
            "",
            "## Why Interesting",
            "",
            row.get("why_summary") or "Unavailable",
            "",
            "## Identifiers",
            "",
            ("- " + "\n- ".join(identifiers)) if identifiers else "- Unavailable",
            "",
            "## Signals",
            "",
            ("- " + "\n- ".join(item["positive_signals"])) if item["positive_signals"] else "- Unavailable",
            "",
            "## Suppressors / Noise",
            "",
            ("- " + "\n- ".join(item["suppressors"])) if item["suppressors"] else "- Unavailable",
            "",
            "## Corroboration",
            "",
            f"- Lanes: {', '.join(item['corroboration_lanes'])}",
            f"- Candidate evidence: {item['candidate_evidence_summary']}",
            "",
            "## Next Records Target",
            "",
            item["next_records_target"],
            "",
            "## Provenance",
            "",
            "- Bundle lead snapshot: ../../exports/lead_snapshot.json",
            "- Bundle review summary: ../../exports/review_summary.json",
        ]
        source_url = _norm_text(row.get("source_url"))
        if source_url:
            content.append(f"- Source URL: {source_url}")
        path.write_text("\n".join(content) + "\n", encoding="utf-8")
        written[item["rank"]] = path
    return written


def _render_adjudication_html(bundle_dir: Path, adjudication_state: dict[str, Any], metrics: dict[str, Any]) -> str:
    adjudications_csv = adjudication_state.get("adjudications_csv")
    summary = metrics.get("summary") if isinstance(metrics.get("summary"), dict) else {}
    label = "Unavailable"
    if isinstance(adjudications_csv, Path):
        label = _bundle_relative(adjudications_csv, bundle_dir)
    summary_rows = [
        ["Adjudications CSV", label],
        ["Reviewed rows", summary.get("reviewed_count")],
        ["Decisive rows", summary.get("decisive_count")],
        ["Acceptance rate", _format_pct(summary.get("acceptance_rate_pct")) if summary else "Unavailable"],
        ["FOIA-ready yes", summary.get("foia_ready_yes_count")],
    ]
    if not metrics:
        return (
            "<section class=\"section\">"
            "<h2>Adjudication</h2>"
            "<div class=\"empty\">Adjudications are present, but no metrics JSON was found yet.</div>"
            f"{_table_html(['Field', 'Value'], summary_rows, fallback='No adjudication summary.')}"
            "</section>"
        )

    precision_rows = []
    for entry in (summary.get("precision_at_k") or {}).values():
        if not isinstance(entry, dict):
            continue
        precision_rows.append(
            [
                entry.get("k"),
                _format_pct(entry.get("precision_pct")),
                entry.get("reviewed_count"),
                entry.get("decisive_count"),
                entry.get("keep_count"),
                entry.get("reject_count"),
            ]
        )
    rejection_rows = []
    for entry in metrics.get("rejection_reasons") or []:
        if not isinstance(entry, dict):
            continue
        rejection_rows.append([entry.get("reason_code"), entry.get("count"), _format_pct(entry.get("share_of_rejects_pct"))])
    family_rows = []
    for entry in (metrics.get("by_lead_family") or [])[:8]:
        if not isinstance(entry, dict):
            continue
        family_rows.append(
            [
                entry.get("lead_family"),
                entry.get("row_count"),
                entry.get("keep_count"),
                entry.get("reject_count"),
                _format_pct(entry.get("acceptance_rate_pct")),
            ]
        )
    return (
        "<section class=\"section\">"
        "<h2>Adjudication</h2>"
        "<div class=\"note\">Reviewer adjudication remains local to the bundle and is shown here only as a calibration aid.</div>"
        f"{_table_html(['Field', 'Value'], summary_rows, fallback='No adjudication summary.')}"
        "<h3>Precision @ k</h3>"
        f"{_table_html(['k', 'Precision', 'Reviewed', 'Decisive', 'Keep', 'Reject'], precision_rows, fallback='No precision metrics.')}"
        "<h3>By Lead Family</h3>"
        f"{_table_html(['Family', 'Rows', 'Keep', 'Reject', 'Acceptance'], family_rows, fallback='No by-family metrics.')}"
        "<h3>Rejection Reasons</h3>"
        f"{_table_html(['Reason', 'Count', 'Share of Rejects'], rejection_rows, fallback='No rejection reasons.')}"
        "</section>"
    )


def _render_adjudication_md(bundle_dir: Path, adjudication_state: dict[str, Any], metrics: dict[str, Any]) -> str:
    adjudications_csv = adjudication_state.get("adjudications_csv")
    summary = metrics.get("summary") if isinstance(metrics.get("summary"), dict) else {}
    label = "Unavailable"
    if isinstance(adjudications_csv, Path):
        label = _bundle_relative(adjudications_csv, bundle_dir)

    lines = [
        "## Adjudication",
        "",
        _md_table(
            ["Field", "Value"],
            [
                ["Adjudications CSV", label],
                ["Reviewed rows", summary.get("reviewed_count")],
                ["Decisive rows", summary.get("decisive_count")],
                ["Acceptance rate", _format_pct(summary.get("acceptance_rate_pct")) if summary else "Unavailable"],
                ["FOIA-ready yes", summary.get("foia_ready_yes_count")],
            ],
            fallback="No adjudication summary.",
        ),
        "",
    ]
    if not metrics:
        lines.extend(["Adjudications are present, but no metrics JSON was found yet.", ""])
        return "\n".join(lines)

    precision_rows = []
    for entry in (summary.get("precision_at_k") or {}).values():
        if not isinstance(entry, dict):
            continue
        precision_rows.append(
            [
                entry.get("k"),
                _format_pct(entry.get("precision_pct")),
                entry.get("reviewed_count"),
                entry.get("decisive_count"),
                entry.get("keep_count"),
                entry.get("reject_count"),
            ]
        )
    lines.extend(
        [
            "### Precision @ k",
            "",
            _md_table(["k", "Precision", "Reviewed", "Decisive", "Keep", "Reject"], precision_rows, fallback="No precision metrics."),
            "",
        ]
    )
    return "\n".join(lines)


def render_foia_lead_review_board_from_bundle(bundle_dir: Path) -> dict[str, Path]:
    root = Path(bundle_dir).expanduser()
    payload = _load_bundle_inputs(root)
    summary = payload["summary"]
    manifest = payload["manifest"]
    lead_snapshot = payload["lead_snapshot"]
    review_summary = payload["review_summary"]
    adjudication_state = payload["adjudication_state"]
    adjudication_metrics = payload["adjudication_metrics"]

    run_metadata = _merged_run_metadata(summary, manifest)
    generated_at = (
        _norm_text(summary.get("generated_at"))
        or _norm_text(manifest.get("generated_at"))
        or datetime.now(timezone.utc).isoformat()
    )
    diagnostics = build_foia_lead_review_diagnostics(
        lead_snapshot=lead_snapshot,
        review_summary=review_summary,
        bundle_dir=root,
        mission_top_n=_MISSION_QUALITY_TOP_LEAD_LIMIT,
        dossier_export_enabled=True,
    )
    rows_raw = diagnostics["rows_raw"]
    derived_rows = diagnostics["derived_rows"]
    family_groups = diagnostics["family_groups"]
    family_distribution = diagnostics["family_distribution"]
    verdict = diagnostics["verdict"]
    mission_quality = diagnostics["mission_quality"]
    family_rows = []
    primary_by_family = {
        str(group.get("lead_family") or ""): group
        for group in family_distribution.get("primary") or []
        if isinstance(group, dict)
    }
    secondary_by_family = {
        str(group.get("lead_family") or ""): group
        for group in family_distribution.get("secondary") or []
        if isinstance(group, dict)
    }
    any_by_family = {
        str(group.get("lead_family") or ""): group
        for group in family_distribution.get("any_assignment") or []
        if isinstance(group, dict)
    }
    family_keys = []
    for group in family_groups:
        if not isinstance(group, dict):
            continue
        family_key = str(group.get("lead_family") or "")
        if family_key and family_key not in family_keys:
            family_keys.append(family_key)
    for group in family_distribution.get("secondary") or []:
        if not isinstance(group, dict):
            continue
        family_key = str(group.get("lead_family") or "")
        if family_key and family_key not in family_keys:
            family_keys.append(family_key)
    for family_key in family_keys[:8]:
        primary_group = primary_by_family.get(family_key, {})
        secondary_group = secondary_by_family.get(family_key, {})
        any_group = any_by_family.get(family_key, {})
        family_rows.append(
            [
                primary_group.get("label") or lead_family_label(family_key) or family_key or "Unassigned",
                primary_group.get("count", 0),
                secondary_group.get("count", 0),
                any_group.get("count", 0),
                primary_group.get("top_rank"),
                primary_group.get("top_score"),
            ]
        )
    routine_noise_pct = _format_pct(mission_quality.get("routine_noise_share_pct"))
    starter_only_pct = _format_pct(mission_quality.get("starter_only_pair_share_pct"))
    top_non_starter_rules = diagnostics["top_non_starter_rules"]

    html_path = root / FOIA_LEAD_REVIEW_BOARD_HTML_PATH
    md_path = root / FOIA_LEAD_REVIEW_BOARD_MD_PATH
    html_path.parent.mkdir(parents=True, exist_ok=True)

    nav_links = [
        _link("Operator bundle report", "bundle_report.html"),
        _link("Markdown companion", "foia_lead_review_board.md"),
        _link("Lead snapshot JSON", "../exports/lead_snapshot.json"),
        _link("Review summary JSON", "../exports/review_summary.json"),
    ]
    metrics_path = adjudication_state.get("metrics_json")
    if isinstance(metrics_path, Path):
        nav_links.append(_link("Adjudication metrics", _relative_href(html_path.parent, metrics_path)))

    run_header_rows = [
        ["Bundle path", str(root.resolve())],
        ["Generated time", generated_at],
        ["Scoring version", summary.get("scoring_version") or lead_snapshot.get("scoring_version") or review_summary.get("scoring_version")],
        ["Ontology profile", _ontology_profile_label(run_metadata)],
        ["Requested window", _requested_window_text(run_metadata)],
        ["Effective review window", _effective_window_text(review_summary)],
    ]

    top_leads_table_rows = []
    for item in derived_rows[: _TOP_LEAD_TABLE_LIMIT]:
        dossier_path = item["dossier_path"]
        dossier_link = (
            _link("dossier", _relative_href(html_path.parent, dossier_path))
            if isinstance(dossier_path, Path)
            else "Unavailable"
        )
        top_leads_table_rows.append(
            [
                item["rank"],
                item["score"],
                item["lead_family_label"],
                item["snippet"],
                item["agency_label"],
                item["identifiers_text"],
                item["why_interesting"],
                item["why_likely_noise"]["summary"],
                item["foia_draftability"]["summary"],
                dossier_link,
            ]
        )

    detail_cards: list[str] = []
    for item in derived_rows[: _TOP_LEAD_DETAIL_LIMIT]:
        row = item["row"]
        positive = "".join([f"<li>{_esc(value)}</li>" for value in item["positive_signals"][:6]]) or "<li>Unavailable</li>"
        suppressors = "".join([f"<li>{_esc(value)}</li>" for value in item["suppressors"][:6]]) or "<li>Unavailable</li>"
        corroboration = "".join([f"<li>{_esc(value)}</li>" for value in item["corroboration_lanes"]]) or "<li>Unavailable</li>"
        vendor_block = "".join([f"<li>{_esc(value)}</li>" for value in item["vendor_block"]])
        place_time = "".join([f"<li>{_esc(value)}</li>" for value in item["place_time_block"]])
        subscore_rows = "".join(
            [
                f"<tr><th>{_esc(label)}</th><td>{_esc(value if value not in (None, '') else 'Unavailable')}</td></tr>"
                for label, value in item["subscores"]
            ]
        )
        dossier_markup = ""
        if isinstance(item["dossier_path"], Path):
            dossier_markup = "<div class=\"tiny-links\">" + _link("Lead dossier", _relative_href(html_path.parent, item["dossier_path"])) + "</div>"
        detail_cards.append(
            "<article class=\"card\">"
            f"<h3 id=\"lead-{item['rank']}\">#{item['rank']} | {_esc(item['lead_family_label'])} | score={item['score']}</h3>"
            f"<p class=\"snippet\">{_esc(_norm_text(row.get('snippet')) or 'No snippet available.')}</p>"
            f"{dossier_markup}"
            "<div class=\"card-grid\">"
            f"<section><h4>v3 Subscore Breakdown</h4><table><tbody>{subscore_rows}</tbody></table></section>"
            f"<section><h4>Top Positive Signals</h4><ul>{positive}</ul></section>"
            f"<section><h4>Suppressors / Noise Hits</h4><ul>{suppressors}</ul></section>"
            f"<section><h4>Corroboration Lanes</h4><ul>{corroboration}</ul></section>"
            f"<section><h4>Candidate Evidence Summary</h4><p>{_esc(item['candidate_evidence_summary'])}</p></section>"
            f"<section><h4>Vendor / Entity Block</h4><ul>{vendor_block}</ul></section>"
            f"<section><h4>Place / Time Block</h4><ul>{place_time}</ul></section>"
            f"<section><h4>Likely Next Records Target</h4><p>{_esc(item['next_records_target'])}</p></section>"
            "</div>"
            "</article>"
        )

    diagnostics_html = (
        "<section class=\"section\">"
        "<h2>Run-Level Diagnostics</h2>"
        "<div class=\"diag-grid\">"
        "<div class=\"diag-card\">"
        "<h3>Family Distribution</h3>"
        + _table_html(
            ["Family", "Primary", "Secondary", "Any assignment", "Top rank", "Top score"],
            family_rows,
            fallback="No family distribution available.",
        )
        + f"<p class=\"note\">Ambiguous leads: {_esc(family_distribution.get('ambiguous_items', 0))} of {_esc(family_distribution.get('total_items', 0))}</p>"
        + "</div>"
        "<div class=\"diag-card\">"
        "<h3>Score Spread / Compression</h3>"
        f"<p>{_esc(_norm_text(_norm_dict(mission_quality.get('score_spread')).get('summary')) or _score_spread_summary(rows_raw))}</p>"
        "</div>"
        "<div class=\"diag-card\">"
        "<h3>Starter-Only Pair Dominance</h3>"
        f"<p>{_esc(starter_only_pct)} of the reviewed top {_MISSION_QUALITY_TOP_LEAD_LIMIT} leads rely on kw_pair plus starter-only ontology support.</p>"
        "</div>"
        "<div class=\"diag-card\">"
        "<h3>Routine-Noise Share</h3>"
        f"<p>{_esc(routine_noise_pct)} of the reviewed top {_MISSION_QUALITY_TOP_LEAD_LIMIT} leads carry routine-noise suppressor hits.</p>"
        "</div>"
        "<div class=\"diag-card wide\">"
        "<h3>Top Non-Starter Packs / Rules</h3>"
        + _table_html(["Rule", "Hits"], [[item["rule"], item["hits"]] for item in top_non_starter_rules], fallback="No non-starter ontology rules were present.")
        + "</div>"
        "</div>"
        "</section>"
    )

    adjudication_html = ""
    if adjudication_state.get("adjudications_csv") is not None or adjudication_metrics:
        adjudication_html = _render_adjudication_html(root, adjudication_state, adjudication_metrics)

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>FOIA Lead Review Board</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --fg: #1d2731;
      --card: #fffdf8;
      --line: #d5cfc2;
      --muted: #5f665f;
      --accent: #294a69;
      --ok: #2c6a45;
      --warn: #8b5e1b;
      --bad: #8a2d2f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 24px;
      background:
        radial-gradient(circle at top right, rgba(41,74,105,0.08), transparent 28%),
        linear-gradient(180deg, #f7f2e8 0%, #f1ede5 100%);
      color: var(--fg);
      font-family: "Segoe UI", Tahoma, Arial, sans-serif;
      line-height: 1.45;
    }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .container {{ max-width: 1320px; margin: 0 auto; }}
    .hero, .section, .card, .diag-card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: 0 8px 24px rgba(20, 27, 34, 0.04);
    }}
    .hero {{ padding: 20px; margin-bottom: 18px; }}
    .hero-top {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 14px;
      align-items: flex-start;
    }}
    h1, h2, h3, h4 {{ margin: 0 0 8px 0; }}
    h1 {{ font-size: 28px; }}
    h2 {{ font-size: 20px; margin-bottom: 12px; }}
    h3 {{ font-size: 16px; }}
    h4 {{ font-size: 14px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }}
    .note, .meta, .empty {{ color: var(--muted); }}
    .banner {{
      padding: 12px 14px;
      border-radius: 12px;
      font-weight: 600;
      max-width: 520px;
    }}
    .banner.promising {{ background: rgba(44,106,69,0.12); color: var(--ok); }}
    .banner.mixed, .banner.thin {{ background: rgba(139,94,27,0.12); color: var(--warn); }}
    .banner.noisy, .banner.empty {{ background: rgba(138,45,47,0.1); color: var(--bad); }}
    .banner p {{ margin: 6px 0 0 0; font-weight: 400; }}
    .links {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }}
    .chip {{
      display: inline-block;
      padding: 4px 10px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #fff;
      color: var(--fg);
      font-size: 13px;
    }}
    .section {{ padding: 16px; margin-bottom: 18px; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 13px; }}
    th, td {{ border: 1px solid var(--line); padding: 8px 9px; text-align: left; vertical-align: top; word-break: break-word; }}
    th {{ background: #f6f2ea; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; }}
    .card {{ padding: 14px; }}
    .card-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .card ul {{ margin: 0; padding-left: 18px; }}
    .snippet {{ margin: 4px 0 10px 0; color: var(--muted); }}
    .diag-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }}
    .diag-card {{ padding: 14px; }}
    .diag-card.wide {{ grid-column: 1 / -1; }}
    .tiny-links {{ margin-bottom: 8px; font-size: 13px; }}
    .empty {{ padding: 10px 0; font-style: italic; }}
    @media (max-width: 720px) {{
      body {{ padding: 12px; }}
      .hero, .section, .card, .diag-card {{ padding: 12px; }}
      h1 {{ font-size: 22px; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <div class="hero-top">
        <div>
          <h1>FOIA Lead Review Board</h1>
          <div class="meta">Reviewer-first bundle surface for fast candidate-grade FOIA triage. Keep conclusions skeptical and source-grounded.</div>
          <div class="links">{''.join([f'<span class="chip">{link}</span>' for link in nav_links])}</div>
        </div>
        <div class="banner {verdict['level']}">
          {_esc(verdict['title'])}
          <p>{_esc(verdict['detail'])}</p>
        </div>
      </div>
    </section>
    <section class="section">
      <h2>Run Header</h2>
      {_table_html(['Field', 'Value'], run_header_rows, fallback='Run metadata unavailable.')}
    </section>
    <section class="section">
      <h2>Top Leads</h2>
      {_table_html(['Rank', 'Score', 'Lead family', 'Snippet', 'Agency / office label', 'Identifiers', 'Why interesting', 'Why likely noise', 'FOIA draftability', 'Dossier / evidence'], top_leads_table_rows, fallback='No lead snapshot rows available for review.')}
    </section>
    <section class="section">
      <h2>Top Lead Detail Cards</h2>
      {''.join(detail_cards) if detail_cards else '<div class="empty">No detailed lead cards are available.</div>'}
    </section>
    {diagnostics_html}
    {adjudication_html}
  </div>
</body>
</html>
"""
    html_path.write_text(html_doc, encoding="utf-8")

    md_lines = [
        "# FOIA Lead Review Board",
        "",
        f"Verdict: {verdict['title']}",
        "",
        verdict["detail"],
        "",
        "## Run Header",
        "",
        _md_table(["Field", "Value"], run_header_rows, fallback="Run metadata unavailable."),
        "",
        "## Top Leads",
        "",
        _md_table(
            ["Rank", "Score", "Lead family", "Snippet", "Agency / office", "Identifiers", "Why interesting", "Why likely noise", "FOIA draftability", "Dossier"],
            [
                [
                    item["rank"],
                    item["score"],
                    item["lead_family_label"],
                    item["snippet"],
                    item["agency_label"],
                    item["identifiers_text"],
                    item["why_interesting"],
                    item["why_likely_noise"]["summary"],
                    item["foia_draftability"]["summary"],
                    _md_link("dossier", _relative_href(md_path.parent, item["dossier_path"])) if isinstance(item["dossier_path"], Path) else "Unavailable",
                ]
                for item in derived_rows[: _TOP_LEAD_TABLE_LIMIT]
            ],
            fallback="No lead snapshot rows available for review.",
        ),
        "",
        "## Top Lead Detail Cards",
        "",
    ]
    if not derived_rows:
        md_lines.append("No detailed lead cards are available.")
        md_lines.append("")
    for item in derived_rows[: _TOP_LEAD_DETAIL_LIMIT]:
        md_lines.extend(
            [
                f"### Lead #{item['rank']} - {item['lead_family_label']}",
                "",
                f"- Score: {item['score']}",
                f"- Why likely noise: {item['why_likely_noise']['summary']}",
                f"- FOIA draftability: {item['foia_draftability']['summary']}",
                f"- Candidate evidence: {item['candidate_evidence_summary']}",
                f"- Likely next records target: {item['next_records_target']}",
                "",
                f"Signals: {'; '.join(item['positive_signals'][:5]) or 'Unavailable'}",
                "",
                f"Suppressors: {'; '.join(item['suppressors'][:5]) or 'Unavailable'}",
                "",
                f"Corroboration lanes: {', '.join(item['corroboration_lanes']) or 'Unavailable'}",
                "",
            ]
        )
    family_distribution_text = ", ".join(
        [f"{row[0]} primary={row[1]} secondary={row[2]} any={row[3]}" for row in family_rows]
    ) or "Unavailable"
    non_starter_text = ", ".join([f"{item['rule']} ({item['hits']})" for item in top_non_starter_rules]) or "Unavailable"
    md_lines.extend(
        [
            "## Run-Level Diagnostics",
            "",
            f"- Family distribution: {family_distribution_text}",
            f"- Ambiguous leads: {family_distribution.get('ambiguous_items', 0)} of {family_distribution.get('total_items', 0)}",
            f"- Score spread / compression: {_norm_text(_norm_dict(mission_quality.get('score_spread')).get('summary')) or _score_spread_summary(rows_raw)}",
            f"- Starter-only pair dominance (top {_MISSION_QUALITY_TOP_LEAD_LIMIT}): {starter_only_pct}",
            f"- Routine-noise share (top {_MISSION_QUALITY_TOP_LEAD_LIMIT}): {routine_noise_pct}",
            f"- Top non-starter packs/rules: {non_starter_text}",
            "",
        ]
    )
    if adjudication_state.get("adjudications_csv") is not None or adjudication_metrics:
        md_lines.append(_render_adjudication_md(root, adjudication_state, adjudication_metrics))

    md_path.write_text("\n".join(md_lines).strip() + "\n", encoding="utf-8")
    return {"html": html_path, "markdown": md_path}


__all__ = [
    "FOIA_LEAD_REVIEW_BOARD_HTML_PATH",
    "FOIA_LEAD_REVIEW_BOARD_MD_PATH",
    "build_foia_lead_review_diagnostics",
    "render_foia_lead_review_board_from_bundle",
]
