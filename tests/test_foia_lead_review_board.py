import json
from pathlib import Path

from backend.services.foia_review_board import (
    FOIA_LEAD_DOSSIER_INDEX_CSV_PATH,
    FOIA_LEAD_DOSSIER_INDEX_JSON_PATH,
    render_foia_lead_review_board_from_bundle,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _lead_row(
    *,
    rank: int,
    score: int,
    event_id: int,
    lead_family: str | None,
    why_summary: str,
    snippet: str,
    top_positive_signals: list[dict],
    top_suppressors: list[dict],
    contributing_lanes: list[str],
    candidate_join_evidence: list[dict],
    linked_source_summary: list[dict],
    matched_ontology_rules: list[str],
    pair_count: int,
    noise_penalty: int,
    has_foia_handles: bool,
    has_agency_target: bool,
    has_vendor_context: bool,
    has_classification_context: bool,
    source_url: str,
    solicitation_number: str | None,
    award_id: str | None,
) -> dict:
    return {
        "snapshot_id": 7,
        "snapshot_item_id": rank,
        "snapshot_scoring_version": "v3",
        "rank": rank,
        "score": score,
        "scoring_version": "v3",
        "lead_family": lead_family,
        "lead_family_label": lead_family.replace("_", " ").title() if lead_family else None,
        "secondary_lead_families": [],
        "why_summary": why_summary,
        "score_details": {
            "scoring_version": "v3",
            "proxy_relevance_score": max(score - 10, 0),
            "investigability_score": 4 if has_foia_handles else 1,
            "corroboration_score": 6 if candidate_join_evidence or len(contributing_lanes) > 1 else 1,
            "structural_context_score": 3 if has_classification_context else 1,
            "noise_penalty": noise_penalty,
            "noise_penalty_applied": noise_penalty,
            "pair_count": pair_count,
            "total_score": score,
            "matched_ontology_rules": matched_ontology_rules,
            "matched_ontology_clauses": [
                {
                    "pack": rule.split(":", 1)[0],
                    "rule": rule.split(":", 1)[1] if ":" in rule else "",
                    "weight": 3,
                }
                for rule in matched_ontology_rules
            ],
        },
        "top_positive_signals": top_positive_signals,
        "top_suppressors": top_suppressors,
        "corroboration_summary": {
            "candidate_join_evidence": candidate_join_evidence,
            "linked_source_summary": linked_source_summary,
        },
        "contributing_lanes": contributing_lanes,
        "linked_source_summary": linked_source_summary,
        "candidate_join_evidence": candidate_join_evidence,
        "event_id": event_id,
        "event_hash": f"lead-{event_id}",
        "entity_id": 900 + event_id,
        "category": "notice",
        "source": "SAM.gov",
        "doc_id": f"DOC-{event_id}",
        "source_url": source_url,
        "snippet": snippet,
        "occurred_at": "2026-03-10T00:00:00+00:00",
        "created_at": "2026-03-11T00:00:00+00:00",
        "place_text": "Arlington, VA",
        "place_region": "VA, USA",
        "solicitation_number": solicitation_number,
        "notice_id": f"NOTICE-{event_id}" if solicitation_number else None,
        "document_id": f"DOC-{event_id}",
        "award_id": award_id,
        "piid": award_id,
        "generated_unique_award_id": f"GUA-{event_id}" if award_id else None,
        "source_record_id": f"SRC-{event_id}",
        "awarding_agency_code": "DOE",
        "awarding_agency_name": "Department of Energy",
        "funding_agency_code": "NNSA",
        "funding_agency_name": "National Nuclear Security Administration",
        "contracting_office_code": "DOE-42",
        "contracting_office_name": "DOE Procurement Office",
        "recipient_name": "Acme Mission Support LLC" if has_vendor_context else None,
        "recipient_uei": "UEI-ACME" if has_vendor_context else None,
        "recipient_parent_uei": None,
        "recipient_duns": None,
        "recipient_cage_code": "CAGE-123" if has_vendor_context else None,
        "vendor_name": "Acme Mission Support LLC" if has_vendor_context else None,
        "vendor_uei": "UEI-ACME" if has_vendor_context else None,
        "vendor_parent_uei": None,
        "vendor_duns": None,
        "vendor_cage_code": "CAGE-123" if has_vendor_context else None,
        "psc_code": "R425",
        "psc_description": "Engineering and Technical Services",
        "naics_code": "541330",
        "naics_description": "Engineering Services",
        "has_core_identifiers": bool(solicitation_number or award_id),
        "has_agency_target": has_agency_target,
        "has_vendor_context": has_vendor_context,
        "has_classification_context": has_classification_context,
        "has_foia_handles": has_foia_handles,
        "completeness_summary": {},
    }


def _write_bundle(bundle: Path, *, include_adjudication: bool = False, include_leads: bool = True) -> Path:
    _write_json(
        bundle / "results" / "workflow_summary.json",
        {
            "generated_at": "2026-03-19T12:00:00+00:00",
            "scoring_version": "v3",
            "run_metadata": {
                "ingest_days": 30,
                "effective_posted_from": "2026-02-18",
                "effective_posted_to": "2026-03-19",
                "ontology_path": "examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json",
            },
            "artifacts": {},
        },
    )
    _write_json(
        bundle / "bundle_manifest.json",
        {
            "generated_at": "2026-03-19T12:00:00+00:00",
            "run_parameters": {
                "ingest_days": 30,
                "effective_posted_from": "2026-02-18",
                "effective_posted_to": "2026-03-19",
                "ontology_path": "examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json",
            },
        },
    )

    if include_leads:
        lead_rows = [
            _lead_row(
                rank=1,
                score=22,
                event_id=101,
                lead_family="vendor_network_contract_lineage",
                why_summary="lead_family=vendor_network_contract_lineage | signals: proxy follow-on context | corroboration: cross-source join",
                snippet="Follow-on engineering support with cross-source incumbent clues and stable procurement handles.",
                top_positive_signals=[{"label": "proxy follow-on context", "bucket": "proxy_relevance", "contribution": 6}],
                top_suppressors=[],
                contributing_lanes=["sam_usaspending_candidate_join", "same_entity", "kw_pair"],
                candidate_join_evidence=[
                    {
                        "status": "candidate",
                        "evidence_types": ["identifier_exact", "contract_family"],
                        "linked_sources": ["USAspending"],
                        "score_signal": 72,
                        "matched_values": {"piid": "PIID-101"},
                    }
                ],
                linked_source_summary=[
                    {
                        "source": "USAspending",
                        "linked_event_count": 3,
                        "lanes": ["sam_usaspending_candidate_join"],
                    }
                ],
                matched_ontology_rules=[
                    "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context",
                    "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
                ],
                pair_count=1,
                noise_penalty=0,
                has_foia_handles=True,
                has_agency_target=True,
                has_vendor_context=True,
                has_classification_context=True,
                source_url="https://sam.gov/opp/101",
                solicitation_number="SOL-101",
                award_id=None,
            ),
            _lead_row(
                rank=2,
                score=9,
                event_id=102,
                lead_family=None,
                why_summary="starter/context support with pair bonus but weak corroboration",
                snippet="Generic facilities support notice with routine admin language and thin corroboration.",
                top_positive_signals=[{"label": "starter or context ontology support", "bucket": "structural_context", "contribution": 2}],
                top_suppressors=[{"label": "operational_noise_terms:admin_facility_ops_noise", "penalty": 4}],
                contributing_lanes=["kw_pair"],
                candidate_join_evidence=[],
                linked_source_summary=[],
                matched_ontology_rules=["sam_procurement_starter:idiq_vehicle"],
                pair_count=2,
                noise_penalty=5,
                has_foia_handles=False,
                has_agency_target=False,
                has_vendor_context=False,
                has_classification_context=False,
                source_url="https://sam.gov/opp/102",
                solicitation_number=None,
                award_id=None,
            ),
            _lead_row(
                rank=3,
                score=16,
                event_id=103,
                lead_family="range_test_infrastructure",
                why_summary="lead_family=range_test_infrastructure | signals: range telemetry support | corroboration: same_agency",
                snippet="Range telemetry instrumentation support with clear office and contract handles.",
                top_positive_signals=[{"label": "range telemetry support", "bucket": "proxy_relevance", "contribution": 5}],
                top_suppressors=[],
                contributing_lanes=["same_agency", "kw_pair"],
                candidate_join_evidence=[],
                linked_source_summary=[],
                matched_ontology_rules=["sam_dod_flight_test_range_instrumentation:range_telemetry_support_services"],
                pair_count=1,
                noise_penalty=1,
                has_foia_handles=True,
                has_agency_target=True,
                has_vendor_context=False,
                has_classification_context=True,
                source_url="https://sam.gov/opp/103",
                solicitation_number="SOL-103",
                award_id=None,
            ),
        ]
        _write_json(
            bundle / "exports" / "lead_snapshot.json",
            {
                "count": len(lead_rows),
                "scoring_version": "v3",
                "family_groups": [],
                "items": lead_rows,
            },
        )
        _write_json(
            bundle / "exports" / "review_summary.json",
            {
                "scoring_version": "v3",
                "effective_window": {
                    "earliest": "2026-03-10T00:00:00+00:00",
                    "latest": "2026-03-11T00:00:00+00:00",
                    "span_days": 1,
                },
                "completeness_counts": {
                    "has_core_identifiers": 2,
                    "has_agency_target": 2,
                    "has_vendor_context": 1,
                    "has_classification_context": 2,
                    "has_foia_handles": 2,
                },
            },
        )

    if include_adjudication:
        adjudications_csv = bundle / "exports" / "lead_adjudications.csv"
        adjudications_csv.parent.mkdir(parents=True, exist_ok=True)
        adjudications_csv.write_text(
            "snapshot_id,snapshot_item_id,rank,decision,foia_ready\n7,1,1,keep,yes\n7,2,2,reject,no\n",
            encoding="utf-8",
        )
        _write_json(
            bundle / "exports" / "lead_adjudication_metrics.json",
            {
                "summary": {
                    "reviewed_count": 2,
                    "decisive_count": 2,
                    "acceptance_rate_pct": 50.0,
                    "foia_ready_yes_count": 1,
                    "precision_at_k": {
                        "1": {
                            "k": 1,
                            "precision_pct": 100.0,
                            "reviewed_count": 1,
                            "decisive_count": 1,
                            "keep_count": 1,
                            "reject_count": 0,
                        }
                    },
                },
                "by_lead_family": [
                    {
                        "lead_family": "vendor_network_contract_lineage",
                        "row_count": 1,
                        "keep_count": 1,
                        "reject_count": 0,
                        "acceptance_rate_pct": 100.0,
                    }
                ],
                "rejection_reasons": [
                    {"reason_code": "routine_noise", "count": 1, "share_of_rejects_pct": 100.0}
                ],
            },
        )

    return bundle


def test_foia_lead_review_board_contains_required_sections_and_links(tmp_path: Path):
    bundle = _write_bundle(tmp_path / "bundle", include_adjudication=False, include_leads=True)

    artifacts = render_foia_lead_review_board_from_bundle(bundle)

    html = artifacts["html"].read_text(encoding="utf-8")
    markdown = artifacts["markdown"].read_text(encoding="utf-8")
    dossier_index = json.loads((bundle / FOIA_LEAD_DOSSIER_INDEX_JSON_PATH).read_text(encoding="utf-8"))

    assert "FOIA Lead Review Board" in html
    assert "Run Header" in html
    assert "Top Leads" in html
    assert "Top Lead Detail Cards" in html
    assert "Run-Level Diagnostics" in html
    assert "Why likely noise" in html
    assert "FOIA draftability" in html
    assert "Dossier index JSON" in html
    assert "Dossier index CSV" in html
    assert "lead_dossiers/lead_001_event_101.md" in html
    assert "starter-only ontology support" in html or "starter-only pair support" in html
    assert "Routine-Noise Share" in html
    assert "Top Non-Starter Packs / Rules" in html
    assert "FOIA Lead Review Board" in markdown
    assert "## Top Leads" in markdown
    assert (bundle / "report" / "lead_dossiers" / "lead_001_event_101.md").exists()
    assert (bundle / FOIA_LEAD_DOSSIER_INDEX_CSV_PATH).exists()
    assert dossier_index["count"] == 3
    assert dossier_index["items"][0]["dossier_path"] == "report/lead_dossiers/lead_001_event_101.md"


def test_foia_lead_review_board_handles_empty_or_partial_bundles(tmp_path: Path):
    bundle = _write_bundle(tmp_path / "bundle_empty", include_adjudication=False, include_leads=False)

    artifacts = render_foia_lead_review_board_from_bundle(bundle)

    html = artifacts["html"].read_text(encoding="utf-8")
    markdown = artifacts["markdown"].read_text(encoding="utf-8")

    assert "No reviewable FOIA leads exported." in html
    assert "No lead snapshot rows available for review." in html
    assert "No detailed lead cards are available." in html
    assert "No reviewable FOIA leads exported." in markdown


def test_foia_lead_review_board_renders_adjudication_when_present(tmp_path: Path):
    bundle = _write_bundle(tmp_path / "bundle_eval", include_adjudication=True, include_leads=True)

    artifacts = render_foia_lead_review_board_from_bundle(bundle)

    html = artifacts["html"].read_text(encoding="utf-8")
    markdown = artifacts["markdown"].read_text(encoding="utf-8")

    assert "Adjudication" in html
    assert "Precision @ k" in html
    assert "routine_noise" in html
    assert "Adjudication metrics" in html
    assert "## Adjudication" in markdown
    assert "### Precision @ k" in markdown


def test_foia_lead_review_board_dossiers_handle_missing_fields_conservatively(tmp_path: Path):
    bundle = _write_bundle(tmp_path / "bundle_partial", include_adjudication=False, include_leads=False)
    partial_row = _lead_row(
        rank=1,
        score=7,
        event_id=201,
        lead_family=None,
        why_summary="Thin partial row for missing-field handling.",
        snippet="Partial row with limited serialized evidence.",
        top_positive_signals=[],
        top_suppressors=[],
        contributing_lanes=["kw_pair"],
        candidate_join_evidence=[],
        linked_source_summary=[],
        matched_ontology_rules=[],
        pair_count=0,
        noise_penalty=0,
        has_foia_handles=False,
        has_agency_target=False,
        has_vendor_context=False,
        has_classification_context=False,
        source_url="",
        solicitation_number=None,
        award_id=None,
    )
    partial_row["psc_code"] = None
    partial_row["psc_description"] = None
    partial_row["naics_code"] = None
    partial_row["naics_description"] = None
    partial_row["place_text"] = None
    partial_row["place_region"] = None
    partial_row["source_url"] = None
    partial_row["entity_id"] = None
    partial_row["occurred_at"] = None
    partial_row["created_at"] = None
    partial_row["doc_id"] = None
    partial_row["document_id"] = None
    partial_row["source_record_id"] = None

    _write_json(
        bundle / "exports" / "lead_snapshot.json",
        {
            "count": 1,
            "scoring_version": "v3",
            "family_groups": [],
            "items": [partial_row],
        },
    )
    _write_json(
        bundle / "exports" / "review_summary.json",
        {
            "scoring_version": "v3",
            "effective_window": {
                "earliest": "2026-03-10T00:00:00+00:00",
                "latest": "2026-03-10T00:00:00+00:00",
                "span_days": 0,
            },
            "completeness_counts": {},
        },
    )

    render_foia_lead_review_board_from_bundle(bundle)

    dossier_text = (bundle / "report" / "lead_dossiers" / "lead_001_event_201.md").read_text(encoding="utf-8")
    dossier_index = json.loads((bundle / FOIA_LEAD_DOSSIER_INDEX_JSON_PATH).read_text(encoding="utf-8"))

    assert "No vendor/entity block available." in dossier_text
    assert "No PSC / NAICS classification anchors available." in dossier_text
    assert "No place/time anchors available." in dossier_text
    assert dossier_index["items"][0]["identifiers"] == []
    assert dossier_index["items"][0]["dossier_path"] == "report/lead_dossiers/lead_001_event_201.md"
