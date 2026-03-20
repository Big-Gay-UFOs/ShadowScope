import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from backend.services.adjudication import evaluate_lead_adjudications, export_lead_adjudication_template
from backend.services.bundle import SAM_BUNDLE_VERSION
from backend.db.models import Event, LeadSnapshotItem, ensure_schema, get_session_factory
from backend.services.foia_review_board import (
    FOIA_LEAD_DOSSIER_EVIDENCE_DIR,
    FOIA_LEAD_DOSSIER_INDEX_CSV_PATH,
    FOIA_LEAD_DOSSIER_INDEX_JSON_PATH,
)
import backend.services.workflow as workflow_module
from backend.services.workflow import (
    run_samgov_evaluation_workflow,
    run_samgov_smoke_workflow,
    run_samgov_validation_workflow,
    run_samgov_workflow,
    run_usaspending_workflow,
)


def _seed_sam_events(db, now: datetime) -> None:
    db.add_all(
        [
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="samwf1",
                created_at=now,
                doc_id="SAM-001",
                source_url="https://sam.gov/opp/1",
                snippet="Sources Sought RFP for engineering support with NAICS context",
                raw_json={
                    "noticeId": "SAM-001",
                    "title": "Sources Sought RFP Engineering Support",
                    "noticeType": "Sources Sought",
                    "solicitationNumber": "DOE-RFP-001",
                    "naicsCode": "541330",
                    "naicsDescription": "Engineering Services",
                    "typeOfSetAside": "SBA",
                    "typeOfSetAsideDescription": "Total Small Business Set-Aside",
                    "responseDeadLine": "2026-03-15",
                    "fullParentPathName": "Department of Energy",
                    "fullParentPathCode": "DOE.HQ",
                    "Recipient Name": "Acme Federal",
                },
                keywords=[],
                clauses=[],
            ),
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="samwf2",
                created_at=now,
                doc_id="SAM-002",
                source_url="https://sam.gov/opp/2",
                snippet="Sources Sought RFP for engineering sustainment with NAICS references",
                raw_json={
                    "noticeId": "SAM-002",
                    "title": "Sources Sought RFP Engineering Sustainment",
                    "noticeType": "Sources Sought",
                    "solicitationNumber": "DOE-RFP-002",
                    "naicsCode": "541330",
                    "naicsDescription": "Engineering Services",
                    "typeOfSetAside": "SBA",
                    "typeOfSetAsideDescription": "Total Small Business Set-Aside",
                    "responseDeadLine": "2026-03-16",
                    "fullParentPathName": "Department of Energy",
                    "fullParentPathCode": "DOE.HQ",
                    "Recipient Name": "Acme Federal",
                },
                keywords=[],
                clauses=[],
            ),
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="samwf3",
                created_at=now,
                doc_id="SAM-003",
                source_url="https://sam.gov/opp/3",
                snippet="Request for Proposal NAICS cybersecurity operations work",
                raw_json={
                    "noticeId": "SAM-003",
                    "title": "RFP Cybersecurity Operations",
                    "noticeType": "Solicitation",
                    "solicitationNumber": "DOE-RFP-003",
                    "naicsCode": "541512",
                    "naicsDescription": "Computer Systems Design Services",
                    "typeOfSetAside": "8A",
                    "typeOfSetAsideDescription": "8(a) Set-Aside",
                    "responseDeadLine": "2026-03-20",
                    "fullParentPathName": "Department of Energy",
                    "fullParentPathCode": "DOE.FIELD",
                    "Recipient Name": "Field Ops",
                },
                keywords=[],
                clauses=[],
            ),
        ]
    )


def _seed_usaspending_events(db, now: datetime) -> None:
    db.add_all(
        [
            Event(
                category="award",
                source="USAspending",
                hash="usawf1",
                created_at=now,
                doc_id="95C67826P0143",
                source_url="https://www.usaspending.gov/award/95C67826P0143",
                snippet=(
                    "FY26 DCSC-22-RFQ-59-D OPTION YEAR 4 PERIOD OF PERFORMANCE MARCH 1, 2026 "
                    "THROUGH FEBRUARY 28, 2027. THE TOTAL AMOUNT OF THIS BPA WILL BE $200K"
                ),
                raw_json={
                    "Award ID": "95C67826P0143",
                    "Recipient Name": "SERVICE MACHINE SHOP, INCORPORATED",
                    "Recipient UEI": "UEI-111",
                    "Description": (
                        "FY26 DCSC-22-RFQ-59-D OPTION YEAR 4 PERIOD OF PERFORMANCE MARCH 1, 2026 "
                        "THROUGH FEBRUARY 28, 2027. THE TOTAL AMOUNT OF THIS BPA WILL BE $200K"
                    ),
                },
                keywords=[],
                clauses=[],
            ),
            Event(
                category="award",
                source="USAspending",
                hash="usawf2",
                created_at=now,
                doc_id="95C67826P0142",
                source_url="https://www.usaspending.gov/award/95C67826P0142",
                snippet=(
                    "FY26 DCSC-22-RFQ-59-A OPTION YEAR 4 PERIOD OF PERFORMANCE MARCH 1, 2026 "
                    "THROUGH FEBRUARY 28, 2027. THE TOTAL AMOUNT OF THIS BPA WILL BE $300K"
                ),
                raw_json={
                    "Award ID": "95C67826P0142",
                    "Recipient Name": "RSC ELECTRICAL & MECHANICAL CONTRACTORS, INC.",
                    "Recipient UEI": "UEI-111",
                    "Description": (
                        "FY26 DCSC-22-RFQ-59-A OPTION YEAR 4 PERIOD OF PERFORMANCE MARCH 1, 2026 "
                        "THROUGH FEBRUARY 28, 2027. THE TOTAL AMOUNT OF THIS BPA WILL BE $300K"
                    ),
                },
                keywords=[],
                clauses=[],
            ),
            Event(
                category="award",
                source="USAspending",
                hash="usawf3",
                created_at=now,
                doc_id="TASK-001",
                source_url="https://www.usaspending.gov/award/TASK-001",
                snippet="TASK ORDER FOR SOFTWARE MAINTENANCE RENEWAL AND SUPPORT SERVICES",
                raw_json={
                    "Award ID": "TASK-001",
                    "Recipient Name": "MAGADIA CONSULTING INC",
                    "Recipient UEI": "UEI-222",
                    "Description": "TASK ORDER FOR SOFTWARE MAINTENANCE RENEWAL AND SUPPORT SERVICES",
                },
                keywords=[],
                clauses=[],
            ),
            Event(
                category="award",
                source="USAspending",
                hash="usawf4",
                created_at=now,
                doc_id="CALL-002",
                source_url="https://www.usaspending.gov/award/CALL-002",
                snippet="CALL ORDER SUBSCRIPTION SERVICE PURCHASE FOR SIEM CERTIFICATES",
                raw_json={
                    "Award ID": "CALL-002",
                    "Recipient Name": "MAGADIA CONSULTING INC",
                    "Recipient UEI": "UEI-222",
                    "Description": "CALL ORDER SUBSCRIPTION SERVICE PURCHASE FOR SIEM CERTIFICATES",
                },
                keywords=[],
                clauses=[],
            ),
            Event(
                category="award",
                source="USAspending",
                hash="usawf5",
                created_at=now,
                doc_id="LIC-003",
                source_url="https://www.usaspending.gov/award/LIC-003",
                snippet="SOFTWARE LICENSE RENEWAL WITH CLOUD HOSTING SUPPORT SERVICES AND CYBERSECURITY OPERATIONS SUPPORT",
                raw_json={
                    "Award ID": "LIC-003",
                    "Recipient Name": "MAGADIA CONSULTING INC",
                    "Recipient UEI": "UEI-222",
                    "Description": "SOFTWARE LICENSE RENEWAL WITH CLOUD HOSTING SUPPORT SERVICES AND CYBERSECURITY OPERATIONS SUPPORT",
                },
                keywords=[],
                clauses=[],
            ),
            Event(
                category="award",
                source="USAspending",
                hash="usawf6",
                created_at=now,
                doc_id="TRN-004",
                source_url="https://www.usaspending.gov/award/TRN-004",
                snippet="CYBERSECURITY TRAINING SERVICES SUPPORT FOR SOFTWARE PLATFORM LICENSE MAINTENANCE",
                raw_json={
                    "Award ID": "TRN-004",
                    "Recipient Name": "MAGADIA CONSULTING INC",
                    "Recipient UEI": "UEI-333",
                    "Description": "CYBERSECURITY TRAINING SERVICES SUPPORT FOR SOFTWARE PLATFORM LICENSE MAINTENANCE",
                },
                keywords=[],
                clauses=[],
            )
        ]
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _mission_review_row(
    *,
    rank: int,
    score: int,
    scoring_version: str,
    event_id: int,
    lead_family: str | None,
    matched_rules: list[str],
    pair_count: int,
    top_suppressors: list[dict],
    contributing_lanes: list[str],
    has_core_identifiers: bool,
    has_agency_target: bool,
    has_vendor_context: bool,
    has_classification_context: bool,
    has_foia_handles: bool,
    solicitation_number: str | None = None,
    award_id: str | None = None,
    candidate_join_evidence: list[dict] | None = None,
    linked_source_summary: list[dict] | None = None,
) -> dict:
    candidate_join_evidence = list(candidate_join_evidence or [])
    linked_source_summary = list(linked_source_summary or [])
    family_label = lead_family.replace("_", " ").title() if lead_family else None
    source_url = f"https://sam.gov/opp/{event_id}" if has_foia_handles else None
    score_details = {
        "scoring_version": scoring_version,
        "matched_ontology_rules": matched_rules,
        "matched_ontology_clauses": [
            {
                "pack": rule.split(":", 1)[0],
                "rule": rule.split(":", 1)[1] if ":" in rule else "",
                "weight": 3,
            }
            for rule in matched_rules
        ],
        "pair_count": pair_count,
        "pair_bonus": pair_count,
        "pair_bonus_applied": pair_count,
        "corroboration_score": 6 if len(contributing_lanes) > 1 or candidate_join_evidence else 1,
        "noise_penalty": sum(int(item.get("penalty") or 0) for item in top_suppressors),
        "noise_penalty_applied": sum(int(item.get("penalty") or 0) for item in top_suppressors),
        "total_score": score,
    }
    if scoring_version == "v3":
        score_details.update(
            {
                "proxy_relevance_score": max(score - 12, 0),
                "investigability_score": 4 if has_foia_handles else 1,
                "structural_context_score": 3 if has_classification_context else 1,
            }
        )
    else:
        score_details.update(
            {
                "clause_score": max(score - 8, 0),
                "keyword_score": 3 if matched_rules else 0,
                "entity_bonus": 1 if has_vendor_context else 0,
            }
        )

    return {
        "snapshot_id": 77,
        "snapshot_item_id": rank,
        "snapshot_scoring_version": scoring_version,
        "rank": rank,
        "score": score,
        "scoring_version": scoring_version,
        "lead_family": lead_family,
        "lead_family_label": family_label,
        "secondary_lead_families": [],
        "why_summary": f"lead_family={lead_family or 'unassigned'} | ranked reviewer fixture",
        "score_details": score_details,
        "top_positive_signals": [
            {
                "label": matched_rules[0] if matched_rules else "starter/context support",
                "bucket": "proxy_relevance" if matched_rules and not matched_rules[0].startswith("sam_procurement_starter:") else "structural_context",
                "contribution": 4,
            }
        ],
        "top_suppressors": top_suppressors,
        "corroboration_summary": {
            "candidate_join_evidence": candidate_join_evidence,
            "linked_source_summary": linked_source_summary,
        },
        "contributing_lanes": contributing_lanes,
        "linked_source_summary": linked_source_summary,
        "candidate_join_evidence": candidate_join_evidence,
        "event_id": event_id,
        "event_hash": f"mission-{event_id}",
        "entity_id": 5000 + event_id if has_vendor_context else None,
        "category": "notice",
        "source": "SAM.gov",
        "doc_id": f"DOC-{event_id}" if has_core_identifiers else None,
        "source_url": source_url,
        "snippet": f"Ranked mission review fixture row {event_id}",
        "occurred_at": "2026-03-10T00:00:00+00:00",
        "created_at": "2026-03-11T00:00:00+00:00",
        "place_text": "Arlington, VA" if has_classification_context else None,
        "place_region": "VA, USA" if has_classification_context else None,
        "solicitation_number": solicitation_number,
        "notice_id": f"NOTICE-{event_id}" if solicitation_number else None,
        "document_id": f"DOC-{event_id}" if has_core_identifiers else None,
        "award_id": award_id,
        "piid": award_id,
        "generated_unique_award_id": f"GUA-{event_id}" if award_id else None,
        "source_record_id": f"SRC-{event_id}" if has_foia_handles else None,
        "awarding_agency_code": "DOE" if has_agency_target else None,
        "awarding_agency_name": "Department of Energy" if has_agency_target else None,
        "funding_agency_code": "NNSA" if has_agency_target else None,
        "funding_agency_name": "National Nuclear Security Administration" if has_agency_target else None,
        "contracting_office_code": "DOE-42" if has_agency_target else None,
        "contracting_office_name": "DOE Procurement Office" if has_agency_target else None,
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
        "psc_code": "R425" if has_classification_context else None,
        "psc_description": "Engineering and Technical Services" if has_classification_context else None,
        "naics_code": "541330" if has_classification_context else None,
        "naics_description": "Engineering Services" if has_classification_context else None,
        "has_core_identifiers": has_core_identifiers,
        "has_agency_target": has_agency_target,
        "has_vendor_context": has_vendor_context,
        "has_classification_context": has_classification_context,
        "has_foia_handles": has_foia_handles,
        "completeness_summary": {},
    }


def _write_mission_review_exports(tmp_path: Path, *, scoring_version: str, rows: list[dict]) -> dict:
    tmp_path.mkdir(parents=True, exist_ok=True)
    lead_csv = tmp_path / "mission_review_fixture.csv"
    lead_csv.write_text("rank,score\n", encoding="utf-8")
    lead_json = tmp_path / "mission_review_fixture.json"
    review_summary_json = tmp_path / "mission_review_fixture_review_summary.json"

    completeness_counts = {
        field: sum(1 for row in rows if bool(row.get(field)))
        for field in (
            "has_core_identifiers",
            "has_agency_target",
            "has_vendor_context",
            "has_classification_context",
            "has_foia_handles",
        )
    }

    _write_json(
        lead_json,
        {
            "count": len(rows),
            "scoring_version": scoring_version,
            "family_groups": [],
            "items": rows,
        },
    )
    _write_json(
        review_summary_json,
        {
            "scoring_version": scoring_version,
            "effective_window": {
                "earliest": "2026-03-10T00:00:00+00:00",
                "latest": "2026-03-11T00:00:00+00:00",
                "span_days": 1,
            },
            "completeness_counts": completeness_counts,
        },
    )
    return {
        "lead_snapshot": {
            "csv": lead_csv,
            "json": lead_json,
            "review_summary_json": review_summary_json,
            "count": len(rows),
        }
    }


def _healthy_sam_doctor_status() -> dict:
    return {
        "db": {"status": "ok"},
        "counts": {
            "events_window": 60,
            "events_with_entity_window": 48,
            "lead_snapshots_total": 1,
        },
        "keywords": {
            "scanned_events": 60,
            "events_with_keywords": 52,
            "coverage_pct": 86.7,
            "unique_keywords": 12,
        },
        "entities": {
            "window_linked_coverage_pct": 80.0,
            "sample_scanned_events": 60,
            "sample_events_with_identity_signal": 60,
            "sample_events_with_identity_signal_linked": 48,
            "sample_identity_signal_coverage_pct": 80.0,
            "sample_events_with_name": 60,
            "sample_events_with_name_linked": 48,
            "sample_name_coverage_pct": 80.0,
        },
        "correlations": {
            "by_lane": {
                "same_keyword": 9,
                "kw_pair": 14,
                "same_sam_naics": 4,
                "same_entity": 6,
                "same_uei": 2,
            }
        },
        "sam_context": {
            "scanned_events": 60,
            "events_with_research_context": 50,
            "research_context_coverage_pct": 83.3,
            "events_with_core_procurement_context": 53,
            "core_procurement_context_coverage_pct": 88.3,
            "avg_context_fields_per_event": 4.2,
            "coverage_by_field_pct": {
                "sam_notice_type": 100.0,
                "sam_solicitation_number": 96.7,
                "sam_naics_code": 86.7,
            },
            "top_notice_types": [],
            "top_naics_codes": [],
            "top_set_aside_codes": [],
        },
        "hints": [],
    }


def _fill_adjudication_csv(path: Path, updates: dict[int, dict[str, str]]) -> None:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    for row in rows:
        rank = int(row["rank"])
        row.update(updates.get(rank, {}))

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_workflow_wrapper_file_output_does_not_clobber(tmp_path: Path):
    db_path = tmp_path / "wf.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(category="award", source="USAspending", hash="wf1", created_at=now, raw_json={"Recipient Name": "Acme Corp", "UEI": "UEI123"}, keywords=["alpha", "beta"], clauses=[]),
                Event(category="award", source="USAspending", hash="wf2", created_at=now, raw_json={"Recipient Name": "Acme Corp", "UEI": "UEI123"}, keywords=["alpha", "beta"], clauses=[]),
                Event(category="award", source="USAspending", hash="wf3", created_at=now, raw_json={"Recipient Name": "Other Corp", "UEI": "UEI999"}, keywords=["alpha", "gamma"], clauses=[]),
            ]
        )
        db.commit()

    # File-style output (suffix present) must not cause exporters to overwrite each other
    out_file = tmp_path / "reports" / "run.csv"

    res = run_usaspending_workflow(
        database_url=db_url,
        output=out_file,
        skip_ingest=True,
        skip_ontology=True,
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,  # make alpha+beta eligible
        max_events_keywords=200,
        max_keywords_per_event=10,
        export_events_flag=False,
    )

    ex = res["exports"]
    ls_csv = Path(ex["lead_snapshot"]["csv"])
    kw_csv = Path(ex["kw_pairs"]["csv"])
    en_csv = Path(ex["entities"]["entities_csv"])
    map_csv = Path(ex["entities"]["event_entities_csv"])

    assert ls_csv.exists()
    assert kw_csv.exists()
    assert en_csv.exists()
    assert map_csv.exists()

    # Ensure distinct stems (no intra-run clobber)
    assert len({ls_csv.name, kw_csv.name, en_csv.name}) == 3

    assert ls_csv.name.startswith("run_lead_snapshot_")
    assert kw_csv.name.startswith("run_kw_pairs_")
    assert en_csv.name.startswith("run_entities_")

    assert ex["kw_pairs"]["count"] >= 1


def test_usaspending_workflow_starter_ontology_produces_keyword_signal(tmp_path: Path):
    db_path = tmp_path / "usaspending_workflow.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_usaspending_events(db, now)
        db.commit()

    res = run_usaspending_workflow(
        database_url=db_url,
        output=tmp_path / "reports" / "usaspending_run.csv",
        skip_ingest=True,
        ontology_path=Path("examples/ontology_usaspending_starter.json"),
        ontology_days=30,
        window_days=30,
        min_events_entity=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        export_events_flag=False,
        min_score=1,
        snapshot_limit=200,
    )

    assert res["source"] == "USAspending"
    assert res["ontology_apply"]["scanned"] >= 6
    assert res["ontology_apply"]["updated"] >= 5
    assert res["correlations"]["same_keyword"]["min_events"] == 2
    assert res["correlations"]["same_keyword"]["eligible_keywords"] >= 1
    assert res["correlations"]["kw_pair"]["eligible_pairs"] >= 1
    assert res["snapshot"]["items"] > 0

    with SessionFactory() as db:
        rows = db.query(Event).filter(Event.source == "USAspending").all()

    with_keywords = [ev for ev in rows if isinstance(ev.keywords, list) and len(ev.keywords) > 0]
    multi_keyword_events = [ev for ev in rows if isinstance(ev.keywords, list) and len(ev.keywords) >= 2]
    assert len(with_keywords) >= 3
    assert len(multi_keyword_events) >= 2

    all_keywords = sorted({kw for ev in with_keywords for kw in (ev.keywords or [])})
    assert any(kw.startswith("procurement_lifecycle:") for kw in all_keywords)
    assert "sustainment_it_ops:software_license_renewal_support" in all_keywords
    assert "sustainment_it_ops:cloud_ops_support_services" in all_keywords


def test_samgov_workflow_fixture_runs_end_to_end(tmp_path: Path):
    db_path = tmp_path / "sam_workflow.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    out_file = tmp_path / "reports" / "sam_run.csv"
    res = run_samgov_workflow(
        database_url=db_url,
        output=out_file,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        ontology_days=30,
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        export_events_flag=True,
        min_score=1,
        snapshot_limit=200,
    )

    assert res["source"] == "SAM.gov"
    assert res["ontology_apply"]["scanned"] >= 3
    assert res["entities_link"]["linked"] >= 2
    assert res["correlations"]["same_entity"]["eligible_entities"] >= 1
    assert res["correlations"]["same_keyword"]["min_events"] == 2
    assert res["correlations"]["same_keyword"]["eligible_keywords"] >= 1
    assert res["correlations"]["same_sam_naics"]["eligible_naics"] >= 1
    assert res["snapshot"]["items"] > 0

    ex = res["exports"]
    assert Path(ex["lead_snapshot"]["csv"]).exists()
    assert Path(ex["kw_pairs"]["csv"]).exists()
    assert Path(ex["entities"]["entities_csv"]).exists()
    assert Path(ex["entities"]["event_entities_csv"]).exists()
    assert Path(ex["events"]["csv"]).exists()

    lead_payload = json.loads(Path(ex["lead_snapshot"]["json"]).read_text(encoding="utf-8"))
    assert lead_payload["count"] > 0
    assert any(item.get("source") == "SAM.gov" for item in lead_payload.get("items", []))
    assert any(item.get("doc_id") for item in lead_payload.get("items", []))
    assert any(item.get("source_url") for item in lead_payload.get("items", []))


def test_samgov_workflow_snapshot_respects_explicit_occurred_window(tmp_path: Path):
    db_path = tmp_path / "sam_workflow_occurred_window.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_window_recent",
                    occurred_at=now - timedelta(days=1),
                    created_at=now - timedelta(days=7),
                    snippet="Recent SAM event inside explicit occurred_at window",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_window_old",
                    occurred_at=now - timedelta(days=10),
                    created_at=now - timedelta(hours=1),
                    snippet="Old SAM event outside explicit occurred_at window",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="award",
                    source="USAspending",
                    hash="usa_window_latest",
                    occurred_at=now - timedelta(minutes=5),
                    created_at=now - timedelta(minutes=5),
                    snippet="Recent cross-source event that should not affect SAM snapshot filtering",
                    raw_json={},
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = run_samgov_workflow(
        database_url=db_url,
        skip_ingest=True,
        skip_ontology=True,
        skip_entities=True,
        skip_correlations=True,
        skip_exports=True,
        occurred_after=now - timedelta(days=2),
        min_score=0,
        snapshot_limit=10,
        scan_limit=10,
        scoring_version="v1",
    )

    assert res["snapshot"]["items"] == 1
    assert res["snapshot"]["scanned"] == 1

    with SessionFactory() as db:
        snapshot_items = (
            db.query(LeadSnapshotItem)
            .filter(LeadSnapshotItem.snapshot_id == int(res["snapshot"]["snapshot_id"]))
            .order_by(LeadSnapshotItem.rank.asc())
            .all()
        )

    assert [item.event_hash for item in snapshot_items] == ["sam_window_recent"]


def test_samgov_smoke_bundle_fixture_captures_baseline(tmp_path: Path):
    db_path = tmp_path / "sam_smoke.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_smoke_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "smoke_artifacts",
        require_nonzero=True,
    )

    assert res["status"] == "warning"
    assert res["smoke_passed"] is True

    artifacts = res["artifacts"]
    bundle_dir = Path(res["bundle_dir"])
    summary_path = Path(artifacts["smoke_summary_json"])
    doctor_path = Path(artifacts["doctor_status_json"])
    workflow_path = Path(artifacts["workflow_result_json"])
    manifest_path = Path(artifacts["bundle_manifest_json"])
    report_path = Path(artifacts["report_html"])
    review_board_path = Path(artifacts["foia_lead_review_board_html"])
    review_board_md_path = Path(artifacts["foia_lead_review_board_md"])
    dossier_index_json_path = bundle_dir / FOIA_LEAD_DOSSIER_INDEX_JSON_PATH
    dossier_index_csv_path = bundle_dir / FOIA_LEAD_DOSSIER_INDEX_CSV_PATH
    dossier_evidence_dir = bundle_dir / FOIA_LEAD_DOSSIER_EVIDENCE_DIR

    assert summary_path.exists()
    assert doctor_path.exists()
    assert workflow_path.exists()
    assert manifest_path.exists()
    assert report_path.exists()
    assert review_board_path.exists()
    assert review_board_md_path.exists()
    assert dossier_index_json_path.exists()
    assert dossier_index_csv_path.exists()
    assert dossier_evidence_dir.exists()

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["workflow_status"] == "warning"
    assert summary_payload["quality"] == "degraded"
    assert summary_payload["smoke_passed"] is True
    assert summary_payload["required_checks_passed"] is True
    assert summary_payload["has_required_failures"] is False
    assert summary_payload["has_advisory_failures"] is True
    assert summary_payload["has_usable_artifacts"] is True
    assert summary_payload["partially_useful"] is True
    assert summary_payload["comparison_requested"] is False
    assert summary_payload["comparison_available"] is False
    assert summary_payload["comparison_empty"] is False
    assert summary_payload["reason_codes"]
    assert summary_payload["operator_messages"]
    assert summary_payload["scoring_version"] == "v3"
    check_names = {c.get("name") for c in summary_payload.get("checks", [])}
    assert "events_window_threshold" in check_names
    assert "sam_research_context_events_threshold" in check_names
    assert "snapshot_items_threshold" in check_names
    assert "scoring_version_is_v3" in check_names

    assert summary_payload.get("thresholds")
    assert summary_payload.get("quality_gate_policy", {}).get("required_checks")
    assert summary_payload.get("failed_required_checks") == []

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload.get("bundle_version") == SAM_BUNDLE_VERSION
    assert manifest_payload.get("workflow_type") == "samgov-smoke"
    assert manifest_payload.get("scoring_version") == "v3"
    assert manifest_payload.get("workflow_status") == "warning"
    assert manifest_payload.get("quality") == "degraded"
    assert manifest_payload.get("lead_dossier_top_n") == 10
    assert (manifest_payload.get("lead_dossiers") or {}).get("count") == 3
    generated_files = manifest_payload.get("generated_files") or {}
    assert "workflow_result_json" in generated_files
    assert "workflow_summary_json" in generated_files
    assert "bundle_manifest_json" in generated_files
    assert "report_html" in generated_files
    assert "foia_lead_review_board_html" in generated_files
    assert "foia_lead_review_board_md" in generated_files
    assert "export_lead_review_summary_json" in generated_files
    assert "lead_dossier_index_json" in generated_files
    assert "lead_dossier_index_csv" in generated_files

    exports = artifacts.get("exports") or {}
    lead_export = exports.get("lead_snapshot") or {}
    kw_export = exports.get("kw_pairs") or {}
    entities_export = exports.get("entities") or {}
    events_export = exports.get("events") or {}
    assert Path(lead_export.get("csv")).name == "lead_snapshot.csv"
    assert Path(kw_export.get("csv")).name == "keyword_pairs.csv"
    assert Path(entities_export.get("entities_csv")).name == "entities.csv"
    assert Path(entities_export.get("event_entities_csv")).name == "event_entities.csv"
    assert Path(events_export.get("csv")).name == "events.csv"
    assert Path(lead_export.get("review_summary_json")).name == "review_summary.json"

    report_html = report_path.read_text(encoding="utf-8")
    assert "SAM.gov Workflow Bundle Report" in report_html
    assert "foia_lead_review_board.html" in report_html
    assert "workflow_type=samgov-smoke" in report_html
    assert "scoring_version=v3" in report_html
    assert "workflow_status" in report_html
    assert "degraded" in report_html
    assert "Pipeline health" in report_html
    assert "Source coverage/context health" in report_html
    assert "Lead-signal quality" in report_html
    assert "Mission Quality" in report_html
    assert "lead_dossiers/dossier_index.json" in report_html
    assert "lead_dossiers/dossier_index.csv" in report_html

    review_board_html = review_board_path.read_text(encoding="utf-8")
    assert "FOIA Lead Review Board" in review_board_html
    assert "Top Leads" in review_board_html
    assert "Dossier index JSON" in review_board_html
    assert "evidence" in review_board_html

    baseline = summary_payload.get("baseline", {})
    entity_cov = baseline.get("entity_coverage", {})
    assert entity_cov.get("window_linked_coverage_pct") is not None
    assert baseline.get("counts", {}).get("events_window", 0) > 0

    sam_ctx = baseline.get("sam_context", {})
    assert sam_ctx.get("events_with_research_context", 0) > 0
    assert baseline.get("correlations_by_lane", {}).get("same_sam_naics", 0) > 0

    checks_by_name = {item.get("name"): item for item in summary_payload.get("checks", [])}
    assert checks_by_name["sam_research_context_coverage_threshold"]["status"] == "pass"
    assert checks_by_name["same_sam_naics_lane_threshold"]["status"] == "pass"

    dossier_index_payload = json.loads(dossier_index_json_path.read_text(encoding="utf-8"))
    assert dossier_index_payload["top_n"] == 10
    assert dossier_index_payload["count"] == 3
    first_dossier = dossier_index_payload["items"][0]
    assert first_dossier["rank"] == 1
    assert first_dossier["event_id"] is not None
    assert first_dossier["scoring_version"] == "v3"
    assert first_dossier["dossier_path"].startswith("report/lead_dossiers/lead_001_event_")
    assert first_dossier["evidence_package_path"].startswith("report/lead_dossiers/evidence_packages/lead_001_event_")

    evidence_packages = sorted(dossier_evidence_dir.glob("lead_*.json"))
    assert len(evidence_packages) == 3
    evidence_payload = json.loads(evidence_packages[0].read_text(encoding="utf-8"))
    assert evidence_payload["package_type"] == "lead_evidence_package"
    assert evidence_payload["lead"]["top_positive_signals"] is not None
    assert evidence_payload["lead"]["top_suppressors"] is not None
    assert evidence_payload["lead"]["place_time"] is not None


def test_samgov_smoke_bundle_records_explicit_posted_window(tmp_path: Path):
    db_path = tmp_path / "sam_smoke_window.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    inside_window = datetime(2024, 2, 15, 12, 0, tzinfo=timezone.utc)
    outside_window = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_window_in_1",
                    occurred_at=inside_window,
                    created_at=inside_window,
                    doc_id="SAM-WIN-001",
                    source_url="https://sam.gov/opp/window-1",
                    snippet="Sources Sought RFP for engineering support with NAICS context",
                    raw_json={
                        "noticeId": "SAM-WIN-001",
                        "title": "Sources Sought RFP Engineering Support",
                        "noticeType": "Sources Sought",
                        "solicitationNumber": "DOE-RFP-WIN-001",
                        "naicsCode": "541330",
                        "naicsDescription": "Engineering Services",
                        "typeOfSetAside": "SBA",
                        "typeOfSetAsideDescription": "Total Small Business Set-Aside",
                        "responseDeadLine": "2024-03-15",
                        "fullParentPathName": "Department of Energy",
                        "fullParentPathCode": "DOE.HQ",
                        "Recipient Name": "Acme Federal",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_window_in_2",
                    occurred_at=inside_window + timedelta(days=1),
                    created_at=inside_window + timedelta(days=1),
                    doc_id="SAM-WIN-002",
                    source_url="https://sam.gov/opp/window-2",
                    snippet="Sources Sought RFP for engineering sustainment with NAICS references",
                    raw_json={
                        "noticeId": "SAM-WIN-002",
                        "title": "Sources Sought RFP Engineering Sustainment",
                        "noticeType": "Sources Sought",
                        "solicitationNumber": "DOE-RFP-WIN-002",
                        "naicsCode": "541330",
                        "naicsDescription": "Engineering Services",
                        "typeOfSetAside": "SBA",
                        "typeOfSetAsideDescription": "Total Small Business Set-Aside",
                        "responseDeadLine": "2024-03-16",
                        "fullParentPathName": "Department of Energy",
                        "fullParentPathCode": "DOE.HQ",
                        "Recipient Name": "Acme Federal",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_window_in_3",
                    occurred_at=inside_window + timedelta(days=2),
                    created_at=inside_window + timedelta(days=2),
                    doc_id="SAM-WIN-003",
                    source_url="https://sam.gov/opp/window-3",
                    snippet="Request for Proposal NAICS cybersecurity operations work",
                    raw_json={
                        "noticeId": "SAM-WIN-003",
                        "title": "RFP Cybersecurity Operations",
                        "noticeType": "Solicitation",
                        "solicitationNumber": "DOE-RFP-WIN-003",
                        "naicsCode": "541512",
                        "naicsDescription": "Computer Systems Design Services",
                        "typeOfSetAside": "8A",
                        "typeOfSetAsideDescription": "8(a) Set-Aside",
                        "responseDeadLine": "2024-03-20",
                        "fullParentPathName": "Department of Energy",
                        "fullParentPathCode": "DOE.FIELD",
                        "Recipient Name": "Field Ops",
                    },
                    keywords=[],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="sam_window_outside",
                    occurred_at=outside_window,
                    created_at=outside_window,
                    doc_id="SAM-OUTSIDE-001",
                    source_url="https://sam.gov/opp/outside",
                    snippet="Sources Sought RFP for engineering support with NAICS context",
                    raw_json={
                        "noticeId": "SAM-OUTSIDE-001",
                        "title": "Sources Sought Outside Window",
                        "noticeType": "Sources Sought",
                        "solicitationNumber": "DOE-RFP-OUT-001",
                        "naicsCode": "541330",
                        "naicsDescription": "Engineering Services",
                        "typeOfSetAside": "SBA",
                        "typeOfSetAsideDescription": "Total Small Business Set-Aside",
                        "responseDeadLine": "2026-03-15",
                        "fullParentPathName": "Department of Energy",
                        "fullParentPathCode": "DOE.HQ",
                        "Recipient Name": "Acme Federal",
                    },
                    keywords=[],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = run_samgov_smoke_workflow(
        database_url=db_url,
        skip_ingest=True,
        posted_from=date(2024, 1, 1),
        posted_to=date(2024, 3, 31),
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        ontology_days=1000,
        entity_days=1000,
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "smoke_artifacts_dates",
        require_nonzero=True,
    )

    assert res["run_metadata"]["posted_window_mode"] == "explicit_dates"
    assert res["run_metadata"]["effective_posted_from"] == "2024-01-01"
    assert res["run_metadata"]["effective_posted_to"] == "2024-03-31"
    assert (res.get("workflow") or {}).get("snapshot", {}).get("items", 0) > 0

    artifacts = res["artifacts"]
    summary_payload = json.loads(Path(artifacts["smoke_summary_json"]).read_text(encoding="utf-8"))
    manifest_payload = json.loads(Path(artifacts["bundle_manifest_json"]).read_text(encoding="utf-8"))
    lead_snapshot_payload = json.loads(
        Path((artifacts.get("exports") or {}).get("lead_snapshot", {}).get("json")).read_text(encoding="utf-8")
    )
    report_html = Path(artifacts["report_html"]).read_text(encoding="utf-8")

    run_metadata = summary_payload.get("run_metadata") or {}
    assert run_metadata.get("posted_window_mode") == "explicit_dates"
    assert run_metadata.get("effective_posted_from") == "2024-01-01"
    assert run_metadata.get("effective_posted_to") == "2024-03-31"
    assert summary_payload.get("scoring_version") == "v3"

    run_parameters = manifest_payload.get("run_parameters") or {}
    assert run_parameters.get("posted_window_mode") == "explicit_dates"
    assert run_parameters.get("effective_posted_from") == "2024-01-01"
    assert run_parameters.get("effective_posted_to") == "2024-03-31"
    assert run_parameters.get("scoring_version") == "v3"

    assert "SAM postedDate window 2024-01-01..2024-03-31" in (lead_snapshot_payload.get("snapshot") or {}).get("notes", "")
    assert lead_snapshot_payload.get("scoring_version") == "v3"
    assert summary_payload.get("outside_window_count") == 0
    assert summary_payload.get("snapshot_event_min", "").startswith("2024-02-15")
    assert summary_payload.get("snapshot_event_max", "").startswith("2024-02-17")
    assert all(str(item.get("occurred_at") or "").startswith("2024-") for item in lead_snapshot_payload.get("items", []))
    assert "2024-01-01" in report_html
    assert "2024-03-31" in report_html
    assert "scoring_version=v3" in report_html


def test_samgov_smoke_bundle_respects_configured_lead_dossier_top_n(tmp_path: Path):
    db_path = tmp_path / "sam_smoke_dossiers.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_smoke_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        lead_dossier_top_n=2,
        bundle_root=tmp_path / "smoke_artifacts_top_two",
        require_nonzero=True,
    )

    bundle_dir = Path(res["bundle_dir"])
    dossier_index_payload = json.loads((bundle_dir / FOIA_LEAD_DOSSIER_INDEX_JSON_PATH).read_text(encoding="utf-8"))
    evidence_packages = sorted((bundle_dir / FOIA_LEAD_DOSSIER_EVIDENCE_DIR).glob("lead_*.json"))

    assert res["lead_dossier_top_n"] == 2
    assert dossier_index_payload["top_n"] == 2
    assert dossier_index_payload["count"] == 2
    assert len(evidence_packages) == 2
    assert all(item["rank"] in {1, 2} for item in dossier_index_payload["items"])


def test_samgov_smoke_bundle_omits_dossier_artifact_paths_when_export_disabled(tmp_path: Path):
    db_path = tmp_path / "sam_smoke_no_dossiers.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_smoke_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        lead_dossier_top_n=0,
        bundle_root=tmp_path / "smoke_artifacts_no_dossiers",
        require_nonzero=True,
    )

    bundle_dir = Path(res["bundle_dir"])
    summary_payload = json.loads(Path(res["artifacts"]["smoke_summary_json"]).read_text(encoding="utf-8"))
    manifest_payload = json.loads(Path(res["artifacts"]["bundle_manifest_json"]).read_text(encoding="utf-8"))
    report_html = Path(res["artifacts"]["report_html"]).read_text(encoding="utf-8")
    review_board_html = Path(res["artifacts"]["foia_lead_review_board_html"]).read_text(encoding="utf-8")

    assert res["lead_dossier_top_n"] == 0
    assert not (bundle_dir / FOIA_LEAD_DOSSIER_INDEX_JSON_PATH).exists()
    assert not (bundle_dir / FOIA_LEAD_DOSSIER_INDEX_CSV_PATH).exists()
    assert res["artifacts"].get("lead_dossiers") is None
    assert (summary_payload.get("artifacts") or {}).get("lead_dossiers") is None
    assert "lead_dossier_index_json" not in (manifest_payload.get("generated_files") or {})
    assert "lead_dossier_index_csv" not in (manifest_payload.get("generated_files") or {})
    assert "lead_dossiers/dossier_index.json" not in report_html
    assert "lead_dossiers/dossier_index.csv" not in report_html
    assert "Dossier index JSON" not in review_board_html
    assert "Dossier index CSV" not in review_board_html


def test_samgov_smoke_bundle_can_emit_scoring_comparison_artifact(tmp_path: Path):
    db_path = tmp_path / "sam_smoke_compare.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_smoke_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        scoring_version="v3",
        compare_scoring_versions=["v2", "v3"],
        bundle_root=tmp_path / "smoke_compare_artifacts",
        require_nonzero=True,
    )

    artifacts = res.get("artifacts") or {}
    comparison = (artifacts.get("exports") or {}).get("scoring_comparison") or {}
    assert Path(comparison.get("csv")).exists()
    assert Path(comparison.get("json")).exists()

    report_html = Path(artifacts["report_html"]).read_text(encoding="utf-8")
    assert "compare_scoring_versions=v2,v3" in report_html


def test_samgov_validation_warning_with_usable_artifacts_is_partially_useful(tmp_path: Path, monkeypatch):
    strong_rows = [
        _mission_review_row(
            rank=1,
            score=20,
            scoring_version="v3",
            event_id=301,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_entity", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-301",
        ),
        _mission_review_row(
            rank=2,
            score=14,
            scoring_version="v3",
            event_id=302,
            lead_family="range_test_infrastructure",
            matched_rules=["sam_dod_flight_test_range_instrumentation:range_telemetry_support_services"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_agency", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=False,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-302",
        ),
        _mission_review_row(
            rank=3,
            score=13,
            scoring_version="v3",
            event_id=303,
            lead_family="facility_security_hardening",
            matched_rules=["sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context"],
            pair_count=0,
            top_suppressors=[],
            contributing_lanes=["same_doc_id"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-303",
        ),
    ]
    exports = _write_mission_review_exports(tmp_path / "validation_warning_exports", scoring_version="v3", rows=strong_rows)

    def fake_run_samgov_workflow(**_kwargs):
        return {
            "source": "SAM.gov",
            "status": "ok",
            "ingest": {"status": "success", "fetched": 25, "inserted": 25, "normalized": 25},
            "snapshot": {"items": len(strong_rows)},
            "exports": exports,
        }

    monkeypatch.setattr(workflow_module, "run_samgov_workflow", fake_run_samgov_workflow)
    monkeypatch.setattr(workflow_module, "doctor_status", lambda **_kwargs: _healthy_sam_doctor_status())
    res = run_samgov_validation_workflow(
        bundle_root=tmp_path / "validation_warning_bundle",
        require_nonzero=True,
        skip_ingest=False,
        threshold_overrides={"same_sam_naics_lane_min": 5.0},
    )

    assert res["workflow_status"] == "warning"
    assert res["quality"] == "degraded"
    assert res["has_required_failures"] is False
    assert res["has_advisory_failures"] is True
    assert res["has_usable_artifacts"] is True
    assert res["partially_useful"] is True
    assert "advisory_check_failed:same_sam_naics_lane_threshold" in res["reason_codes"]
    assert "quality_degraded" in res["reason_codes"]
    assert res["operator_messages"]

    summary_payload = json.loads(Path(res["artifacts"]["smoke_summary_json"]).read_text(encoding="utf-8"))
    assert summary_payload["workflow_status"] == "warning"
    assert summary_payload["quality"] == "degraded"
    assert summary_payload["partially_useful"] is True
    assert summary_payload["reason_codes"]
    assert summary_payload["operator_messages"]


def test_samgov_smoke_comparison_empty_is_explicit_and_reported(tmp_path: Path, monkeypatch):
    comparison_csv = tmp_path / "comparison_fixture.csv"
    comparison_json = tmp_path / "comparison_fixture.json"
    comparison_csv.write_text("event_id\n", encoding="utf-8")
    comparison_json.write_text(
        json.dumps(
            {
                "baseline_version": "v2",
                "target_version": "v3",
                "versions": ["v2", "v3"],
                "count": 0,
                "state_counts": {"shared": 0, "entered_target": 0, "dropped_from_target": 0},
                "items": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    def fake_run_samgov_workflow(**_kwargs):
        return {
            "source": "SAM.gov",
            "status": "ok",
            "ingest": {"status": "success", "fetched": 25, "inserted": 25, "normalized": 25},
            "snapshot": {"items": 5},
            "exports": {
                "scoring_comparison": {
                    "csv": comparison_csv,
                    "json": comparison_json,
                    "count": 0,
                    "baseline_version": "v2",
                    "target_version": "v3",
                    "versions": ["v2", "v3"],
                }
            },
        }

    def fake_doctor_status(**_kwargs):
        return {
            "db": {"status": "ok"},
            "counts": {
                "events_window": 30,
                "events_with_entity_window": 20,
                "lead_snapshots_total": 1,
            },
            "keywords": {
                "scanned_events": 30,
                "events_with_keywords": 20,
                "coverage_pct": 66.7,
                "unique_keywords": 5,
            },
            "entities": {
                "window_linked_coverage_pct": 66.7,
                "sample_scanned_events": 30,
                "sample_events_with_identity_signal": 30,
                "sample_events_with_identity_signal_linked": 20,
                "sample_identity_signal_coverage_pct": 66.7,
                "sample_events_with_name": 30,
                "sample_events_with_name_linked": 20,
                "sample_name_coverage_pct": 66.7,
            },
            "correlations": {
                "by_lane": {
                    "same_keyword": 5,
                    "kw_pair": 5,
                    "same_sam_naics": 2,
                    "same_entity": 3,
                    "same_uei": 0,
                }
            },
            "sam_context": {
                "scanned_events": 30,
                "events_with_research_context": 20,
                "research_context_coverage_pct": 66.7,
                "events_with_core_procurement_context": 20,
                "core_procurement_context_coverage_pct": 66.7,
                "avg_context_fields_per_event": 3.2,
                "coverage_by_field_pct": {
                    "sam_notice_type": 100.0,
                    "sam_solicitation_number": 100.0,
                    "sam_naics_code": 80.0,
                },
                "top_notice_types": [],
                "top_naics_codes": [],
                "top_set_aside_codes": [],
            },
            "hints": [],
        }

    monkeypatch.setattr(workflow_module, "run_samgov_workflow", fake_run_samgov_workflow)
    monkeypatch.setattr(workflow_module, "doctor_status", fake_doctor_status)

    res = run_samgov_smoke_workflow(
        bundle_root=tmp_path / "comparison_empty_bundle",
        require_nonzero=True,
        skip_ingest=False,
        compare_scoring_versions=["v2", "v3"],
    )

    assert res["workflow_status"] == "warning"
    assert res["quality"] == "degraded"
    assert res["comparison_requested"] is True
    assert res["comparison_available"] is True
    assert res["comparison_empty"] is True
    assert res["partially_useful"] is True
    assert "comparison_requested_but_empty" in res["reason_codes"]
    assert any("comparison artifact contains zero comparable rows" in message.lower() for message in res["operator_messages"])

    summary_payload = json.loads(Path(res["artifacts"]["smoke_summary_json"]).read_text(encoding="utf-8"))
    assert summary_payload["comparison_requested"] is True
    assert summary_payload["comparison_available"] is True
    assert summary_payload["comparison_empty"] is True
    assert "comparison_requested_but_empty" in summary_payload["reason_codes"]

    report_html = Path(res["artifacts"]["report_html"]).read_text(encoding="utf-8")
    assert "comparison_empty" in report_html
    assert "comparison_requested_but_empty" in report_html
    assert "zero comparable rows" in report_html


def test_samgov_bundle_report_renders_requested_and_effective_comparison_windows(tmp_path: Path, monkeypatch):
    comparison_csv = tmp_path / "comparison_window_fixture.csv"
    comparison_json = tmp_path / "comparison_window_fixture.json"
    comparison_csv.write_text("event_id,state\n1,shared\n", encoding="utf-8")
    comparison_json.write_text(
        json.dumps(
            {
                "baseline_version": "v2",
                "target_version": "v3",
                "versions": ["v2", "v3"],
                "count": 1,
                "state_counts": {"shared": 1},
                "items": [{"event_id": 1, "state": "shared"}],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    effective_window = {
        "mode": "explicit_dates",
        "requested_days": None,
        "effective_days": 32,
        "posted_from": "2024-02-01",
        "posted_to": "2024-03-04",
        "calendar_span_days": 32,
    }

    def fake_run_samgov_workflow(**_kwargs):
        return {
            "source": "SAM.gov",
            "status": "ok",
            "ingest": {
                "status": "success",
                "fetched": 25,
                "inserted": 25,
                "normalized": 25,
                "date_window": effective_window,
            },
            "snapshot": {"items": 5},
            "exports": {
                "scoring_comparison": {
                    "csv": comparison_csv,
                    "json": comparison_json,
                    "count": 1,
                    "baseline_version": "v2",
                    "target_version": "v3",
                    "versions": ["v2", "v3"],
                }
            },
        }

    def fake_doctor_status(**_kwargs):
        return {
            "db": {"status": "ok"},
            "counts": {
                "events_window": 30,
                "events_with_entity_window": 20,
                "lead_snapshots_total": 1,
            },
            "keywords": {
                "scanned_events": 30,
                "events_with_keywords": 20,
                "coverage_pct": 66.7,
                "unique_keywords": 5,
            },
            "entities": {
                "window_linked_coverage_pct": 66.7,
                "sample_scanned_events": 30,
                "sample_events_with_identity_signal": 30,
                "sample_events_with_identity_signal_linked": 20,
                "sample_identity_signal_coverage_pct": 66.7,
                "sample_events_with_name": 30,
                "sample_events_with_name_linked": 20,
                "sample_name_coverage_pct": 66.7,
            },
            "correlations": {
                "by_lane": {
                    "same_keyword": 5,
                    "kw_pair": 5,
                    "same_sam_naics": 2,
                    "same_entity": 3,
                    "same_uei": 0,
                }
            },
            "sam_context": {
                "scanned_events": 30,
                "events_with_research_context": 20,
                "research_context_coverage_pct": 66.7,
                "events_with_core_procurement_context": 20,
                "core_procurement_context_coverage_pct": 66.7,
                "avg_context_fields_per_event": 3.2,
                "coverage_by_field_pct": {
                    "sam_notice_type": 100.0,
                    "sam_solicitation_number": 100.0,
                    "sam_naics_code": 80.0,
                },
                "top_notice_types": [],
                "top_naics_codes": [],
                "top_set_aside_codes": [],
            },
            "hints": [],
        }

    monkeypatch.setattr(workflow_module, "run_samgov_workflow", fake_run_samgov_workflow)
    monkeypatch.setattr(workflow_module, "doctor_status", fake_doctor_status)

    res = run_samgov_smoke_workflow(
        bundle_root=tmp_path / "comparison_window_bundle",
        require_nonzero=True,
        skip_ingest=False,
        posted_from=date(2024, 1, 1),
        posted_to=date(2024, 3, 31),
        compare_scoring_versions=["v2", "v3"],
    )

    assert res["workflow_status"] == "warning"
    assert "requested_window_differs_from_effective_window" in res["comparison_reason_codes"]
    report_html = Path(res["artifacts"]["report_html"]).read_text(encoding="utf-8")
    assert "requested_window" in report_html
    assert "effective_window" in report_html
    assert "2024-01-01..2024-03-31" in report_html
    assert "2024-02-01..2024-03-04" in report_html
    assert "comparison_effective_window_matches_request" in report_html


def test_samgov_bundle_reports_include_adjudication_metrics_when_present(tmp_path: Path):
    db_path = tmp_path / "sam_smoke_adjudications.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_smoke_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "smoke_adjudication_bundle",
        require_nonzero=True,
    )

    bundle_dir = Path(res["bundle_dir"])
    snapshot_id = int((res["workflow"].get("snapshot") or {}).get("snapshot_id"))
    template = export_lead_adjudication_template(
        snapshot_id=snapshot_id,
        database_url=db_url,
        bundle_dir=bundle_dir,
    )
    adjudications_csv = Path(template["csv"])
    _fill_adjudication_csv(
        adjudications_csv,
        {
            1: {"decision": "keep", "foia_ready": "yes", "lead_family": "alpha_family"},
            2: {"decision": "reject", "foia_ready": "no", "reason_code": "low_signal", "lead_family": "beta_family"},
            3: {"decision": "unclear", "lead_family": "alpha_family"},
        },
    )

    metrics = evaluate_lead_adjudications(
        adjudications=[adjudications_csv],
        precision_at_k=[1, 2],
        bundle_dir=bundle_dir,
    )

    artifacts = metrics.get("artifacts") or {}
    manifest = json.loads(Path(artifacts["bundle_manifest_json"]).read_text(encoding="utf-8"))
    generated_files = manifest.get("generated_files") or {}
    assert "export_lead_adjudications_csv" in generated_files
    assert "export_lead_adjudication_metrics_json" in generated_files

    bundle_report_path = artifacts.get("bundle_report_html") or artifacts.get("report_html")
    bundle_report_html = Path(bundle_report_path).read_text(encoding="utf-8")
    report_html = Path(artifacts["report_html"]).read_text(encoding="utf-8")
    review_board_html = Path(artifacts["foia_lead_review_board_html"]).read_text(encoding="utf-8")
    assert "Evaluation" in bundle_report_html
    assert "Precision @ k" in bundle_report_html
    assert "low_signal" in bundle_report_html
    assert "alpha_family" in bundle_report_html
    assert "Evaluation" in report_html
    assert "By Scoring Version" in report_html
    assert "alpha_family" in report_html
    assert "Adjudication" in review_board_html
    assert "Precision @ k" in review_board_html








def test_samgov_validation_workflow_emits_larger_mode_metadata(tmp_path: Path, monkeypatch):
    strong_rows = [
        _mission_review_row(
            rank=1,
            score=21,
            scoring_version="v3",
            event_id=401,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_entity", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-401",
        ),
        _mission_review_row(
            rank=2,
            score=15,
            scoring_version="v3",
            event_id=402,
            lead_family="range_test_infrastructure",
            matched_rules=["sam_dod_flight_test_range_instrumentation:range_telemetry_support_services"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_agency", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=False,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-402",
        ),
        _mission_review_row(
            rank=3,
            score=14,
            scoring_version="v3",
            event_id=403,
            lead_family="facility_security_hardening",
            matched_rules=["sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context"],
            pair_count=0,
            top_suppressors=[],
            contributing_lanes=["same_doc_id"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-403",
        ),
    ]
    exports = _write_mission_review_exports(tmp_path / "validation_metadata_exports", scoring_version="v3", rows=strong_rows)

    def fake_run_samgov_workflow(**_kwargs):
        return {
            "source": "SAM.gov",
            "status": "ok",
            "ingest": {"status": "success", "fetched": 25, "inserted": 25, "normalized": 25},
            "snapshot": {"items": len(strong_rows)},
            "exports": exports,
        }

    monkeypatch.setattr(workflow_module, "run_samgov_workflow", fake_run_samgov_workflow)
    monkeypatch.setattr(workflow_module, "doctor_status", lambda **_kwargs: _healthy_sam_doctor_status())

    res = run_samgov_validation_workflow(
        bundle_root=tmp_path / "validation_artifacts",
        require_nonzero=True,
        skip_ingest=False,
    )

    assert res.get("validation_mode") == "larger"
    assert res.get("workflow_type") == "samgov-validation"
    assert res.get("scoring_version") == "v3"
    assert res.get("quality_gate_policy", {}).get("required_checks")
    assert res.get("quality_gate_policy", {}).get("advisory_checks")
    assert res.get("status") in {"ok", "warning"}
    assert "workflow_execution" in (res.get("quality_gate_policy", {}).get("required_checks") or [])
    assert "ingest_nonzero" in (res.get("quality_gate_policy", {}).get("required_checks") or [])
    assert "workflow_execution" in (res.get("quality_gate_policy", {}).get("effective_required_checks") or [])
    assert "ingest_nonzero" in (res.get("quality_gate_policy", {}).get("effective_required_checks") or [])
    overrides = res.get("quality_gate_policy", {}).get("policy_overrides") or []
    assert overrides == []

    artifacts = res.get("artifacts") or {}
    manifest_path = Path(artifacts.get("bundle_manifest_json"))
    summary_path = Path(artifacts.get("smoke_summary_json"))
    report_path = Path(artifacts.get("report_html"))
    assert manifest_path.exists()
    assert summary_path.exists()
    assert report_path.exists()

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload.get("validation_mode") == "larger"
    assert manifest_payload.get("workflow_type") == "samgov-validation"
    check_summary = manifest_payload.get("check_summary") or {}
    assert check_summary.get("required_total", 0) > 0
    assert check_summary.get("advisory_total", 0) > 0
    by_category = check_summary.get("by_category") or {}
    assert by_category.get("source_coverage_context_health", {}).get("required_total", 0) > 0
    assert by_category.get("lead_signal_quality", {}).get("required_total", 0) > 0

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    checks_by_name = {item.get("name"): item for item in summary_payload.get("checks", [])}
    assert checks_by_name["workflow_execution"]["policy_level"] == "required"
    assert checks_by_name["workflow_execution"]["passed"] is True
    assert checks_by_name["events_window_threshold"]["policy_level"] == "required"
    assert checks_by_name["events_window_threshold"]["severity"] == "error"
    assert checks_by_name["events_with_keywords_coverage_threshold"]["policy_level"] == "advisory"
    assert checks_by_name["same_sam_naics_lane_threshold"]["policy_level"] == "advisory"
    assert checks_by_name["snapshot_items_threshold"]["policy_level"] == "required"

    report_html = report_path.read_text(encoding="utf-8")
    assert "Severity" in report_html
    assert "Policy" in report_html
    assert "Lead-signal quality" in report_html
    assert "advisory" in report_html
    assert "required" in report_html


def test_samgov_validation_fails_on_mission_quality_weak_ranked_surface(tmp_path: Path, monkeypatch):
    weak_rows = [
        _mission_review_row(
            rank=1,
            score=12,
            scoring_version="v2",
            event_id=101,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_procurement_starter:idiq_vehicle"],
            pair_count=2,
            top_suppressors=[{"label": "operational_noise_terms:admin_facility_ops_noise", "penalty": 4}],
            contributing_lanes=["kw_pair"],
            has_core_identifiers=False,
            has_agency_target=False,
            has_vendor_context=False,
            has_classification_context=False,
            has_foia_handles=False,
        ),
        _mission_review_row(
            rank=2,
            score=12,
            scoring_version="v2",
            event_id=102,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_procurement_starter:task_or_delivery_order"],
            pair_count=2,
            top_suppressors=[{"label": "operational_noise_terms:generic_facility_maintenance_noise", "penalty": 4}],
            contributing_lanes=["kw_pair"],
            has_core_identifiers=False,
            has_agency_target=False,
            has_vendor_context=False,
            has_classification_context=False,
            has_foia_handles=False,
        ),
        _mission_review_row(
            rank=3,
            score=11,
            scoring_version="v2",
            event_id=103,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_procurement_starter:idiq_vehicle"],
            pair_count=1,
            top_suppressors=[{"label": "operational_noise_terms:security_training_noise", "penalty": 4}],
            contributing_lanes=["kw_pair"],
            has_core_identifiers=False,
            has_agency_target=False,
            has_vendor_context=False,
            has_classification_context=False,
            has_foia_handles=False,
        ),
        _mission_review_row(
            rank=4,
            score=11,
            scoring_version="v2",
            event_id=104,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_procurement_starter:idiq_vehicle"],
            pair_count=1,
            top_suppressors=[{"label": "operational_noise_terms:admin_facility_ops_noise", "penalty": 4}],
            contributing_lanes=["kw_pair"],
            has_core_identifiers=False,
            has_agency_target=False,
            has_vendor_context=False,
            has_classification_context=False,
            has_foia_handles=False,
        ),
        _mission_review_row(
            rank=5,
            score=11,
            scoring_version="v2",
            event_id=105,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_procurement_starter:task_or_delivery_order"],
            pair_count=2,
            top_suppressors=[{"label": "operational_noise_terms:generic_medical_clinical_noise", "penalty": 4}],
            contributing_lanes=["kw_pair"],
            has_core_identifiers=False,
            has_agency_target=False,
            has_vendor_context=False,
            has_classification_context=False,
            has_foia_handles=False,
        ),
        _mission_review_row(
            rank=6,
            score=10,
            scoring_version="v2",
            event_id=106,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_procurement_starter:idiq_vehicle"],
            pair_count=1,
            top_suppressors=[{"label": "operational_noise_terms:admin_facility_ops_noise", "penalty": 4}],
            contributing_lanes=["kw_pair"],
            has_core_identifiers=False,
            has_agency_target=False,
            has_vendor_context=False,
            has_classification_context=False,
            has_foia_handles=False,
        ),
        _mission_review_row(
            rank=7,
            score=10,
            scoring_version="v2",
            event_id=107,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_procurement_starter:task_or_delivery_order"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["kw_pair"],
            has_core_identifiers=False,
            has_agency_target=False,
            has_vendor_context=False,
            has_classification_context=False,
            has_foia_handles=False,
        ),
        _mission_review_row(
            rank=8,
            score=10,
            scoring_version="v2",
            event_id=108,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_procurement_starter:idiq_vehicle"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["kw_pair"],
            has_core_identifiers=False,
            has_agency_target=False,
            has_vendor_context=False,
            has_classification_context=False,
            has_foia_handles=False,
        ),
        _mission_review_row(
            rank=9,
            score=9,
            scoring_version="v2",
            event_id=109,
            lead_family="vendor_network_contract_lineage",
            matched_rules=[
                "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context"
            ],
            pair_count=0,
            top_suppressors=[],
            contributing_lanes=["same_entity"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-109",
            candidate_join_evidence=[
                {
                    "status": "candidate",
                    "evidence_types": ["identifier_exact"],
                    "linked_sources": ["USAspending"],
                    "score_signal": 63,
                }
            ],
        ),
        _mission_review_row(
            rank=10,
            score=9,
            scoring_version="v2",
            event_id=110,
            lead_family="range_test_infrastructure",
            matched_rules=[
                "sam_dod_flight_test_range_instrumentation:range_telemetry_support_services"
            ],
            pair_count=0,
            top_suppressors=[],
            contributing_lanes=["same_agency"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=False,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-110",
        ),
    ]
    exports = _write_mission_review_exports(tmp_path / "mission_weak_exports", scoring_version="v2", rows=weak_rows)

    def fake_run_samgov_workflow(**_kwargs):
        return {
            "source": "SAM.gov",
            "status": "ok",
            "ingest": {"status": "success", "fetched": 50, "inserted": 50, "normalized": 50},
            "snapshot": {"items": len(weak_rows)},
            "exports": exports,
        }

    monkeypatch.setattr(workflow_module, "run_samgov_workflow", fake_run_samgov_workflow)
    monkeypatch.setattr(workflow_module, "doctor_status", lambda **_kwargs: _healthy_sam_doctor_status())

    res = run_samgov_validation_workflow(
        bundle_root=tmp_path / "validation_mission_fail",
        require_nonzero=True,
        skip_ingest=False,
    )

    assert res["status"] == "failed"
    assert res["required_failure_categories"] == ["mission_quality"]
    assert "mission_quality_failed" in res["reason_codes"]
    assert any("Mission-quality review gates failed" in message for message in res["operator_messages"])

    failed_by_name = {item.get("name"): item for item in res.get("failed_required_checks", [])}
    assert "scoring_version_is_v3" in failed_by_name
    assert "top_leads_core_field_coverage_threshold" in failed_by_name
    assert "top_leads_family_diversity_threshold" in failed_by_name
    assert "nonstarter_pack_presence_threshold" in failed_by_name
    assert "starter_only_pair_dominance_threshold" in failed_by_name
    assert "score_spread_threshold" in failed_by_name
    assert "routine_noise_share_threshold" in failed_by_name
    assert "foia_draftability_threshold" in failed_by_name

    scoring_check = failed_by_name["scoring_version_is_v3"]
    assert scoring_check["category"] == "mission_quality"
    assert scoring_check["policy_level"] == "required"
    assert scoring_check["threshold"] == "v3"
    assert scoring_check["observed"]["lead_snapshot_scoring_version"] == "v2"
    assert scoring_check["why"]
    assert scoring_check["hint"]

    summary_payload = json.loads(Path(res["artifacts"]["smoke_summary_json"]).read_text(encoding="utf-8"))
    summary_checks = {item.get("name"): item for item in summary_payload.get("checks", [])}
    assert summary_checks["scoring_version_is_v3"]["category"] == "mission_quality"
    assert summary_checks["foia_draftability_threshold"]["category"] == "mission_quality"
    assert summary_checks["starter_only_pair_dominance_threshold"]["required"] is True
    assert summary_payload["mission_quality"]["scoring_version"] == "v2"
    assert summary_payload["mission_quality"]["family_diversity"]["unique_primary_families"] == 2

    manifest_payload = json.loads(Path(res["artifacts"]["bundle_manifest_json"]).read_text(encoding="utf-8"))
    assert manifest_payload["check_summary"]["by_category"]["mission_quality"]["failed_required"] >= 1

    report_html = Path(res["artifacts"]["report_html"]).read_text(encoding="utf-8")
    assert "Mission Quality" in report_html
    assert "scoring_version_is_v3" in report_html
    assert "starter_only_pair_dominance_threshold" in report_html
    assert "Mission-quality review gates failed" in report_html


def test_samgov_validation_passes_when_mission_quality_is_strong_and_serialized(tmp_path: Path, monkeypatch):
    pass_rows = [
        _mission_review_row(
            rank=1,
            score=26,
            scoring_version="v3",
            event_id=201,
            lead_family="vendor_network_contract_lineage",
            matched_rules=[
                "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context",
                "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
            ],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["sam_usaspending_candidate_join", "same_entity", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-201",
            candidate_join_evidence=[
                {
                    "status": "candidate",
                    "evidence_types": ["identifier_exact", "contract_family"],
                    "linked_sources": ["USAspending"],
                    "score_signal": 76,
                }
            ],
            linked_source_summary=[{"source": "USAspending", "linked_event_count": 2, "lanes": ["sam_usaspending_candidate_join"]}],
        ),
        _mission_review_row(
            rank=2,
            score=24,
            scoring_version="v3",
            event_id=202,
            lead_family="range_test_infrastructure",
            matched_rules=["sam_dod_flight_test_range_instrumentation:range_telemetry_support_services"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_agency", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=False,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-202",
        ),
        _mission_review_row(
            rank=3,
            score=23,
            scoring_version="v3",
            event_id=203,
            lead_family="facility_security_hardening",
            matched_rules=["sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_doc_id", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-203",
        ),
        _mission_review_row(
            rank=4,
            score=21,
            scoring_version="v3",
            event_id=204,
            lead_family="exploitation_materials_handling",
            matched_rules=["sam_proxy_materials_exploitation_forensics:materials_forensic_lab_context"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_naics", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            award_id="AWD-204",
        ),
        _mission_review_row(
            rank=5,
            score=20,
            scoring_version="v3",
            event_id=205,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_proxy_operator_site_program_pairs:operator_site_pair_proxy_context"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_contract_id", "same_entity"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=False,
            has_foia_handles=True,
            award_id="AWD-205",
        ),
        _mission_review_row(
            rank=6,
            score=18,
            scoring_version="v3",
            event_id=206,
            lead_family="range_test_infrastructure",
            matched_rules=["sam_proxy_optical_tracking_transient_collection:optical_ir_tracking_context"],
            pair_count=1,
            top_suppressors=[],
            contributing_lanes=["same_place_region", "kw_pair"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=False,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-206",
        ),
        _mission_review_row(
            rank=7,
            score=17,
            scoring_version="v3",
            event_id=207,
            lead_family="facility_security_hardening",
            matched_rules=["sam_proxy_classified_contract_security_admin:comsec_type1_secure_comms_context"],
            pair_count=0,
            top_suppressors=[],
            contributing_lanes=["same_doc_id"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-207",
        ),
        _mission_review_row(
            rank=8,
            score=16,
            scoring_version="v3",
            event_id=208,
            lead_family="exploitation_materials_handling",
            matched_rules=["sam_proxy_controlled_sample_containment_storage:glovebox_inert_sample_handling_context"],
            pair_count=0,
            top_suppressors=[],
            contributing_lanes=["same_agency"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-208",
        ),
        _mission_review_row(
            rank=9,
            score=15,
            scoring_version="v3",
            event_id=209,
            lead_family="range_test_infrastructure",
            matched_rules=["sam_procurement_starter:idiq_vehicle"],
            pair_count=1,
            top_suppressors=[{"label": "operational_noise_terms:admin_facility_ops_noise", "penalty": 2}],
            contributing_lanes=["kw_pair", "same_agency"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=False,
            has_classification_context=True,
            has_foia_handles=True,
            solicitation_number="SOL-209",
        ),
        _mission_review_row(
            rank=10,
            score=14,
            scoring_version="v3",
            event_id=210,
            lead_family="vendor_network_contract_lineage",
            matched_rules=["sam_proxy_procurement_continuity_classified_followon:classified_annex_continuity_context"],
            pair_count=0,
            top_suppressors=[],
            contributing_lanes=["same_entity"],
            has_core_identifiers=True,
            has_agency_target=True,
            has_vendor_context=True,
            has_classification_context=False,
            has_foia_handles=True,
            award_id="AWD-210",
        ),
    ]
    exports = _write_mission_review_exports(tmp_path / "mission_pass_exports", scoring_version="v3", rows=pass_rows)

    def fake_run_samgov_workflow(**_kwargs):
        return {
            "source": "SAM.gov",
            "status": "ok",
            "ingest": {"status": "success", "fetched": 50, "inserted": 50, "normalized": 50},
            "snapshot": {"items": len(pass_rows)},
            "exports": exports,
        }

    monkeypatch.setattr(workflow_module, "run_samgov_workflow", fake_run_samgov_workflow)
    monkeypatch.setattr(workflow_module, "doctor_status", lambda **_kwargs: _healthy_sam_doctor_status())

    res = run_samgov_validation_workflow(
        bundle_root=tmp_path / "validation_mission_pass",
        require_nonzero=True,
        skip_ingest=False,
    )

    assert res["status"] == "ok"
    assert res["required_checks_passed"] is True
    assert res["required_failure_categories"] == []
    checks_by_name = {item.get("name"): item for item in res.get("checks", [])}
    assert checks_by_name["scoring_version_is_v3"]["passed"] is True
    assert checks_by_name["foia_draftability_threshold"]["passed"] is True
    assert checks_by_name["starter_only_pair_dominance_threshold"]["passed"] is True
    assert checks_by_name["routine_noise_share_threshold"]["passed"] is True
    assert checks_by_name["dossier_linkage_threshold"]["passed"] is True

    summary_payload = json.loads(Path(res["artifacts"]["smoke_summary_json"]).read_text(encoding="utf-8"))
    mission_quality = summary_payload["mission_quality"]
    assert mission_quality["scoring_version"] == "v3"
    assert mission_quality["row_scoring_versions"] == ["v3"]
    assert mission_quality["core_field_coverage_pct"] >= 70.0
    assert mission_quality["family_diversity"]["unique_primary_families"] >= 3
    assert mission_quality["nonstarter_pack_presence_pct"] >= 60.0
    assert mission_quality["starter_only_pair_share_pct"] <= 35.0
    assert mission_quality["routine_noise_share_pct"] <= 35.0
    assert mission_quality["foia_draftability"]["draftable_share_pct"] >= 40.0
    assert mission_quality["dossier_linkage_pct"] == 100.0

    scoring_check = next(item for item in summary_payload["checks"] if item["name"] == "scoring_version_is_v3")
    assert sorted(scoring_check.keys()) == sorted(
        [
            "actual",
            "category",
            "category_label",
            "comparator",
            "expected",
            "hint",
            "kind",
            "name",
            "observed",
            "ok",
            "passed",
            "policy_level",
            "required",
            "result",
            "severity",
            "status",
            "threshold",
            "unit",
            "why",
        ]
    )

    report_html = Path(res["artifacts"]["report_html"]).read_text(encoding="utf-8")
    assert "Mission Quality" in report_html
    assert "lead_snapshot_scoring_version" in report_html
    assert "foia_draftability_pct" in report_html


def test_samgov_evaluation_workflow_emits_native_artifacts_and_contracts(tmp_path: Path):
    db_path = tmp_path / "sam_evaluation.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_evaluation_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "evaluation_artifacts",
        require_nonzero=True,
    )

    assert res.get("workflow_type") == "samgov-evaluate"

    artifacts = res.get("artifacts") or {}
    summary_path = Path(artifacts.get("evaluation_summary_json"))
    comparison_path = Path(artifacts.get("scoring_comparison_json"))
    review_board_path = Path(artifacts.get("review_board_md"))
    evaluation_report_path = Path(artifacts.get("evaluation_report_md"))
    dossiers_dir = Path(artifacts.get("dossiers_dir"))
    dossiers_index_path = Path(artifacts.get("dossiers_index_json"))
    manifest_path = Path(artifacts.get("bundle_manifest_json"))

    assert summary_path.exists()
    assert comparison_path.exists()
    assert review_board_path.exists()
    assert evaluation_report_path.exists()
    assert dossiers_dir.exists() and dossiers_dir.is_dir()
    assert dossiers_index_path.exists()
    assert manifest_path.exists()

    evaluation_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert evaluation_summary.get("workflow_type") == "samgov-evaluate"
    assert evaluation_summary.get("scoring_version") == "v3"
    assert evaluation_summary.get("artifact_completeness", {}).get("complete") is True
    assert evaluation_summary.get("signal_metrics", {}).get("row_count", 0) > 0

    comparison_payload = json.loads(comparison_path.read_text(encoding="utf-8"))
    assert comparison_payload.get("summary", {}).get("v2_row_count", 0) > 0
    assert comparison_payload.get("summary", {}).get("v3_row_count", 0) > 0

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload.get("workflow_type") == "samgov-evaluate"
    generated_files = manifest_payload.get("generated_files") or {}
    assert "evaluation_summary_json" in generated_files
    assert "scoring_comparison_json" in generated_files
    assert "review_board_md" in generated_files
    assert "evaluation_report_md" in generated_files
    assert "dossiers_dir" in generated_files
    assert "dossiers_index_json" in generated_files

    summary_payload = json.loads(Path(artifacts.get("smoke_summary_json")).read_text(encoding="utf-8"))
    check_names = {item.get("name") for item in summary_payload.get("checks", [])}
    assert "evaluation_artifact_completeness" in check_names
    assert "evaluation_top10_proxy_or_pairbacked_threshold" in check_names
    assert "evaluation_family_collapse_threshold" in check_names
    artifact_check = next(item for item in summary_payload.get("checks", []) if item.get("name") == "evaluation_artifact_completeness")
    assert artifact_check.get("passed") is True

    review_board = review_board_path.read_text(encoding="utf-8")
    evaluation_report = evaluation_report_path.read_text(encoding="utf-8")
    assert "FOIA Lead Review Board" in review_board
    assert "FOIA Rationale" in review_board
    assert "SAM.gov Evaluation Report" in evaluation_report
    assert "Top-10 proxy or pair-backed count" in evaluation_report

    dossiers_index = json.loads(dossiers_index_path.read_text(encoding="utf-8"))
    assert dossiers_index.get("count", 0) > 0
    first_dossier = Path(res.get("bundle_dir")) / dossiers_index["items"][0]["file"]
    dossier_payload = json.loads(first_dossier.read_text(encoding="utf-8"))
    assert dossier_payload.get("supporting_records")
    assert dossier_payload["supporting_records"][0]["role"] == "focal_lead"
    assert len(dossier_payload["supporting_records"]) <= 1 + 8


def test_samgov_evaluation_sparse_historical_window_stays_empty_and_honest(tmp_path: Path):
    db_path = tmp_path / "sam_evaluation_sparse.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_evaluation_workflow(
        database_url=db_url,
        skip_ingest=True,
        posted_from=date(2024, 1, 1),
        posted_to=date(2024, 1, 31),
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "evaluation_sparse_artifacts",
        require_nonzero=False,
    )

    assert res.get("workflow_type") == "samgov-evaluate"
    assert (res.get("workflow") or {}).get("snapshot", {}).get("items") == 0

    artifacts = res.get("artifacts") or {}
    lead_snapshot_payload = json.loads(
        Path((artifacts.get("exports") or {}).get("lead_snapshot", {}).get("json")).read_text(encoding="utf-8")
    )
    evaluation_summary = json.loads(Path(artifacts.get("evaluation_summary_json")).read_text(encoding="utf-8"))
    dossiers_index = json.loads(Path(artifacts.get("dossiers_index_json")).read_text(encoding="utf-8"))
    review_board = Path(artifacts.get("review_board_md")).read_text(encoding="utf-8")

    assert lead_snapshot_payload.get("count") == 0
    assert evaluation_summary.get("outside_window_count") == 0
    assert evaluation_summary.get("signal_metrics", {}).get("row_count") == 0
    assert evaluation_summary.get("snapshot_event_min") is None
    assert evaluation_summary.get("snapshot_event_max") is None
    assert dossiers_index.get("count") == 0
    assert "No ranked leads" in review_board


def test_samgov_smoke_threshold_contract_fails_with_context_and_naics_misses(tmp_path: Path):
    db_path = tmp_path / "sam_smoke_fail.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_smoke_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "smoke_artifacts_fail",
        require_nonzero=True,
        threshold_overrides={
            "events_with_research_context_min": 4.0,
            "same_sam_naics_lane_min": 2.0,
        },
    )

    assert res["status"] == "failed"
    assert res["smoke_passed"] is False

    failed_by_name = {item.get("name"): item for item in res.get("failed_required_checks", [])}
    assert "sam_research_context_events_threshold" in failed_by_name
    assert "same_sam_naics_lane_threshold" in failed_by_name

    context_fail = failed_by_name["sam_research_context_events_threshold"]
    assert context_fail["expected"] == ">= 4"
    assert float(context_fail["observed"]) < 4.0
    assert "SAM.gov" in context_fail["why"]
    assert 'ss doctor status --source "SAM.gov"' in context_fail["hint"]

    naics_fail = failed_by_name["same_sam_naics_lane_threshold"]
    assert naics_fail["expected"] == ">= 2"
    assert int((naics_fail.get("actual") or {}).get("same_sam_naics", 0)) < 2
    assert "rebuild-sam-naics" in naics_fail["hint"]


def test_samgov_validation_required_quality_misses_fail_larger_mode(tmp_path: Path, monkeypatch):
    def fake_run_samgov_workflow(**_kwargs):
        return {
            "source": "SAM.gov",
            "status": "ok",
            "ingest": {"status": "success", "fetched": 25, "inserted": 25, "normalized": 25},
            "snapshot": {"items": 0},
            "exports": {},
        }

    def fake_doctor_status(**_kwargs):
        return {
            "db": {"status": "ok"},
            "counts": {
                "events_window": 30,
                "events_with_entity_window": 5,
                "lead_snapshots_total": 1,
            },
            "keywords": {
                "scanned_events": 30,
                "events_with_keywords": 6,
                "coverage_pct": 20.0,
                "unique_keywords": 2,
            },
            "entities": {
                "window_linked_coverage_pct": 16.7,
                "sample_scanned_events": 30,
                "sample_events_with_identity_signal": 30,
                "sample_events_with_identity_signal_linked": 5,
                "sample_identity_signal_coverage_pct": 16.7,
                "sample_events_with_name": 30,
                "sample_events_with_name_linked": 5,
                "sample_name_coverage_pct": 16.7,
            },
            "correlations": {
                "by_lane": {
                    "same_keyword": 0,
                    "kw_pair": 0,
                    "same_sam_naics": 0,
                    "same_entity": 1,
                    "same_uei": 0,
                }
            },
            "sam_context": {
                "scanned_events": 30,
                "events_with_research_context": 6,
                "research_context_coverage_pct": 20.0,
                "events_with_core_procurement_context": 6,
                "core_procurement_context_coverage_pct": 20.0,
                "avg_context_fields_per_event": 1.2,
                "coverage_by_field_pct": {
                    "sam_notice_type": 100.0,
                    "sam_solicitation_number": 100.0,
                    "sam_naics_code": 20.0,
                },
                "top_notice_types": [],
                "top_naics_codes": [],
                "top_set_aside_codes": [],
            },
            "hints": [],
        }

    monkeypatch.setattr(workflow_module, "run_samgov_workflow", fake_run_samgov_workflow)
    monkeypatch.setattr(workflow_module, "doctor_status", fake_doctor_status)

    res = run_samgov_validation_workflow(
        bundle_root=tmp_path / "validation_required_fail",
        require_nonzero=True,
        skip_ingest=False,
    )

    assert res["status"] == "failed"
    assert res["required_checks_passed"] is False
    assert res["quality"] == "degraded"
    assert res["required_failure_categories"] == [
        "source_coverage_context_health",
        "lead_signal_quality",
        "mission_quality",
    ]

    failed_by_name = {item.get("name"): item for item in res.get("failed_required_checks", [])}
    assert "events_with_entity_coverage_threshold" in failed_by_name
    assert "sam_research_context_coverage_threshold" in failed_by_name
    assert "keyword_or_kw_pair_signal_threshold" in failed_by_name
    assert "snapshot_items_threshold" in failed_by_name
    assert "top_leads_core_field_coverage_threshold" in failed_by_name
    assert "foia_draftability_threshold" in failed_by_name

    advisory_by_name = {item.get("name"): item for item in res.get("failed_advisory_checks", [])}
    assert "events_with_keywords_coverage_threshold" in advisory_by_name
    assert "same_sam_naics_lane_threshold" in advisory_by_name

    res_no_exit_gate = run_samgov_validation_workflow(
        bundle_root=tmp_path / "validation_required_fail_no_exit_gate",
        require_nonzero=False,
        skip_ingest=False,
    )
    assert res_no_exit_gate["status"] == "failed"
    assert res_no_exit_gate["required_checks_passed"] is False

def test_samgov_smoke_keyword_coverage_uses_sampled_population(tmp_path: Path, monkeypatch):
    def fake_run_samgov_workflow(**_kwargs):
        return {
            "source": "SAM.gov",
            "ingest": {"status": "success", "fetched": 10, "inserted": 10, "normalized": 10},
            "snapshot": {"items": 10},
            "exports": {},
        }

    def fake_doctor_status(**_kwargs):
        return {
            "db": {"status": "ok"},
            "counts": {
                "events_window": 1000,
                "events_with_entity_window": 1000,
                "lead_snapshots_total": 1,
            },
            "keywords": {
                "scanned_events": 50,
                "events_with_keywords": 50,
                "coverage_pct": 100.0,
                "unique_keywords": 2,
            },
            "entities": {
                "window_linked_coverage_pct": 100.0,
                "sample_scanned_events": 50,
                "sample_events_with_identity_signal": 50,
                "sample_events_with_identity_signal_linked": 50,
                "sample_identity_signal_coverage_pct": 100.0,
                "sample_events_with_name": 50,
                "sample_events_with_name_linked": 50,
                "sample_name_coverage_pct": 100.0,
            },
            "correlations": {
                "by_lane": {
                    "same_keyword": 2,
                    "kw_pair": 2,
                    "same_sam_naics": 2,
                    "same_entity": 2,
                    "same_uei": 0,
                }
            },
            "sam_context": {
                "scanned_events": 50,
                "events_with_research_context": 50,
                "research_context_coverage_pct": 100.0,
                "events_with_core_procurement_context": 50,
                "core_procurement_context_coverage_pct": 100.0,
                "avg_context_fields_per_event": 3.2,
                "coverage_by_field_pct": {
                    "sam_notice_type": 100.0,
                    "sam_solicitation_number": 100.0,
                    "sam_naics_code": 100.0,
                },
                "top_notice_types": [],
                "top_naics_codes": [],
                "top_set_aside_codes": [],
            },
            "hints": [],
        }

    monkeypatch.setattr(workflow_module, "run_samgov_workflow", fake_run_samgov_workflow)
    monkeypatch.setattr(workflow_module, "doctor_status", fake_doctor_status)

    res = run_samgov_smoke_workflow(
        bundle_root=tmp_path / "smoke_artifacts_sample_cov",
        require_nonzero=True,
        skip_ingest=False,
    )

    assert res["status"] == "warning"
    checks_by_name = {item.get("name"): item for item in res.get("checks", [])}
    kw_cov = checks_by_name["events_with_keywords_coverage_threshold"]

    assert kw_cov["status"] == "pass"
    assert kw_cov["observed"] == 100.0
    assert kw_cov["actual"] == {
        "events_with_keywords": 50,
        "sample_scanned_events": 50,
        "coverage_pct": 100.0,
    }
