import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from backend.services.adjudication import evaluate_lead_adjudications, export_lead_adjudication_template
from backend.db.models import Event, LeadSnapshotItem, ensure_schema, get_session_factory
import backend.services.workflow as workflow_module
from backend.services.workflow import (
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

    assert res["status"] == "ok"
    assert res["smoke_passed"] is True

    artifacts = res["artifacts"]
    summary_path = Path(artifacts["smoke_summary_json"])
    doctor_path = Path(artifacts["doctor_status_json"])
    workflow_path = Path(artifacts["workflow_result_json"])
    manifest_path = Path(artifacts["bundle_manifest_json"])
    report_path = Path(artifacts["report_html"])
    review_board_path = Path(artifacts["foia_lead_review_board_html"])
    review_board_md_path = Path(artifacts["foia_lead_review_board_md"])

    assert summary_path.exists()
    assert doctor_path.exists()
    assert workflow_path.exists()
    assert manifest_path.exists()
    assert report_path.exists()
    assert review_board_path.exists()
    assert review_board_md_path.exists()

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["smoke_passed"] is True
    assert summary_payload["required_checks_passed"] is True
    assert summary_payload["scoring_version"] == "v3"
    check_names = {c.get("name") for c in summary_payload.get("checks", [])}
    assert "events_window_threshold" in check_names
    assert "sam_research_context_events_threshold" in check_names
    assert "snapshot_items_threshold" in check_names

    assert summary_payload.get("thresholds")
    assert summary_payload.get("quality_gate_policy", {}).get("required_checks")
    assert summary_payload.get("failed_required_checks") == []

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload.get("bundle_version") == "samgov.bundle.v1"
    assert manifest_payload.get("workflow_type") == "samgov-smoke"
    assert manifest_payload.get("scoring_version") == "v3"
    generated_files = manifest_payload.get("generated_files") or {}
    assert "workflow_result_json" in generated_files
    assert "workflow_summary_json" in generated_files
    assert "bundle_manifest_json" in generated_files
    assert "report_html" in generated_files
    assert "foia_lead_review_board_html" in generated_files
    assert "foia_lead_review_board_md" in generated_files
    assert "export_lead_review_summary_json" in generated_files

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
    assert "Pipeline health" in report_html
    assert "Source coverage/context health" in report_html
    assert "Lead-signal quality" in report_html

    review_board_html = review_board_path.read_text(encoding="utf-8")
    assert "FOIA Lead Review Board" in review_board_html
    assert "Top Leads" in review_board_html

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


def test_samgov_smoke_bundle_records_explicit_posted_window(tmp_path: Path):
    db_path = tmp_path / "sam_smoke_window.db"
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
        posted_from=date(2024, 1, 1),
        posted_to=date(2024, 3, 31),
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "smoke_artifacts_dates",
        require_nonzero=True,
    )

    assert res["status"] == "ok"
    assert res["run_metadata"]["posted_window_mode"] == "explicit_dates"
    assert res["run_metadata"]["effective_posted_from"] == "2024-01-01"
    assert res["run_metadata"]["effective_posted_to"] == "2024-03-31"

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
    assert "2024-01-01" in report_html
    assert "2024-03-31" in report_html
    assert "scoring_version=v3" in report_html


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








def test_samgov_validation_workflow_emits_larger_mode_metadata(tmp_path: Path):
    db_path = tmp_path / "sam_validation.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_sam_events(db, now)
        db.commit()

    res = run_samgov_validation_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "validation_artifacts",
        require_nonzero=True,
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
    assert "ingest_nonzero" in (res.get("quality_gate_policy", {}).get("effective_advisory_checks") or [])
    overrides = res.get("quality_gate_policy", {}).get("policy_overrides") or []
    assert any(item.get("name") == "ingest_nonzero" for item in overrides)

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
    assert res["quality"]["required_failure_categories"] == [
        "source_coverage_context_health",
        "lead_signal_quality",
    ]

    failed_by_name = {item.get("name"): item for item in res.get("failed_required_checks", [])}
    assert "events_with_entity_coverage_threshold" in failed_by_name
    assert "sam_research_context_coverage_threshold" in failed_by_name
    assert "keyword_or_kw_pair_signal_threshold" in failed_by_name
    assert "snapshot_items_threshold" in failed_by_name

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

    assert res["status"] == "ok"
    checks_by_name = {item.get("name"): item for item in res.get("checks", [])}
    kw_cov = checks_by_name["events_with_keywords_coverage_threshold"]

    assert kw_cov["status"] == "pass"
    assert kw_cov["observed"] == 100.0
    assert kw_cov["actual"] == {
        "events_with_keywords": 50,
        "sample_scanned_events": 50,
        "coverage_pct": 100.0,
    }
