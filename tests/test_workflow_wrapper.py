import json
from datetime import datetime, timezone
from pathlib import Path

from backend.db.models import Event, ensure_schema, get_session_factory
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

    assert summary_path.exists()
    assert doctor_path.exists()
    assert workflow_path.exists()
    assert manifest_path.exists()
    assert report_path.exists()

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["smoke_passed"] is True
    check_names = {c.get("name") for c in summary_payload.get("checks", [])}
    assert "events_window_threshold" in check_names
    assert "sam_research_context_events_threshold" in check_names
    assert "snapshot_items_threshold" in check_names

    assert summary_payload.get("thresholds")
    assert summary_payload.get("failed_required_checks") == []

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload.get("bundle_version") == "samgov.bundle.v1"
    assert manifest_payload.get("workflow_type") == "samgov-smoke"
    generated_files = manifest_payload.get("generated_files") or {}
    assert "workflow_result_json" in generated_files
    assert "workflow_summary_json" in generated_files
    assert "bundle_manifest_json" in generated_files
    assert "report_html" in generated_files

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

    report_html = report_path.read_text(encoding="utf-8")
    assert "SAM.gov Workflow Bundle Report" in report_html
    assert "workflow_type=samgov-smoke" in report_html

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
    assert res.get("status") in {"ok", "warning"}

    artifacts = res.get("artifacts") or {}
    manifest_path = Path(artifacts.get("bundle_manifest_json"))
    summary_path = Path(artifacts.get("smoke_summary_json"))
    assert manifest_path.exists()
    assert summary_path.exists()

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload.get("validation_mode") == "larger"
    assert manifest_payload.get("workflow_type") == "samgov-validation"
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
