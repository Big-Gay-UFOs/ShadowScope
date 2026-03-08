import json
from datetime import datetime, timezone
from pathlib import Path

from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.workflow import (
    run_samgov_smoke_workflow,
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
                snippet="Sources Sought RFP for construction generator upgrades with NAICS coverage",
                raw_json={
                    "noticeId": "SAM-001",
                    "title": "Sources Sought RFP Construction Generator",
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
                snippet="Sources Sought RFP for construction generator services with NAICS references",
                raw_json={
                    "noticeId": "SAM-002",
                    "title": "Sources Sought RFP Construction Services",
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
                snippet="Request for Proposal NAICS construction valve repair work",
                raw_json={
                    "noticeId": "SAM-003",
                    "title": "RFP Construction Valve Repair",
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
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        export_events_flag=False,
        min_score=1,
        snapshot_limit=200,
    )

    assert res["source"] == "USAspending"
    assert res["ontology_apply"]["scanned"] >= 4
    assert res["ontology_apply"]["updated"] >= 3
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
    assert res["correlations"]["same_keyword"]["eligible_keywords"] >= 1
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

    assert summary_path.exists()
    assert doctor_path.exists()
    assert workflow_path.exists()

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["smoke_passed"] is True
    check_names = {c.get("name") for c in summary_payload.get("checks", [])}
    assert "events_window_nonzero" in check_names
    assert "snapshot_items_nonzero" in check_names

    baseline = summary_payload.get("baseline", {})
    entity_cov = baseline.get("entity_coverage", {})
    assert entity_cov.get("window_linked_coverage_pct") is not None
    assert baseline.get("counts", {}).get("events_window", 0) > 0
