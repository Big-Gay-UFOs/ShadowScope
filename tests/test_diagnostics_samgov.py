from datetime import datetime, timezone
from pathlib import Path

from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.bundle import inspect_bundle
from backend.services.diagnostics import diagnose_samgov
from backend.services.workflow import run_samgov_smoke_workflow


def _seed_events(db, now: datetime) -> None:
    db.add_all(
        [
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="diag-sam-1",
                created_at=now,
                doc_id="DIAG-001",
                source_url="https://sam.gov/opp/diag-1",
                snippet="Sources sought for engineering sustainment support",
                raw_json={
                    "noticeId": "DIAG-001",
                    "title": "Sources Sought Engineering Sustainment",
                    "noticeType": "Sources Sought",
                    "solicitationNumber": "DOE-DIAG-001",
                    "naicsCode": "541330",
                    "typeOfSetAside": "SBA",
                    "responseDeadLine": "2026-03-15",
                    "fullParentPathCode": "DOE.HQ",
                    "Recipient Name": "Diag Corp",
                },
                keywords=[],
                clauses=[],
            ),
            Event(
                category="opportunity",
                source="SAM.gov",
                hash="diag-sam-2",
                created_at=now,
                doc_id="DIAG-002",
                source_url="https://sam.gov/opp/diag-2",
                snippet="Cyber operations procurement",
                raw_json={
                    "noticeId": "DIAG-002",
                    "title": "Cyber Operations Procurement",
                    "noticeType": "Solicitation",
                    "solicitationNumber": "DOE-DIAG-002",
                    "naicsCode": "541512",
                    "typeOfSetAside": "8A",
                    "responseDeadLine": "2026-03-16",
                    "fullParentPathCode": "DOE.FIELD",
                    "Recipient Name": "Diag Labs",
                },
                keywords=[],
                clauses=[],
            ),
        ]
    )


def test_inspect_bundle_reads_manifest_and_files(tmp_path: Path):
    db_path = tmp_path / "diag_bundle.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_events(db, now)
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
        bundle_root=tmp_path / "smoke_bundles",
        require_nonzero=True,
    )

    bundle_dir = Path(res["bundle_dir"])
    inspected = inspect_bundle(bundle_dir)

    assert inspected["status"] == "ok"
    manifest = inspected.get("manifest") or {}
    assert manifest.get("bundle_version") == "samgov.bundle.v1"
    generated_files = inspected.get("generated_files") or {}
    assert "workflow_result_json" in generated_files
    assert "report_html" in generated_files
    assert generated_files["report_html"]["exists"] is True


def test_diagnose_samgov_reports_bundle_and_gap_metrics(tmp_path: Path):
    db_path = tmp_path / "diag_status.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_events(db, now)
        db.commit()

    smoke = run_samgov_smoke_workflow(
        database_url=db_url,
        skip_ingest=True,
        ontology_path=Path("examples/ontology_sam_procurement_starter.json"),
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,
        max_events_keywords=200,
        max_keywords_per_event=10,
        bundle_root=tmp_path / "smoke_bundles_2",
        require_nonzero=True,
    )

    diag = diagnose_samgov(
        days=30,
        scan_limit=200,
        max_keywords_per_event=10,
        database_url=db_url,
        bundle_path=Path(smoke["bundle_dir"]),
    )

    assert diag.get("source") == "SAM.gov"
    assert diag.get("classification") in {
        "healthy",
        "partially_useful",
        "sparse_valid",
        "rate_limited_degraded",
        "broken",
        "degraded",
    }
    bundle = diag.get("bundle") or {}
    inspection = bundle.get("inspection") or {}
    assert inspection.get("status") == "ok"

    gaps = diag.get("gaps") or {}
    assert "untagged_events" in gaps
    assert "events_without_entities" in gaps
    assert "events_without_lead_value" in gaps
    assert isinstance(diag.get("recommendations"), list)

