import json
import re
from pathlib import Path

from backend.services.reporting import (
    find_latest_sam_smoke_bundle,
    generate_sam_report,
    generate_sam_report_from_bundle,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _sample_payloads(bundle_dir: Path) -> tuple[dict, dict, dict, dict]:
    lead_json = bundle_dir / "exports" / "lead_snapshot_demo.json"
    entities_json = bundle_dir / "exports" / "entities_demo.json"

    _write_json(
        lead_json,
        {
            "count": 1,
            "items": [
                {
                    "rank": 1,
                    "score": 9,
                    "doc_id": "DOC-001",
                    "entity_id": 101,
                    "source_url": "https://sam.gov/opp/1",
                    "why_summary": "keywords + entity + pair",
                }
            ],
        },
    )
    _write_json(
        entities_json,
        {
            "count": 1,
            "items": [
                {
                    "entity_id": 101,
                    "name": "Acme Federal",
                    "uei": "UEI-ACME",
                    "cage": "ABC12",
                    "type": "vendor",
                }
            ],
        },
    )

    workflow = {
        "source": "SAM.gov",
        "status": "ok",
        "ingest": {
            "status": "success",
            "run_id": 1,
            "fetched": 10,
            "inserted": 10,
            "normalized": 10,
            "snapshot_dir": str(bundle_dir / "raw"),
        },
        "ontology_apply": {"scanned": 10, "updated": 9, "unchanged": 1},
        "entities_link": {"scanned": 10, "linked": 9, "entities_created": 2},
        "correlations": {
            "same_entity": {"correlations_created": 1, "correlations_updated": 0, "links_created": 2},
            "kw_pair": {"correlations_created": 4, "correlations_updated": 1, "links_created": 9},
        },
        "snapshot": {"snapshot_id": 7, "items": 3, "scanned": 10},
        "exports": {
            "lead_snapshot": {"json": str(lead_json), "csv": str(bundle_dir / "exports" / "lead_snapshot_demo.csv")},
            "entities": {
                "entities_json": str(entities_json),
                "entities_csv": str(bundle_dir / "exports" / "entities_demo.csv"),
                "event_entities_json": str(bundle_dir / "exports" / "event_entities_demo.json"),
                "event_entities_csv": str(bundle_dir / "exports" / "event_entities_demo.csv"),
            },
        },
    }

    doctor = {
        "db": {"status": "ok", "url": "sqlite:///tmp.db"},
        "window": {"days": 30, "since": "2026-03-01T00:00:00+00:00", "source": "SAM.gov"},
        "counts": {"events_total": 10, "events_window": 10, "events_with_entity_window": 9},
        "entities": {"window_linked_coverage_pct": 90.0},
        "keywords": {
            "coverage_pct": 100.0,
            "unique_keywords": 3,
            "top_keywords": [{"keyword": "procurement", "count": 5}],
        },
        "correlations": {"by_lane": {"same_entity": 1, "kw_pair": 4, "same_sam_naics": 1}},
        "last_runs": {"ingest": {"id": 1, "status": "success", "source": "SAM.gov"}},
        "hints": ["none"],
    }

    artifacts = {
        "workflow_result_json": str(bundle_dir / "workflow_result.json"),
        "doctor_status_json": str(bundle_dir / "doctor_status.json"),
        "smoke_summary_json": str(bundle_dir / "smoke_summary.json"),
        "exports": workflow["exports"],
    }

    smoke = {
        "generated_at": "2026-03-09T12:00:00+00:00",
        "source": "SAM.gov",
        "smoke_passed": True,
        "checks": [],
        "baseline": {},
        "artifacts": artifacts,
        "run_metadata": {
            "source": "SAM.gov",
            "workflow_type": "samgov-smoke",
            "run_timestamp": "2026-03-09T12:00:00+00:00",
            "ingest_days": 30,
            "posted_window_mode": "explicit_dates",
            "effective_posted_from": "2024-01-01",
            "effective_posted_to": "2024-03-31",
            "pages": 2,
            "page_size": 100,
            "max_records": 50,
            "start_page": 1,
            "window_days": 30,
        },
    }

    return workflow, doctor, smoke, artifacts


def _write_hardened_bundle(bundle_dir: Path, *, status: str, required_checks_passed: bool, quality_name: str) -> None:
    workflow, doctor, smoke, _artifacts = _sample_payloads(bundle_dir)
    results_dir = bundle_dir / "results"
    workflow_json = results_dir / "workflow_result.json"
    doctor_json = results_dir / "doctor_status.json"
    summary_json = results_dir / "workflow_summary.json"

    smoke.update(
        {
            "workflow_type": "samgov-validation",
            "validation_mode": "larger",
            "status": status,
            "smoke_passed": required_checks_passed,
            "required_checks_passed": required_checks_passed,
            "failed_required_checks": [],
            "failed_advisory_checks": [
                {
                    "name": "events_with_keywords_coverage_threshold",
                    "category": "lead_signal_quality",
                    "category_label": "Lead-signal quality",
                    "policy_level": "advisory",
                    "passed": False,
                }
            ]
            if status == "warning"
            else [],
            "quality": {
                "quality": quality_name,
                "required_failure_categories": [],
                "advisory_failure_categories": ["lead_signal_quality"] if status == "warning" else [],
            },
            "check_groups": {
                "pipeline_health": {
                    "category_label": "Pipeline health",
                    "required_total": 3,
                    "advisory_total": 1,
                    "failed_required": 0,
                    "failed_advisory": 0,
                },
                "lead_signal_quality": {
                    "category_label": "Lead-signal quality",
                    "required_total": 2,
                    "advisory_total": 1,
                    "failed_required": 0,
                    "failed_advisory": 1 if status == "warning" else 0,
                },
            },
            "artifacts": {
                "workflow_result_json": str(workflow_json),
                "doctor_status_json": str(doctor_json),
                "smoke_summary_json": str(summary_json),
                "workflow_summary_json": str(summary_json),
                "bundle_manifest_json": str(bundle_dir / "bundle_manifest.json"),
                "exports": workflow["exports"],
            },
            "run_metadata": {
                "source": "SAM.gov",
                "workflow_type": "samgov-validation",
                "run_timestamp": "2026-03-09T12:00:00+00:00",
                "ingest_days": 30,
                "pages": 5,
                "page_size": 100,
                "max_records": 250,
                "start_page": 1,
                "window_days": 30,
                "validation_mode": "larger",
            },
        }
    )

    _write_json(workflow_json, {"generated_at": "2026-03-09T12:00:00+00:00", "result": workflow})
    _write_json(doctor_json, {"generated_at": "2026-03-09T12:00:00+00:00", "result": doctor})
    _write_json(summary_json, smoke)
    _write_json(
        bundle_dir / "bundle_manifest.json",
        {
            "bundle_version": "samgov.bundle.v1",
            "source": "SAM.gov",
            "workflow_type": "samgov-validation",
            "validation_mode": "larger",
            "generated_at": "2026-03-09T12:00:00+00:00",
            "status": status,
            "quality": smoke["quality"],
            "run_parameters": smoke["run_metadata"],
            "generated_files": {
                "workflow_result_json": "results/workflow_result.json",
                "doctor_status_json": "results/doctor_status.json",
                "workflow_summary_json": "results/workflow_summary.json",
            },
        },
    )


def test_generate_sam_report_contains_expected_sections(tmp_path: Path):
    bundle = tmp_path / "bundle"
    workflow, doctor, smoke, artifacts = _sample_payloads(bundle)

    report_path = generate_sam_report(
        bundle_dir=bundle,
        workflow_type="samgov-smoke",
        source="SAM.gov",
        generated_at="2026-03-09T12:00:00+00:00",
        run_metadata=smoke["run_metadata"],
        workflow_result=workflow,
        doctor_status_result=doctor,
        smoke_summary=smoke,
        artifacts=artifacts,
    )

    assert report_path.exists()
    html = report_path.read_text(encoding="utf-8")
    assert "SAM.gov Workflow Report" in html
    assert "PASS" in html
    assert "Run Metadata" in html
    assert "Ingest Summary" in html
    assert "Doctor Summary" in html
    assert "Top Keywords" in html
    assert "Correlation Lanes" in html
    assert "Top Leads" in html
    assert "Top Entities" in html
    assert "Artifacts" in html
    assert "DOC-001" in html
    assert "Acme Federal" in html
    assert "2024-01-01" in html
    assert "2024-03-31" in html


def test_generate_sam_report_handles_missing_optional_sections(tmp_path: Path):
    bundle = tmp_path / "bundle_missing"

    report_path = generate_sam_report(
        bundle_dir=bundle,
        workflow_type="samgov",
        source="SAM.gov",
        generated_at="2026-03-09T12:00:00+00:00",
        run_metadata={},
        workflow_result={},
        doctor_status_result={},
        smoke_summary={},
        artifacts={},
    )

    assert report_path.exists()
    html = report_path.read_text(encoding="utf-8")
    assert "WARNING" in html
    assert "Unavailable" in html
    assert "Top keyword data unavailable." in html


def test_generate_sam_report_from_bundle_path(tmp_path: Path):
    bundle = tmp_path / "bundle_from_files"
    workflow, doctor, smoke, artifacts = _sample_payloads(bundle)

    _write_json(bundle / "workflow_result.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": workflow})
    _write_json(bundle / "doctor_status.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": doctor})
    _write_json(bundle / "smoke_summary.json", smoke)

    res = generate_sam_report_from_bundle(bundle / "smoke_summary.json")

    report_path = Path(res["report_html"])
    assert res["status"] == "PASS"
    assert res["workflow_type"] == "samgov-smoke"
    assert report_path.exists()
    html = report_path.read_text(encoding="utf-8")
    assert "DOC-001" in html
    assert "procurement" in html


def test_generate_sam_report_from_results_bundle_layout(tmp_path: Path):
    bundle = tmp_path / "bundle_results_layout"
    workflow, doctor, smoke, artifacts = _sample_payloads(bundle)

    _write_json(bundle / "results" / "workflow_result.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": workflow})
    _write_json(bundle / "results" / "doctor_status.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": doctor})
    _write_json(bundle / "results" / "workflow_summary.json", smoke)

    res = generate_sam_report_from_bundle(bundle)

    report_path = Path(res["report_html"])
    assert res["status"] == "PASS"
    assert report_path.exists()
    html = report_path.read_text(encoding="utf-8")
    assert "2024-01-01" in html
    assert "2024-03-31" in html


def test_generate_sam_report_from_hardened_bundle_manifest_paths(tmp_path: Path):
    bundle = tmp_path / "bundle_hardened"
    _write_hardened_bundle(bundle, status="warning", required_checks_passed=True, quality_name="partially_useful")

    res = generate_sam_report_from_bundle(bundle)

    report_path = Path(res["report_html"])
    assert res["status"] == "WARNING"
    assert report_path.exists()
    html = report_path.read_text(encoding="utf-8")
    assert "Validation Categories" in html
    assert "Workflow Gate Status" in html
    assert "Lead-signal quality" in html
    assert "partially_useful" in html


def test_find_latest_sam_smoke_bundle_uses_latest_stamp(tmp_path: Path):
    root = tmp_path / "smoke" / "samgov"
    old = root / "20260309_100000"
    new = root / "20260309_120000"
    old.mkdir(parents=True, exist_ok=True)
    new.mkdir(parents=True, exist_ok=True)

    assert find_latest_sam_smoke_bundle(root) == new


def test_generate_report_resolves_bundle_prefixed_relative_artifact_paths(tmp_path: Path):
    bundle = tmp_path / "tmp_rel_bundle"
    workflow, doctor, smoke, artifacts = _sample_payloads(bundle)

    artifacts["workflow_result_json"] = f"{bundle.name}/workflow_result.json"
    artifacts["doctor_status_json"] = f"{bundle.name}/doctor_status.json"
    artifacts["smoke_summary_json"] = f"{bundle.name}/smoke_summary.json"
    smoke["artifacts"] = artifacts

    _write_json(bundle / "workflow_result.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": workflow})
    _write_json(bundle / "doctor_status.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": doctor})
    _write_json(bundle / "smoke_summary.json", smoke)

    res = generate_sam_report_from_bundle(bundle)
    report_html = Path(res["report_html"]).read_text(encoding="utf-8")

    assert re.search(r"<tr><td>workflow_result_json</td><td>.*?</td><td>yes</td></tr>", report_html)
    assert re.search(r"<tr><td>doctor_status_json</td><td>.*?</td><td>yes</td></tr>", report_html)


def test_find_latest_sam_smoke_bundle_ignores_non_stamped_directories(tmp_path: Path):
    root = tmp_path / "smoke" / "samgov"
    (root / "archive").mkdir(parents=True, exist_ok=True)
    (root / "tmp").mkdir(parents=True, exist_ok=True)
    stamped = root / "20260309_120000"
    stamped.mkdir(parents=True, exist_ok=True)

    assert find_latest_sam_smoke_bundle(root) == stamped


def test_find_latest_sam_smoke_bundle_returns_none_when_no_stamped_runs(tmp_path: Path):
    root = tmp_path / "smoke" / "samgov"
    (root / "archive").mkdir(parents=True, exist_ok=True)
    (root / "tmp").mkdir(parents=True, exist_ok=True)

    assert find_latest_sam_smoke_bundle(root) is None


def test_generate_report_does_not_resolve_non_bundle_prefixed_parent_artifacts(tmp_path: Path):
    root = tmp_path / "smoke" / "samgov"
    bundle = root / "20260309_130000"
    helper = root / "helper"

    workflow, doctor, smoke, artifacts = _sample_payloads(bundle)

    # Deliberately point at a parent-level helper path that is not bundle-prefixed.
    artifacts["workflow_result_json"] = "helper/workflow_result.json"
    smoke["artifacts"] = artifacts

    helper.mkdir(parents=True, exist_ok=True)
    _write_json(helper / "workflow_result.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": {"source": "SAM.gov"}})

    _write_json(bundle / "workflow_result.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": workflow})
    _write_json(bundle / "doctor_status.json", {"generated_at": "2026-03-09T12:00:00+00:00", "result": doctor})
    _write_json(bundle / "smoke_summary.json", smoke)

    res = generate_sam_report_from_bundle(bundle)
    report_html = Path(res["report_html"]).read_text(encoding="utf-8")

    assert re.search(r"<tr><td>workflow_result_json</td><td>.*?</td><td>no</td></tr>", report_html)
