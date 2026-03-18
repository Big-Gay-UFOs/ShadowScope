from pathlib import Path

import pytest
from typer.testing import CliRunner

from shadowscope import cli as cli_module


runner = CliRunner()


def test_workflow_samgov_accepts_days_alias(monkeypatch):
    captured = {}

    def fake_run_samgov_workflow(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "source": "SAM.gov"}

    monkeypatch.setattr("backend.services.workflow.run_samgov_workflow", fake_run_samgov_workflow)

    result = runner.invoke(cli_module.app, ["workflow", "samgov", "--days", "11", "--json"])

    assert result.exit_code == 0, result.stdout
    assert captured.get("ingest_days") == 11


def test_workflow_samgov_accepts_explicit_posted_window(monkeypatch):
    captured = {}

    def fake_run_samgov_workflow(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "source": "SAM.gov"}

    monkeypatch.setattr("backend.services.workflow.run_samgov_workflow", fake_run_samgov_workflow)

    result = runner.invoke(
        cli_module.app,
        ["workflow", "samgov", "--posted-from", "2024-01-01", "--posted-to", "2024-03-31", "--json"],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("ingest_days") is None
    assert captured.get("posted_from").isoformat() == "2024-01-01"
    assert captured.get("posted_to").isoformat() == "2024-03-31"


def test_workflow_samgov_rejects_mixed_days_and_posted_window(monkeypatch):
    def fake_run_samgov_workflow(**kwargs):
        return {"status": "ok", "source": "SAM.gov"}

    monkeypatch.setattr("backend.services.workflow.run_samgov_workflow", fake_run_samgov_workflow)

    result = runner.invoke(
        cli_module.app,
        [
            "workflow",
            "samgov",
            "--days",
            "30",
            "--posted-from",
            "2024-01-01",
            "--posted-to",
            "2024-03-31",
        ],
    )

    assert result.exit_code != 0
    assert "Use either days or posted_from/posted_to, but not both." in result.output


def test_workflow_samgov_profile_defaults_to_starter(monkeypatch):
    captured = {}

    def fake_run_samgov_workflow(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "source": "SAM.gov"}

    monkeypatch.setattr("backend.services.workflow.run_samgov_workflow", fake_run_samgov_workflow)

    result = runner.invoke(cli_module.app, ["workflow", "samgov", "--json"])

    assert result.exit_code == 0, result.stdout
    assert captured.get("ontology_path") == Path("examples/ontology_sam_procurement_starter.json")


def test_workflow_samgov_profile_maps_dod_foia(monkeypatch):
    captured = {}

    def fake_run_samgov_workflow(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "source": "SAM.gov"}

    monkeypatch.setattr("backend.services.workflow.run_samgov_workflow", fake_run_samgov_workflow)

    result = runner.invoke(
        cli_module.app,
        ["workflow", "samgov", "--ontology-profile", "dod_foia", "--json"],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("ontology_path") == Path("examples/ontology_sam_dod_foia_companion.json")


def test_workflow_samgov_explicit_ontology_overrides_profile(monkeypatch):
    captured = {}

    def fake_run_samgov_workflow(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "source": "SAM.gov"}

    monkeypatch.setattr("backend.services.workflow.run_samgov_workflow", fake_run_samgov_workflow)

    custom = Path("examples/custom_override.json")
    result = runner.invoke(
        cli_module.app,
        [
            "workflow",
            "samgov",
            "--ontology-profile",
            "dod_foia",
            "--ontology",
            str(custom),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("ontology_path") == custom


def test_workflow_samgov_smoke_accepts_days_alias(monkeypatch):
    captured = {}

    def fake_run_samgov_smoke_workflow(**kwargs):
        captured.update(kwargs)
        return {
            "status": "ok",
            "smoke_passed": True,
            "bundle_dir": "data/exports/smoke/samgov/test",
            "checks": [],
            "baseline": {},
            "artifacts": {},
        }

    monkeypatch.setattr("backend.services.workflow.run_samgov_smoke_workflow", fake_run_samgov_smoke_workflow)

    result = runner.invoke(cli_module.app, ["workflow", "samgov-smoke", "--days", "9", "--json"])

    assert result.exit_code == 0, result.stdout
    assert captured.get("ingest_days") == 9


def test_workflow_samgov_smoke_accepts_explicit_posted_window(monkeypatch):
    captured = {}

    def fake_run_samgov_smoke_workflow(**kwargs):
        captured.update(kwargs)
        return {
            "status": "ok",
            "smoke_passed": True,
            "bundle_dir": "data/exports/smoke/samgov/test",
            "checks": [],
            "baseline": {},
            "artifacts": {},
            "run_metadata": {
                "posted_window_mode": "explicit_dates",
                "effective_posted_from": "2024-01-01",
                "effective_posted_to": "2024-03-31",
            },
        }

    monkeypatch.setattr("backend.services.workflow.run_samgov_smoke_workflow", fake_run_samgov_smoke_workflow)

    result = runner.invoke(
        cli_module.app,
        ["workflow", "samgov-smoke", "--posted-from", "2024-01-01", "--posted-to", "2024-03-31", "--json"],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("ingest_days") is None
    assert captured.get("posted_from").isoformat() == "2024-01-01"
    assert captured.get("posted_to").isoformat() == "2024-03-31"


def test_workflow_samgov_smoke_rejects_partial_posted_window(monkeypatch):
    def fake_run_samgov_smoke_workflow(**kwargs):
        return {
            "status": "ok",
            "smoke_passed": True,
            "bundle_dir": "data/exports/smoke/samgov/test",
            "checks": [],
            "baseline": {},
            "artifacts": {},
        }

    monkeypatch.setattr("backend.services.workflow.run_samgov_smoke_workflow", fake_run_samgov_smoke_workflow)

    result = runner.invoke(
        cli_module.app,
        ["workflow", "samgov-smoke", "--posted-from", "2024-01-01"],
    )

    assert result.exit_code != 0
    assert "posted_from and posted_to must be provided together in YYYY-MM-DD format." in result.output


def test_workflow_samgov_smoke_profile_maps_starter_plus_dod(monkeypatch):
    captured = {}

    def fake_run_samgov_smoke_workflow(**kwargs):
        captured.update(kwargs)
        return {
            "status": "ok",
            "smoke_passed": True,
            "bundle_dir": "data/exports/smoke/samgov/test",
            "checks": [],
            "baseline": {},
            "artifacts": {},
        }

    monkeypatch.setattr("backend.services.workflow.run_samgov_smoke_workflow", fake_run_samgov_smoke_workflow)

    result = runner.invoke(
        cli_module.app,
        ["workflow", "samgov-smoke", "--ontology-profile", "starter_plus_dod_foia", "--json"],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("ontology_path") == Path("examples/ontology_sam_procurement_plus_dod_foia.json")


def test_workflow_usaspending_default_min_events_keywords_is_two(monkeypatch):
    captured = {}

    def fake_run_usaspending_workflow(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "source": "USAspending"}

    monkeypatch.setattr("backend.services.workflow.run_usaspending_workflow", fake_run_usaspending_workflow)

    result = runner.invoke(cli_module.app, ["workflow", "usaspending", "--json"])

    assert result.exit_code == 0, result.stdout
    assert captured.get("min_events_keywords") == 2


def test_workflow_samgov_validate_accepts_days_alias(monkeypatch):
    captured = {}

    def fake_run_samgov_validation_workflow(**kwargs):
        captured.update(kwargs)
        return {
            "status": "ok",
            "smoke_passed": True,
            "bundle_dir": "data/exports/validation/samgov/test",
            "checks": [],
            "baseline": {},
            "artifacts": {},
        }

    monkeypatch.setattr("backend.services.workflow.run_samgov_validation_workflow", fake_run_samgov_validation_workflow)

    result = runner.invoke(cli_module.app, ["workflow", "samgov-validate", "--days", "21", "--json"])

    assert result.exit_code == 0, result.stdout
    assert captured.get("ingest_days") == 21


def test_workflow_samgov_validate_profile_maps_dod_foia(monkeypatch):
    captured = {}

    def fake_run_samgov_validation_workflow(**kwargs):
        captured.update(kwargs)
        return {
            "status": "ok",
            "smoke_passed": True,
            "bundle_dir": "data/exports/validation/samgov/test",
            "checks": [],
            "baseline": {},
            "artifacts": {},
        }

    monkeypatch.setattr("backend.services.workflow.run_samgov_validation_workflow", fake_run_samgov_validation_workflow)

    result = runner.invoke(
        cli_module.app,
        ["workflow", "samgov-validate", "--ontology-profile", "dod_foia", "--json"],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("ontology_path") == Path("examples/ontology_sam_dod_foia_companion.json")


def test_workflow_samgov_validate_cli_surfaces_required_failures(monkeypatch):
    def fake_run_samgov_validation_workflow(**_kwargs):
        return {
            "status": "failed",
            "required_checks_passed": False,
            "validation_mode": "larger",
            "bundle_dir": "data/exports/validation/samgov/test",
            "artifacts": {},
            "quality": {
                "quality": "hard_failure",
                "required_failure_categories": ["lead_signal_quality"],
                "advisory_failure_categories": ["source_coverage_context_health"],
            },
            "check_groups": {
                "lead_signal_quality": {
                    "category_label": "Lead-signal quality",
                    "required_total": 2,
                    "advisory_total": 1,
                    "failed_required": 1,
                    "failed_advisory": 0,
                }
            },
            "checks": [
                {
                    "name": "snapshot_items_threshold",
                    "result": "fail",
                    "severity": "error",
                    "policy_level": "required",
                    "category_label": "Lead-signal quality",
                    "observed": 0,
                    "expected": ">= 1",
                    "passed": False,
                    "why": "Lead snapshots must contain actionable SAM.gov rows for operator review.",
                    "hint": 'ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200',
                }
            ],
            "failed_required_checks": [
                {
                    "name": "snapshot_items_threshold",
                    "result": "fail",
                    "severity": "error",
                    "policy_level": "required",
                    "category_label": "Lead-signal quality",
                    "observed": 0,
                    "expected": ">= 1",
                    "passed": False,
                    "why": "Lead snapshots must contain actionable SAM.gov rows for operator review.",
                    "hint": 'ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200',
                }
            ],
            "failed_advisory_checks": [],
            "warning_checks": [],
        }

    monkeypatch.setattr("backend.services.workflow.run_samgov_validation_workflow", fake_run_samgov_validation_workflow)

    result = runner.invoke(cli_module.app, ["workflow", "samgov-validate"])

    assert result.exit_code == 2, result.stdout
    assert "required_failed=1" in result.stdout
    assert "Required failure categories: lead_signal_quality" in result.stdout
    assert "[FAIL][ERROR][REQUIRED][Lead-signal quality] snapshot_items_threshold" in result.stdout


@pytest.mark.parametrize(
    ("profile", "expected_path"),
    [
        ("hidden_program_proxy", Path("examples/ontology_sam_hidden_program_proxy_companion.json")),
        ("hidden_program_proxy_exploratory", Path("examples/ontology_sam_hidden_program_proxy_exploratory.json")),
        (
            "starter_plus_dod_foia_hidden_program_proxy",
            Path("examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json"),
        ),
        (
            "starter_plus_dod_foia_hidden_program_proxy_exploratory",
            Path("examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json"),
        ),
    ],
)
def test_workflow_samgov_profile_maps_hidden_program_proxy_variants(monkeypatch, profile, expected_path):
    captured = {}

    def fake_run_samgov_workflow(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "source": "SAM.gov"}

    monkeypatch.setattr("backend.services.workflow.run_samgov_workflow", fake_run_samgov_workflow)

    result = runner.invoke(
        cli_module.app,
        ["workflow", "samgov", "--ontology-profile", profile, "--json"],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("ontology_path") == expected_path


@pytest.mark.parametrize(
    ("seed_path", "expected_first", "expected_last", "expected_count"),
    [
        (
            Path("examples/terms/sam_hidden_program_proxy_core_seeds.txt"),
            "program protection",
            "restricted data",
            35,
        ),
        (
            Path("examples/terms/sam_hidden_program_proxy_expansion_seeds.txt"),
            "shielded enclosure",
            "dynamic positioning",
            51,
        ),
        (
            Path("examples/terms/sam_hidden_program_proxy_exploratory_seeds.txt"),
            "thermal vacuum chamber",
            "plasma spray",
            44,
        ),
    ],
)
def test_workflow_samgov_keywords_file_parses_proxy_seed_lists(monkeypatch, seed_path, expected_first, expected_last, expected_count):
    captured = {}

    def fake_run_samgov_workflow(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "source": "SAM.gov"}

    monkeypatch.setattr("backend.services.workflow.run_samgov_workflow", fake_run_samgov_workflow)

    result = runner.invoke(
        cli_module.app,
        ["workflow", "samgov", "--keywords-file", str(seed_path), "--json"],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("keywords")[0] == expected_first
    assert captured.get("keywords")[-1] == expected_last
    assert len(captured.get("keywords") or []) == expected_count


def test_ingest_samgov_keywords_file_merges_with_repeat_keyword(monkeypatch, tmp_path):
    captured = {}
    seed_file = tmp_path / "seed_terms.txt"
    seed_file.write_text("alpha\nbeta\n", encoding="utf-8")

    def fake_ingest_sam_opportunities(**kwargs):
        captured.update(kwargs)
        return {
            "status": "success",
            "run_id": 1,
            "fetched": 0,
            "inserted": 0,
            "normalized": 0,
            "snapshot_dir": str(tmp_path / "raw"),
        }

    monkeypatch.setattr(cli_module, "ingest_sam_opportunities", fake_ingest_sam_opportunities)

    result = runner.invoke(
        cli_module.app,
        [
            "ingest",
            "samgov",
            "--api-key",
            "dummy",
            "--keyword",
            "alpha",
            "--keyword",
            "gamma",
            "--keywords-file",
            str(seed_file),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("keywords") == ["alpha", "gamma", "beta"]

def test_ingest_samgov_accepts_explicit_posted_window(monkeypatch, tmp_path):
    captured = {}

    def fake_ingest_sam_opportunities(**kwargs):
        captured.update(kwargs)
        return {
            "status": "success",
            "run_id": 1,
            "fetched": 0,
            "inserted": 0,
            "normalized": 0,
            "snapshot_dir": str(tmp_path / "raw"),
            "date_window": {
                "mode": "explicit_dates",
                "posted_from": "2024-01-01",
                "posted_to": "2024-03-31",
            },
        }

    monkeypatch.setattr(cli_module, "ingest_sam_opportunities", fake_ingest_sam_opportunities)

    result = runner.invoke(
        cli_module.app,
        ["ingest", "samgov", "--api-key", "dummy", "--posted-from", "2024-01-01", "--posted-to", "2024-03-31"],
    )

    assert result.exit_code == 0, result.stdout
    assert captured.get("days") is None
    assert captured.get("posted_from").isoformat() == "2024-01-01"
    assert captured.get("posted_to").isoformat() == "2024-03-31"


def test_ingest_samgov_rejects_mixed_days_and_posted_window(monkeypatch):
    def fake_ingest_sam_opportunities(**kwargs):
        return {"status": "success", "run_id": 1, "fetched": 0, "inserted": 0, "normalized": 0, "snapshot_dir": "raw"}

    monkeypatch.setattr(cli_module, "ingest_sam_opportunities", fake_ingest_sam_opportunities)

    result = runner.invoke(
        cli_module.app,
        [
            "ingest",
            "samgov",
            "--api-key",
            "dummy",
            "--days",
            "7",
            "--posted-from",
            "2024-01-01",
            "--posted-to",
            "2024-03-31",
        ],
    )

    assert result.exit_code != 0
    assert "Use either days or posted_from/posted_to, but not both." in result.output
