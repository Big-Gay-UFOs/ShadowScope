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
