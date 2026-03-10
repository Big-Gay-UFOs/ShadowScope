from pathlib import Path

from typer.testing import CliRunner

from shadowscope import cli as cli_module


runner = CliRunner()


def test_report_samgov_command_generates_report(tmp_path: Path):
    bundle = tmp_path / "bundle"
    bundle.mkdir(parents=True, exist_ok=True)

    result = runner.invoke(cli_module.app, ["report", "samgov", "--bundle", str(bundle)])

    assert result.exit_code == 0, result.stdout
    assert "Report status:" in result.stdout
    assert "Report HTML:" in result.stdout
    assert (bundle / "report.html").exists()


def test_report_latest_command_uses_latest_bundle(tmp_path: Path):
    root = tmp_path / "smoke" / "samgov"
    older = root / "20260309_090000"
    latest = root / "20260309_120000"
    older.mkdir(parents=True, exist_ok=True)
    latest.mkdir(parents=True, exist_ok=True)

    result = runner.invoke(
        cli_module.app,
        ["report", "latest", "--source", "SAM.gov", "--bundle-root", str(root)],
    )

    assert result.exit_code == 0, result.stdout
    assert "Report status:" in result.stdout
    assert str(latest.resolve()) in result.stdout
    assert (latest / "report.html").exists()
