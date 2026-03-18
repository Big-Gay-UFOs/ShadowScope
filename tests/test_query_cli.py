import json
import os
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory
from shadowscope import cli as cli_module


runner = CliRunner()


def test_leads_query_cli_returns_filtered_json(tmp_path: Path):
    db_path = tmp_path / "cli_query.db"
    db_url = f"sqlite:///{db_path.as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        event = Event(
            category="notice",
            source="SAM.gov",
            hash="cli_lead_1",
            snippet="alpha beta item",
            place_text="Northern Virginia",
            doc_id="sam-1",
            source_url="http://example.com/sam/1",
            awarding_agency_name="Department of Energy",
            recipient_uei="UEI-CLI-1",
            raw_json={},
            keywords=["alpha", "beta"],
            clauses=[{"pack": "focus", "rule": "alpha_beta", "weight": 6}],
            created_at=datetime.now(timezone.utc),
        )
        db.add(event)
        db.flush()
        corr = Correlation(
            correlation_key="kw_pair|SAM.gov|30|pair:cli",
            score="4",
            window_days=30,
            radius_km=0.0,
            lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": 2, "score_signal": 4},
        )
        db.add(corr)
        db.flush()
        db.add(CorrelationLink(correlation_id=int(corr.id), event_id=int(event.id)))
        db.commit()

    os.environ["DATABASE_URL"] = db_url
    result = runner.invoke(
        cli_module.app,
        [
            "leads",
            "query",
            "--min-score",
            "0",
            "--source",
            "SAM.gov",
            "--lane",
            "kw_pair",
            "--min-score-signal",
            "4",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["total"] == 1
    assert payload["items"][0]["doc_id"] == "sam-1"


def test_export_evidence_package_cli_reports_output(monkeypatch, tmp_path: Path):
    expected = tmp_path / "pkg.json"
    expected.write_text("{}", encoding="utf-8")

    def fake_export(**kwargs):
        return {
            "json": expected,
            "package_type": "lead_evidence_package",
            "source_record_count": 2,
        }

    monkeypatch.setattr("backend.services.evidence_package.export_evidence_package", fake_export)

    result = runner.invoke(
        cli_module.app,
        ["export", "evidence-package", "--snapshot-id", "7", "--lead-event-id", "11"],
    )

    assert result.exit_code == 0, result.stdout
    assert "Evidence package JSON:" in result.stdout
    assert "Package type: lead_evidence_package" in result.stdout


def test_export_leads_cli_forwards_lead_family(monkeypatch, tmp_path: Path):
    expected_csv = tmp_path / "lead_snapshot.csv"
    expected_json = tmp_path / "lead_snapshot.json"
    expected_csv.write_text("rank,lead_family\n1,vendor_network_contract_lineage\n", encoding="utf-8")
    expected_json.write_text("{}", encoding="utf-8")
    captured: dict[str, object] = {}

    def fake_export_lead_snapshot(**kwargs):
        captured.update(kwargs)
        return {
            "csv": expected_csv,
            "json": expected_json,
            "count": 1,
        }

    monkeypatch.setattr(cli_module, "export_lead_snapshot", fake_export_lead_snapshot)

    result = runner.invoke(
        cli_module.app,
        [
            "export",
            "leads",
            "--snapshot-id",
            "7",
            "--lead-family",
            "vendor_network_contract_lineage",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured["snapshot_id"] == 7
    assert captured["lead_family"] == "vendor_network_contract_lineage"
    assert "Rows exported: 1" in result.stdout
