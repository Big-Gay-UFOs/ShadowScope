"""Typer-based command line interface for ShadowScope."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

from backend.db.ops import reset_schema, stamp_head, sync_database
from backend.logging_config import configure_logging
from backend.runtime import ensure_runtime_directories
from backend.services.export import export_events
from backend.services.ingest import ingest_sam_opportunities, ingest_usaspending

app = typer.Typer(help="ShadowScope control plane")
db_app = typer.Typer(help="Database lifecycle commands")
ingest_app = typer.Typer(help="Data ingestion routines")
export_app = typer.Typer(help="Data export routines")
ontology_app = typer.Typer(help="Ontology utilities")
leads_app = typer.Typer(help="Lead utilities")
entities_app = typer.Typer(help="Entity utilities")

app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(export_app, name="export")
app.add_typer(ontology_app, name="ontology")
app.add_typer(leads_app, name="leads")
app.add_typer(entities_app, name="entities")


@app.callback()
def main_callback() -> None:
    load_dotenv()
    configure_logging()
    ensure_runtime_directories()


@db_app.command("init")
def db_init(database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command.")):
    status = sync_database(database_url)
    typer.echo(f"Database synchronized ({status}).")


@db_app.command("stamp")
def db_stamp(database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command.")):
    stamp_head(database_url)
    typer.echo("Alembic stamped to head.")


@db_app.command("reset")
def db_reset(
    destructive: bool = typer.Option(False, "--destructive", help="Confirm dropping and recreating the public schema."),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    if not destructive:
        raise typer.BadParameter("Reset requires --destructive confirmation.")
    reset_schema(database_url)
    typer.echo("Database schema dropped and recreated.")


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8000):
    import uvicorn
    uvicorn.run("backend.app:app", host=host, port=port, reload=False)


@app.command()
def test() -> None:
    env = os.environ.copy()
    env.setdefault("TEST_DATABASE_URL", env.get("DATABASE_URL", ""))
    cmd = [sys.executable, "-m", "pytest", "-q", "backend/tests"]
    typer.echo("Running pytest...")
    subprocess.run(cmd, check=True, env=env)


@ingest_app.command("usaspending")
def ingest_usaspending_cli(
    days: int = typer.Option(7, help="Days of history to request"),
    pages: int = typer.Option(1, help="Maximum API pages to request"),
    page_size: int = typer.Option(100, "--page-size", help="Records per API page (max 100)"),
    max_records: Optional[int] = typer.Option(None, "--max-records", "--limit", help="Total cap across all pages"),
    start_page: int = typer.Option(1, "--start-page", help="Start page (resume/chunking)"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    result = ingest_usaspending(
        days=days,
        pages=pages,
        page_size=page_size,
        max_records=max_records,
        start_page=start_page,
        database_url=database_url,
    )
    run_id = result.get("run_id")
    typer.echo(f"Run ID: {run_id}")
    typer.echo(
        f"Summary: source=USAspending run_id={run_id} fetched={result['fetched']} inserted={result['inserted']} normalized={result['normalized']}"
    )
    typer.echo(f"Ingested {result['fetched']} rows ({result['inserted']} inserted, {result['normalized']} normalized).")
    typer.echo(f"Raw snapshots: {Path(result['snapshot_dir']).resolve()}")


@ingest_app.command("sam")
def ingest_sam_cli(api_key: Optional[str] = typer.Option(None, help="Override SAM_API_KEY from environment")):
    token = api_key or os.getenv("SAM_API_KEY")
    result = ingest_sam_opportunities(token)
    typer.echo(f"SAM ingest status: {result['status']}")


@export_app.command("events")
def export_events_cli(
    out: Optional[str] = typer.Option(None, "--out", help="Output directory or CSV file path"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    export_path = Path(out).expanduser() if out else None
    results = export_events(database_url=database_url, output=export_path)
    typer.echo(f"Events CSV: {results['csv'].resolve()}")
    typer.echo(f"Events JSONL: {results['jsonl'].resolve()}")
    typer.echo(f"Rows exported: {results['count']}")


@ontology_app.command("validate")
def ontology_validate(path: Path = typer.Option(Path("ontology.json"), "--path", "-p", help="Path to ontology.json")):
    from backend.analysis.ontology import load_ontology, validate_ontology, summarize_ontology

    obj = load_ontology(path)
    errs = validate_ontology(obj)
    if errs:
        typer.echo("Ontology INVALID:")
        for e in errs:
            typer.echo(f"- {e}")
        raise typer.Exit(code=2)

    summary = summarize_ontology(obj)
    typer.echo("Ontology OK")
    typer.echo(json.dumps(summary, indent=2))


@ontology_app.command("apply")
def ontology_apply(
    path: Path = typer.Option(Path("ontology.json"), "--path", "-p", help="Path to ontology.json"),
    days: int = typer.Option(30, "--days", help="Tag events created in the last N days (and/or occurred_at within window or null)"),
    source: str = typer.Option("USAspending", "--source", help="Event source to tag (default USAspending)"),
    batch: int = typer.Option(500, "--batch", help="DB batch size"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute changes but do not write updates"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    from backend.services.tagging import apply_ontology_to_events

    result = apply_ontology_to_events(
        ontology_path=path,
        days=days,
        source=source,
        batch=batch,
        dry_run=dry_run,
        database_url=database_url,
    )

    ont = result["ontology"]
    arid = result.get("analysis_run_id")
    ar = f"analysis_run_id={arid} " if arid else ""
    typer.echo(
        "Ontology apply summary: "
        f"{ar}dry_run={result['dry_run']} source={result['source']} days={result['days']} "
        f"scanned={result['scanned']} updated={result['updated']} unchanged={result['unchanged']} "
        f"ontology_hash={ont.get('hash')} rules={ont.get('total_rules')}"
    )


@leads_app.command("snapshot")
def leads_snapshot(
    analysis_run_id: Optional[int] = typer.Option(None, "--analysis-run-id", help="Optional link to an analysis_runs.id"),
    source: Optional[str] = typer.Option(None, "--source", help="Filter by event source (e.g. USAspending)"),
    exclude_source: Optional[str] = typer.Option(None, "--exclude-source", help="Exclude an event source"),
    min_score: int = typer.Option(1, "--min-score", help="Minimum score to include"),
    limit: int = typer.Option(200, "--limit", help="Max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="How many recent events to scan before ranking"),
    scoring_version: str = typer.Option("v1", "--scoring-version", help="Scoring version label"),
    notes: Optional[str] = typer.Option(None, "--notes", help="Optional snapshot notes"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    from backend.services.leads import create_lead_snapshot

    result = create_lead_snapshot(
        analysis_run_id=analysis_run_id,
        source=source,
        exclude_source=exclude_source,
        min_score=min_score,
        limit=limit,
        scan_limit=scan_limit,
        scoring_version=scoring_version,
        notes=notes,
        database_url=database_url,
    )

    typer.echo(
        "Lead snapshot created: "
        f"snapshot_id={result['snapshot_id']} items={result['items']} scanned={result['scanned']} "
        f"analysis_run_id={result['analysis_run_id']} source={result['source']} min_score={result['min_score']} "
        f"limit={result['limit']} scoring_version={result['scoring_version']}"
    )


@leads_app.command("delta")
def leads_delta(
    from_snapshot_id: int = typer.Option(..., "--from-snapshot-id", help="Baseline lead snapshot id"),
    to_snapshot_id: int = typer.Option(..., "--to-snapshot-id", help="Comparison lead snapshot id"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
    json_out: bool = typer.Option(True, "--json/--no-json", help="Print full JSON delta output"),
):
    from backend.services.deltas import compute_lead_deltas

    res = compute_lead_deltas(
        from_snapshot_id=from_snapshot_id,
        to_snapshot_id=to_snapshot_id,
        database_url=database_url,
    )

    c = res.get("counts", {})
    typer.echo(
        f"Delta summary: from={from_snapshot_id} to={to_snapshot_id} "
        f"new={c.get('new')} removed={c.get('removed')} changed={c.get('changed')}"
    )
    if json_out:
        typer.echo(json.dumps(res, indent=2))

@entities_app.command("link")
def entities_link(
    source: str = typer.Option("USAspending", "--source", help="Event source to link (default USAspending)"),
    days: int = typer.Option(30, "--days", help="Only process events created in the last N days"),
    batch: int = typer.Option(500, "--batch", help="DB batch size"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute changes but do not write updates"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    from backend.services.entities import link_entities_from_events

    res = link_entities_from_events(
        source=source,
        days=days,
        batch=batch,
        dry_run=dry_run,
        database_url=database_url,
    )
    typer.echo(
        "Entity link summary: "
        f"dry_run={res['dry_run']} source={res['source']} days={res['days']} "
        f"scanned={res['scanned']} linked={res['linked']} skipped_no_name={res['skipped_no_name']} "
        f"entities_created={res['entities_created']}"
    )

def run() -> None:
    app()


if __name__ == "__main__":
    run()
# ---- Correlation utilities (M4-02) ----
correlate_app = typer.Typer(help="Correlation utilities")

@correlate_app.command("rebuild")
def correlate_rebuild(
    window_days: int = typer.Option(30, "--window-days", help="Lookback window (days) for correlation grouping"),
    source: str = typer.Option("USAspending", "--source", help="Event source to correlate (blank for all)"),
    min_events: int = typer.Option(2, "--min-events", help="Minimum events required to form a correlation"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute only; do not write to DB"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.correlate import correlate
    res = correlate.rebuild_entity_correlations(
        window_days=window_days,
        source=source if source else None,
        min_events=min_events,
        dry_run=dry_run,
        database_url=database_url,
    )
    typer.echo(
        "Correlation rebuild: "
        + " ".join([f"{k}={v}" for k, v in res.items() if k in ("dry_run","source","window_days","min_events","entities_seen","eligible_entities","deleted_correlations","deleted_links","correlations_created","links_created")])
    )


@correlate_app.command("rebuild-uei")
def correlate_rebuild_uei(
    window_days: int = typer.Option(30, "--window-days", help="Lookback window (days)"),
    source: str = typer.Option("USAspending", "--source", help="Event source (blank for all)"),
    min_events: int = typer.Option(2, "--min-events", help="Minimum events to form a correlation"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute only; do not write to DB"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.correlate import correlate
    res = correlate.rebuild_uei_correlations(
        window_days=window_days,
        source=source if source else None,
        min_events=min_events,
        dry_run=dry_run,
        database_url=database_url,
    )
    typer.echo(
        "UEI correlation rebuild: "
        + " ".join([f"{k}={v}" for k, v in res.items() if k in ("dry_run","source","window_days","min_events","ueis_seen","eligible_ueis","correlations_created","correlations_updated","correlations_deleted","links_created")])
    )
app.add_typer(correlate_app, name="correlate")
@export_app.command("correlations")
def export_correlations_cmd(
    out: str = typer.Option("data/exports/correlations.json", "--out", help="Output JSON path"),
    source: str = typer.Option("USAspending", "--source", help="Event source filter (blank for all)"),
    window_days: int = typer.Option(None, "--window-days", help="Filter correlations by window_days"),
    min_score: int = typer.Option(None, "--min-score", help="Minimum numeric score"),
    limit: int = typer.Option(500, "--limit", help="Max correlations to export"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.services.export_correlations import export_correlations
    res = export_correlations(
        out_path=out,
        source=source if source else None,
        window_days=window_days,
        min_score=min_score,
        limit=limit,
        database_url=database_url,
    )
    typer.echo(f"Exported correlations: count={res['count']} out={res['out_path']}")
