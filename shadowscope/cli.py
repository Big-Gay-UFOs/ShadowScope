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

app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(export_app, name="export")
app.add_typer(ontology_app, name="ontology")


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
    typer.echo(f"Summary: source=USAspending run_id={run_id} fetched={result['fetched']} inserted={result['inserted']} normalized={result['normalized']}")
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
    typer.echo(
        "Ontology apply summary: "
        f"dry_run={result['dry_run']} source={result['source']} days={result['days']} "
        f"scanned={result['scanned']} updated={result['updated']} unchanged={result['unchanged']} "
        f"ontology_hash={ont.get('hash')} rules={ont.get('total_rules')}"
    )

def run() -> None:
    app()


if __name__ == "__main__":
    run()