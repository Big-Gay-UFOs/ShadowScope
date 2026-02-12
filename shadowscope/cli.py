"""Typer-based command line interface for ShadowScope."""
from __future__ import annotations

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

app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(export_app, name="export")


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
    destructive: bool = typer.Option(
        False,
        "--destructive",
        help="Confirm dropping and recreating the public schema.",
    ),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    if not destructive:
        raise typer.BadParameter("Reset requires --destructive confirmation.")
    reset_schema(database_url)
    typer.echo("Database schema dropped and recreated.")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Host interface for the API"),
    port: int = typer.Option(8000, help="Port for the API"),
):
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
    days: int = typer.Option(7, help="Number of days of history to request"),
    limit: int = typer.Option(100, help="Maximum records to pull"),
    pages: int = typer.Option(1, help="Maximum API pages to request"),
    start_page: int = typer.Option(1, "--start-page", help="Start page (for resume/chunking)"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    result = ingest_usaspending(days=days, limit=limit, pages=pages, start_page=start_page, database_url=database_url)
    typer.echo(
        f"Ingested {result['fetched']} rows ({result['inserted']} inserted, {result['normalized']} normalized)."
    )
    typer.echo(f"Raw snapshots: {Path(result['snapshot_dir']).resolve()}")


@ingest_app.command("sam")
def ingest_sam_cli(api_key: Optional[str] = typer.Option(None, help="Override SAM_API_KEY from the environment")):
    token = api_key or os.getenv("SAM_API_KEY")
    result = ingest_sam_opportunities(token)
    typer.echo(f"SAM ingest status: {result['status']}")


@export_app.command("events")
def export_events_cli(
    out: Optional[str] = typer.Option(
        None,
        "--out",
        help="Optional directory or CSV file path for the export",
    ),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    export_path = Path(out).expanduser() if out else None
    results = export_events(database_url=database_url, output=export_path)
    typer.echo(f"Events CSV: {results['csv'].resolve()}")
    typer.echo(f"Events JSONL: {results['jsonl'].resolve()}")
    typer.echo(f"Rows exported: {results['count']}")


def run() -> None:
    app()


if __name__ == "__main__":
    run()

