"""Minimal CLI for running common ShadowScope workflows."""
from __future__ import annotations

from typing import Optional

import typer

from backend.connectors import usaspending
from backend.db.models import ensure_schema

app = typer.Typer(help="ShadowScope developer CLI")


@app.command()
def health(database_url: Optional[str] = typer.Option(None, envvar="DATABASE_URL")) -> None:
    """Ensure the database schema exists."""
    ensure_schema(database_url)
    typer.echo("Database schema ensured")


@app.command()
def ingest_usaspending(
    since: str = typer.Option("2008-01-01", help="Start date for awards"),
    limit: int = typer.Option(200, help="Maximum awards to fetch"),
) -> None:
    """Fetch and display a summary of USAspending awards."""
    records = list(usaspending.fetch_awards(since=since, limit=limit))
    events = usaspending.normalize_awards(records)
    typer.echo(f"Fetched {len(records)} records and normalized {len(events)} events")


if __name__ == "__main__":
    app()
