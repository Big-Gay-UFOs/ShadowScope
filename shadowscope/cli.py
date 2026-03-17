"""Typer-based command line interface for ShadowScope."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, List

import typer
from dotenv import load_dotenv

from backend.db.ops import reset_schema, stamp_head, sync_database
from backend.logging_config import configure_logging
from backend.runtime import ensure_runtime_directories
from backend.services.export import export_events
from backend.services.export_correlations import export_kw_pairs
from backend.services.export_leads import export_lead_snapshot, export_lead_deltas
from backend.services.ingest import ingest_sam_opportunities, ingest_usaspending

app = typer.Typer(help="ShadowScope control plane")
db_app = typer.Typer(help="Database lifecycle commands")
ingest_app = typer.Typer(help="Data ingestion routines")
export_app = typer.Typer(help="Data export routines")
ontology_app = typer.Typer(help="Ontology utilities")
leads_app = typer.Typer(help="Lead utilities")
entities_app = typer.Typer(help="Entity utilities")
doctor_app = typer.Typer(help="Operator diagnosis utilities")
diagnose_app = typer.Typer(help="Source-aware diagnostic utilities")
inspect_app = typer.Typer(help="Bundle inspection utilities")
workflow_app = typer.Typer(help="One-command workflows")
report_app = typer.Typer(help="Report generation utilities")

app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(export_app, name="export")
app.add_typer(ontology_app, name="ontology")
app.add_typer(leads_app, name="leads")
app.add_typer(entities_app, name="entities")
app.add_typer(doctor_app, name="doctor")
app.add_typer(diagnose_app, name="diagnose")
app.add_typer(inspect_app, name="inspect")
app.add_typer(workflow_app, name="workflow")
app.add_typer(report_app, name="report")


@app.callback()
def main_callback() -> None:
    load_dotenv()
    configure_logging()
    ensure_runtime_directories()


def _parse_threshold_overrides(raw: Optional[List[str]], allowed: Optional[set[str]] = None) -> dict[str, float]:
    overrides: dict[str, float] = {}
    for item in raw or []:
        token = str(item).strip()
        if not token:
            continue
        if "=" not in token:
            raise typer.BadParameter(
                f"Invalid --threshold value '{token}'. Use key=value (for example: sam_naics_code_coverage_pct_min=60)."
            )
        key, value_raw = token.split("=", 1)
        key = key.strip()
        value_raw = value_raw.strip()
        if not key:
            raise typer.BadParameter(f"Invalid --threshold value '{token}': missing key.")
        if allowed is not None and key not in allowed:
            allowed_list = ", ".join(sorted(allowed))
            raise typer.BadParameter(f"Unknown threshold key '{key}'. Allowed keys: {allowed_list}")
        try:
            overrides[key] = float(value_raw)
        except ValueError as exc:
            raise typer.BadParameter(f"Invalid numeric threshold for '{key}': '{value_raw}'") from exc
    return overrides


class SamOntologyProfile(str, Enum):
    starter = "starter"
    dod_foia = "dod_foia"
    starter_plus_dod_foia = "starter_plus_dod_foia"
    hidden_program_proxy = "hidden_program_proxy"
    hidden_program_proxy_exploratory = "hidden_program_proxy_exploratory"
    starter_plus_dod_foia_hidden_program_proxy = "starter_plus_dod_foia_hidden_program_proxy"
    starter_plus_dod_foia_hidden_program_proxy_exploratory = "starter_plus_dod_foia_hidden_program_proxy_exploratory"


_SAM_ONTOLOGY_PROFILE_PATHS: dict[SamOntologyProfile, Path] = {
    SamOntologyProfile.starter: Path("examples/ontology_sam_procurement_starter.json"),
    SamOntologyProfile.dod_foia: Path("examples/ontology_sam_dod_foia_companion.json"),
    SamOntologyProfile.starter_plus_dod_foia: Path("examples/ontology_sam_procurement_plus_dod_foia.json"),
    SamOntologyProfile.hidden_program_proxy: Path("examples/ontology_sam_hidden_program_proxy_companion.json"),
    SamOntologyProfile.hidden_program_proxy_exploratory: Path("examples/ontology_sam_hidden_program_proxy_exploratory.json"),
    SamOntologyProfile.starter_plus_dod_foia_hidden_program_proxy: Path(
        "examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json"
    ),
    SamOntologyProfile.starter_plus_dod_foia_hidden_program_proxy_exploratory: Path(
        "examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json"
    ),
}


def _resolve_sam_ontology_path(
    *,
    ontology_profile: SamOntologyProfile,
    ontology_path: Optional[Path],
) -> Path:
    if ontology_path is not None:
        return Path(ontology_path)
    return _SAM_ONTOLOGY_PROFILE_PATHS[ontology_profile]


# Minimal newline-delimited keyword-file support for SAM ingest/workflow seeding.
def _load_newline_terms_file(path: Path) -> list[str]:
    try:
        text = Path(path).expanduser().read_text(encoding="utf-8-sig")
    except OSError as exc:
        raise typer.BadParameter(f"Unable to read keywords file '{path}': {exc}") from exc

    terms: list[str] = []
    seen: set[str] = set()
    for raw_line in text.splitlines():
        term = str(raw_line).strip()
        if not term or term.startswith("#"):
            continue
        if term in seen:
            continue
        seen.add(term)
        terms.append(term)
    return terms


def _merge_terms(*groups: Optional[List[str]]) -> Optional[List[str]]:
    terms: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for item in group or []:
            term = str(item or "").strip()
            if not term or term in seen:
                continue
            seen.add(term)
            terms.append(term)
    return terms or None


def _resolve_sam_ingest_keywords(*, keyword: Optional[List[str]], keywords_file: Optional[Path]) -> Optional[List[str]]:
    file_terms = _load_newline_terms_file(keywords_file) if keywords_file is not None else None
    return _merge_terms(keyword, file_terms)


def _parse_datetime_option(value: Optional[str], *, option_name: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must be an ISO-8601 datetime") from exc


def _resolve_lead_window_kwargs(
    *,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    occurred_after: Optional[str] = None,
    occurred_before: Optional[str] = None,
    created_after: Optional[str] = None,
    created_before: Optional[str] = None,
    since_days: Optional[int] = None,
) -> dict[str, Optional[object]]:
    return {
        "date_from": _parse_datetime_option(date_from, option_name="date_from"),
        "date_to": _parse_datetime_option(date_to, option_name="date_to"),
        "occurred_after": _parse_datetime_option(occurred_after, option_name="occurred_after"),
        "occurred_before": _parse_datetime_option(occurred_before, option_name="occurred_before"),
        "created_after": _parse_datetime_option(created_after, option_name="created_after"),
        "created_before": _parse_datetime_option(created_before, option_name="created_before"),
        "since_days": since_days,
    }


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
    recipient_search_text: Optional[List[str]] = typer.Option(None, "--recipient", help="Recipient search text (repeat --recipient)."),
    keywords: Optional[List[str]] = typer.Option(None, "--keyword", help="Keyword filters (repeat --keyword)."),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    result = ingest_usaspending(
        days=days,
        pages=pages,
        page_size=page_size,
        max_records=max_records,
        start_page=start_page, database_url=database_url, recipient_search_text=recipient_search_text, keywords=keywords,
    )
    run_id = result.get("run_id")
    typer.echo(f"Run ID: {run_id}")
    typer.echo(
        f"Summary: source=USAspending run_id={run_id} fetched={result['fetched']} inserted={result['inserted']} normalized={result['normalized']}"
    )
    typer.echo(f"Ingested {result['fetched']} rows ({result['inserted']} inserted, {result['normalized']} normalized).")
    typer.echo(f"Raw snapshots: {Path(result['snapshot_dir']).resolve()}")


@ingest_app.command("samgov")
@ingest_app.command("sam")
def ingest_samgov_cli(
    days: int = typer.Option(7, help="Days of history to request (lookback window)"),
    pages: int = typer.Option(1, help="Maximum API pages to request"),
    page_size: int = typer.Option(100, "--page-size", help="Records per API page (max 1000)"),
    max_records: Optional[int] = typer.Option(None, "--max-records", "--limit", help="Total cap across pages (and across keyword union when multiple --keyword are used)."),
    start_page: int = typer.Option(1, "--start-page", help="Start page (resume/chunking)"),
    keywords: Optional[List[str]] = typer.Option(None, "--keyword", help="Optional title search terms (repeat --keyword)."),
    keywords_file: Optional[Path] = typer.Option(None, "--keywords-file", help="Optional newline-delimited title search terms file."),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="Override SAM_API_KEY from environment for this command (not printed)."),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    """Ingest SAM.gov opportunities into events (bounded window + paging).

    Notes:
      - API key is read from SAM_API_KEY unless --api-key is provided.
      - Raw snapshots are written under data/raw/sam/YYYYMMDD/.
    """
    resolved_keywords = _resolve_sam_ingest_keywords(keyword=keywords, keywords_file=keywords_file)
    try:
        result = ingest_sam_opportunities(
            api_key=api_key,
            days=days,
            pages=pages,
            page_size=page_size,
            max_records=max_records,
            start_page=start_page,
            keywords=resolved_keywords,
            database_url=database_url,
        )
    except Exception as exc:
        typer.secho(f'SAM.gov ingest failed: {exc}', fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    status = result.get('status')
    if status == 'skipped':
        typer.secho(
            'SAM.gov ingest skipped: SAM_API_KEY is not set. Set $env:SAM_API_KEY for this session or add SAM_API_KEY=... to your local .env (gitignored).',
            fg=typer.colors.YELLOW,
        )
        return

    run_id = result.get('run_id')
    typer.echo(f'Run ID: {run_id}')
    typer.echo(
        f"Summary: source=SAM.gov run_id={run_id} fetched={result['fetched']} inserted={result['inserted']} normalized={result['normalized']}"
    )
    typer.echo(f"Ingested {result['fetched']} rows ({result['inserted']} inserted, {result['normalized']} normalized).")
    typer.echo(f"Raw snapshots: {Path(result['snapshot_dir']).resolve()}")



@export_app.command("events")
def export_events_cli(
    out: Optional[str] = typer.Option(None, "--out", help="Output directory or CSV file path"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Optional max events to export after filtering"),
    offset: int = typer.Option(0, "--offset", help="Skip this many filtered events before export"),
    source: Optional[str] = typer.Option(None, "--source", help="Filter by event source"),
    date_from: Optional[str] = typer.Option(None, "--date-from", help="Inclusive ISO-8601 start datetime"),
    date_to: Optional[str] = typer.Option(None, "--date-to", help="Inclusive ISO-8601 end datetime"),
    entity_id: Optional[int] = typer.Option(None, "--entity-id", help="Filter by entity_id"),
    keyword: Optional[str] = typer.Option(None, "--keyword", help="Filter by keyword tag"),
    agency: Optional[str] = typer.Option(None, "--agency", help="Filter by agency code or name"),
    psc: Optional[str] = typer.Option(None, "--psc", help="Filter by PSC code or description"),
    naics: Optional[str] = typer.Option(None, "--naics", help="Filter by NAICS code or description"),
    award_id: Optional[str] = typer.Option(None, "--award-id", help="Filter by award id"),
    recipient_uei: Optional[str] = typer.Option(None, "--recipient-uei", help="Filter by recipient UEI"),
    place_region: Optional[str] = typer.Option(None, "--place-region", help="Filter by state/country region"),
    sort_by: str = typer.Option("occurred_at", "--sort-by", help="Sort by occurred_at, created_at, id, or source"),
    sort_dir: str = typer.Option("desc", "--sort-dir", help="Sort direction asc or desc"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    export_path = Path(out).expanduser() if out else None
    results = export_events(
        database_url=database_url,
        output=export_path,
        limit=limit,
        offset=int(offset),
        source=source,
        date_from=_parse_datetime_option(date_from, option_name="date_from"),
        date_to=_parse_datetime_option(date_to, option_name="date_to"),
        entity_id=entity_id,
        keyword=keyword,
        agency=agency,
        psc=psc,
        naics=naics,
        award_id=award_id,
        recipient_uei=recipient_uei,
        place_region=place_region,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    typer.echo(f"Events CSV: {results['csv'].resolve()}")
    typer.echo(f"Events JSONL: {results['jsonl'].resolve()}")
    typer.echo(f"Rows exported: {results['count']}")
@export_app.command("entities")
def export_entities_cli(
    out: Optional[str] = typer.Option(None, "--out", help="Output directory or base file path"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    from backend.services.export_entities import export_entities_bundle

    export_path = Path(out).expanduser() if out else None
    res = export_entities_bundle(database_url=database_url, output=export_path)
    typer.echo(f"Entities CSV: {res['entities_csv'].resolve()}")
    typer.echo(f"Entities JSON: {res['entities_json'].resolve()}")
    typer.echo(f"Event->Entity CSV: {res['event_entities_csv'].resolve()}")
    typer.echo(f"Event->Entity JSON: {res['event_entities_json'].resolve()}")
    typer.echo(f"Entities: {res['entities_count']}  Event mappings: {res['event_entities_count']}")

@export_app.command("leads")
@export_app.command("lead-snapshot")
def export_lead_snapshot_cli(
    snapshot_id: int = typer.Option(..., "--snapshot-id", help="Lead snapshot ID to export"),
    out: Optional[str] = typer.Option(None, "--out", help="Output directory or base file path"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    export_path = Path(out).expanduser() if out else None
    results = export_lead_snapshot(snapshot_id=int(snapshot_id), database_url=database_url, output=export_path)
    typer.echo(f"Lead snapshot CSV: {results['csv'].resolve()}")
    typer.echo(f"Lead snapshot JSON: {results['json'].resolve()}")
    typer.echo(f"Rows exported: {results['count']}")

@export_app.command("lead-deltas")
def export_lead_deltas_cli(
    from_snapshot_id: int = typer.Option(..., "--from", "--from-snapshot-id", help="From lead snapshot ID"),
    to_snapshot_id: int = typer.Option(..., "--to", "--to-snapshot-id", help="To lead snapshot ID"),
    out: Optional[str] = typer.Option(None, "--out", help="Output directory or base file path"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    export_path = Path(out).expanduser() if out else None
    results = export_lead_deltas(from_snapshot_id=int(from_snapshot_id), to_snapshot_id=int(to_snapshot_id), database_url=database_url, output=export_path)
    typer.echo(f"Lead deltas CSV: {results['csv'].resolve()}")
    typer.echo(f"Lead deltas JSON: {results['json'].resolve()}")
    typer.echo(f"Rows exported: {results['count']}")

@export_app.command("evidence-package")
def export_evidence_package_cli(
    snapshot_id: Optional[int] = typer.Option(None, "--snapshot-id", help="Lead snapshot ID when exporting a lead package"),
    lead_event_id: Optional[int] = typer.Option(None, "--lead-event-id", help="Lead event_id within the snapshot"),
    lead_rank: Optional[int] = typer.Option(None, "--lead-rank", help="Lead rank within the snapshot (alternative to --lead-event-id)"),
    correlation_id: Optional[int] = typer.Option(None, "--correlation-id", help="Correlation ID to export instead of a lead"),
    out: Optional[str] = typer.Option(None, "--out", help="Output directory or JSON file path"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    from backend.services.evidence_package import export_evidence_package

    export_path = Path(out).expanduser() if out else None
    results = export_evidence_package(
        snapshot_id=snapshot_id,
        lead_event_id=lead_event_id,
        lead_rank=lead_rank,
        correlation_id=correlation_id,
        database_url=database_url,
        output=export_path,
    )
    typer.echo(f"Evidence package JSON: {results['json'].resolve()}")
    typer.echo(f"Package type: {results['package_type']}")
    typer.echo(f"Source records packaged: {results['source_record_count']}")
@export_app.command("kw-pair-clusters")
@export_app.command("kw-pairs")
def export_kw_pairs_cli(
    out: Optional[str] = typer.Option(None, "--out", help="Output directory or base file path"),
    limit: int = typer.Option(200, "--limit", help="Max pairs to export"),
    min_event_count: int = typer.Option(2, "--min-event-count", help="Minimum event_count for a pair"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    export_path = Path(out).expanduser() if out else None
    res = export_kw_pairs(database_url=database_url, output=export_path, limit=int(limit), min_event_count=int(min_event_count))
    typer.echo(f"KW pairs CSV: {res['csv'].resolve()}")
    typer.echo(f"KW pairs JSON: {res['json'].resolve()}")
    typer.echo(f"Rows exported: {res['count']}")

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



@ontology_app.command("lint")
def ontology_lint(
    path: Path = typer.Option(Path("ontology.json"), "--path", "-p", help="Path to ontology.json"),
    strict: bool = typer.Option(False, "--strict", help="Exit non-zero when lint issues are found"),
):
    from backend.services.tagging import TAGGABLE_EVENT_FIELDS, lint_ontology_definition

    report = lint_ontology_definition(path, supplied_fields=TAGGABLE_EVENT_FIELDS)
    validation_errors = report.get("validation_errors") or []
    lint = report.get("lint") or {}
    issues = lint.get("issues") or []

    typer.echo("Ontology lint summary:")
    typer.echo(
        json.dumps(
            {
                "ontology": report.get("ontology"),
                "supplied_fields": lint.get("supplied_fields"),
                "validation_error_count": len(validation_errors),
                "lint_issue_count": len(issues),
            },
            indent=2,
            ensure_ascii=False,
        )
    )

    if validation_errors:
        typer.echo("Validation errors:")
        for err in validation_errors:
            typer.echo(f"- {err}")

    if issues:
        typer.echo("Lint issues:")
        for item in issues:
            scope = item.get("scope") or "ontology"
            issue_type = item.get("type") or "issue"
            message = item.get("message") or ""
            typer.echo(f"- [{issue_type}] {scope}: {message}")

    if validation_errors or (strict and issues):
        raise typer.Exit(code=2)
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
    date_from: Optional[str] = typer.Option(None, "--date-from", help="Inclusive ISO-8601 event-time start"),
    date_to: Optional[str] = typer.Option(None, "--date-to", help="Inclusive ISO-8601 event-time end"),
    occurred_after: Optional[str] = typer.Option(None, "--occurred-after", help="Inclusive ISO-8601 occurred_at start"),
    occurred_before: Optional[str] = typer.Option(None, "--occurred-before", help="Inclusive ISO-8601 occurred_at end"),
    created_after: Optional[str] = typer.Option(None, "--created-after", help="Inclusive ISO-8601 created_at start"),
    created_before: Optional[str] = typer.Option(None, "--created-before", help="Inclusive ISO-8601 created_at end"),
    since_days: Optional[int] = typer.Option(None, "--since-days", help="Event-time lookback window helper"),
    min_score: int = typer.Option(1, "--min-score", help="Minimum score to include"),
    limit: int = typer.Option(200, "--limit", help="Max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="How many recent events to scan before ranking"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Scoring version (v1 or v2)"),
    notes: Optional[str] = typer.Option(None, "--notes", help="Optional snapshot notes"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    from backend.services.leads import create_lead_snapshot

    lead_window_kwargs = _resolve_lead_window_kwargs(
        date_from=date_from,
        date_to=date_to,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        created_after=created_after,
        created_before=created_before,
        since_days=since_days,
    )
    result = create_lead_snapshot(
        analysis_run_id=analysis_run_id,
        source=source,
        exclude_source=exclude_source,
        **lead_window_kwargs,
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

@leads_app.command("query")
def leads_query(
    limit: int = typer.Option(50, "--limit", help="Max leads to return"),
    offset: int = typer.Option(0, "--offset", help="Skip this many matching leads"),
    min_score: int = typer.Option(1, "--min-score", help="Minimum lead score"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="How many filtered events to score before paging"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Scoring version (v1 or v2)"),
    source: Optional[str] = typer.Option(None, "--source", help="Filter by event source"),
    exclude_source: Optional[str] = typer.Option(None, "--exclude-source", help="Exclude an event source"),
    date_from: Optional[str] = typer.Option(None, "--date-from", help="Inclusive ISO-8601 start datetime"),
    date_to: Optional[str] = typer.Option(None, "--date-to", help="Inclusive ISO-8601 end datetime"),
    occurred_after: Optional[str] = typer.Option(None, "--occurred-after", help="Inclusive ISO-8601 occurred_at start"),
    occurred_before: Optional[str] = typer.Option(None, "--occurred-before", help="Inclusive ISO-8601 occurred_at end"),
    created_after: Optional[str] = typer.Option(None, "--created-after", help="Inclusive ISO-8601 created_at start"),
    created_before: Optional[str] = typer.Option(None, "--created-before", help="Inclusive ISO-8601 created_at end"),
    since_days: Optional[int] = typer.Option(None, "--since-days", help="Event-time lookback window helper"),
    entity_id: Optional[int] = typer.Option(None, "--entity-id", help="Filter by entity_id"),
    keyword: Optional[str] = typer.Option(None, "--keyword", help="Filter by keyword tag"),
    agency: Optional[str] = typer.Option(None, "--agency", help="Filter by agency code or name"),
    psc: Optional[str] = typer.Option(None, "--psc", help="Filter by PSC code or description"),
    naics: Optional[str] = typer.Option(None, "--naics", help="Filter by NAICS code or description"),
    award_id: Optional[str] = typer.Option(None, "--award-id", help="Filter by award id"),
    recipient_uei: Optional[str] = typer.Option(None, "--recipient-uei", help="Filter by recipient UEI"),
    place_region: Optional[str] = typer.Option(None, "--place-region", help="Filter by state/country region"),
    lane: Optional[str] = typer.Option(None, "--lane", help="Require a contributing lane"),
    min_event_count: Optional[int] = typer.Option(None, "--min-event-count", help="Minimum contributing correlation event_count"),
    min_score_signal: Optional[float] = typer.Option(None, "--min-score-signal", help="Minimum contributing correlation score_signal"),
    sort_by: str = typer.Option("score", "--sort-by", help="Sort by score, occurred_at, created_at, id, pair_strength, pair_count, or source"),
    sort_dir: str = typer.Option("desc", "--sort-dir", help="Sort direction asc or desc"),
    include_details: bool = typer.Option(True, "--include-details/--no-include-details", help="Include score_details in JSON output"),
    json_out: bool = typer.Option(False, "--json", help="Print the full JSON payload"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    from backend.db.models import get_session_factory
    from backend.services.leads import normalize_scoring_version
    from backend.services.query_surfaces import query_leads

    limit_i = int(limit)
    offset_i = int(offset)
    scan_i = int(scan_limit)
    if limit_i < 1 or limit_i > 200:
        raise typer.BadParameter("limit must be between 1 and 200")
    if offset_i < 0:
        raise typer.BadParameter("offset must be >= 0")
    if scan_i < 1 or scan_i > 5000:
        raise typer.BadParameter("scan_limit must be between 1 and 5000")
    if scan_i < (limit_i + offset_i):
        scan_i = limit_i + offset_i
    scoring_version = normalize_scoring_version(scoring_version)
    lead_window_kwargs = _resolve_lead_window_kwargs(
        date_from=date_from,
        date_to=date_to,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        created_after=created_after,
        created_before=created_before,
        since_days=since_days,
    )

    SessionFactory = get_session_factory(database_url)
    with SessionFactory() as db:
        payload = query_leads(
            db,
            limit=limit_i,
            offset=offset_i,
            min_score=int(min_score),
            scan_limit=scan_i,
            scoring_version=scoring_version,
            source=source,
            exclude_source=exclude_source,
            **lead_window_kwargs,
            entity_id=entity_id,
            keyword=keyword,
            agency=agency,
            psc=psc,
            naics=naics,
            award_id=award_id,
            recipient_uei=recipient_uei,
            place_region=place_region,
            lane=lane,
            min_event_count=min_event_count,
            min_score_signal=min_score_signal,
            include_details=include_details,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    if json_out:
        typer.echo(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        return

    typer.echo(f"Lead query: total={payload.get('total')} scanned={payload.get('scanned')} returned={len(payload.get('items') or [])}")
    for item in payload.get("items") or []:
        typer.echo(
            f"- score={item.get('score')} id={item.get('id')} source={item.get('source')} "
            f"doc_id={item.get('doc_id')} occurred_at={item.get('occurred_at')} "
            f"lanes={','.join([str(v) for v in item.get('contributing_lanes') or []])}"
        )
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

@doctor_app.command("status")
def doctor_status_cli(
    days: int = typer.Option(30, "--days", help="Lookback window for status checks"),
    source: str = typer.Option("USAspending", "--source", help="Event source filter (blank for all)"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Max recent events to scan for keyword stats"),
    max_keywords_per_event: int = typer.Option(10, "--max-keywords-per-event", help="Heuristic threshold for pair explosion guard"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload"),
):
    from backend.services.doctor import doctor_status

    try:
        res = doctor_status(
            days=days,
            source=source if source else None,
            scan_limit=scan_limit,
            max_keywords_per_event=max_keywords_per_event,
            database_url=database_url,
        )
    except Exception as e:
        typer.echo(f"Doctor status failed: {e}")
        raise typer.Exit(code=2)

    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False))
        return

    db = res.get("db", {})
    window = res.get("window", {})
    counts = res.get("counts", {})
    entities_diag = res.get("entities", {})
    kw = res.get("keywords", {})
    corr = res.get("correlations", {})
    sam_ctx = res.get("sam_context", {})
    last = res.get("last_runs", {})
    hints = res.get("hints", [])

    typer.echo("ShadowScope Doctor Status")
    typer.echo(f"DB: {db.get('status')} url={db.get('url')}")
    typer.echo(f"Window: days={window.get('days')} source={window.get('source')} since={window.get('since')}")

    typer.echo(
        "Counts: "
        f"events_total={counts.get('events_total')} events_window={counts.get('events_window')} "
        f"events_with_entity_window={counts.get('events_with_entity_window')} entities_total={counts.get('entities_total')} "
        f"correlations_total={counts.get('correlations_total')} lead_snapshots_total={counts.get('lead_snapshots_total')}"
    )

    if entities_diag:
        typer.echo(
            "Entities (coverage): "
            f"window_linked_pct={entities_diag.get('window_linked_coverage_pct')} "
            f"sample_identity={entities_diag.get('sample_events_with_identity_signal')} "
            f"sample_identity_linked={entities_diag.get('sample_events_with_identity_signal_linked')} "
            f"sample_identity_linked_pct={entities_diag.get('sample_identity_signal_coverage_pct')}"
        )

    lane = corr.get("by_lane") or {}
    if lane:
        typer.echo("Correlations by lane: " + " ".join([f"{k}={v}" for k, v in lane.items()]))

    typer.echo(
        "Keywords (sample): "
        f"scanned={kw.get('scanned_events')} with_keywords={kw.get('events_with_keywords')} "
        f"coverage_pct={kw.get('coverage_pct')} unique={kw.get('unique_keywords')} "
        f"gt_{kw.get('max_keywords_per_event')}={kw.get('events_keywords_gt_max')}"
    )

    top = kw.get("top_keywords") or []
    if top:
        typer.echo("Top keywords (sample):")
        for item in top[:10]:
            typer.echo(f"- {item.get('keyword')}: {item.get('count')}")

    if sam_ctx and int(sam_ctx.get("scanned_events") or 0) > 0:
        typer.echo(
            "SAM context (sample): "
            f"scanned={sam_ctx.get('scanned_events')} "
            f"research_context={sam_ctx.get('events_with_research_context')} "
            f"research_context_pct={sam_ctx.get('research_context_coverage_pct')} "
            f"avg_fields={sam_ctx.get('avg_context_fields_per_event')}"
        )
        cov = sam_ctx.get("coverage_by_field_pct") or {}
        if cov:
            ordered_keys = [
                "sam_notice_type",
                "sam_naics_code",
                "sam_set_aside_code",
                "sam_solicitation_number",
                "sam_agency_path_code",
                "sam_response_deadline",
            ]
            cov_parts = [f"{k}={cov.get(k)}" for k in ordered_keys if k in cov]
            if cov_parts:
                typer.echo("SAM context coverage pct: " + " ".join(cov_parts))

        top_notice_types = sam_ctx.get("top_notice_types") or []
        if top_notice_types:
            typer.echo("Top SAM notice types (sample):")
            for item in top_notice_types[:10]:
                typer.echo(f"- {item.get('notice_type')}: {item.get('count')}")

        top_naics = sam_ctx.get("top_naics_codes") or []
        if top_naics:
            typer.echo("Top SAM NAICS (sample):")
            for item in top_naics[:10]:
                typer.echo(f"- {item.get('naics_code')}: {item.get('count')}")

    if last.get("ingest"):
        i = last["ingest"]
        typer.echo(
            "Last ingest: "
            f"id={i.get('id')} source={i.get('source')} status={i.get('status')} "
            f"started_at={i.get('started_at')} ended_at={i.get('ended_at')} "
            f"fetched={i.get('fetched')} inserted={i.get('inserted')} normalized={i.get('normalized')}"
        )

    if last.get("ontology_apply"):
        a = last["ontology_apply"]
        typer.echo(
            "Last ontology_apply: "
            f"id={a.get('id')} status={a.get('status')} source={a.get('source')} days={a.get('days')} "
            f"scanned={a.get('scanned')} updated={a.get('updated')} unchanged={a.get('unchanged')} "
            f"ended_at={a.get('ended_at')}"
        )

    if last.get("lead_snapshot"):
        s = last["lead_snapshot"]
        typer.echo(
            "Last lead snapshot: "
            f"id={s.get('id')} source={s.get('source')} created_at={s.get('created_at')} items={s.get('items')}"
        )

    if hints:
        typer.echo("Hints:")
        for h in hints:
            typer.echo(f"- {h}")



@diagnose_app.command("samgov")
def diagnose_samgov_cli(
    days: int = typer.Option(30, "--days", help="Diagnostic lookback window"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Max recent events to inspect"),
    max_keywords_per_event: int = typer.Option(10, "--max-keywords-per-event", help="Pair-explosion heuristic threshold"),
    bundle_path: Optional[str] = typer.Option(None, "--bundle", help="Optional SAM bundle directory to inspect"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload"),
):
    from backend.services.diagnostics import diagnose_samgov

    res = diagnose_samgov(
        days=int(days),
        scan_limit=int(scan_limit),
        max_keywords_per_event=int(max_keywords_per_event),
        database_url=database_url,
        bundle_path=(Path(bundle_path).expanduser() if bundle_path else None),
    )

    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False, default=str))
        return

    typer.echo("SAM.gov Diagnose")
    typer.echo(f"Classification: {res.get('classification')}")

    doctor = res.get("doctor") or {}
    counts = doctor.get("counts") or {}
    typer.echo(
        "Window counts: "
        f"events_window={counts.get('events_window')} "
        f"events_with_entity_window={counts.get('events_with_entity_window')} "
        f"lead_snapshots_total={counts.get('lead_snapshots_total')}"
    )

    gaps = res.get("gaps") or {}
    typer.echo(
        "Gap metrics: "
        f"untagged={gaps.get('untagged_events')} "
        f"without_entities={gaps.get('events_without_entities')} "
        f"without_lead_value={gaps.get('events_without_lead_value')} "
        f"low_context={gaps.get('low_context_events')}"
    )

    bundle = res.get("bundle") or {}
    if bundle.get("latest_bundle_dir"):
        typer.echo(f"Latest bundle: {Path(bundle.get('latest_bundle_dir')).resolve()}")
        typer.echo(
            "Bundle: "
            f"integrity={bundle.get('bundle_integrity_status')} "
            f"workflow_status={bundle.get('workflow_status')} "
            f"quality={bundle.get('bundle_quality')}"
        )
        required_failure_categories = bundle.get("required_failure_categories") or []
        advisory_failure_categories = bundle.get("advisory_failure_categories") or []
        if required_failure_categories:
            typer.echo(
                "Required failure categories: " + ", ".join([str(item) for item in required_failure_categories])
            )
        if advisory_failure_categories:
            typer.echo(
                "Advisory failure categories: " + ", ".join([str(item) for item in advisory_failure_categories])
            )

    retries = int(res.get("rate_limit_retries") or 0)
    if retries > 0:
        typer.echo(f"Rate-limit retries observed: {retries}")

    recs = res.get("recommendations") or []
    if recs:
        typer.echo("Recommendations:")
        for item in recs:
            typer.echo(f"- {item}")


@inspect_app.command("bundle")
def inspect_bundle_cli(
    path: str = typer.Option(..., "--path", help="Bundle directory path"),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload"),
):
    from backend.services.bundle import inspect_bundle

    result = inspect_bundle(Path(path).expanduser())
    if json_out:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return

    typer.echo(f"Bundle dir: {Path(result.get('bundle_dir')).resolve()}")
    typer.echo(f"Bundle integrity: {result.get('bundle_integrity_status') or result.get('status')}")
    if result.get("workflow_status") is not None:
        typer.echo(
            f"Workflow status: {result.get('workflow_status')} quality={result.get('workflow_quality')}"
        )
    check_summary = result.get("check_summary") or {}
    if check_summary:
        typer.echo(
            "Check summary: "
            f"required_failed={check_summary.get('failed_required')} "
            f"advisory_failed={check_summary.get('failed_advisory', check_summary.get('warnings'))}"
        )

    manifest_path = result.get("bundle_manifest_json")
    if manifest_path:
        typer.echo(f"Manifest: {Path(manifest_path).resolve()}")

    missing = result.get("missing_files") or []
    if missing:
        typer.echo("Missing files:")
        for item in missing:
            typer.echo(f"- {item.get('id')}: {item.get('path')}")


def _echo_validation_gate_summary(label: str, res: dict) -> None:
    status = str(res.get("status") or "unknown").upper()
    failed_required = res.get("failed_required_checks") or []
    failed_advisory = res.get("failed_advisory_checks") or res.get("warning_checks") or []
    quality = res.get("quality") or {}
    typer.echo(f"{label}: {status}")
    typer.echo(
        "Gate summary: "
        f"required_checks_passed={res.get('required_checks_passed', res.get('smoke_passed'))} "
        f"required_failed={len(failed_required)} "
        f"advisory_failed={len(failed_advisory)} "
        f"quality={quality.get('quality')}"
    )

    required_failure_categories = quality.get("required_failure_categories") or []
    advisory_failure_categories = quality.get("advisory_failure_categories") or []
    if required_failure_categories:
        typer.echo(
            "Required failure categories: " + ", ".join([str(item) for item in required_failure_categories])
        )
    if advisory_failure_categories:
        typer.echo(
            "Advisory failure categories: " + ", ".join([str(item) for item in advisory_failure_categories])
        )

    for group in (res.get("check_groups") or {}).values():
        if not isinstance(group, dict):
            continue
        label_text = group.get("category_label")
        if label_text is None:
            continue
        typer.echo(
            f"- {label_text}: "
            f"required_total={group.get('required_total')} "
            f"advisory_total={group.get('advisory_total')} "
            f"failed_required={group.get('failed_required')} "
            f"failed_advisory={group.get('failed_advisory')}"
        )


def _echo_validation_checks(res: dict, *, include_passes: bool = False) -> None:
    checks = list(res.get("checks") or [])
    if not include_passes:
        checks = [item for item in checks if not bool(item.get("passed"))]
    for chk in checks:
        typer.echo(
            f"- [{str(chk.get('result') or '').upper()}]"
            f"[{str(chk.get('severity') or '').upper()}]"
            f"[{str(chk.get('policy_level') or '').upper()}]"
            f"[{chk.get('category_label')}] "
            f"{chk.get('name')} observed={chk.get('observed')} threshold={chk.get('expected')}"
        )
        if not chk.get("passed"):
            if chk.get("why"):
                typer.echo(f"  why: {chk.get('why')}")
            if chk.get("hint"):
                typer.echo(f"  next: {chk.get('hint')}")


def _echo_workflow_summary(label: str, res: dict) -> None:
    typer.echo(f"Workflow complete: {label}")
    if res.get("ingest"):
        ing = res["ingest"]
        typer.echo(
            f"Ingest: run_id={ing.get('run_id')} fetched={ing.get('fetched')} inserted={ing.get('inserted')} normalized={ing.get('normalized')}"
        )
        if ing.get("snapshot_dir"):
            typer.echo(f"Raw snapshots: {Path(ing.get('snapshot_dir')).resolve()}")
    if res.get("ontology_apply"):
        ont = res["ontology_apply"]
        typer.echo(
            f"Ontology: analysis_run_id={ont.get('analysis_run_id')} scanned={ont.get('scanned')} updated={ont.get('updated')} unchanged={ont.get('unchanged')}"
        )
    if res.get("entities_link"):
        ent = res["entities_link"]
        typer.echo(
            f"Entities: scanned={ent.get('scanned')} linked={ent.get('linked')} entities_created={ent.get('entities_created')}"
        )
    if res.get("correlations"):
        c = res["correlations"]
        typer.echo("Correlations:")
        preferred_order = ["same_entity", "same_uei", "same_keyword", "kw_pair", "same_sam_naics"]
        ordered_lanes = preferred_order + sorted([k for k in c.keys() if k not in preferred_order])
        for k in ordered_lanes:
            if k in c:
                typer.echo(
                    f"- {k}: "
                    + " ".join(
                        [
                            f"{kk}={vv}"
                            for kk, vv in c[k].items()
                            if kk
                            in (
                                "correlations_created",
                                "correlations_updated",
                                "correlations_deleted",
                                "links_created",
                                "eligible_pairs",
                                "eligible_keywords",
                                "eligible_entities",
                                "eligible_ueis",
                                "eligible_naics",
                            )
                        ]
                    )
                )
    if res.get("snapshot"):
        s = res["snapshot"]
        typer.echo(
            f"Snapshot: snapshot_id={s.get('snapshot_id')} items={s.get('items')} scanned={s.get('scanned')} scoring_version={s.get('scoring_version')}"
        )
    if res.get("exports"):
        ex = res["exports"]
        if ex.get("lead_snapshot"):
            ls = ex["lead_snapshot"]
            typer.echo(
                f"Export lead snapshot: csv={Path(ls['csv']).resolve()} json={Path(ls['json']).resolve()} rows={ls.get('count')}"
            )
        if ex.get("kw_pairs"):
            kw = ex["kw_pairs"]
            typer.echo(
                f"Export kw_pairs: csv={Path(kw['csv']).resolve()} json={Path(kw['json']).resolve()} rows={kw.get('count')}"
            )
        if ex.get("entities"):
            en = ex["entities"]
            typer.echo(
                f"Export entities: csv={Path(en['entities_csv']).resolve()} json={Path(en['entities_json']).resolve()}"
            )
            typer.echo(
                f"Export event->entity: csv={Path(en['event_entities_csv']).resolve()} json={Path(en['event_entities_json']).resolve()}"
            )
        if ex.get("events"):
            ev = ex["events"]
            typer.echo(
                f"Export events: csv={Path(ev['csv']).resolve()} jsonl={Path(ev['jsonl']).resolve()} rows={ev.get('count')}"
            )


@workflow_app.command("usaspending")
def workflow_usaspending(
    ingest_days: int = typer.Option(30, "--ingest-days", "--days", help="Ingest: days of history to request (--days alias supported)"),
    pages: int = typer.Option(1, "--pages", help="Ingest: maximum API pages to request"),
    page_size: int = typer.Option(100, "--page-size", help="Ingest: records per API page (max 100)"),
    max_records: Optional[int] = typer.Option(
        None, "--max-records", "--limit", help="Ingest: total cap across all pages"
    ),
    start_page: int = typer.Option(1, "--start-page", help="Ingest: start page (resume/chunking)"),
    recipient: Optional[List[str]] = typer.Option(
        None, "--recipient", help="Ingest: recipient search text (repeat --recipient)"
    ),
    keyword: Optional[List[str]] = typer.Option(None, "--keyword", help="Ingest: keyword filter (repeat --keyword)"),
    ontology_path: Path = typer.Option(Path("examples/ontology_usaspending_starter.json"), "--ontology", "-o", help="Ontology: path to USAspending ontology JSON"),
    ontology_days: int = typer.Option(30, "--ontology-days", help="Ontology: tag events in last N days"),
    window_days: int = typer.Option(30, "--window-days", help="Correlations: lookback window (days)"),
    min_events_entity: int = typer.Option(2, "--min-events-entity", help="Correlations: min events for entity/UEI lanes"),
    min_events_keywords: int = typer.Option(
        2, "--min-events-keywords", help="Correlations: min events for keyword/kw-pair lanes"
    ),
    max_events_keywords: int = typer.Option(
        200, "--max-events-keywords", help="Correlations: skip keywords/pairs matching more than this many events"
    ),
    max_keywords_per_event: int = typer.Option(
        10, "--max-keywords-per-event", help="Correlations: skip events with too many keywords (pair explosion guard)"
    ),
    date_from: Optional[str] = typer.Option(None, "--date-from", help="Snapshot: inclusive ISO-8601 event-time start"),
    date_to: Optional[str] = typer.Option(None, "--date-to", help="Snapshot: inclusive ISO-8601 event-time end"),
    occurred_after: Optional[str] = typer.Option(None, "--occurred-after", help="Snapshot: inclusive ISO-8601 occurred_at start"),
    occurred_before: Optional[str] = typer.Option(None, "--occurred-before", help="Snapshot: inclusive ISO-8601 occurred_at end"),
    created_after: Optional[str] = typer.Option(None, "--created-after", help="Snapshot: inclusive ISO-8601 created_at start"),
    created_before: Optional[str] = typer.Option(None, "--created-before", help="Snapshot: inclusive ISO-8601 created_at end"),
    since_days: Optional[int] = typer.Option(None, "--since-days", help="Snapshot: event-time lookback window helper"),
    entity_days: int = typer.Option(30, "--entity-days", help="Entities: link events created in last N days"),
    min_score: int = typer.Option(1, "--min-score", help="Snapshot: minimum score to include"),
    snapshot_limit: int = typer.Option(200, "--snapshot-limit", help="Snapshot: max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Snapshot: how many recent events to scan"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Snapshot: scoring version (v1 or v2)"),
    notes: Optional[str] = typer.Option(None, "--notes", help="Snapshot: optional snapshot notes"),
    out: Optional[str] = typer.Option(None, "--out", help="Exports: output directory or base file path"),
    export_events_flag: bool = typer.Option(False, "--export-events", help="Exports: also export events CSV/JSONL"),
    skip_ingest: bool = typer.Option(False, "--skip-ingest", help="Skip ingest step (no network)"),
    skip_ontology: bool = typer.Option(False, "--skip-ontology", help="Skip ontology apply step"),
    skip_entities: bool = typer.Option(False, "--skip-entities", help="Skip entity linking step"),
    skip_correlations: bool = typer.Option(False, "--skip-correlations", help="Skip correlation rebuild step"),
    skip_snapshot: bool = typer.Option(False, "--skip-snapshot", help="Skip snapshot creation step"),
    skip_exports: bool = typer.Option(False, "--skip-exports", help="Skip export step"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload (default=str for paths)"),
):
    from backend.services.workflow import run_usaspending_workflow

    export_path = Path(out).expanduser() if out else None
    lead_window_kwargs = _resolve_lead_window_kwargs(
        date_from=date_from,
        date_to=date_to,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        created_after=created_after,
        created_before=created_before,
        since_days=since_days,
    )
    res = run_usaspending_workflow(
        ingest_days=ingest_days,
        pages=pages,
        page_size=page_size,
        max_records=max_records,
        start_page=start_page,
        recipient_search_text=recipient,
        keywords=keyword,
        ontology_path=ontology_path,
        ontology_days=ontology_days,
        window_days=window_days,
        min_events_entity=min_events_entity,
        min_events_keywords=min_events_keywords,
        max_events_keywords=max_events_keywords,
        max_keywords_per_event=max_keywords_per_event,
        **lead_window_kwargs,
        entity_days=entity_days,
        min_score=min_score,
        snapshot_limit=snapshot_limit,
        scan_limit=scan_limit,
        scoring_version=scoring_version,
        notes=notes,
        output=export_path,
        export_events_flag=export_events_flag,
        database_url=database_url,
        skip_ingest=skip_ingest,
        skip_ontology=skip_ontology,
        skip_entities=skip_entities,
        skip_correlations=skip_correlations,
        skip_snapshot=skip_snapshot,
        skip_exports=skip_exports,
    )

    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False, default=str))
        return

    _echo_workflow_summary("USAspending", res)


@workflow_app.command("samgov")
@workflow_app.command("sam")
def workflow_samgov(
    ingest_days: int = typer.Option(30, "--ingest-days", "--days", help="Ingest: days of history to request (--days alias supported)"),
    pages: int = typer.Option(2, "--pages", help="Ingest: maximum API pages to request"),
    page_size: int = typer.Option(100, "--page-size", help="Ingest: records per API page (max 1000)"),
    max_records: Optional[int] = typer.Option(
        50, "--max-records", "--limit", help="Ingest: total cap across pages"
    ),
    start_page: int = typer.Option(1, "--start-page", help="Ingest: start page (resume/chunking)"),
    keyword: Optional[List[str]] = typer.Option(None, "--keyword", help="Ingest: title search terms (repeat --keyword)"),
    keywords_file: Optional[Path] = typer.Option(None, "--keywords-file", help="Ingest: newline-delimited title search terms file"),
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="Override SAM_API_KEY from environment for this command (not printed)."
    ),
    ontology_profile: SamOntologyProfile = typer.Option(
        SamOntologyProfile.starter,
        "--ontology-profile",
        help="Ontology profile: starter | dod_foia | starter_plus_dod_foia | hidden_program_proxy | hidden_program_proxy_exploratory | starter_plus_dod_foia_hidden_program_proxy | starter_plus_dod_foia_hidden_program_proxy_exploratory",
    ),
    ontology_path: Optional[Path] = typer.Option(
        None,
        "--ontology",
        "-o",
        help="Ontology: explicit path override for SAM ontology JSON",
    ),
    ontology_days: int = typer.Option(30, "--ontology-days", help="Ontology: tag events in last N days"),
    window_days: int = typer.Option(30, "--window-days", help="Correlations: lookback window (days)"),
    min_events_entity: int = typer.Option(2, "--min-events-entity", help="Correlations: min events for entity/UEI lanes"),
    min_events_keywords: int = typer.Option(
        2, "--min-events-keywords", help="Correlations: min events for keyword/kw-pair lanes"
    ),
    max_events_keywords: int = typer.Option(
        200, "--max-events-keywords", help="Correlations: skip keywords/pairs matching more than this many events"
    ),
    max_keywords_per_event: int = typer.Option(
        10, "--max-keywords-per-event", help="Correlations: skip events with too many keywords (pair explosion guard)"
    ),
    date_from: Optional[str] = typer.Option(None, "--date-from", help="Snapshot: inclusive ISO-8601 event-time start"),
    date_to: Optional[str] = typer.Option(None, "--date-to", help="Snapshot: inclusive ISO-8601 event-time end"),
    occurred_after: Optional[str] = typer.Option(None, "--occurred-after", help="Snapshot: inclusive ISO-8601 occurred_at start"),
    occurred_before: Optional[str] = typer.Option(None, "--occurred-before", help="Snapshot: inclusive ISO-8601 occurred_at end"),
    created_after: Optional[str] = typer.Option(None, "--created-after", help="Snapshot: inclusive ISO-8601 created_at start"),
    created_before: Optional[str] = typer.Option(None, "--created-before", help="Snapshot: inclusive ISO-8601 created_at end"),
    since_days: Optional[int] = typer.Option(None, "--since-days", help="Snapshot: event-time lookback window helper"),
    entity_days: int = typer.Option(30, "--entity-days", help="Entities: link events created in last N days"),
    min_score: int = typer.Option(1, "--min-score", help="Snapshot: minimum score to include"),
    snapshot_limit: int = typer.Option(200, "--snapshot-limit", help="Snapshot: max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Snapshot: how many recent events to scan"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Snapshot: scoring version (v1 or v2)"),
    notes: Optional[str] = typer.Option(None, "--notes", help="Snapshot: optional snapshot notes"),
    out: Optional[str] = typer.Option(None, "--out", help="Exports: output directory or base file path"),
    export_events_flag: bool = typer.Option(True, "--export-events/--no-export-events", help="Exports: include events CSV/JSONL"),
    skip_ingest: bool = typer.Option(False, "--skip-ingest", help="Skip ingest step (offline replay from existing data)"),
    skip_ontology: bool = typer.Option(False, "--skip-ontology", help="Skip ontology apply step"),
    skip_entities: bool = typer.Option(False, "--skip-entities", help="Skip entity linking step"),
    skip_correlations: bool = typer.Option(False, "--skip-correlations", help="Skip correlation rebuild step"),
    skip_snapshot: bool = typer.Option(False, "--skip-snapshot", help="Skip snapshot creation step"),
    skip_exports: bool = typer.Option(False, "--skip-exports", help="Skip export step"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload (default=str for paths)"),
):
    from backend.services.workflow import run_samgov_workflow

    export_path = Path(out).expanduser() if out else None
    resolved_keywords = _resolve_sam_ingest_keywords(keyword=keyword, keywords_file=keywords_file)
    resolved_ontology_path = _resolve_sam_ontology_path(ontology_profile=ontology_profile, ontology_path=ontology_path)
    lead_window_kwargs = _resolve_lead_window_kwargs(
        date_from=date_from,
        date_to=date_to,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        created_after=created_after,
        created_before=created_before,
        since_days=since_days,
    )
    res = run_samgov_workflow(
        ingest_days=ingest_days,
        pages=pages,
        page_size=page_size,
        max_records=max_records,
        start_page=start_page,
        keywords=resolved_keywords,
        api_key=api_key,
        ontology_path=resolved_ontology_path,
        ontology_days=ontology_days,
        window_days=window_days,
        min_events_entity=min_events_entity,
        min_events_keywords=min_events_keywords,
        max_events_keywords=max_events_keywords,
        max_keywords_per_event=max_keywords_per_event,
        **lead_window_kwargs,
        entity_days=entity_days,
        min_score=min_score,
        snapshot_limit=snapshot_limit,
        scan_limit=scan_limit,
        scoring_version=scoring_version,
        notes=notes,
        output=export_path,
        export_events_flag=export_events_flag,
        database_url=database_url,
        skip_ingest=skip_ingest,
        skip_ontology=skip_ontology,
        skip_entities=skip_entities,
        skip_correlations=skip_correlations,
        skip_snapshot=skip_snapshot,
        skip_exports=skip_exports,
    )

    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False, default=str))
        if res.get("status") == "skipped":
            raise typer.Exit(code=2)
        return

    if res.get("status") == "skipped":
        typer.secho(
            "SAM.gov workflow ingest skipped: SAM_API_KEY is not set. Set $env:SAM_API_KEY for this session or add SAM_API_KEY=... to your local .env (gitignored).",
            fg=typer.colors.YELLOW,
        )
        raise typer.Exit(code=2)

    _echo_workflow_summary("SAM.gov", res)



@workflow_app.command("samgov-validate")
def workflow_samgov_validate(
    ingest_days: int = typer.Option(30, "--ingest-days", "--days", help="Ingest: days of history to request (--days alias supported)"),
    pages: int = typer.Option(5, "--pages", help="Ingest: maximum API pages to request"),
    page_size: int = typer.Option(100, "--page-size", help="Ingest: records per API page (max 1000)"),
    max_records: Optional[int] = typer.Option(250, "--max-records", "--limit", help="Ingest: total cap across pages"),
    start_page: int = typer.Option(1, "--start-page", help="Ingest: start page (resume/chunking)"),
    keyword: Optional[List[str]] = typer.Option(None, "--keyword", help="Ingest: title search terms (repeat --keyword)"),
    keywords_file: Optional[Path] = typer.Option(None, "--keywords-file", help="Ingest: newline-delimited title search terms file"),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="Override SAM_API_KEY from environment for this command (not printed)."),
    ontology_profile: SamOntologyProfile = typer.Option(
        SamOntologyProfile.starter,
        "--ontology-profile",
        help="Ontology profile: starter | dod_foia | starter_plus_dod_foia | hidden_program_proxy | hidden_program_proxy_exploratory | starter_plus_dod_foia_hidden_program_proxy | starter_plus_dod_foia_hidden_program_proxy_exploratory",
    ),
    ontology_path: Optional[Path] = typer.Option(
        None,
        "--ontology",
        "-o",
        help="Ontology: explicit path override for SAM ontology JSON",
    ),
    ontology_days: int = typer.Option(30, "--ontology-days", help="Ontology: tag events in last N days"),
    window_days: int = typer.Option(30, "--window-days", help="Correlation/doctor lookback window (days)"),
    min_events_entity: int = typer.Option(2, "--min-events-entity", help="Correlations: min events for entity/UEI lanes"),
    min_events_keywords: int = typer.Option(2, "--min-events-keywords", help="Correlations: min events for keyword/kw-pair lanes"),
    max_events_keywords: int = typer.Option(200, "--max-events-keywords", help="Correlations: skip keywords/pairs matching more than this many events"),
    max_keywords_per_event: int = typer.Option(10, "--max-keywords-per-event", help="Correlations: skip events with too many keywords (pair explosion guard)"),
    date_from: Optional[str] = typer.Option(None, "--date-from", help="Snapshot: inclusive ISO-8601 event-time start"),
    date_to: Optional[str] = typer.Option(None, "--date-to", help="Snapshot: inclusive ISO-8601 event-time end"),
    occurred_after: Optional[str] = typer.Option(None, "--occurred-after", help="Snapshot: inclusive ISO-8601 occurred_at start"),
    occurred_before: Optional[str] = typer.Option(None, "--occurred-before", help="Snapshot: inclusive ISO-8601 occurred_at end"),
    created_after: Optional[str] = typer.Option(None, "--created-after", help="Snapshot: inclusive ISO-8601 created_at start"),
    created_before: Optional[str] = typer.Option(None, "--created-before", help="Snapshot: inclusive ISO-8601 created_at end"),
    since_days: Optional[int] = typer.Option(None, "--since-days", help="Snapshot: event-time lookback window helper"),
    entity_days: int = typer.Option(30, "--entity-days", help="Entities: link events created in last N days"),
    min_score: int = typer.Option(1, "--min-score", help="Snapshot: minimum score to include"),
    snapshot_limit: int = typer.Option(200, "--snapshot-limit", help="Snapshot: max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Snapshot/doctor scan window"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Snapshot: scoring version (v1 or v2)"),
    notes: Optional[str] = typer.Option("samgov larger-run validation", "--notes", help="Snapshot: optional notes"),
    bundle_root: Optional[str] = typer.Option(None, "--bundle-root", help="Artifact bundle root directory (defaults to data/exports/validation/samgov)"),
    require_nonzero: bool = typer.Option(True, "--require-nonzero/--no-require-nonzero", help="Fail with exit code 2 when required checks fail"),
    threshold: Optional[List[str]] = typer.Option(None, "--threshold", help="Threshold override key=value (repeat)."),
    skip_ingest: bool = typer.Option(False, "--skip-ingest", help="Skip ingest step (offline fixture replay)"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload (default=str for paths)"),
):
    from backend.services.workflow import DEFAULT_SAM_SMOKE_THRESHOLDS, run_samgov_validation_workflow

    bundle_path = Path(bundle_root).expanduser() if bundle_root else None
    resolved_keywords = _resolve_sam_ingest_keywords(keyword=keyword, keywords_file=keywords_file)
    threshold_overrides = _parse_threshold_overrides(threshold, allowed=set(DEFAULT_SAM_SMOKE_THRESHOLDS.keys()))
    resolved_ontology_path = _resolve_sam_ontology_path(ontology_profile=ontology_profile, ontology_path=ontology_path)
    lead_window_kwargs = _resolve_lead_window_kwargs(
        date_from=date_from,
        date_to=date_to,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        created_after=created_after,
        created_before=created_before,
        since_days=since_days,
    )
    res = run_samgov_validation_workflow(
        ingest_days=ingest_days,
        pages=pages,
        page_size=page_size,
        max_records=max_records,
        start_page=start_page,
        keywords=resolved_keywords,
        api_key=api_key,
        ontology_path=resolved_ontology_path,
        ontology_days=ontology_days,
        entity_days=entity_days,
        window_days=window_days,
        min_events_entity=min_events_entity,
        min_events_keywords=min_events_keywords,
        max_events_keywords=max_events_keywords,
        max_keywords_per_event=max_keywords_per_event,
        **lead_window_kwargs,
        min_score=min_score,
        snapshot_limit=snapshot_limit,
        scan_limit=scan_limit,
        scoring_version=scoring_version,
        notes=notes,
        bundle_root=bundle_path,
        database_url=database_url,
        require_nonzero=require_nonzero,
        skip_ingest=skip_ingest,
        threshold_overrides=threshold_overrides,
    )

    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False, default=str))
    else:
        _echo_validation_gate_summary("SAM.gov larger-run validation", res)
        typer.echo(f"Bundle dir: {Path(res.get('bundle_dir')).resolve()}")
        artifacts = res.get("artifacts") or {}
        if artifacts.get("bundle_manifest_json"):
            typer.echo(f"Bundle manifest: {Path(artifacts.get('bundle_manifest_json')).resolve()}")
        if artifacts.get("report_html"):
            typer.echo(f"Bundle report: {Path(artifacts.get('report_html')).resolve()}")
        _echo_validation_checks(res, include_passes=False)

    if require_nonzero and res.get("status") == "failed":
        raise typer.Exit(code=2)
@workflow_app.command("samgov-smoke")
def workflow_samgov_smoke(
    ingest_days: int = typer.Option(30, "--ingest-days", "--days", help="Ingest: days of history to request (--days alias supported)"),
    pages: int = typer.Option(2, "--pages", help="Ingest: maximum API pages to request"),
    page_size: int = typer.Option(100, "--page-size", help="Ingest: records per API page (max 1000)"),
    max_records: Optional[int] = typer.Option(
        50, "--max-records", "--limit", help="Ingest: total cap across pages"
    ),
    start_page: int = typer.Option(1, "--start-page", help="Ingest: start page (resume/chunking)"),
    keyword: Optional[List[str]] = typer.Option(None, "--keyword", help="Ingest: title search terms (repeat --keyword)"),
    keywords_file: Optional[Path] = typer.Option(None, "--keywords-file", help="Ingest: newline-delimited title search terms file"),
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="Override SAM_API_KEY from environment for this command (not printed)."
    ),
    ontology_profile: SamOntologyProfile = typer.Option(
        SamOntologyProfile.starter,
        "--ontology-profile",
        help="Ontology profile: starter | dod_foia | starter_plus_dod_foia | hidden_program_proxy | hidden_program_proxy_exploratory | starter_plus_dod_foia_hidden_program_proxy | starter_plus_dod_foia_hidden_program_proxy_exploratory",
    ),
    ontology_path: Optional[Path] = typer.Option(
        None,
        "--ontology",
        "-o",
        help="Ontology: explicit path override for SAM ontology JSON",
    ),
    ontology_days: int = typer.Option(30, "--ontology-days", help="Ontology: tag events in last N days"),
    window_days: int = typer.Option(30, "--window-days", help="Correlation/doctor lookback window (days)"),
    min_events_entity: int = typer.Option(2, "--min-events-entity", help="Correlations: min events for entity/UEI lanes"),
    min_events_keywords: int = typer.Option(
        2, "--min-events-keywords", help="Correlations: min events for keyword/kw-pair lanes"
    ),
    max_events_keywords: int = typer.Option(
        200, "--max-events-keywords", help="Correlations: skip keywords/pairs matching more than this many events"
    ),
    max_keywords_per_event: int = typer.Option(
        10, "--max-keywords-per-event", help="Correlations: skip events with too many keywords (pair explosion guard)"
    ),
    date_from: Optional[str] = typer.Option(None, "--date-from", help="Snapshot: inclusive ISO-8601 event-time start"),
    date_to: Optional[str] = typer.Option(None, "--date-to", help="Snapshot: inclusive ISO-8601 event-time end"),
    occurred_after: Optional[str] = typer.Option(None, "--occurred-after", help="Snapshot: inclusive ISO-8601 occurred_at start"),
    occurred_before: Optional[str] = typer.Option(None, "--occurred-before", help="Snapshot: inclusive ISO-8601 occurred_at end"),
    created_after: Optional[str] = typer.Option(None, "--created-after", help="Snapshot: inclusive ISO-8601 created_at start"),
    created_before: Optional[str] = typer.Option(None, "--created-before", help="Snapshot: inclusive ISO-8601 created_at end"),
    since_days: Optional[int] = typer.Option(None, "--since-days", help="Snapshot: event-time lookback window helper"),
    entity_days: int = typer.Option(30, "--entity-days", help="Entities: link events created in last N days"),
    min_score: int = typer.Option(1, "--min-score", help="Snapshot: minimum score to include"),
    snapshot_limit: int = typer.Option(200, "--snapshot-limit", help="Snapshot: max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Snapshot/doctor scan window"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Snapshot: scoring version (v1 or v2)"),
    notes: Optional[str] = typer.Option("samgov smoke workflow", "--notes", help="Snapshot: optional notes"),
    bundle_root: Optional[str] = typer.Option(
        None, "--bundle-root", help="Artifact bundle root directory (defaults to data/exports/smoke/samgov)"
    ),
    require_nonzero: bool = typer.Option(
        True, "--require-nonzero/--no-require-nonzero", help="Fail with exit code 2 when required checks fail"
    ),
    threshold: Optional[List[str]] = typer.Option(None, "--threshold", help="Threshold override key=value (repeat)."),
    skip_ingest: bool = typer.Option(False, "--skip-ingest", help="Skip ingest step (offline fixture replay)"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload (default=str for paths)"),
):
    from backend.services.workflow import DEFAULT_SAM_SMOKE_THRESHOLDS, run_samgov_smoke_workflow

    bundle_path = Path(bundle_root).expanduser() if bundle_root else None
    resolved_keywords = _resolve_sam_ingest_keywords(keyword=keyword, keywords_file=keywords_file)
    threshold_overrides = _parse_threshold_overrides(threshold, allowed=set(DEFAULT_SAM_SMOKE_THRESHOLDS.keys()))
    resolved_ontology_path = _resolve_sam_ontology_path(ontology_profile=ontology_profile, ontology_path=ontology_path)
    lead_window_kwargs = _resolve_lead_window_kwargs(
        date_from=date_from,
        date_to=date_to,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        created_after=created_after,
        created_before=created_before,
        since_days=since_days,
    )
    res = run_samgov_smoke_workflow(
        ingest_days=ingest_days,
        pages=pages,
        page_size=page_size,
        max_records=max_records,
        start_page=start_page,
        keywords=resolved_keywords,
        api_key=api_key,
        ontology_path=resolved_ontology_path,
        ontology_days=ontology_days,
        entity_days=entity_days,
        window_days=window_days,
        min_events_entity=min_events_entity,
        min_events_keywords=min_events_keywords,
        max_events_keywords=max_events_keywords,
        max_keywords_per_event=max_keywords_per_event,
        **lead_window_kwargs,
        min_score=min_score,
        snapshot_limit=snapshot_limit,
        scan_limit=scan_limit,
        scoring_version=scoring_version,
        notes=notes,
        bundle_root=bundle_path,
        database_url=database_url,
        require_nonzero=require_nonzero,
        skip_ingest=skip_ingest,
        threshold_overrides=threshold_overrides,
    )

    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False, default=str))
    else:
        _echo_validation_gate_summary("SAM.gov smoke workflow", res)
        typer.echo(f"Bundle dir: {Path(res.get('bundle_dir')).resolve()}")
        artifacts = res.get("artifacts") or {}
        if artifacts.get("smoke_summary_json"):
            typer.echo(f"Workflow summary: {Path(artifacts.get('smoke_summary_json')).resolve()}")
        if artifacts.get("doctor_status_json"):
            typer.echo(f"Doctor status: {Path(artifacts.get('doctor_status_json')).resolve()}")
        if artifacts.get("bundle_manifest_json"):
            typer.echo(f"Bundle manifest: {Path(artifacts.get('bundle_manifest_json')).resolve()}")
        if artifacts.get("report_html"):
            typer.echo(f"Bundle report: {Path(artifacts.get('report_html')).resolve()}")
        thresholds_used = res.get("thresholds") or {}
        if thresholds_used:
            typer.echo(f"Threshold contract: {thresholds_used}")
        _echo_validation_checks(res, include_passes=True)
        entities_diag = (res.get("baseline") or {}).get("entity_coverage") or {}
        typer.echo(
            "Entity coverage baseline: "
            f"window_linked_pct={entities_diag.get('window_linked_coverage_pct')} "
            f"sample_identity={entities_diag.get('sample_events_with_identity_signal')} "
            f"sample_identity_linked={entities_diag.get('sample_events_with_identity_signal_linked')} "
            f"sample_identity_linked_pct={entities_diag.get('sample_identity_signal_coverage_pct')}"
        )

    if require_nonzero and res.get("status") == "failed":
        raise typer.Exit(code=2)


@report_app.command("samgov")
def report_samgov(
    bundle: str = typer.Option(..., "--bundle", help="Path to SAM workflow/smoke bundle directory"),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload"),
):
    from backend.services.reporting import generate_sam_report_from_bundle, resolve_bundle_directory

    bundle_path = resolve_bundle_directory(Path(bundle).expanduser())
    if not bundle_path.exists() or not bundle_path.is_dir():
        typer.secho(f"Bundle directory not found: {bundle_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    res = generate_sam_report_from_bundle(bundle_path)
    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False, default=str))
        return

    typer.echo(f"Report status: {res.get('status')}")
    typer.echo(f"Bundle dir: {Path(res.get('bundle_dir')).resolve()}")
    typer.echo(f"Report HTML: {Path(res.get('report_html')).resolve()}")


@report_app.command("latest")
def report_latest(
    source: str = typer.Option("SAM.gov", "--source", help="Source to resolve latest bundle for"),
    bundle_root: Optional[str] = typer.Option(
        None,
        "--bundle-root",
        help="Optional bundle root (defaults to data/exports/smoke/samgov)",
    ),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload"),
):
    from backend.services.reporting import find_latest_sam_smoke_bundle, generate_sam_report_from_bundle

    normalized_source = source.strip().lower().replace(" ", "")
    if normalized_source not in {"sam.gov", "samgov", "sam"}:
        raise typer.BadParameter("Only source=SAM.gov is supported for this sprint.")

    root_path = Path(bundle_root).expanduser() if bundle_root else None
    latest = find_latest_sam_smoke_bundle(root_path)
    if latest is None:
        typer.secho("No SAM.gov smoke bundles found.", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    res = generate_sam_report_from_bundle(latest, workflow_type="samgov-smoke")
    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False, default=str))
        return

    typer.echo(f"Report status: {res.get('status')}")
    typer.echo(f"Bundle dir: {Path(res.get('bundle_dir')).resolve()}")
    typer.echo(f"Report HTML: {Path(res.get('report_html')).resolve()}")

@report_app.command("candidate-joins")
def report_candidate_joins(
    window_days: int = typer.Option(None, "--window-days", help="Filter by recent SAM lookback window"),
    min_score: int = typer.Option(None, "--min-score", help="Minimum deterministic confidence score"),
    limit: int = typer.Option(20, "--limit", help="Max joins to summarize"),
    incumbent_only: bool = typer.Option(False, "--incumbent-only", help="Only summarize likely incumbent-style joins"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload"),
):
    from backend.services.export_correlations import summarize_candidate_joins

    res = summarize_candidate_joins(
        database_url=database_url,
        window_days=window_days,
        min_score=min_score,
        limit=limit,
        incumbent_only=incumbent_only,
    )
    if json_out:
        typer.echo(json.dumps(res, indent=2, ensure_ascii=False, default=str))
        return

    typer.echo(
        "Candidate joins summary: "
        f"count={res.get('count')} likely_incumbent={res.get('likely_incumbent_count')} score_bands={res.get('score_bands')}"
    )
    typer.echo(f"Evidence counts: {res.get('evidence_type_counts')}")
    for item in res.get('items', []):
        sam = item.get('sam_event') or {}
        usa = item.get('usaspending_event') or {}
        typer.echo(
            f"- score={item.get('score')} incumbent={item.get('likely_incumbent')} "
            f"sam={sam.get('hash') or sam.get('doc_id')} usa={usa.get('hash') or usa.get('doc_id')} "
            f"evidence={','.join([str(x) for x in item.get('evidence_types') or []])}"
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

@correlate_app.command("rebuild-keywords")
def correlate_rebuild_keywords(
    window_days: int = typer.Option(30, "--window-days", help="Lookback window (days)"),
    source: str = typer.Option("USAspending", "--source", help="Event source (blank for all)"),
    min_events: int = typer.Option(3, "--min-events", help="Minimum events per keyword"),
    max_events: int = typer.Option(200, "--max-events", help="Skip keywords that match more than this many events"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute only; do not write to DB"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.correlate import correlate
    res = correlate.rebuild_keyword_correlations(
        window_days=window_days,
        source=source if source else None,
        min_events=min_events,
        max_events=max_events,
        dry_run=dry_run,
        database_url=database_url,
    )
    typer.echo(
        "Keyword correlation rebuild: "
        + " ".join([f"{k}={v}" for k, v in res.items() if k in ("dry_run","source","window_days","min_events","max_events","keywords_seen","eligible_keywords","correlations_created","correlations_updated","correlations_deleted","links_created")])
    )
@correlate_app.command("rebuild-keyword-pairs")
def correlate_rebuild_keyword_pairs(
    window_days: int = typer.Option(30, "--window-days", help="Lookback window (days)"),
    source: str = typer.Option("USAspending", "--source", help="Event source (blank for all)"),
    min_events: int = typer.Option(3, "--min-events", help="Minimum events per keyword-pair"),
    max_events: int = typer.Option(200, "--max-events", help="Skip pairs that match more than this many events"),
    max_keywords_per_event: int = typer.Option(10, "--max-keywords-per-event", help="Skip events with too many keywords (pair explosion guard)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute only; do not write to DB"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.correlate import correlate
    res = correlate.rebuild_keyword_pair_correlations(
        window_days=window_days,
        source=source if source else None,
        min_events=min_events,
        max_events=max_events,
        max_keywords_per_event=max_keywords_per_event,
        dry_run=dry_run,
        database_url=database_url,
    )
    typer.echo(
        "Keyword-pair correlation rebuild: "
        + " ".join(
            [
                f"{k}={v}"
                for k, v in res.items()
                if k
                in (
                    "dry_run",
                    "source",
                    "window_days",
                    "min_events",
                    "max_events",
                    "max_keywords_per_event",
                    "pairs_seen",
                    "eligible_pairs",
                    "correlations_created",
                    "correlations_updated",
                    "correlations_deleted",
                    "links_created",
                )
            ]
        )
    )


@correlate_app.command("rebuild-sam-naics")
def correlate_rebuild_sam_naics(
    window_days: int = typer.Option(30, "--window-days", help="Lookback window (days)"),
    source: str = typer.Option("SAM.gov", "--source", help="Event source (blank for all)"),
    min_events: int = typer.Option(2, "--min-events", help="Minimum events per NAICS code"),
    max_events: int = typer.Option(200, "--max-events", help="Skip NAICS codes matching more than this many events"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute only; do not write to DB"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.correlate import correlate

    res = correlate.rebuild_sam_naics_correlations(
        window_days=window_days,
        source=source if source else None,
        min_events=min_events,
        max_events=max_events,
        dry_run=dry_run,
        database_url=database_url,
    )
    typer.echo(
        "SAM NAICS correlation rebuild: "
        + " ".join(
            [
                f"{k}={v}"
                for k, v in res.items()
                if k
                in (
                    "dry_run",
                    "source",
                    "window_days",
                    "min_events",
                    "max_events",
                    "naics_seen",
                    "eligible_naics",
                    "correlations_created",
                    "correlations_updated",
                    "correlations_deleted",
                    "links_created",
                )
            ]
        )
    )



@correlate_app.command("rebuild-normalized")
def correlate_rebuild_normalized(
    window_days: int = typer.Option(30, "--window-days", help="Lookback window (days)"),
    source: str = typer.Option("", "--source", help="Event source (blank for all)"),
    min_events: int = typer.Option(2, "--min-events", help="Minimum events required per lane key"),
    max_events: int = typer.Option(200, "--max-events", help="Cap very common keys to avoid noisy/giant clusters"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute only; do not write to DB"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.correlate import correlate

    src = source if source else None
    res = {
        "same_award_id": correlate.rebuild_award_id_correlations(
            window_days=window_days,
            source=src,
            min_events=min_events,
            max_events=max_events,
            dry_run=dry_run,
            database_url=database_url,
        ),
        "same_contract_id": correlate.rebuild_contract_id_correlations(
            window_days=window_days,
            source=src,
            min_events=min_events,
            max_events=max_events,
            dry_run=dry_run,
            database_url=database_url,
        ),
        "same_doc_id": correlate.rebuild_doc_id_correlations(
            window_days=window_days,
            source=src,
            min_events=min_events,
            max_events=max_events,
            dry_run=dry_run,
            database_url=database_url,
        ),
        "same_agency": correlate.rebuild_agency_correlations(
            window_days=window_days,
            source=src,
            min_events=min_events,
            max_events=max_events,
            dry_run=dry_run,
            database_url=database_url,
        ),
        "same_psc": correlate.rebuild_psc_correlations(
            window_days=window_days,
            source=src,
            min_events=min_events,
            max_events=max_events,
            dry_run=dry_run,
            database_url=database_url,
        ),
        "same_naics": correlate.rebuild_naics_correlations(
            window_days=window_days,
            source=src,
            min_events=min_events,
            max_events=max_events,
            dry_run=dry_run,
            database_url=database_url,
        ),
        "same_place_region": correlate.rebuild_place_region_correlations(
            window_days=window_days,
            source=src,
            min_events=min_events,
            max_events=max_events,
            dry_run=dry_run,
            database_url=database_url,
        ),
    }
    typer.echo(json.dumps(res, ensure_ascii=False, indent=2))


@correlate_app.command("rebuild-sam-usaspending-joins")
def correlate_rebuild_sam_usaspending_joins(
    window_days: int = typer.Option(30, "--window-days", help="Recent SAM lookback window (days)"),
    history_days: int = typer.Option(365, "--history-days", help="USAspending history lookback window (days)"),
    min_score: int = typer.Option(45, "--min-score", help="Minimum deterministic confidence score to keep"),
    max_matches_per_key: int = typer.Option(25, "--max-matches-per-key", help="Skip overly common keys with more than this many USA matches"),
    max_candidates_per_sam: int = typer.Option(10, "--max-candidates-per-sam", help="Keep at most this many candidate joins per SAM event"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute only; do not write to DB"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.correlate.candidate_joins import rebuild_sam_usaspending_candidate_joins

    res = rebuild_sam_usaspending_candidate_joins(
        window_days=window_days,
        history_days=history_days,
        min_score=min_score,
        max_matches_per_key=max_matches_per_key,
        max_candidates_per_sam=max_candidates_per_sam,
        dry_run=dry_run,
        database_url=database_url,
    )
    typer.echo(
        "SAM<->USAspending candidate join rebuild: "
        + " ".join(
            [
                f"{k}={v}"
                for k, v in res.items()
                if k
                in (
                    "dry_run",
                    "window_days",
                    "history_days",
                    "min_score",
                    "sam_events_seen",
                    "usaspending_events_seen",
                    "sam_events_with_candidates",
                    "candidate_pairs_considered",
                    "candidate_pairs_above_threshold",
                    "candidate_pairs_trimmed",
                    "likely_incumbent_count",
                    "correlations_created",
                    "correlations_updated",
                    "correlations_deleted",
                )
            ]
        )
    )
app.add_typer(correlate_app, name="correlate")


@export_app.command("candidate-joins")
def export_candidate_joins_cmd(
    out: Optional[str] = typer.Option(None, "--out", help="Output directory or base file path"),
    window_days: int = typer.Option(None, "--window-days", help="Filter by recent SAM lookback window"),
    min_score: int = typer.Option(None, "--min-score", help="Minimum deterministic confidence score"),
    limit: int = typer.Option(200, "--limit", help="Max candidate joins to export"),
    incumbent_only: bool = typer.Option(False, "--incumbent-only", help="Only export likely incumbent-style joins"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.services.export_correlations import export_candidate_joins

    export_path = Path(out).expanduser() if out else None
    res = export_candidate_joins(
        database_url=database_url,
        output=export_path,
        window_days=window_days,
        min_score=min_score,
        limit=limit,
        incumbent_only=incumbent_only,
    )
    typer.echo(f"Candidate joins CSV: {res['csv'].resolve()}")
    typer.echo(f"Candidate joins JSON: {res['json'].resolve()}")
    typer.echo(f"Rows exported: {res['count']}")
@export_app.command("correlations")
def export_correlations_cmd(
    out: str = typer.Option("data/exports/correlations.json", "--out", help="Output JSON path"),
    source: str = typer.Option("USAspending", "--source", help="Event source filter (blank for all)"),
    date_from: Optional[str] = typer.Option(None, "--date-from", help="Inclusive ISO-8601 start datetime"),
    date_to: Optional[str] = typer.Option(None, "--date-to", help="Inclusive ISO-8601 end datetime"),
    entity_id: Optional[int] = typer.Option(None, "--entity-id", help="Filter linked events by entity_id"),
    keyword: Optional[str] = typer.Option(None, "--keyword", help="Filter linked events by keyword tag"),
    min_score: int = typer.Option(None, "--min-score", help="Minimum numeric score"),
    agency: Optional[str] = typer.Option(None, "--agency", help="Filter linked events by agency code or name"),
    psc: Optional[str] = typer.Option(None, "--psc", help="Filter linked events by PSC code or description"),
    naics: Optional[str] = typer.Option(None, "--naics", help="Filter linked events by NAICS code or description"),
    award_id: Optional[str] = typer.Option(None, "--award-id", help="Filter linked events by award id"),
    recipient_uei: Optional[str] = typer.Option(None, "--recipient-uei", help="Filter linked events by recipient UEI"),
    place_region: Optional[str] = typer.Option(None, "--place-region", help="Filter linked events by state/country region"),
    lane: str = typer.Option("", "--lane", help="Correlation lane filter (blank for all; e.g., same_entity, same_uei)"),
    window_days: int = typer.Option(None, "--window-days", help="Filter correlations by window_days"),
    min_event_count: Optional[int] = typer.Option(None, "--min-event-count", help="Minimum linked events within the active filters"),
    min_score_signal: Optional[float] = typer.Option(None, "--min-score-signal", help="Minimum score_signal / lane score"),
    sort_by: str = typer.Option("score_signal", "--sort-by", help="Sort by score_signal, event_count, created_at, or id"),
    sort_dir: str = typer.Option("desc", "--sort-dir", help="Sort direction asc or desc"),
    offset: int = typer.Option(0, "--offset", help="Skip this many matching correlations"),
    limit: int = typer.Option(500, "--limit", help="Max correlations to export"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.services.export_correlations import export_correlations

    res = export_correlations(
        out_path=out,
        source=source if source else None,
        date_from=_parse_datetime_option(date_from, option_name="date_from"),
        date_to=_parse_datetime_option(date_to, option_name="date_to"),
        entity_id=entity_id,
        keyword=keyword,
        min_score=min_score,
        agency=agency,
        psc=psc,
        naics=naics,
        award_id=award_id,
        recipient_uei=recipient_uei,
        place_region=place_region,
        lane=lane if lane else None,
        window_days=window_days,
        min_event_count=min_event_count,
        min_score_signal=min_score_signal,
        offset=offset,
        limit=limit,
        sort_by=sort_by,
        sort_dir=sort_dir,
        database_url=database_url,
    )
    typer.echo("Exported correlations: count=%s out=%s" % (res.get("count"), res.get("out_path")))

