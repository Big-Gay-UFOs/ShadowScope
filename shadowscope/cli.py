"""Typer-based command line interface for ShadowScope."""
from __future__ import annotations

import json
import os
import subprocess
import sys
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
workflow_app = typer.Typer(help="One-command workflows")
report_app = typer.Typer(help="Report generation utilities")

app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(export_app, name="export")
app.add_typer(ontology_app, name="ontology")
app.add_typer(leads_app, name="leads")
app.add_typer(entities_app, name="entities")
app.add_typer(doctor_app, name="doctor")
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
    api_key: Optional[str] = typer.Option(None, "--api-key", help="Override SAM_API_KEY from environment for this command (not printed)."),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    """Ingest SAM.gov opportunities into events (bounded window + paging).

    Notes:
      - API key is read from SAM_API_KEY unless --api-key is provided.
      - Raw snapshots are written under data/raw/sam/YYYYMMDD/.
    """
    try:
        result = ingest_sam_opportunities(
            api_key=api_key,
            days=days,
            pages=pages,
            page_size=page_size,
            max_records=max_records,
            start_page=start_page,
            keywords=keywords,
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
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
):
    export_path = Path(out).expanduser() if out else None
    results = export_events(database_url=database_url, output=export_path)
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
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Scoring version label"),
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
    entity_days: int = typer.Option(30, "--entity-days", help="Entities: link events created in last N days"),
    min_score: int = typer.Option(1, "--min-score", help="Snapshot: minimum score to include"),
    snapshot_limit: int = typer.Option(200, "--snapshot-limit", help="Snapshot: max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Snapshot: how many recent events to scan"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Snapshot: scoring version label"),
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
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="Override SAM_API_KEY from environment for this command (not printed)."
    ),
    ontology_path: Path = typer.Option(
        Path("examples/ontology_sam_procurement_starter.json"),
        "--ontology",
        "-o",
        help="Ontology: path to SAM ontology JSON",
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
    entity_days: int = typer.Option(30, "--entity-days", help="Entities: link events created in last N days"),
    min_score: int = typer.Option(1, "--min-score", help="Snapshot: minimum score to include"),
    snapshot_limit: int = typer.Option(200, "--snapshot-limit", help="Snapshot: max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Snapshot: how many recent events to scan"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Snapshot: scoring version label"),
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
    res = run_samgov_workflow(
        ingest_days=ingest_days,
        pages=pages,
        page_size=page_size,
        max_records=max_records,
        start_page=start_page,
        keywords=keyword,
        api_key=api_key,
        ontology_path=ontology_path,
        ontology_days=ontology_days,
        window_days=window_days,
        min_events_entity=min_events_entity,
        min_events_keywords=min_events_keywords,
        max_events_keywords=max_events_keywords,
        max_keywords_per_event=max_keywords_per_event,
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
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="Override SAM_API_KEY from environment for this command (not printed)."
    ),
    ontology_path: Path = typer.Option(
        Path("examples/ontology_sam_procurement_starter.json"),
        "--ontology",
        "-o",
        help="Ontology: path to SAM ontology JSON",
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
    entity_days: int = typer.Option(30, "--entity-days", help="Entities: link events created in last N days"),
    min_score: int = typer.Option(1, "--min-score", help="Snapshot: minimum score to include"),
    snapshot_limit: int = typer.Option(200, "--snapshot-limit", help="Snapshot: max leads to store"),
    scan_limit: int = typer.Option(5000, "--scan-limit", help="Snapshot/doctor scan window"),
    scoring_version: str = typer.Option("v2", "--scoring-version", help="Snapshot: scoring version label"),
    notes: Optional[str] = typer.Option("samgov smoke workflow", "--notes", help="Snapshot: optional notes"),
    bundle_root: Optional[str] = typer.Option(
        None, "--bundle-root", help="Artifact bundle root directory (defaults to data/exports/smoke/samgov)"
    ),
    require_nonzero: bool = typer.Option(
        True, "--require-nonzero/--no-require-nonzero", help="Fail with exit code 2 when required non-zero checks fail"
    ),
    threshold: Optional[List[str]] = typer.Option(None, "--threshold", help="Threshold override key=value (repeat)."),
    skip_ingest: bool = typer.Option(False, "--skip-ingest", help="Skip ingest step (offline fixture replay)"),
    database_url: Optional[str] = typer.Option(None, "--database-url", help="Override DATABASE_URL for this command."),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload (default=str for paths)"),
):
    from backend.services.workflow import DEFAULT_SAM_SMOKE_THRESHOLDS, run_samgov_smoke_workflow

    bundle_path = Path(bundle_root).expanduser() if bundle_root else None
    threshold_overrides = _parse_threshold_overrides(threshold, allowed=set(DEFAULT_SAM_SMOKE_THRESHOLDS.keys()))
    res = run_samgov_smoke_workflow(
        ingest_days=ingest_days,
        pages=pages,
        page_size=page_size,
        max_records=max_records,
        start_page=start_page,
        keywords=keyword,
        api_key=api_key,
        ontology_path=ontology_path,
        ontology_days=ontology_days,
        entity_days=entity_days,
        window_days=window_days,
        min_events_entity=min_events_entity,
        min_events_keywords=min_events_keywords,
        max_events_keywords=max_events_keywords,
        max_keywords_per_event=max_keywords_per_event,
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
        typer.echo(f"SAM.gov smoke workflow: {'PASS' if res.get('smoke_passed') else 'FAIL'}")
        typer.echo(f"Bundle dir: {Path(res.get('bundle_dir')).resolve()}")
        artifacts = res.get("artifacts") or {}
        if artifacts.get("smoke_summary_json"):
            typer.echo(f"Smoke summary: {Path(artifacts.get('smoke_summary_json')).resolve()}")
        if artifacts.get("doctor_status_json"):
            typer.echo(f"Doctor status: {Path(artifacts.get('doctor_status_json')).resolve()}")
        if artifacts.get("report_html"):
            typer.echo(f"Report HTML: {Path(artifacts.get('report_html')).resolve()}")
        thresholds_used = res.get("thresholds") or {}
        if thresholds_used:
            typer.echo(f"Threshold contract: {thresholds_used}")
        for chk in res.get("checks", []):
            status = str(chk.get("status") or ("pass" if chk.get("ok") else "fail")).upper()
            req = "" if chk.get("required", True) else " (info)"
            observed = chk.get("observed", chk.get("actual"))
            typer.echo(
                f"- [{status}] {chk.get('name')}{req} observed={observed} expected={chk.get('expected')}"
            )
            if not chk.get("ok"):
                if chk.get("why"):
                    typer.echo(f"  why: {chk.get('why')}")
                if chk.get("hint"):
                    typer.echo(f"  next: {chk.get('hint')}")
        entities_diag = (res.get("baseline") or {}).get("entity_coverage") or {}
        typer.echo(
            "Entity coverage baseline: "
            f"window_linked_pct={entities_diag.get('window_linked_coverage_pct')} "
            f"sample_identity={entities_diag.get('sample_events_with_identity_signal')} "
            f"sample_identity_linked={entities_diag.get('sample_events_with_identity_signal_linked')} "
            f"sample_identity_linked_pct={entities_diag.get('sample_identity_signal_coverage_pct')}"
        )

    if require_nonzero and res.get("status") != "ok":
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


app.add_typer(correlate_app, name="correlate")

@export_app.command("correlations")
def export_correlations_cmd(
    out: str = typer.Option("data/exports/correlations.json", "--out", help="Output JSON path"),
    source: str = typer.Option("USAspending", "--source", help="Event source filter (blank for all)"),
    lane: str = typer.Option("", "--lane", help="Correlation lane filter (blank for all; e.g., same_entity, same_uei)"),
    window_days: int = typer.Option(None, "--window-days", help="Filter correlations by window_days"),
    min_score: int = typer.Option(None, "--min-score", help="Minimum numeric score"),
    limit: int = typer.Option(500, "--limit", help="Max correlations to export"),
    database_url: str = typer.Option(None, "--database-url", help="Override DB URL"),
):
    from backend.services.export_correlations import export_correlations
    res = export_correlations(
        out_path=out,
        source=source if source else None,
        lane=lane if lane else None,
        window_days=window_days,
        min_score=min_score,
        limit=limit,
        database_url=database_url,
    )
    typer.echo("Exported correlations: count=%s out=%s" % (res.get("count"), res.get("out_path")))





