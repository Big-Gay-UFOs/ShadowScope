# ShadowScope Runbook

This runbook documents the standard ?investigator? workflow for ShadowScope using the CLI (`ss`).

## Prereqs
- Docker services running (Postgres, OpenSearch, API if needed):
  - `docker compose up -d`
- Environment:
  - `DATABASE_URL` points at your Postgres DB (default in `.env` / runtime)
- Validate CLI:
  - `ss --help`

## Core workflow (recommended order)

### 1) Ingest (USAspending)
Pull a bounded slice of awards to avoid pulling the entire firehose.

Example (30-day window, two pages of 100 each, keyword narrowing):
- `ss ingest usaspending --days 30 --pages 2 --page-size 100 --keyword "DOE" --keyword "NNSA"`

Artifacts:
- Raw snapshots: `data/raw/usaspending/YYYYMMDD/page_*.json`

### 2) Ontology tagging
Apply ontology rules to populate `events.keywords` and `events.clauses`.

Example:
- `ss ontology apply --path .\ontology.foia.json --days 30 --source USAspending`

### 3) Correlations
Rebuild correlation lanes over a time window.

All lanes:
- `ss correlate rebuild --window-days 30 --source USAspending --min-events 2`

Keyword pairs:
- `ss correlate rebuild-keyword-pairs --window-days 30 --source USAspending --min-events 2 --max-events 500 --max-keywords-per-event 50`

Notes:
- kw-pairs require events with >=2 keywords.
- If your dataset is small, lower `--min-events` to 2.

### 4) Entity linking
Link events to entities from recipient identifiers.

Example:
- `ss entities link --source USAspending --days 30`

### 5) Leads snapshot (ranking)
Create a snapshot of the top-scoring leads using scoring v2 by default.

Example:
- `ss leads snapshot --source USAspending --min-score 1 --limit 200 --scan-limit 5000 --scoring-version v2 --notes "daily snapshot"`

### 6) Exports (artifacts)
Lead snapshot export:
- `ss export lead-snapshot --snapshot-id <ID> --out .\data\exports\`

Lead deltas export:
- `ss export lead-deltas --from <ID> --to <ID> --out .\data\exports\`

kw-pairs export:
- `ss export kw-pairs --min-event-count 2 --limit 200 --out .\data\exports\`

Correlations export (generic):
- `ss export correlations --source USAspending --lane kw_pair --window-days 30 --limit 500`

## Troubleshooting

### No keyword pairs produced
Symptoms:
- `pairs_seen=0` or `eligible_pairs=0`

Checks:
- ensure ontology tagging produced keywords:
  - verify events have >=2 keywords
- widen window:
  - `--window-days 90`
- loosen thresholds:
  - `--min-events 2`
  - increase `--max-keywords-per-event`

### Entity linking skips everything
Symptoms:
- `skipped_no_name` is high

Checks:
- inspect `events.raw_json` for `Recipient Name`, `Recipient UEI`, `Recipient DUNS Number`, `Recipient CAGE Code`
- confirm ingest is using the USAspending connector fields list

## Dev workflow
- Do not push directly to `origin/main`.
- Always use: feature branch -> PR -> squash merge.
- Optional: install a local pre-push hook to reduce accidental pushes to `origin/main` (not enforced by the repo).

## PowerShell notes
- Do not paste placeholders like `<ID>` or `<SNAPSHOT_ID>` into PowerShell; `<` and `>` are treated as operators.
  - Use the numeric value directly (example: `--snapshot-id 2`).
- Correlation commands use `--window-days` (not `--days`):
  - `ss correlate rebuild --window-days 30 ...`
  - `ss correlate rebuild-keyword-pairs --window-days 30 ...`
- USAspending raw snapshots are written as:
  - `data/raw/usaspending/YYYYMMDD/page_*.json` (not `.jsonl`).

### Optional local pre-push hook
Git hooks are not versioned by default. If you want a local guardrail on your machine:

- Create `.git/hooks/pre-push` that rejects pushes to `origin/main`.
- Keep it local (do not commit it).
## Doctor / Status

Use this when the pipeline "looks empty" (no events, no keywords, no kw-pairs, no entities, etc.) or when you want a fast sanity check.

Examples:

- `ss doctor status --source USAspending --days 30`
- `ss doctor status --source USAspending --days 30 --json`

What it reports:

- DB connectivity (safe URL)
- Counts: events, entities, correlations, lead snapshots
- Correlations by lane (kw_pair, same_keyword, same_uei, same_entity)
- Keyword coverage on a recent sample window + top keywords
- Last ingest / ontology apply / lead snapshot metadata (when present)
- Actionable hints (common failure causes + next commands to run)

## Export: Entities

Generate an entity list export plus an event->entity mapping export:

- `ss export entities`
- `ss export entities --out data/exports`

Outputs:
- Entities CSV/JSON
- Event->Entity mapping CSV/JSON (includes recipient identifiers when present in raw_json)

## Workflow wrapper (optional)

One command to run the standard USAspending pipeline end-to-end:

- `ss workflow usaspending --ingest-days 30 --pages 2 --page-size 100 --ontology .\ontology.json --window-days 30`

Notes:
- Use `--skip-ingest` to run offline (no network calls).
- The workflow runs: ingest -> ontology -> entities -> correlations -> snapshot -> exports.
- If --out is a file path (example: .\\reports\\run.csv), the workflow generates per-artifact files (prefix + timestamp) to avoid overwriting.
 
