# ShadowScope Windows Quick Start

## Prerequisites

- Windows 11 with PowerShell 5+ or PowerShell 7
- Git
- PostgreSQL running locally on `localhost:5432` with a database user `postgres` / `postgres`
- Optional: `SAM_API_KEY` for SAM.gov ingestion (the bootstrap will skip SAM when the key is absent)

## One-time bootstrap

Open PowerShell as a normal user (the script sets execution policy for the current process only) and run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

The script will:

1. Create or reuse `.venv` with Python 3.13 and install dependencies via `pip install -e .`.
2. Copy `.env.example` to `.env` if the latter is missing.
3. Initialize the Postgres database with Alembic migrations using `ss db init`.
4. Run the pytest suite (`ss test`).
5. Execute a short USAspending ingest (`ss ingest usaspending --days 3 --limit 25 --pages 1`).
6. Export events to CSV and JSONL (`ss export events`) and print the absolute paths.
7. Display log file locations (`logs/app.log`, `logs/ingest.log`).
8. Start the API (`ss serve --host 127.0.0.1 --port 8000`).

Press <kbd>Ctrl</kbd> + <kbd>C</kbd> to stop the API when you are done reviewing the data.

## After bootstrap

- Use the `ss` CLI for ongoing tasks (for example: `ss ingest usaspending`, `ss export events`).
- Raw API snapshots live under `data/raw/usaspending/<YYYYMMDD>/`.
- Exports are written to `data/exports/` with timestamped filenames.
- Logs are rotated under `logs/app.log` and `logs/ingest.log`.
- Visit `http://127.0.0.1:8000/docs` for the FastAPI interactive documentation.

## Optional: SAM.gov ingestion

If you have a SAM.gov API key, keep it local and load it for your current PowerShell session.

Recommended helper (execution-policy safe):

- `powershell -NoProfile -ExecutionPolicy Bypass -File .\examples\powershell\set-shadow-env.ps1`
- Or in the current shell:
  - `Set-ExecutionPolicy -Scope Process Bypass -Force`
  - `.\examples\powershell\set-shadow-env.ps1`

This helper can load `.env` values and will prompt for `SAM_API_KEY` if missing.

Optional retry tuning via local `.env`:

- `SAM_API_TIMEOUT_SECONDS=60`
- `SAM_API_MAX_RETRIES=8`
- `SAM_API_BACKOFF_BASE=0.75`

Then run a bounded ingest:

- `ss ingest samgov --days 30 --pages 2 --limit 50`

Raw snapshots:

- `data/raw/sam/YYYYMMDD/`

If the command fails, check the troubleshooting table in [README.md](../README.md#troubleshooting-quick-start).

## Doctor / Status

If something seems off (empty outputs, no correlations, no leads), run:

- `ss doctor status --source USAspending --days 30`
- `ss doctor status --source "SAM.gov" --days 30`

For a full payload:

- `ss doctor status --source USAspending --days 30 --json`
- `ss doctor status --source "SAM.gov" --days 30 --json`

## USAspending untagged diagnostics

Inspect recent untagged USAspending rows with a schema-safe query helper:

```powershell
psql -U postgres -d shadowscope -v window_days=30 -v row_limit=50 -f .\tools\diagnose_untagged_usaspending.sql
```

## Export: Entities

Generate an entity list export plus an event->entity mapping export:

- `ss export entities`
- `ss export entities --out data/exports`

Outputs:
- Entities CSV/JSON
- Event->Entity mapping CSV/JSON (includes recipient identifiers when present in `raw_json`)

## Workflow wrappers (optional)

One command to run end-to-end pipelines:

- USAspending:
  - `ss workflow usaspending --ingest-days 30 --pages 2 --page-size 100 --ontology .\examples\ontology_usaspending_starter.json --window-days 30`
- SAM.gov:
  - `ss workflow samgov --days 30 --pages 2 --limit 50 --ontology .\examples\ontology_sam_procurement_starter.json --window-days 30`
- SAM.gov smoke bundle:
  - `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30`

Notes:
- Use `--skip-ingest` to run offline (no network calls).
- Workflows run: ingest -> ontology -> entities -> correlations -> snapshot -> exports.
- `samgov-smoke` additionally captures `doctor status` and writes `workflow_result.json`, `doctor_status.json`, and `smoke_summary.json` in a timestamped bundle.
- SAM workflow commands accept both `--ingest-days` and `--days`.
- If `--out` is a file path (example: `.\reports\run.csv`), workflows generate per-artifact files (prefix + timestamp) to avoid overwriting.
