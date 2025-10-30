# ShadowScope

ShadowScope is an open-source intelligence (OSINT) pipeline focused on surfacing potential "program shadow" signals across FFRDCs, UARCs, and associated cut-outs. The system ingests public procurement, property, regulatory, security, and transport datasets; normalizes entities; and correlates multi-lane event clusters to highlight leads for further human review.

## Quick start

### Windows 11 (PowerShell)

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

After the bootstrap script completes you can start the API in a console window with:

```powershell
Start-Process -FilePath .\Start-ShadowScope.cmd
```

The helper command file sets up a local SQLite database (`dev.db`) and launches Uvicorn on
`http://127.0.0.1:8000` so you have a quick way to verify the stack without opening the
virtual environment manually.

The bootstrap script:

1. Creates/activates a `.venv` running on Python 3.11.
2. Installs ShadowScope in editable mode (`pip install -e .`).
3. Copies `.env.example` → `.env` (preserving any existing `.env`).
4. Initializes the Postgres database with Alembic migrations (`ss db init`).
5. Runs the test suite (`ss test`).
6. Performs a small USAspending ingest (`ss ingest usaspending --days 3 --limit 25 --pages 1`).
7. Exports events to CSV and JSONL (paths are printed in the console).
8. Prints log locations (`logs/app.log`, `logs/ingest.log`).
9. Starts the API on `http://127.0.0.1:8000`.

### macOS / Linux (Bash)

```bash
./scripts/bootstrap.sh
```

The Unix bootstrap performs the same steps as the Windows script and leaves the API process running in the foreground.

### Where things land

- **Virtual environment:** `.venv`
- **Environment variables:** `.env` (based on `.env.example`)
- **Raw snapshots:** `data/raw/usaspending/<YYYYMMDD>/page_<n>.json`
- **Parsed SAM.gov artifacts:** `data/parsed/sam/<notice_id>/`
- **Exports:** `data/exports/events_<timestamp>.csv` and `.jsonl`
- **Logs:** `logs/app.log`, `logs/ingest.log`
- **API:** `http://127.0.0.1:8000` → `/health`, `/api/events`, `/api/events/export`

## ShadowScope CLI (`ss`)

Installing the project (`pip install -e .`) registers the `ss` command powered by Typer:

| Command | Description |
| --- | --- |
| `ss db init` | Create the database if needed and align Alembic migrations (auto-stamps existing tables). |
| `ss db stamp` | Force Alembic to stamp to head when tables already exist. |
| `ss db reset --destructive` | Drop and recreate the public schema, then upgrade to head. |
| `ss serve --host 127.0.0.1 --port 8000` | Run the FastAPI app via uvicorn. |
| `ss test` | Execute `pytest -q backend/tests` using `TEST_DATABASE_URL` if provided. |
| `ss ingest usaspending --days 7 --limit 100 --pages 2` | Fetch recent USAspending awards, snapshot raw JSON, and upsert deduplicated events. |
| `ss ingest sam` | Placeholder SAM.gov ingest (skips when `SAM_API_KEY` is missing). |
| `ss export events --out data/exports` | Write CSV and JSONL exports and print absolute paths. |

## Troubleshooting quick start

| Symptom | Suggested fix |
| --- | --- |
| PowerShell blocks the script | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` then re-run. |
| `psycopg` cannot connect | Ensure Postgres is running on `localhost:5432` and the credentials in `.env` are correct. |
| `ss db init` reports "connection refused" | Verify the database server is up; on Windows install [PostgreSQL](https://www.postgresql.org/download/windows/). |
| Port 8000 already in use | Stop the conflicting service or run `ss serve --port 8001`. |
| SAM ingest skipped | Provide `SAM_API_KEY` in `.env` when ready to integrate the real API. |

## Repository structure

```
ShadowScope/
  backend/             # FastAPI service, connectors, database layer, and services
  data/                # Raw, parsed, and exported artifacts (created on demand)
  docs/                # Quick start guides and design notes
  scripts/             # Bootstrap scripts for Windows and Unix
  tests/               # Legacy test harnesses (pytest collects backend/tests by default)
  ui/                  # Developer tooling prototypes
```

Key entry points:

- `backend/app.py` – FastAPI application configured with database and logging.
- `backend/services/ingest.py` – USAspending ingest workflow with deduplication and raw snapshots.
- `backend/services/export.py` – CSV/JSONL export helpers.
- `shadowscope/cli.py` – Typer CLI exposing all common workflows.

## Additional notes

- Alembic configuration reads `DATABASE_URL` from `.env` automatically (`python-dotenv` is loaded in `backend/db/models.py`).
- Running `ss ingest usaspending` multiple times deduplicates events via the `hash` column (unique constraint enforced in migrations).
- The FastAPI `/api/events/export` endpoint streams the latest CSV so non-developers can download data without the CLI.

## License

TBD – select an OSI-approved license before public release.
