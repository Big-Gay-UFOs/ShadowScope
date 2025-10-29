# ShadowScope Windows Quick Start

## Prerequisites

- Windows 11 with PowerShell 5+ or PowerShell 7
- Git
- PostgreSQL running locally on `localhost:5432` with a database user `postgres` / `postgres`
- Optional: `SAM_API_KEY` for SAM.gov ingestion (the bootstrap will skip SAM when the key is absent)

## One-time bootstrap

Open PowerShell **as a normal user** (the script sets execution policy for the current process only) and run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

The script will:

1. Create or reuse `.venv` with Python 3.11 and install dependencies via `pip install -e .`.
2. Copy `.env.example` to `.env` if the latter is missing.
3. Initialize the Postgres database with Alembic migrations using `ss db init`.
4. Run the pytest suite (`ss test`).
5. Execute a short USAspending ingest (`ss ingest usaspending --days 3 --limit 25 --pages 1`).
6. Export events to CSV and JSONL (`ss export events`) and print the absolute paths.
7. Display log file locations (`logs/app.log`, `logs/ingest.log`).
8. Start the API (`ss serve --host 127.0.0.1 --port 8000`).

Press <kbd>Ctrl</kbd> + <kbd>C</kbd> to stop the API when you are done reviewing the data.

## After bootstrap

- Use the `ss` CLI for ongoing tasks (e.g., `ss ingest usaspending`, `ss export events`).
- Raw API snapshots live under `data/raw/usaspending/<YYYYMMDD>/`.
- Exports are written to `data/exports/` with timestamped filenames.
- Logs are rotated under `logs/app.log` and `logs/ingest.log`.
- Visit `http://127.0.0.1:8000/docs` for the FastAPI interactive documentation.

If the script fails, check the [troubleshooting table in README.md](../README.md#troubleshooting-quick-start).
