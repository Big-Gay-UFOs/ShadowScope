# ShadowScope macOS / Linux Quick Start

## Prerequisites

- macOS 13+ or a recent Linux distribution
- Git
- Python 3.11 available on `PATH` (Homebrew `python@3.11` or system package)
- PostgreSQL running locally on `localhost:5432` with `postgres` / `postgres`
- Optional: `SAM_API_KEY` for SAM.gov ingestion (the bootstrap will skip when missing)

## One-time bootstrap

From the repository root, run:

```bash
./scripts/bootstrap.sh
```

The script performs the following steps:

1. Creates or reuses `.venv` with Python 3.11 and installs dependencies via `pip install -e .`.
2. Copies `.env.example` to `.env` if missing.
3. Initializes the database (`ss db init`).
4. Runs tests (`ss test`).
5. Executes a short USAspending ingest (`ss ingest usaspending --days 3 --limit 25 --pages 1`).
6. Exports events to CSV/JSONL (`ss export events`) and prints the absolute paths.
7. Prints log file locations (`logs/app.log`, `logs/ingest.log`).
8. Starts the API (`ss serve --host 127.0.0.1 --port 8000`).

Press <kbd>Ctrl</kbd> + <kbd>C</kbd> to stop the API.

## After bootstrap

- Run `source .venv/bin/activate` in new shells before using the `ss` CLI.
- Raw snapshots, exports, and logs are stored under `data/` and `logs/`.
- Visit `http://127.0.0.1:8000/api/events` to browse ingested data.
- Use `ss export events --out data/exports` to create additional exports.

Refer to the [README troubleshooting guide](../README.md#troubleshooting-quick-start) for common fixes.
