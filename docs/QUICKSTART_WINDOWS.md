# ShadowScope Windows Quick Start

## Prerequisites
- Windows PowerShell 5+ or PowerShell 7
- Git
- PostgreSQL available to your configured `DATABASE_URL`
- Optional for live SAM: `SAM_API_KEY`

## One-time bootstrap

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

Bootstrap sets up `.venv`, installs deps, initializes DB, runs tests, and starts API.

## SAM-first operator quick path

### 1) Load SAM env for this shell

Recommended helper:
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\examples\powershell\set-shadow-env.ps1`

In current shell:
- `Set-ExecutionPolicy -Scope Process Bypass -Force`
- `.\examples\powershell\set-shadow-env.ps1`

### 2) Run bounded SAM workflow

- `ss workflow samgov --days 30 --pages 2 --limit 50 --ontology .\examples\ontology_sam_procurement_starter.json --window-days 30`

### 3) Validate smoke bundle

- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30`
- JSON output:
  - `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

### 4) Review diagnostics

- `ss doctor status --source "SAM.gov" --days 30`
- full payload:
  - `ss doctor status --source "SAM.gov" --days 30 --json`

Target checks for healthy SAM research flow:
- `events_window > 0`
- `events_with_keywords > 0`
- `same_keyword > 0 OR kw_pair > 0`
- `events_with_research_context > 0`

### 5) Repeatable SAM tuning loop (offline-friendly)

1. Re-run without ingest:
   - `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_starter.json`
2. Rebuild correlation lanes:
   - `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
   - `ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
   - `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10`
3. Refresh leads:
   - `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000 --scoring-version v2 --notes "sam context tuning pass"`

## Maintenance mode: USAspending
USAspending remains supported but is not the sprint primary.

Quick health check:
- `ss doctor status --source USAspending --days 30`

## Notes
- SAM workflow commands accept `--ingest-days` and `--days`.
- Correlation rebuild commands use `--window-days` (not `--days`).
- Smoke artifacts are stored under `data/exports/smoke/samgov/<timestamp>/`.
