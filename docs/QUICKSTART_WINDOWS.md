# ShadowScope Windows Quick Start

## Prerequisites
- Windows PowerShell 5+ or PowerShell 7
- PostgreSQL reachable from `DATABASE_URL`
- Optional for live SAM ingest: `SAM_API_KEY`

## One-time bootstrap

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

## Current sprint scope (important)
- SAM.gov threshold calibration and operator diagnostics hardening only.
- USAspending is maintenance mode.
- No SAM<->USAspending linkage in this sprint.
- No keyword/term expansion in this sprint.

## Fast operator path (SAM-only)

### 1) Load env
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\examples\powershell\set-shadow-env.ps1`

### 2) Bounded SAM smoke
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

### 3) Review diagnostics
- `ss doctor status --source "SAM.gov" --days 30 --json`

### 4) Understand default threshold gates
`samgov-smoke` enforces:
- `events_window >= 3`
- `events_with_keywords_coverage_pct >= 60%`
- `events_with_entity_coverage_pct >= 60%`
- `keyword_signal_total >= 3`
- `events_with_research_context >= 2`
- `research_context_coverage_pct >= 60%`
- `events_with_core_procurement_context >= 2`
- `core_procurement_context_coverage_pct >= 60%`
- `avg_context_fields_per_event >= 2.5`
- `sam_notice_type_coverage_pct >= 70%`
- `sam_solicitation_number_coverage_pct >= 70%`
- `sam_naics_code_coverage_pct >= 60%`
- `same_sam_naics >= 1`
- `snapshot_items >= 1`

Each check prints expected threshold, observed value, pass/fail, and next command.

### 5) Threshold tuning loop
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --threshold sam_naics_code_coverage_pct_min=65 --threshold same_sam_naics_lane_min=2 --json`
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_starter.json`
- `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`

### 6) Fixture verification (offline)
- `.\.venv\Scripts\python.exe -m pytest -q tests/test_workflow_wrapper.py tests/test_doctor_status_source_hints.py`

## USAspending maintenance check
- `ss doctor status --source USAspending --days 30`
## SAM Smoke vs Larger Validation (Windows)

```powershell
# 1) Small smoke
ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json

# 2) Larger bounded validation
ss workflow samgov-validate --days 30 --pages 5 --limit 250 --window-days 30 --json

# 3) Diagnose and inspect without psql
ss diagnose samgov --days 30 --json
ss inspect bundle --path <bundle_dir> --json
```

Key SAM bundle files:

- `bundle_manifest.json`
- `results/workflow_summary.json`
- `results/workflow_result.json`
- `results/doctor_status.json`
- `report/bundle_report.html`
- stable `exports/*.csv|json|jsonl`

If larger runs are slow/rate-limited, tune:

- `SAM_API_TIMEOUT_SECONDS`
- `SAM_API_MAX_RETRIES`
- `SAM_API_BACKOFF_BASE`
