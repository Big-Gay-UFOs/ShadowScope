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
- SAM.gov smoke/report productization and operator diagnostics hardening only.
- USAspending is maintenance mode.
- No SAM<->USAspending linkage in this sprint.
- No keyword/term expansion in this sprint.

## Fast operator path (SAM-only)

### 1) Load env
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\examples\powershell\set-shadow-env.ps1`

### 2) Run canonical SAM smoke
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30`

Expected CLI lines:
- `SAM.gov smoke workflow: PASS|FAIL`
- `Bundle dir: ...`
- `Report HTML: ...`

### 3) Open canonical review report
- `ss report latest --source "SAM.gov"`
- or `ss report samgov --bundle data\exports\smoke\samgov\<timestamp>`

### 4) Understand smoke bundle contents
- `smoke_summary.json`
- `doctor_status.json`
- `workflow_result.json`
- `report.html`
- `exports/` artifacts (lead snapshot, correlations, entities, event->entity, events)

### 5) Understand default threshold gates
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

### 6) Threshold tuning loop
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --threshold sam_naics_code_coverage_pct_min=65 --threshold same_sam_naics_lane_min=2 --json`
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_starter.json`
- `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`

### 7) Fixture verification (offline)
- `.\.venv\Scripts\pytest.exe -q tests\test_reporting.py tests\test_report_cli.py tests\test_workflow_wrapper.py tests\test_workflow_cli_flags.py`

## USAspending maintenance check
- `ss doctor status --source USAspending --days 30`
