# ShadowScope Runbook

This runbook captures the SAM-first operator flow for the current sprint.

## Sprint boundaries
- Active scope: SAM.gov smoke/report productization and operator trust hardening.
- USAspending: maintenance mode only (`doctor` health checks).
- Out of scope: SAM<->USAspending linkage and keyword/term expansion.

## Prereqs
- `docker compose up -d`
- Local env configured: `DATABASE_URL`
- Live SAM runs require local `SAM_API_KEY`
- CLI available: `ss --help`

## 1) Canonical SAM smoke run

- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30`

Expected operator output includes:
- `PASS` / `FAIL`
- `Bundle dir`
- `Smoke summary`
- `Doctor status`
- `Report HTML`

## 2) Canonical review surface

Open the generated report:
- `ss report latest --source "SAM.gov"`
- or `ss report samgov --bundle data\exports\smoke\samgov\<timestamp>`

Bundle layout (current contract):
- `smoke_summary.json`
- `doctor_status.json`
- `workflow_result.json`
- `report.html`
- `exports/` (lead snapshot, correlations, entities, event->entity, events when enabled)

## 3) Report interpretation (what success looks like)

A healthy run should show:
- Header status `PASS`
- Non-zero ingest summary (`fetched`/`inserted`/`normalized`)
- Doctor summary with non-zero event/keyword/lane coverage
- Top keyword table populated
- Correlation lane table populated (including `same_keyword` or `kw_pair`)
- Top lead rows present
- Artifact links resolving inside the bundle

## 4) Diagnostics drill-down (only when needed)

- `ss doctor status --source "SAM.gov" --days 30 --json`

Review these fields first:
- `events_window`
- `events_with_keywords`
- `same_keyword`, `kw_pair`, `same_sam_naics`
- `events_with_research_context`
- `events_with_core_procurement_context`
- `avg_context_fields_per_event`
- `coverage_by_field_pct.sam_notice_type`
- `coverage_by_field_pct.sam_solicitation_number`
- `coverage_by_field_pct.sam_naics_code`

## 5) Threshold tuning loop

Use repeatable threshold overrides when you want stricter local gates:
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --threshold sam_naics_code_coverage_pct_min=65 --threshold same_sam_naics_lane_min=2 --json`

If smoke fails, run the suggested next command from the failing check.

Standard offline rebuild loop:
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_starter.json`
- `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200`

## 6) Fixture verification (offline)

- `.\.venv\Scripts\pytest.exe -q tests\test_reporting.py tests\test_report_cli.py tests\test_workflow_wrapper.py tests\test_workflow_cli_flags.py`

## 7) USAspending maintenance check

- `ss doctor status --source USAspending --days 30`

No USAspending feature/linkage expansion is part of this sprint.
