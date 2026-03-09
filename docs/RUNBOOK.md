# ShadowScope Runbook

This runbook captures the SAM-only operator flow for the current sprint.

## Sprint boundaries
- Active scope: SAM.gov threshold calibration and diagnostics trust hardening.
- USAspending: maintenance mode only (`doctor` health checks).
- Out of scope: SAM<->USAspending linkage and keyword/term expansion.

## Prereqs
- `docker compose up -d`
- Local env configured: `DATABASE_URL`
- Live SAM runs require local `SAM_API_KEY`
- CLI available: `ss --help`

## 1) Bounded SAM smoke run

- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

Artifacts are written under:
- `data/exports/smoke/samgov/<timestamp>/`

## 2) Diagnostics review (SAM.gov)

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

## 3) Calibrated threshold defaults

`samgov-smoke` now enforces:
- `events_window >= 3`
- `events_with_keywords_coverage_pct >= 60%`
- `events_with_entity_coverage_pct >= 60%`
- `keyword_signal_total(same_keyword + kw_pair) >= 3`
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

Each check includes `expected`, `observed`, pass/fail status, and a next command hint.

## 4) Threshold tuning loop

Use repeatable threshold overrides when you want stricter local gates:

- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --threshold sam_naics_code_coverage_pct_min=65 --threshold same_sam_naics_lane_min=2 --json`

If smoke fails, run the suggested next command from the failing check.

Standard offline rebuild loop:
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_starter.json`
- `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200`

## 5) Fixture verification (offline)

- `.\.venv\Scripts\python.exe -m pytest -q tests/test_workflow_wrapper.py tests/test_doctor_status_source_hints.py`

## 6) USAspending maintenance check

- `ss doctor status --source USAspending --days 30`

No USAspending feature/linkage expansion is part of this sprint.