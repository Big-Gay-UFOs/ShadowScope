# ShadowScope Runbook

This runbook defines the SAM-first operator flow for the current sprint.

## Prereqs
- Docker services running (Postgres/API as needed):
  - `docker compose up -d`
- Local env configured:
  - `DATABASE_URL`
  - `SAM_API_KEY` (for live SAM ingest)
- CLI available:
  - `ss --help`

## SAM-first bounded workflow

### 1) Bounded SAM workflow
Use a controlled operator slice to keep runs reproducible.

- `ss workflow samgov --days 30 --pages 2 --limit 50 --ontology .\examples\ontology_sam_procurement_starter.json --window-days 30`

What this runs:
- ingest -> ontology -> entities -> correlations (`same_entity`, `same_uei`, `same_keyword`, `kw_pair`, `same_sam_naics`) -> lead snapshot -> exports

### 2) Smoke bundle validation
Run smoke checks and save artifacts for auditability.

- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30`
- JSON output variant:
  - `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

Expected required checks include:
- `events_window_nonzero`
- `events_with_keywords_nonzero`
- `keyword_or_kw_pair_lane_nonzero`
- `sam_research_context_nonzero`
- `snapshot_items_nonzero`

### 3) Diagnostics review
Review source-scoped status and context depth.

- `ss doctor status --source "SAM.gov" --days 30`
- JSON payload for full detail:
  - `ss doctor status --source "SAM.gov" --days 30 --json`

Focus fields:
- keyword coverage and lane counts
- entity coverage diagnostics
- SAM context diagnostics (`events_with_research_context`, `coverage_by_field_pct`, top notice/NAICS/set-aside)

### 4) Repeatable SAM tuning loop
After ontology/context edits, re-run offline from existing data first.

1. Re-run workflow without ingest:
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_starter.json`

2. Rebuild SAM correlations explicitly (including new NAICS lane):
- `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10`

3. Refresh ranked outputs:
- `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000 --scoring-version v2 --notes "sam context tuning pass"`

4. Export review artifacts:
- `ss export lead-snapshot --snapshot-id <ID> --out .\data\exports\`
- `ss export kw-pairs --min-event-count 2 --limit 200 --out .\data\exports\`
- `ss export entities --out .\data\exports\`

## Context field contract (SAM normalization)
The SAM ingest normalization persists canonical context fields in `events.raw_json`:
- agency path: `sam_agency_path_name`, `sam_agency_path_code`
- notice metadata: `sam_notice_type`, `sam_notice_type_code`, `sam_solicitation_number`, `sam_classification_code`
- procurement classification: `sam_naics_code`, `sam_naics_description`, `sam_set_aside_code`, `sam_set_aside_description`
- key dates: `sam_posted_date`, `sam_response_deadline`, `sam_archive_date`
- region pivots: `sam_place_state_code`, `sam_place_country_code`

## Maintenance mode: USAspending
USAspending remains available but is not the primary sprint focus.

Quick maintenance health check:
- `ss doctor status --source USAspending --days 30`

## Troubleshooting
- Missing SAM key: ingest/workflow can skip when `SAM_API_KEY` is unset.
- Low context depth in doctor output:
  - inspect `sam_*` fields in `events.raw_json`
  - rerun bounded ingest and workflow
- Empty NAICS lane:
  - confirm `sam_naics_code` extraction coverage in doctor JSON
  - lower `--min-events` only after reviewing noise impact
