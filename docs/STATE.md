# ShadowScope State Snapshot (2026-03-09)

## Validated SAM baseline
- SAM.gov bounded smoke slice is healthy and repeatable (`--days 30 --pages 2 --limit 50`).
  - `events_window=50`
  - `with_keywords=50`
  - `same_keyword=7`
  - `kw_pair=14`
  - entity coverage: 100%
- SAM smoke/doctor now additionally surface context-depth metrics so operators can confirm research pivot readiness.

## What landed in this SAM-first pass
- Added a focused SAM context contract in normalization (`sam_*` keys persisted in `events.raw_json`):
  - agency hierarchy (`sam_agency_path_name`, `sam_agency_path_code`)
  - notice/solicitation metadata (`sam_notice_type`, `sam_solicitation_number`, `sam_classification_code`)
  - procurement classification (`sam_naics_code`, `sam_naics_description`, `sam_set_aside_code`, `sam_set_aside_description`)
  - key dates (`sam_posted_date`, `sam_response_deadline`, `sam_archive_date`)
- Added SAM correlation utility lane: `same_sam_naics`.
- Expanded `doctor status` with SAM context diagnostics:
  - `events_with_research_context`
  - `events_with_core_procurement_context`
  - `coverage_by_field_pct`
  - top notice types / NAICS / set-aside codes
- Expanded `samgov-smoke` checks/baseline with SAM research context non-zero validation.
- Tuned `examples/ontology_sam_procurement_starter.json` for high precision and lower noise.
- Added fixture tests for:
  - SAM context extraction + persistence
  - non-zero SAM NAICS correlation utility
  - SAM context diagnostics payload
  - SAM ontology over-tagging guardrails

## Operator validation path (SAM-first)
1. Run bounded SAM workflow:
   - `ss workflow samgov --days 30 --pages 2 --limit 50 --ontology .\examples\ontology_sam_procurement_starter.json --window-days 30`
2. Validate smoke bundle:
   - `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`
3. Review diagnostics:
   - `ss doctor status --source "SAM.gov" --days 30 --json`
4. Repeatable SAM tuning loop:
   - `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_starter.json`
   - `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
   - `ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
   - `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10`
   - `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000 --scoring-version v2 --notes "sam context tuning pass"`

## Maintenance mode note
- USAspending is intentionally maintenance-mode in this sprint; SAM.gov context hardening is the active focus.

## Current risks
- Live SAM validation still depends on local `SAM_API_KEY` availability.
- Rate-limit behavior can still vary by time-of-day/usage despite retry/backoff handling.
- `same_sam_naics` lane density depends on window volume and may require threshold tuning as live baselines accumulate.
