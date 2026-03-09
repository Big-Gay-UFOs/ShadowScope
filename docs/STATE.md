# ShadowScope State Snapshot (2026-03-09)

## Sprint state

Current sprint theme: **SAM Workflow Report Productization + Operator Trust Hardening**.

### Scope boundaries
- SAM.gov is the active source for workflow validation and review hardening.
- USAspending remains maintenance mode this sprint.
- Cross-source SAM<->USAspending linkage work is out of scope.
- Keyword/term expansion for either source is deferred.

## Bounded SAM calibration evidence

Command used:
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

Bundles used:
- `data/exports/smoke/samgov/20260309_112458`
- `data/exports/smoke/samgov/20260309_112520`
- `data/exports/smoke/samgov/20260309_115814`

Observed ranges:
- `events_window`: `50..53`
- `events_with_keywords`: `50..53`
- `same_keyword`: `9..9`
- `kw_pair`: `30..30`
- `same_sam_naics`: `6..7`
- `events_with_research_context`: `50..53`
- `events_with_core_procurement_context`: `50..53`
- `avg_context_fields_per_event`: `5.22..5.23`
- `coverage_by_field_pct.sam_notice_type`: `100.0..100.0`
- `coverage_by_field_pct.sam_solicitation_number`: `100.0..100.0`
- `coverage_by_field_pct.sam_naics_code`: `90.6..92.0`

## Calibrated SAM smoke threshold contract (defaults)
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

## Canonical review bundle contract
- `smoke_summary.json`
- `doctor_status.json`
- `workflow_result.json`
- `report.html`
- `exports/` artifacts (lead snapshot, correlations, entities, event->entity, events)

`report.html` is the canonical SAM run review surface for this sprint.

## Operator run loop
1. Run smoke:
   - `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30`
2. Review report:
   - `ss report latest --source "SAM.gov"`
   - Open `<bundle_dir>\report.html` and validate PASS/FAIL + summary tables.
3. Drill down diagnostics only when needed:
   - `ss doctor status --source "SAM.gov" --days 30 --json`
4. Tune thresholds when needed:
   - `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --threshold sam_naics_code_coverage_pct_min=65 --threshold same_sam_naics_lane_min=2 --json`
5. Rebuild from local data (offline loop):
   - `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_starter.json`
   - `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
6. Fixture verification:
   - `.\.venv\Scripts\pytest.exe -q tests\test_reporting.py tests\test_report_cli.py tests\test_workflow_wrapper.py tests\test_workflow_cli_flags.py`
