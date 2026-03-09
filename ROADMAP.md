# ShadowScope Roadmap

_Last updated: 2026-03-09_

## Current sprint

### Theme
SAM-only Threshold Calibration + Operator Trust Hardening

### Scope
- Calibrate SAM smoke/doctor thresholds from bounded SAM runs.
- Enforce thresholds in `ss workflow samgov-smoke` with deterministic fixture tests.
- Improve SAM.gov operator-facing failure hints (what failed, why it matters, next command).
- Keep CI-facing checks offline/fixture-based.

### Explicit boundaries
- USAspending is maintenance mode for this sprint.
- No SAM<->USAspending linkage/join/correlation work.
- No keyword/term expansion for SAM or USAspending.

### Completed in this sprint
- [x] Captured bounded SAM smoke calibration bundles and extracted metric ranges.
- [x] Implemented calibrated SAM smoke threshold contract with default gates.
- [x] Added threshold-aware smoke output fields: expected threshold, observed value, pass/fail status, actionable hint.
- [x] Added repeatable threshold override entrypoint (`--threshold key=value`, repeatable).
- [x] Added fixture tests for threshold pass and deterministic fail behavior (context-depth + `same_sam_naics`).
- [x] Hardened SAM doctor hints with source-specific, command-ready guidance.
- [x] Kept USAspending unchanged except maintenance-safe health check paths.

### Calibration snapshot (bounded SAM runs)
Bundles:
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

### Calibrated default threshold contract
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

## Operator validation commands
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`
- `ss doctor status --source "SAM.gov" --days 30 --json`
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --threshold sam_naics_code_coverage_pct_min=65 --threshold same_sam_naics_lane_min=2 --json`
- `.\.venv\Scripts\python.exe -m pytest -q tests/test_workflow_wrapper.py tests/test_doctor_status_source_hints.py`

## Deferred (explicitly out of scope this sprint)
- SAM<->USAspending linkage/candidate join surfaces.
- USAspending ontology expansion and term-pack growth.
- Keyword/term expansion for SAM starter ontology (defer to dedicated precision/recall sprint).