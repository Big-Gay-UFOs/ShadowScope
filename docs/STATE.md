# ShadowScope State Snapshot (2026-03-08)

## What exists now
- SAM.gov pipeline is healthy enough for current sprint goals: ingest, ontology apply, entity linking, correlations, lead snapshot, and smoke-bundle validation all run through `ss workflow samgov` / `ss workflow samgov-smoke`.
- USAspending ingest and entity linking are healthy, but ontology quality was the remaining gap (very low keyword coverage and no keyword-correlation lanes on representative windows).
- Source-aware diagnostics are in place via `ss doctor status`, including SAM entity-coverage metrics and lane-level keyword diagnostics.
- Ontology assets are now:
  - `examples/ontology_sam_procurement_starter.json`
  - `examples/ontology_sam_kwpair_demo.json`
  - `examples/ontology_usaspending_starter.json` (new baseline for USAspending workflow runs)

## Recently landed
- First-class SAM workflow wrapper: `ss workflow samgov`
- Repeatable SAM smoke workflow with artifact bundle: `ss workflow samgov-smoke`
- USAspending starter ontology + fixture regression coverage for non-zero tagging/correlation usefulness
- SAM workflow ergonomics: `--days` alias support in `workflow samgov` and `workflow samgov-smoke`
- Schema-safe untagged-row diagnostic helper: `tools/diagnose_untagged_usaspending.sql`

## Current operational objective
- Keep SAM live repeatability stable with source-scoped non-zero smoke checks.
- Improve and iterate USAspending ontology quality using measured coverage/correlation outputs from representative windows.

## Current risks
- Missing `SAM_API_KEY` in runtime still blocks SAM live validation.
- Rate limiting remains possible even with retry/backoff safeguards.
- USAspending starter ontology is intentionally conservative and will still need domain tuning over time.

## Next verification path
- Run `ss workflow usaspending --ingest-days 30 --pages 2 --page-size 100 --ontology .\examples\ontology_usaspending_starter.json --window-days 30`.
- Inspect recent untagged USAspending rows with:
  - `psql -U postgres -d shadowscope -v window_days=30 -v row_limit=50 -f .\tools\diagnose_untagged_usaspending.sql`
- Run `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30` and archive `smoke_summary.json` + `doctor_status.json`.
