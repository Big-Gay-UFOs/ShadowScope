# ShadowScope State Snapshot (2026-03-09)

## Validated baseline
- SAM.gov is healthy end-to-end on the validated 30-day smoke slice.
  - `events_window=50`
  - `with_keywords=50`
  - `same_keyword=7`
  - `kw_pair=14`
  - entity coverage: 100%
- USAspending remains the active quality gap.
  - `events_window=200`
  - `with_keywords=29` (14.5%)
  - `same_keyword=5`
  - `kw_pair=0` at stricter defaults, `kw_pair=1` when keyword min-events is 2

## What landed in this tuning pass
- USAspending starter ontology tuned (v2 pass) with focused sustainment/service patterns for recurring untagged terms.
- USA workflow default keyword threshold calibrated to practical slices (`--min-events-keywords 2`).
- Fixture-based regression coverage expanded for:
  - non-zero USAspending keyword tagging
  - non-zero `same_keyword`
  - non-zero `kw_pair` under workflow defaults
  - over-tagging guardrail behavior
- `tools/diagnose_untagged_usaspending.sql` extended with recurring-term prevalence columns while staying JSON-safe (no JSONB assumptions).

## Operator validation path (USAspending)
1. Run bounded workflow:
   - `ss workflow usaspending --ingest-days 30 --pages 2 --page-size 100 --ontology .\examples\ontology_usaspending_starter.json --window-days 30`
2. Check source-scoped status:
   - `ss doctor status --source USAspending --days 30`
3. Inspect untagged prevalence/sample rows:
   - `psql -U postgres -d shadowscope -v window_days=30 -v row_limit=50 -f .\tools\diagnose_untagged_usaspending.sql`
4. Rebuild keyword lanes after ontology edits:
   - `ss correlate rebuild-keywords --window-days 30 --source USAspending --min-events 2 --max-events 200`
   - `ss correlate rebuild-keyword-pairs --window-days 30 --source USAspending --min-events 2 --max-events 200 --max-keywords-per-event 10`
5. Refresh ranked outputs:
   - `ss leads snapshot --source USAspending --min-score 1 --limit 200 --scan-limit 5000 --scoring-version v2 --notes "usa tuning pass"`

## Current risks
- Live SAM validation still depends on local `SAM_API_KEY` availability.
- USAspending tuning is still iterative; precision is prioritized over broad keyword inflation.
- Correlation lane utility depends on slice size/windowing and may require threshold adjustment per dataset volume.
