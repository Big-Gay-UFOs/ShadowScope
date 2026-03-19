# ShadowScope Runbook

This runbook captures the SAM-only operator flow for the current sprint.

## Sprint boundaries
- Active scope: SAM.gov threshold calibration and diagnostics trust hardening.
- USAspending: maintenance mode only (`doctor` health checks).
- Out of scope: SAM<->USAspending linkage and broad single-term expansion; in-scope is precision-first DoD companion expansion with starter behavior unchanged.

## Prereqs
- `docker compose up -d`
- Local env configured: `DATABASE_URL`
- Live SAM runs require local `SAM_API_KEY`
- CLI available: `ss --help`

## SAM ontology profiles (new)

SAM workflow commands now support `--ontology-profile`:
- `starter` (default): structural SAM starter ontology.
- `dod_foia`: existing DoD FOIA companion packs only.
- `starter_plus_dod_foia`: starter + DoD FOIA companion + operational noise suppressors.
- `hidden_program_proxy`: new default precision proxy-language companion only.
- `hidden_program_proxy_exploratory`: new lower-weight opt-in exploratory companion only.
- `starter_plus_dod_foia_hidden_program_proxy`: starter + existing DoD FOIA companion + new default precision proxy companion.
- `starter_plus_dod_foia_hidden_program_proxy_exploratory`: starter + existing DoD FOIA companion + default precision proxy companion + optional exploratory companion.
- `dod_foia` and the new proxy profiles keep explicit lore suppressors; the exploratory layer does not weaken them.

Examples:
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology-profile starter`
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology-profile dod_foia`
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology-profile starter_plus_dod_foia_hidden_program_proxy`
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology-profile starter_plus_dod_foia_hidden_program_proxy_exploratory`
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --ontology-profile starter_plus_dod_foia_hidden_program_proxy --json`

Keyword seed files live under `examples/terms/` and can be passed with `--keywords-file`; repeated `--keyword` values are merged and deduped.

`--ontology` still overrides profile mapping when you need an explicit file path.

## Historical replay windows

Use `--posted-from YYYY-MM-DD --posted-to YYYY-MM-DD` on `ss ingest samgov`, `ss workflow samgov`, `ss workflow samgov-smoke`, or `ss workflow samgov-validate` when you need a fixed historical slice instead of a rolling lookback.

- Do not combine `--days` with `--posted-from/--posted-to`.
- Example bounded historical smoke:
  `ss workflow samgov-smoke --posted-from 2024-01-01 --posted-to 2024-03-31 --pages 2 --limit 50 --window-days 90 --json`

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

Standard offline rebuild loops:
- Default precision hidden-program proxy:
  `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json`
  `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
  `ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
  `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10`
  `ss correlate rebuild-sam-usaspending-joins --window-days 30 --history-days 365 --min-score 45`
  `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000`
  `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --compare-scoring-versions v2,v3 --json`
  `ss doctor status --source "SAM.gov" --days 30 --json`
- Optional exploratory add-on:
  `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json`
  `ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
  `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10`
  `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000`

On a fixed window, look for directional improvement in useful keyword density and `kw_pair` signal without degrading pipeline health or existing suppressor behavior.

## Lead window filters

Lead queries, snapshots, and workflow-generated snapshots now accept additive lead-window filters:

- `--date-from` / `--date-to`: filter on event time using `occurred_at` with `created_at` fallback.
- `--occurred-after` / `--occurred-before`: filter only on `occurred_at`.
- `--created-after` / `--created-before`: filter only on `created_at`.
- `--since-days`: quick event-time lookback helper for source-specific snapshots.

Examples:
- `ss leads snapshot --source "SAM.gov" --occurred-after 2026-03-01T00:00:00+00:00 --min-score 1 --limit 200 --scan-limit 5000`
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --occurred-after 2026-03-01T00:00:00+00:00`

## 5) Fixture verification (offline)

- `.\.venv\Scripts\python.exe -m pytest -q tests/test_workflow_wrapper.py tests/test_doctor_status_source_hints.py`

## 6) Reviewer adjudication loop

Use this when you want scoring or ontology changes measured against human reviewer decisions instead of anecdotal bundle inspection.

1. Export a reviewer template from the snapshot you want to judge:
   - `ss export adjudication-template --snapshot-id 123 --out .\reviews\sam_snapshot_123_adjudications.csv`
2. Fill the CSV locally with:
   - `decision` (`keep`, `reject`, or `unclear`)
   - `reason_code`
   - `reviewer_notes`
   - `foia_ready` (`yes` or `no`)
   - optional `lead_family_override`
3. Compute objective ranking metrics and refresh the bundle report:
   - `ss leads adjudication-metrics --adjudications .\reviews\sam_snapshot_123_adjudications.csv --k 5 --k 10 --k 25 --bundle .\data\exports\smoke\samgov\20260309_112458 --json`

Outputs:

- `exports/lead_adjudications.csv`: normalized local copy used for bundle/report refresh
- `exports/lead_adjudication_metrics.json`: acceptance rate, precision@k, rejection reasons, by-family, and by-scoring-version metrics

Interpretation:

- Precision@k uses decisive reviewer labels only (`keep` / `reject`).
- `unclear` stays visible as uncertainty and does not get converted into either acceptance or rejection.
- `lead_family_override` lets reviewers correct family grouping without mutating the original snapshot evidence.

## 7) USAspending maintenance check

- `ss doctor status --source USAspending --days 30`

No USAspending feature/linkage expansion is part of this sprint.

## Relationship matrix rationale

DoD ontology keywords are emitted as `pack_id:rule_id` tags and flow directly into the existing correlation lanes:
- `same_keyword`: repeated DoD pack/rule hits across events.
- `kw_pair`: co-occurring DoD/context tags used for pair-strength support.
- Rationale: `same_keyword` rewards repeated precise handles; `kw_pair` rewards anchor+pair co-occurrence so relationship strength is tied to context, not lore terms.
- `same_entity`, `same_uei`, `same_sam_naics`: existing structural/entity lanes that stay unchanged.

Lead scoring now exposes FOIA triage metadata (`dod_lane_count`, `dod_keyword_hit_count`, `foia_matrix_bonus`, `foia_potential_tier`) so analysts can see lane diversity and pair-backed DoD context at a glance.

`ss leads snapshot` and `ss leads query` default to `v3` for the new FOIA-worthiness / proxy-quality scorer. Use `--scoring-version v2` only when you intentionally want an older comparison surface, and `--compare-scoring-versions v2,v3` when you want a side-by-side artifact inside SAM workflow bundles.

## SAM Larger-Run Validation Runbook (2026-03-09)

Use this sequence for SAM operator validation:

```powershell
# Fast wiring check
ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json

# Larger bounded validation pass
ss workflow samgov-validate --days 30 --pages 5 --limit 250 --window-days 30 --json

# Diagnose sparse/degraded vs healthy outcomes
ss diagnose samgov --days 30 --json

# Inspect exact bundle contract for automation/scripting
ss inspect bundle --path <bundle_dir> --json
```

Interpretation:

- `status=ok`: required checks passed.
- `status=warning`: required checks passed, but advisory checks missed threshold and artifacts are only partially useful/sparse/degraded.
- `status=failed`: required checks failed; treat as hard failure.
- Validation output separates failures into `pipeline_health`, `source_coverage_context_health`, and `lead_signal_quality`.
- Each check now serializes `name`, `observed`, `threshold`, `severity`, `required` vs `advisory`, and pass/fail.
- `ss diagnose samgov`, `ss inspect bundle`, and bundle-backed reports now read the manifest/gate status directly so larger-run warnings and failures are not flattened into legacy smoke-style PASS output.

Bundle contract (`samgov.bundle.v1`) is manifest-driven via `bundle_manifest.json` and stable `generated_files` entries.

Retry tuning for larger SAM windows:

```powershell
$env:SAM_API_TIMEOUT_SECONDS = "90"
$env:SAM_API_MAX_RETRIES = "12"
$env:SAM_API_BACKOFF_BASE = "1.25"
```
