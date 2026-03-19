# ShadowScope


## Command Matrix (LLM-First)

Use this section as the default operator/agent playbook.

### 1) Environment + health

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\examples\powershell\set-shadow-env.ps1
ss doctor status
```

### 2) Fast SAM smoke (default starter profile)

```powershell
ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --ontology-profile starter --json
```

### 3) DoD FOIA triage mode (recommended for relationship matrix work)

```powershell
ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --ontology-profile starter_plus_dod_foia --json
```

### 4) Ontology profile map

- `starter` -> `examples/ontology_sam_procurement_starter.json`
- `dod_foia` -> `examples/ontology_sam_dod_foia_companion.json`
- `starter_plus_dod_foia` -> `examples/ontology_sam_procurement_plus_dod_foia.json`
- `hidden_program_proxy` -> `examples/ontology_sam_hidden_program_proxy_companion.json`
- `hidden_program_proxy_exploratory` -> `examples/ontology_sam_hidden_program_proxy_exploratory.json`
- `starter_plus_dod_foia_hidden_program_proxy` -> `examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json`
- `starter_plus_dod_foia_hidden_program_proxy_exploratory` -> `examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json`
- `--ontology <path>` always overrides `--ontology-profile`
- `dod_foia` keeps the existing precision-first DoD FOIA companion with explicit UAP-lore suppressors in `operational_noise_terms`.
- `hidden_program_proxy` is the new conservative public-records proxy companion for SAM.gov support-footprint triage.
- `hidden_program_proxy_exploratory` is lower-weight and opt-in only; it is not mixed into the default precision companion.

### 5) Seeded SAM keyword files

- `examples/terms/sam_hidden_program_proxy_core_seeds.txt`
- `examples/terms/sam_hidden_program_proxy_expansion_seeds.txt`
- `examples/terms/sam_hidden_program_proxy_exploratory_seeds.txt`
- Use `--keywords-file <path>` with `ss ingest samgov`, `ss workflow samgov`, `ss workflow samgov-smoke`, or `ss workflow samgov-validate` for newline-delimited seed terms.
- Repeated `--keyword` values are merged with file terms, comments beginning with `#` are ignored, and duplicates are removed while preserving order.

### 6) Fixed historical SAM windows

- Use `--posted-from YYYY-MM-DD --posted-to YYYY-MM-DD` on `ss ingest samgov`, `ss workflow samgov`, `ss workflow samgov-smoke`, or `ss workflow samgov-validate` when you need a reproducible historical replay.
- Use either `--days` or `--posted-from/--posted-to`; do not mix them.
- Example: `ss workflow samgov-smoke --posted-from 2024-01-01 --posted-to 2024-03-31 --pages 2 --limit 50 --window-days 90 --json`

### 7) Offline rebuild loops (after ontology edits)

Default precision hidden-program proxy loop:

```powershell
ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json
ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200
ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200
ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10
ss correlate rebuild-sam-usaspending-joins --window-days 30 --history-days 365 --min-score 45
ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000
ss doctor status --source "SAM.gov" --days 30 --json
```

Optional exploratory hidden-program proxy loop:

```powershell
ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology .\examples\ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json
ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200
ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10
ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000
```

On a fixed window, treat improvement as directional: we want denser useful keyword and `kw_pair` signal without degrading pipeline health or weakening the existing suppressors.

### 8) Verification commands

```powershell
.\.venv\Scripts\pytest.exe -q tests/test_example_ontologies.py tests/test_workflow_cli_flags.py tests/test_samgov_ontology_tuning.py tests/test_leads_foia_matrix.py backend/tests/test_tagger.py
.\.venv\Scripts\pytest.exe -q tests/test_workflow_wrapper.py
```

### 9) Reviewer adjudication loop

- Export a reviewer-editable CSV from a lead snapshot:
  `ss export adjudication-template --snapshot-id 123 --out .\reviews\sam_snapshot_123_adjudications.csv`
- Reviewer fills `decision`, `reason_code`, `reviewer_notes`, `foia_ready`, and optional `lead_family_override`.
- Evaluate ranking quality and refresh a bundle-backed report:
  `ss leads adjudication-metrics --adjudications .\reviews\sam_snapshot_123_adjudications.csv --k 5 --k 10 --k 25 --bundle .\data\exports\smoke\samgov\20260309_112458 --json`
- Bundle artifacts are kept local under `exports/lead_adjudications.csv` and `exports/lead_adjudication_metrics.json`.
- Precision@k uses decisive reviewer labels (`keep` / `reject`) only; `unclear` remains visible without being upgraded into a forced verdict.

## Quickstart

<!-- SHADOWSCOPE:OVERVIEW:START -->

## What this repo is

**ShadowScope** is a small data pipeline + API that turns public U.S. federal procurement/spending feeds into a
single, queryable dataset of **events** (things that happened) and **entities** (who they happened to).
The goal is to help you spot patterns - recurring agencies, vendors, UEIs/DUNS, and keyword themes - and produce
reviewable "leads" without manually bouncing between multiple government sites.

## What it's for

- Ingest public datasets (e.g., solicitations and awards) into a database
- Keep a **raw snapshot** trail for debugging/reproducibility
- Normalize records into "events" you can filter/search/export
- Optionally enrich events by:
  - linking organizations/entities (e.g., via UEI/DUNS-style identifiers when present)
  - tagging events using an ontology/keyword list
  - computing deterministic correlation lanes and candidate joins (e.g., shared keywords, same entity/identifier, SAM<->USAspending incumbent candidates)

You can drive the workflow via the `ss` CLI and/or the backend API.

## Key concepts

- **Source**: A feed you ingest from (e.g., SAM.gov or USAspending).
- **Raw snapshot**: The original JSON responses saved under `data/raw/<source>/<YYYYMMDD>/...` (useful for audits/debugging).
- **Event**: A normalized record in the DB (an opportunity, an award, etc.).
- **Entity**: A real-world org/vendor/agency that events can link to.
- **Correlation lane**: A lightweight way of saying "these things are related" (shared keywords, same UEI, etc.).
- **Lead snapshot**: A ranked/reviewable output built from events + correlations.

## Hypothetical example

Imagine you're doing business development for a small IT services firm and you care about "zero trust" work.

Every morning you want to answer:

- "Did any new solicitations drop that match our focus areas?"
- "Has this agency funded similar work recently?"
- "Is the likely incumbent vendor identifiable from prior awards?"

With ShadowScope you could:

1. Ingest recent SAM.gov opportunities (solicitations) and recent USAspending awards.
2. Apply your ontology so events get tagged with terms like `zero trust`, `MFA`, `SIEM`, `cloud migration`.
3. Build correlations so a new solicitation automatically connects to:
   - recent awards from the same agency,
   - the same vendor/entity (when identifiers are available),
   - similar notices that share key phrases.
4. Export a lead snapshot to share internally (CSV/JSON) with a clear "why this looks relevant" trail
   (shared keywords/entities/correlations).

Instead of manual searching across multiple sites, you get a reproducible dataset and explainable links.

## Data sources

ShadowScope currently focuses on public U.S. government datasets such as:

- **SAM.gov Contract Opportunities** (solicitations/opportunities)
- **USAspending** (awards/spending)

(Connectors live under `backend/connectors/`.)

## Quickstart (local)

> Commands can evolve - use `ss --help` and `ss doctor status` for "what to run next" in your current version.

1. Configure your database connection:
   - `DATABASE_URL` in your environment or a local `.env` (gitignored)

2. Verify the system can talk to your DB:
   - `ss doctor status`

3. Run a small ingest:
   - `ss ingest usaspending --pages 1 --limit 25`
   - `ss ingest samgov --days 7 --pages 1 --limit 25` (requires `SAM_API_KEY`)
   - Fixed historical replay: `ss ingest samgov --posted-from 2024-01-01 --posted-to 2024-03-31 --pages 1 --limit 25`

4. If you're using ontology/correlation features, follow the hints from:
   - `ss doctor status --source "SAM.gov" --days 7`

### Configuration notes

Common environment variables:

- `DATABASE_URL` - database connection string
- `SAM_API_KEY` - SAM.gov public API key (required for SAM.gov ingestion)
- `SAM_API_BASE_URL` - optional SAM.gov endpoint override
  If blank/whitespace, ShadowScope falls back to the default `/prod` URL.

PowerShell reminder:

- `SAM_API_KEY` is session-scoped if you set it via `$env:SAM_API_KEY = "..."`.
  If you open a new terminal, you'll need to set it again (or use a local `.env`).

<!-- SHADOWSCOPE:OVERVIEW:END -->

<!-- SHADOWSCOPE:SPRINT:START -->

## Sprint roadmap

_Last updated: 2026-03-09_

### Sprint theme

SAM-only Threshold Calibration + Operator Trust Hardening

### Scope boundaries (this sprint)

- In scope: calibrated SAM smoke threshold gates, fixture-based pass/fail coverage, and clearer SAM operator diagnostics.
- In scope: deterministic/offline CI validation.
- USAspending ingest remains lightweight, but deterministic SAM<->USAspending candidate joins are now supported for investigator review.
- Cross-source joins are stored as candidate correlations with evidence and scores, not asserted identity merges.
- Out of scope: broad/single-term expansion; only precision-first DoD companion expansion is in scope, with starter/default behavior unchanged.

### Calibration evidence (bounded SAM smoke)

Runs used for calibration (bounded operator command):

- `data/exports/smoke/samgov/20260309_112458`
- `data/exports/smoke/samgov/20260309_112520`
- `data/exports/smoke/samgov/20260309_115814`

Observed ranges from those bundles:

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

### Calibrated SAM smoke threshold contract (defaults)

- `events_window >= 3`
- `events_with_keywords_coverage_pct >= 60%`
- `events_with_entity_coverage_pct >= 60%`
- `keyword_signal_total(same_keyword + kw_pair) >= 3`
- `events_with_research_context >= 2`
- `research_context_coverage_pct >= 60%`
- `events_with_core_procurement_context >= 2`
- `core_procurement_context_coverage_pct >= 60%`
- `avg_context_fields_per_event >= 2.5`
- `coverage_by_field_pct.sam_notice_type >= 70%`
- `coverage_by_field_pct.sam_solicitation_number >= 70%`
- `coverage_by_field_pct.sam_naics_code >= 60%`
- `same_sam_naics >= 1`
- `snapshot_items >= 1`

### Operator commands

Bounded SAM smoke run:
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

Diagnostics review:
- `ss doctor status --source "SAM.gov" --days 30 --json`

Threshold tuning loop (example override):
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --threshold sam_naics_code_coverage_pct_min=65 --threshold same_sam_naics_lane_min=2 --json`
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology-profile starter_plus_dod_foia_hidden_program_proxy`
- `ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology-profile starter_plus_dod_foia_hidden_program_proxy_exploratory`
- `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`

Fixture test verification:
- `.\.venv\Scripts\python.exe -m pytest -q tests/test_workflow_wrapper.py tests/test_doctor_status_source_hints.py`

<!-- SHADOWSCOPE:SPRINT:END -->

<!-- SHADOWSCOPE:DEMO:START -->

## PowerShell demo walkthrough

This repo includes the following SAM.gov ontology options:

- **Starter (default):** `examples/ontology_sam_procurement_starter.json` (structural baseline)
- **DoD FOIA companion:** `examples/ontology_sam_dod_foia_companion.json` (existing DoD mission-intent packs + operational noise suppressors)
- **Starter + DoD FOIA:** `examples/ontology_sam_procurement_plus_dod_foia.json` (combined practical profile)
- **Hidden-program proxy companion:** `examples/ontology_sam_hidden_program_proxy_companion.json` (new default precision proxy-language companion)
- **Hidden-program proxy exploratory companion:** `examples/ontology_sam_hidden_program_proxy_exploratory.json` (new lower-weight opt-in expansion)
- **Starter + DoD FOIA + hidden-program proxy:** `examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy.json` (recommended precision proxy workflow)
- **Starter + DoD FOIA + hidden-program proxy exploratory:** `examples/ontology_sam_procurement_plus_dod_foia_hidden_program_proxy_exploratory.json` (opt-in exploratory workflow)
- **Demo (non-production):** `examples/ontology_sam_kwpair_demo.json` (broad smoke signal only)

For PowerShell session setup, use:

- `powershell -NoProfile -ExecutionPolicy Bypass -File .\\examples\\powershell\\set-shadow-env.ps1` (works even when script execution is restricted).
- If you already opened PowerShell, run `Set-ExecutionPolicy -Scope Process Bypass -Force` and then `./examples/powershell/set-shadow-env.ps1`.

```powershell
# 1) Load local env defaults and ensure SAM_API_KEY is set for this PowerShell session
.\examples\powershell\set-shadow-env.ps1

# 2) Ingest a bounded SAM.gov slice
ss ingest samgov --days 30 --pages 2 --limit 50

# 3) Apply the conservative starter ontology (recommended)
ss ontology apply --path .\examples\ontology_sam_procurement_starter.json --days 30 --source "SAM.gov"

# 4) Build correlations
ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2
ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10
ss correlate rebuild-sam-usaspending-joins --window-days 30 --history-days 365 --min-score 45

# 5) Generate a lead snapshot
ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200

# 6) Inspect pipeline health and source-specific hints
ss doctor status --source "SAM.gov" --days 30
```

### One-command SAM workflows

If you want a single operator command instead of manual sequencing:

```powershell
# Full SAM workflow (ingest -> ontology -> entities -> correlations -> snapshot -> exports)
ss workflow samgov --days 30 --pages 2 --limit 50 --ontology-profile starter --window-days 30

# Recommended precision hidden-program proxy workflow
ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology-profile starter_plus_dod_foia_hidden_program_proxy

# Optional exploratory add-on workflow
ss workflow samgov --skip-ingest --days 30 --window-days 30 --ontology-profile starter_plus_dod_foia_hidden_program_proxy_exploratory

# Seeded search terms from a newline-delimited file
ss workflow samgov --days 30 --pages 2 --limit 50 --keywords-file .\examples\terms\sam_hidden_program_proxy_core_seeds.txt --ontology-profile hidden_program_proxy --window-days 30

# Smoke workflow (same chain + doctor checks + artifact bundle)
ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --ontology-profile starter_plus_dod_foia_hidden_program_proxy --json
```
### Optional smoke-test mode

If you want stronger signal just to validate end-to-end wiring, swap step 3 to the broader demo ontology:

```powershell
ss ontology apply --path .\examples\ontology_sam_kwpair_demo.json --days 30 --source "SAM.gov"
```

### What success looks like

A healthy SAM.gov run should show success in the SAM.gov-specific outputs, not just global totals.

Use these checks:

- `ss ingest samgov ...` reports non-zero fetched/inserted or normalized rows
- `ss ontology apply --source "SAM.gov" ...` reports `updated > 0` or `unchanged > 0`
- `ss correlate rebuild-keywords --source "SAM.gov" ...` reports `correlations_created > 0` or `correlations_updated > 0`
- `ss correlate rebuild-keyword-pairs --source "SAM.gov" ...` reports `correlations_created > 0` or `correlations_updated > 0`
- `ss leads snapshot --source "SAM.gov" ...` reports `items > 0`

Then confirm in `ss doctor status --source "SAM.gov" --days 30` that you see:

- `events_window > 0`
- `with_keywords > 0`
- `same_keyword > 0`
- `kw_pair > 0`
- `Last lead snapshot: ... source=SAM.gov`

Do not use `lead_snapshots_total > 0` as a SAM.gov-specific success check. That count is global across sources.

### Notes

- Starter ontology remains the default recommendation for general SAM workflow health checks.
- `starter_plus_dod_foia_hidden_program_proxy` is the recommended precision-first profile when you want the existing DoD FOIA companion plus the new public-records proxy-language packs.
- `starter_plus_dod_foia_hidden_program_proxy_exploratory` is opt-in and lower-weight; use it only when you explicitly want broader exploratory context.
- Seed term files live under `examples/terms/` and can be supplied via `--keywords-file`.
- Demo ontology is intentionally broad for quick smoke tests.
- `SAM_API_KEY` is session-scoped in PowerShell unless you also persist it in local `.env`.

<!-- SHADOWSCOPE:DEMO:END -->


Typical workflow (SAM.gov tuning loop):

1) Run bounded SAM workflow
- `ss workflow samgov --days 30 --pages 2 --limit 50 --ontology-profile starter --window-days 30`

2) Validate smoke bundle + non-zero checks
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

3) Review SAM context/signal diagnostics
- `ss doctor status --source "SAM.gov" --days 30 --json`
- Target checks: `events_window > 0`, `events_with_keywords > 0`, `same_keyword > 0 OR kw_pair > 0`, `events_with_research_context > 0`

4) Repeatable SAM tuning loop after ontology/context edits
- `ss workflow samgov --skip-ingest --ontology-profile starter_plus_dod_foia_hidden_program_proxy --window-days 30 --days 30`
- `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10`
- `ss correlate rebuild-sam-usaspending-joins --window-days 30 --history-days 365 --min-score 45`
- `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000 --notes "sam context tuning pass"`
- Optional scoring-surface comparison: `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --compare-scoring-versions v2,v3 --json`
- Optional exploratory add-on: `ss workflow samgov --skip-ingest --ontology-profile starter_plus_dod_foia_hidden_program_proxy_exploratory --window-days 30 --days 30`

5) Optional maintenance-mode USAspending check
- `ss doctor status --source USAspending --days 30`

More detail: see `docs/RUNBOOK.md`.



## Status

- SAM.gov baseline is healthy and repeatable; use `ss workflow samgov` / `ss workflow samgov-smoke` for ongoing smoke checks.
- USAspending health checks remain lightweight; use `ss doctor status --source USAspending --days 30` for feed sanity and `ss correlate rebuild-sam-usaspending-joins --window-days 30 --history-days 365 --min-score 45` for cross-source candidate joins.
- Roadmap/checklist: see `ROADMAP.md` (authoritative tracker).

### Notes

- PowerShell: do not paste placeholders like `<ID>`; use numeric values directly.
- Correlations: use `--window-days` for rebuild commands (not `--days`).
- SAM workflows accept both `--ingest-days` and `--days` (alias).
- SAM ingest/workflow commands also accept `--posted-from YYYY-MM-DD --posted-to YYYY-MM-DD` for fixed posted-date windows; do not combine them with `--days`.
- Raw ingest snapshots:
  - USAspending: `data/raw/usaspending/YYYYMMDD/page_*.json`
  - SAM.gov: `data/raw/sam/YYYYMMDD/page_*.json`

ShadowScope is a batch investigative OSINT pipeline for surfacing support footprints inside public procurement data.

It is designed for repeatable investigator runs:

1) ingest a time window (seeded searches)
2) normalize + persist to Postgres (idempotent)
3) tag with ontology signals (keywords + clause hits)
4) score/rank leads (v3 scoring by default for SAM/operator review)
5) snapshot leads (repeatability + deltas)
6) cluster related records (entity / UEI / keyword / keyword-pair)
## Runbook
One command:
- `tools/runbook.ps1`

Windows execution policy (one-time):
- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\runbook.ps1`

## Investigator query surfaces

API:
- `/api/events` now supports investigator filters like `source`, `date_from`, `date_to`, `entity_id`, `keyword`, `agency`, `psc`, `naics`, `award_id`, `recipient_uei`, `place_region`, plus `sort_by` / `sort_dir`.
- `/api/leads` adds the same event-side filters plus `lane`, `min_event_count`, `min_score_signal`, `lead_family`, optional `group_by_family=true`, paging via `offset`, and lead sorting.
- `/api/correlations/` now accepts the same linked-event filters plus `lane`, `min_event_count`, `min_score_signal`, and investigator-friendly sorting.

CLI:
- `ss export events --source "SAM.gov" --date-from 2026-03-01T00:00:00 --agency DOE --limit 100 --sort-by occurred_at`
- `ss leads query --source "SAM.gov" --agency DOE --lane kw_pair --min-score-signal 4 --json`
- `ss export leads --snapshot-id 123 --lead-family vendor_network_contract_lineage --out .\out\lead_snapshot`
- `ss export correlations --source "SAM.gov" --lane kw_pair --recipient-uei UEI123 --place-region "VA,USA" --sort-by event_count`
- `ss export evidence-package --snapshot-id <id> --lead-event-id <event_id>` packages a lead from a snapshot.
- `ss export evidence-package --correlation-id <id>` packages a correlation.

Evidence package guardrail:
- The evidence package is packaging only. It includes source-backed records, identifiers, agencies/vendors, ontology matches, score details, lanes, and a mini timeline.
- It does not draft FOIA letters or infer claims beyond the captured data.
- Lead snapshot exports and API lead views now expose `lead_family`, secondary families, correlation types hit, candidate-join evidence, and linked-source summaries as separate corroboration fields.
## Key FOIA sprint additions
- Seeded ingest: `--keyword`, `--recipient`
- FOIA ontology companion: `examples/ontology_sam_dod_foia_companion.json` (precision-first anchor+pair+exact-probe rules + suppressors)
- Correlation lanes include `kw_pair` (co-term clustering) and `sam_usaspending_candidate_join` (pairwise cross-source incumbent candidates)
- Relationship matrix rationale: `same_keyword` captures repeated precision tags while `kw_pair` promotes anchor+pair co-occurrence evidence for triage confidence.
- Default lead snapshots are **v3**
- Use `--scoring-version v2` only when you intentionally want an older comparison surface.
- Operational noise handling (HRP/DACTS, NASA sponsoring agreement)
- DOE/NNSA weapons complex pivots (SRS, Y-12, Pantex, KCNSC, CNS, SRNS)

## Resume points
- `docs/STATE.md` (current state snapshot)
- `ROADMAP.md` (milestones + next steps)
- `examples/ontology_sam_dod_foia_companion.json` (DoD FOIA companion ontology)
- `tools/runbook.ps1` (repeatable run)
- `tools/diagnose_untagged_usaspending.sql` (schema-safe untagged USAspending diagnostics)



<!-- BEGIN SHADOWSCOPE-AUDIT-2026-02-24 -->
## Audit Notes & Implementation Checklist (2026-02-24)

This repo now has an audit-derived implementation plan + checklist so we do not lose detail as we execute M4.2 -> M5.

**Top priorities (P0/P1)**
- **Event schema enrichment**: promote high-value USAspending fields (agency/PSC/NAICS/award-id/UEI/etc.) to first-class columns so we can build richer correlation lanes and better investigator filters.
- **Ontology: enable `raw_json` tagging** by safely stringifying `raw_json` and passing it into the tagger (so ontology rules targeting `raw_json` actually fire).
- **Scoring alignment**: make **v2** scoring the default everywhere (API + snapshots) while keeping v1 available explicitly.
- **kw_pair signal upgrade**: promote kw_pair from "count" to "signal" (PMI/log-odds/Fisher/Bayesian shrinkage path) + add explainability exports.
- **API filtering improvements**: add investigator-friendly query params to events/leads/correlations.

Full details + checklists live here:
- `docs/AUDIT_BACKLOG_2026-02-24.md`

<!-- END SHADOWSCOPE-AUDIT-2026-02-24 -->






## SAM Larger-Run Validation + Bundle Contract (2026-03-09)

ShadowScope now distinguishes two SAM.gov validation intents:

- Small bounded smoke: quick pass/fail confidence on core workflow wiring.
- Larger-run validation: bigger bounded windows/pages with explicit required and advisory quality gates.

Recommended operator sequence:

```powershell
# 1) Small smoke validation (fast gate)
ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json

# 2) Larger bounded validation pass (operator-focused diagnostics + warnings)
ss workflow samgov-validate --days 30 --pages 5 --limit 250 --window-days 30 --json

# 3) Diagnose SAM status and gaps (no psql required)
ss diagnose samgov --days 30 --json

# 4) Inspect a specific bundle contract/manifest
ss inspect bundle --path <bundle_dir> --json
```

`ss diagnose samgov`, `ss inspect bundle`, and bundle-backed reports now honor the manifest-driven workflow gate status, required/advisory split, and category failures instead of relying on smoke-only pass heuristics.

Normalized SAM bundle contract (`samgov.bundle.v1`):

```text
<bundle_dir>/
  bundle_manifest.json
  results/
    workflow_result.json
    workflow_summary.json
    doctor_status.json
  exports/
    lead_snapshot.csv/json
    keyword_pairs.csv/json
    entities.csv/json
    event_entities.csv/json
    events.csv/jsonl
  report/
    bundle_report.html
```

Bundle interpretation:

- `workflow_summary.json`: machine-readable run quality/check outcomes (`ok`, `warning`, `failed`) and partial-usefulness classification.
- `bundle_manifest.json`: single source of truth for bundle discovery (`generated_files`, status, summary counts, run parameters).
- `bundle_report.html`: human-oriented run review surface aligned to manifest paths.

Warnings vs failures:

- `failed`: one or more required checks failed. The bundle/report now labels whether the failure came from pipeline health, source coverage/context health, or lead-signal quality.
- `warning`: required checks passed, but one or more advisory checks missed threshold and the run is degraded/partially useful.
- `ok`: required checks passed and no warning-level misses.

Validation policy:

- Smoke mode keeps the calibrated threshold contract as a strict gate.
- Larger mode now mixes required and advisory checks instead of downgrading all threshold misses to warnings.
- Each serialized check includes `name`, `observed`, `threshold`, `severity`, `required` vs `advisory`, and pass/fail outcome.

Retry tuning knobs for larger SAM windows:

- `SAM_API_TIMEOUT_SECONDS`
- `SAM_API_MAX_RETRIES`
- `SAM_API_BACKOFF_BASE`
