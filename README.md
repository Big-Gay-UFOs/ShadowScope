# ShadowScope


## Quickstart

<!-- SHADOWSCOPE:OVERVIEW:START -->

## What this repo is

**ShadowScope** is a small data pipeline + API that turns public U.S. federal procurement/spending feeds into a
single, queryable dataset of **events** (things that happened) and **entities** (who they happened to).
The goal is to help you spot patterns ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â recurring agencies, vendors, UEIs/DUNS, and keyword themes ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â and produce
reviewable ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œleadsÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â without manually bouncing between multiple government sites.

## What itÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢s for

- Ingest public datasets (e.g., solicitations and awards) into a database
- Keep a **raw snapshot** trail for debugging/reproducibility
- Normalize records into ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œeventsÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â you can filter/search/export
- Optionally enrich events by:
  - linking organizations/entities (e.g., via UEI/DUNS-style identifiers when present)
  - tagging events using an ontology/keyword list
  - computing simple ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œcorrelation lanesÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â (e.g., shared keywords, same entity/identifier)

You can drive the workflow via the `ss` CLI and/or the backend API.

## Key concepts

- **Source**: A feed you ingest from (e.g., SAM.gov or USAspending).
- **Raw snapshot**: The original JSON responses saved under `data/raw/<source>/<YYYYMMDD>/...` (useful for audits/debugging).
- **Event**: A normalized record in the DB (an opportunity, an award, etc.).
- **Entity**: A real-world org/vendor/agency that events can link to.
- **Correlation lane**: A lightweight way of saying ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œthese things are relatedÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â (shared keywords, same UEI, etc.).
- **Lead snapshot**: A ranked/reviewable output built from events + correlations.

## Hypothetical example

Imagine youÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢re doing business development for a small IT services firm and you care about ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œzero trustÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â work.

Every morning you want to answer:

- ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œDid any new solicitations drop that match our focus areas?ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â
- ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œHas this agency funded similar work recently?ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â
- ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œIs the likely incumbent vendor identifiable from prior awards?ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â

With ShadowScope you could:

1. Ingest recent SAM.gov opportunities (solicitations) and recent USAspending awards.
2. Apply your ontology so events get tagged with terms like `zero trust`, `MFA`, `SIEM`, `cloud migration`.
3. Build correlations so a new solicitation automatically connects to:
   - recent awards from the same agency,
   - the same vendor/entity (when identifiers are available),
   - similar notices that share key phrases.
4. Export a lead snapshot to share internally (CSV/JSON) with a clear ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œwhy this looks relevantÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â trail
   (shared keywords/entities/correlations).

Instead of manual searching across multiple sites, you get a reproducible dataset and explainable links.

## Data sources

ShadowScope currently focuses on public U.S. government datasets such as:

- **SAM.gov Contract Opportunities** (solicitations/opportunities)
- **USAspending** (awards/spending)

(Connectors live under `backend/connectors/`.)

## Quickstart (local)

> Commands can evolve ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â use `ss --help` and `ss doctor status` for ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œwhat to run nextÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â in your current version.

1. Configure your database connection:
   - `DATABASE_URL` in your environment or a local `.env` (gitignored)

2. Verify the system can talk to your DB:
   - `ss doctor status`

3. Run a small ingest:
   - `ss ingest usaspending --pages 1 --limit 25`
   - `ss ingest samgov --days 7 --pages 1 --limit 25` (requires `SAM_API_KEY`)

4. If youÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢re using ontology/correlation features, follow the hints from:
   - `ss doctor status --source "SAM.gov" --days 7`

### Configuration notes

Common environment variables:

- `DATABASE_URL` ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â database connection string
- `SAM_API_KEY` ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â SAM.gov public API key (required for SAM.gov ingestion)
- `SAM_API_BASE_URL` ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â optional SAM.gov endpoint override
  If blank/whitespace, ShadowScope falls back to the default `/prod` URL.

PowerShell reminder:

- `SAM_API_KEY` is session-scoped if you set it via `$env:SAM_API_KEY = "..."`.
  If you open a new terminal, youÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ll need to set it again (or use a local `.env`).

<!-- SHADOWSCOPE:OVERVIEW:END -->

<!-- SHADOWSCOPE:SPRINT:START -->

## Sprint roadmap

_Last updated: 2026-03-09_

### Sprint goal

SAM.gov Research Context Hardening (SAM-first): improve context depth, signal quality, and operator diagnostics using fixture-tested/offline-safe changes.

### Quick status summary (plain English)

- **Done:** SAM.gov normalization now persists a focused `sam_*` context contract in `raw_json` (agency path, notice metadata, solicitation ID, NAICS, set-aside, key dates, place pivots).
- **Done:** SAM workflow correlation rebuild now adds a SAM context lane: `same_sam_naics`.
- **Done:** `ss doctor status --source "SAM.gov"` now reports SAM context-depth diagnostics (coverage-by-field, research-context coverage, top notice types/NAICS/set-asides).
- **Done:** `ss workflow samgov-smoke` now checks SAM context depth (`sam_research_context_nonzero`) and records SAM context baseline metrics in smoke bundles.
- **Done:** `examples/ontology_sam_procurement_starter.json` was tuned for higher precision (context-scoped procurement indicators; broad noisy single-word rules removed).
- **Done:** New fixture-based tests cover SAM context extraction/persistence, SAM NAICS correlation utility, SAM context diagnostics, and SAM ontology over-tagging guardrails.
- **Maintenance mode:** USAspending remains available and stable, but SAM.gov is the primary sprint focus.

### Checklist

#### Completed

- [x] SAM context gap audit + focused field contract
- [x] SAM normalization updates for reliable context persistence
- [x] SAM correlation enhancement (`same_sam_naics`)
- [x] SAM smoke/doctor context diagnostics hardening
- [x] SAM ontology tuning pass (high precision, low-noise bias)
- [x] Fixture-based regression coverage for SAM context/signal/guardrails
- [x] Docs refresh to SAM-first operator flow

#### Next up (out of scope this sprint)

- [ ] Cross-source (SAM <-> USAspending) linkage lanes
- [ ] Live-key threshold tightening for `same_sam_naics` after more operator bundles
- [ ] USAspending ontology expansion beyond maintenance-mode fixes
- [ ] kw_pair signal-model upgrades (PMI/log-odds/etc.)

### Proposed next sprint (draft)

**Sprint theme:** SAM Cross-Source Validation and Threshold Calibration

**Primary objective:** convert SAM context hardening into calibrated, operator-trusted thresholds and begin pragmatic SAM-to-USAspending linkage experiments.

**Recommended scope:**
- lock two additional live SAM smoke bundles and calibrate stricter `samgov-smoke` pass gates from observed lane/context baselines
- add one reviewable SAM->USAspending candidate linkage surface (export/report), without high-risk schema churn
- tune `same_sam_naics` threshold defaults based on observed false-positive/false-negative behavior
- preserve fixture-first CI checks; keep live API checks optional/manual

**Suggested acceptance targets:**
- smoke bundles pass with calibrated thresholds on at least two independent bounded runs
- non-zero, reviewable SAM linkage candidate output is generated from deterministic fixtures
- full pytest remains green with no new live-API-dependent tests

#### Known issues / risks

- **Key scope:** `SAM_API_KEY` set via `$env:SAM_API_KEY = ...` is per terminal session unless persisted in local `.env`.
- **Rate limiting (HTTP 429):** mitigated via retries/backoff + `Retry-After`, but still possible under heavier usage.
- **Signal volume sensitivity:** lane counts can vary by window size and feed mix; keep bounded windows and validate with smoke bundles.

### How to help (when reporting issues)

Attach:
- last ~200 lines of `logs/ingest.log` (if present)
- the latest raw snapshot JSON under `data/raw/sam/<YYYYMMDD>/...`
- the smoke bundle directory generated by `ss workflow samgov-smoke`

<!-- SHADOWSCOPE:SPRINT:END -->

<!-- SHADOWSCOPE:DEMO:START -->

## PowerShell demo walkthrough

This repo includes two SAM.gov ontology options:

- **Starter (recommended):** `examples/ontology_sam_procurement_starter.json` (conservative baseline)
- **Demo (smoke test):** `examples/ontology_sam_kwpair_demo.json` (broad signal to prove full pipeline wiring)

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
ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2

# 5) Generate a lead snapshot
ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200

# 6) Inspect pipeline health and source-specific hints
ss doctor status --source "SAM.gov" --days 30
```

### One-command SAM workflows

If you want a single operator command instead of manual sequencing:

```powershell
# Full SAM workflow (ingest -> ontology -> entities -> correlations -> snapshot -> exports)
ss workflow samgov --days 30 --pages 2 --limit 50 --ontology .\examples\ontology_sam_procurement_starter.json --window-days 30

# Smoke workflow (same chain + doctor checks + artifact bundle)
ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30
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

- Starter ontology is the default recommendation for realistic signal.
- Demo ontology is intentionally broad for quick smoke tests.
- `SAM_API_KEY` is session-scoped in PowerShell unless you also persist it in local `.env`.

<!-- SHADOWSCOPE:DEMO:END -->


Typical workflow (SAM.gov tuning loop):

1) Run bounded SAM workflow
- `ss workflow samgov --days 30 --pages 2 --limit 50 --ontology .\examples\ontology_sam_procurement_starter.json --window-days 30`

2) Validate smoke bundle + non-zero checks
- `ss workflow samgov-smoke --days 30 --pages 2 --limit 50 --window-days 30 --json`

3) Review SAM context/signal diagnostics
- `ss doctor status --source "SAM.gov" --days 30 --json`
- Target checks: `events_window > 0`, `events_with_keywords > 0`, `same_keyword > 0 OR kw_pair > 0`, `events_with_research_context > 0`

4) Repeatable SAM tuning loop after ontology/context edits
- `ss workflow samgov --skip-ingest --ontology .\examples\ontology_sam_procurement_starter.json --window-days 30 --days 30`
- `ss correlate rebuild-sam-naics --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200`
- `ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2 --max-events 200 --max-keywords-per-event 10`
- `ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200 --scan-limit 5000 --scoring-version v2 --notes "sam context tuning pass"`

5) Optional maintenance-mode USAspending check
- `ss doctor status --source USAspending --days 30`

More detail: see `docs/RUNBOOK.md`.



## Status

- SAM.gov baseline is healthy and repeatable; use `ss workflow samgov` / `ss workflow samgov-smoke` for ongoing smoke checks.
- USAspending is in maintenance mode this sprint; keep lightweight health checks with `ss doctor status --source USAspending --days 30`.
- Roadmap/checklist: see `ROADMAP.md` (authoritative tracker).

### Notes

- PowerShell: do not paste placeholders like `<ID>`; use numeric values directly.
- Correlations: use `--window-days` for rebuild commands (not `--days`).
- SAM workflows accept both `--ingest-days` and `--days` (alias).
- Raw ingest snapshots:
  - USAspending: `data/raw/usaspending/YYYYMMDD/page_*.json`
  - SAM.gov: `data/raw/sam/YYYYMMDD/page_*.json`

ShadowScope is a batch investigative OSINT pipeline for surfacing support footprints inside public procurement data.

It is designed for repeatable investigator runs:

1) ingest a time window (seeded searches)
2) normalize + persist to Postgres (idempotent)
3) tag with ontology signals (keywords + clause hits)
4) score/rank leads (v2 scoring by default)
5) snapshot leads (repeatability + deltas)
6) cluster related records (entity / UEI / keyword / keyword-pair)
## Runbook
One command:
- `tools/runbook.ps1`

Windows execution policy (one-time):
- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\runbook.ps1`

## Key FOIA sprint additions
- Seeded ingest: `--keyword`, `--recipient`
- FOIA ontology: `ontology.foia.json` (apply via `--path`)
- Correlation lanes include `kw_pair` (co-term clustering)
- Default lead snapshots are **v2**
- Operational noise handling (HRP/DACTS, NASA sponsoring agreement)
- DOE/NNSA weapons complex pivots (SRS, Y-12, Pantex, KCNSC, CNS, SRNS)

## Resume points
- `docs/STATE.md` (current state snapshot)
- `ROADMAP.md` (milestones + next steps)
- `ontology.foia.json` (FOIA ontology)
- `tools/runbook.ps1` (repeatable run)
- `tools/diagnose_untagged_usaspending.sql` (schema-safe untagged USAspending diagnostics)



<!-- BEGIN SHADOWSCOPE-AUDIT-2026-02-24 -->
## Audit Notes & Implementation Checklist (2026-02-24)

This repo now has an audit-derived implementation plan + checklist so we donÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢t lose detail as we execute M4.2 ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ M5.

**Top priorities (P0/P1)**
- **Event schema enrichment**: promote high-value USAspending fields (agency/PSC/NAICS/award-id/UEI/etc.) to first-class columns so we can build richer correlation lanes and better investigator filters.
- **Ontology: enable `raw_json` tagging** by safely stringifying `raw_json` and passing it into the tagger (so ontology rules targeting `raw_json` actually fire).
- **Scoring alignment**: make **v2** scoring the default everywhere (API + snapshots) while keeping v1 available explicitly.
- **kw_pair signal upgrade**: promote kw_pair from ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œcountÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â to ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œsignalÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â (PMI/log-odds/Fisher/Bayesian shrinkage path) + add explainability exports.
- **API filtering improvements**: add investigator-friendly query params to events/leads/correlations.

Full details + checklists live here:
- `docs/AUDIT_BACKLOG_2026-02-24.md`

<!-- END SHADOWSCOPE-AUDIT-2026-02-24 -->





