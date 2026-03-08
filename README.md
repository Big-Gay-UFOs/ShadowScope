# ShadowScope


## Quickstart

<!-- SHADOWSCOPE:OVERVIEW:START -->

## What this repo is

**ShadowScope** is a small data pipeline + API that turns public U.S. federal procurement/spending feeds into a
single, queryable dataset of **events** (things that happened) and **entities** (who they happened to).
The goal is to help you spot patterns ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â recurring agencies, vendors, UEIs/DUNS, and keyword themes ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â and produce
reviewable ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“leadsÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â without manually bouncing between multiple government sites.

## What itÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢s for

- Ingest public datasets (e.g., solicitations and awards) into a database
- Keep a **raw snapshot** trail for debugging/reproducibility
- Normalize records into ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“eventsÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â you can filter/search/export
- Optionally enrich events by:
  - linking organizations/entities (e.g., via UEI/DUNS-style identifiers when present)
  - tagging events using an ontology/keyword list
  - computing simple ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“correlation lanesÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â (e.g., shared keywords, same entity/identifier)

You can drive the workflow via the `ss` CLI and/or the backend API.

## Key concepts

- **Source**: A feed you ingest from (e.g., SAM.gov or USAspending).
- **Raw snapshot**: The original JSON responses saved under `data/raw/<source>/<YYYYMMDD>/...` (useful for audits/debugging).
- **Event**: A normalized record in the DB (an opportunity, an award, etc.).
- **Entity**: A real-world org/vendor/agency that events can link to.
- **Correlation lane**: A lightweight way of saying ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“these things are relatedÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â (shared keywords, same UEI, etc.).
- **Lead snapshot**: A ranked/reviewable output built from events + correlations.

## Hypothetical example

Imagine youÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢re doing business development for a small IT services firm and you care about ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“zero trustÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â work.

Every morning you want to answer:

- ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“Did any new solicitations drop that match our focus areas?ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â
- ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“Has this agency funded similar work recently?ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â
- ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“Is the likely incumbent vendor identifiable from prior awards?ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â

With ShadowScope you could:

1. Ingest recent SAM.gov opportunities (solicitations) and recent USAspending awards.
2. Apply your ontology so events get tagged with terms like `zero trust`, `MFA`, `SIEM`, `cloud migration`.
3. Build correlations so a new solicitation automatically connects to:
   - recent awards from the same agency,
   - the same vendor/entity (when identifiers are available),
   - similar notices that share key phrases.
4. Export a lead snapshot to share internally (CSV/JSON) with a clear ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“why this looks relevantÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â trail
   (shared keywords/entities/correlations).

Instead of manual searching across multiple sites, you get a reproducible dataset and explainable links.

## Data sources

ShadowScope currently focuses on public U.S. government datasets such as:

- **SAM.gov Contract Opportunities** (solicitations/opportunities)
- **USAspending** (awards/spending)

(Connectors live under `backend/connectors/`.)

## Quickstart (local)

> Commands can evolve ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â use `ss --help` and `ss doctor status` for ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“what to run nextÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â in your current version.

1. Configure your database connection:
   - `DATABASE_URL` in your environment or a local `.env` (gitignored)

2. Verify the system can talk to your DB:
   - `ss doctor status`

3. Run a small ingest:
   - `ss ingest usaspending --pages 1 --limit 25`
   - `ss ingest samgov --days 7 --pages 1 --limit 25` (requires `SAM_API_KEY`)

4. If youÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢re using ontology/correlation features, follow the hints from:
   - `ss doctor status --source "SAM.gov" --days 7`

### Configuration notes

Common environment variables:

- `DATABASE_URL` ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â database connection string
- `SAM_API_KEY` ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â SAM.gov public API key (required for SAM.gov ingestion)
- `SAM_API_BASE_URL` ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â optional SAM.gov endpoint override
  If blank/whitespace, ShadowScope falls back to the default `/prod` URL.

PowerShell reminder:

- `SAM_API_KEY` is session-scoped if you set it via `$env:SAM_API_KEY = "..."`.
  If you open a new terminal, youÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ll need to set it again (or use a local `.env`).

<!-- SHADOWSCOPE:OVERVIEW:END -->

<!-- SHADOWSCOPE:SPRINT:START -->

## Sprint roadmap

_Last updated: 2026-03-08_

### Sprint goal

Keep SAM.gov operational and close USAspending ontology quality gaps: repeatable bounded workflows with source-scoped non-zero outputs and reviewable artifacts.

### Quick status summary (plain English)

- **Done:** SAM.gov base URL behavior is safe (defaults to `/prod`; blank overrides are treated as unset).
- **Done:** Ingest runs no longer get stuck as `running` if interrupted; they finalize as `aborted`.
- **Done:** SAM.gov HTTP retries are env-tunable (`SAM_API_MAX_RETRIES`, `SAM_API_BACKOFF_BASE`, `SAM_API_TIMEOUT_SECONDS`) and honor `Retry-After` on HTTP 429.
- **Done:** `ss doctor status` includes source-aware next-step hints and entity coverage diagnostics for SAM.gov workflows.
- **Done:** SAM.gov entity-linking fallback is in place, and recipient identity now matches before parent-path fallback when both are present.
- **Done:** Conservative starter ontology added at `examples/ontology_sam_procurement_starter.json`.
- **Done:** `ss workflow samgov` now provides a first-class SAM end-to-end wrapper.
- **Done:** `ss workflow samgov-smoke` now runs ingest -> entities -> ontology -> correlations -> snapshot -> doctor and writes a timestamped artifact bundle (`workflow_result.json`, `doctor_status.json`, `smoke_summary.json`, exports).
- **Done:** PR #74 is merged to `main`.
- **Next:** continue SAM live-key smoke baselining and tune USAspending ontology quality using untagged-row diagnostics.

### Checklist

#### Completed

- [x] SAM.gov connector hardening (base URL, retry/backoff, run finalization)
- [x] Source-aware doctor hints + entity coverage diagnostics
- [x] Conservative SAM starter ontology
- [x] SAM workflow wrapper: `ss workflow samgov`
- [x] Repeatable smoke bundle path: `ss workflow samgov-smoke`
- [x] Merge PR #74

#### Next up

- [ ] Execute a live SAM key smoke run from a clean Windows PowerShell session and archive the bundle
- [ ] Confirm required source-scoped non-zero checks on live data (`events_window`, `events_with_keywords`, `events_with_entity_window`, `same_keyword|kw_pair`, snapshot items)
- [ ] Capture baseline entity-coverage metrics from smoke output and tighten thresholds after two clean live runs
- [ ] Add deterministic fixture regression coverage for both SAM and USAspending ontology/correlation behavior

#### Known issues / risks

- **Key scope:** `SAM_API_KEY` set via `$env:SAM_API_KEY = ...` is per terminal session unless persisted in local `.env`.
- **Rate limiting (HTTP 429):** mitigated via backoff + `Retry-After`, but still possible under heavier usage.
- **Ontology quality:** SAM and USAspending starter ontologies are intentionally conservative and require domain tuning over time.
- **Repo admin checks:** branch-protection/required-check verification remains an admin-side validation item.

### How to help (when reporting issues)

Attach:
- last ~200 lines of `logs/ingest.log` (if present)
- the latest raw snapshot JSON under `data/raw/<source>/<YYYYMMDD>/...`
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


Typical workflow:

1) Ingest a bounded slice of USAspending data
- `ss ingest usaspending --days 30 --pages 2 --page-size 100 --keyword "DOE" --keyword "NNSA"`

2) Apply ontology tagging
- `ss ontology apply --path .\examples\ontology_usaspending_starter.json --days 30 --source USAspending`
- Optional FOIA-focused alternative: `ss ontology apply --path .\ontology.foia.json --days 30 --source USAspending`

3) Rebuild correlations (including keyword pairs)
- `ss correlate rebuild --window-days 30 --source USAspending --min-events 2`
- `ss correlate rebuild-keyword-pairs --window-days 30 --source USAspending --min-events 2 --max-events 500 --max-keywords-per-event 50`

4) Link entities
- `ss entities link --source USAspending --days 30`

5) Create a lead snapshot and export artifacts
- `ss leads snapshot --source USAspending --min-score 1 --limit 200 --scan-limit 5000 --scoring-version v2 --notes "snapshot"`
- `ss export lead-snapshot --snapshot-id <ID> --out .\data\exports\`
- `ss export lead-deltas --from <ID1> --to <ID2> --out .\data\exports\`
- `ss export kw-pairs --min-event-count 2 --limit 200 --out .\data\exports\`

More detail: see `docs/RUNBOOK.md`.



## Status

- Current focus: keep SAM.gov live repeatability healthy and improve USAspending ontology usefulness (keyword coverage + keyword-correlation lanes).
- Roadmap/checklist: see `ROADMAP.md` (authoritative tracker).
- SAM.gov and USAspending ingestion are both supported; use `ss workflow samgov` / `ss workflow samgov-smoke` for SAM, and `ss workflow usaspending --ontology .\\examples\\ontology_usaspending_starter.json` for USAspending baseline runs.

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

This repo now has an audit-derived implementation plan + checklist so we donÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢t lose detail as we execute M4.2 ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ M5.

**Top priorities (P0/P1)**
- **Event schema enrichment**: promote high-value USAspending fields (agency/PSC/NAICS/award-id/UEI/etc.) to first-class columns so we can build richer correlation lanes and better investigator filters.
- **Ontology: enable `raw_json` tagging** by safely stringifying `raw_json` and passing it into the tagger (so ontology rules targeting `raw_json` actually fire).
- **Scoring alignment**: make **v2** scoring the default everywhere (API + snapshots) while keeping v1 available explicitly.
- **kw_pair signal upgrade**: promote kw_pair from ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“countÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â to ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“signalÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â (PMI/log-odds/Fisher/Bayesian shrinkage path) + add explainability exports.
- **API filtering improvements**: add investigator-friendly query params to events/leads/correlations.

Full details + checklists live here:
- `docs/AUDIT_BACKLOG_2026-02-24.md`

<!-- END SHADOWSCOPE-AUDIT-2026-02-24 -->


