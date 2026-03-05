# ShadowScope


## Quickstart

<!-- SHADOWSCOPE:OVERVIEW:START -->

## What this repo is

**ShadowScope** is a small data pipeline + API that turns public U.S. federal procurement/spending feeds into a
single, queryable dataset of **events** (things that happened) and **entities** (who they happened to).
The goal is to help you spot patterns — recurring agencies, vendors, UEIs/DUNS, and keyword themes — and produce
reviewable “leads” without manually bouncing between multiple government sites.

## What it’s for

- Ingest public datasets (e.g., solicitations and awards) into a database
- Keep a **raw snapshot** trail for debugging/reproducibility
- Normalize records into “events” you can filter/search/export
- Optionally enrich events by:
  - linking organizations/entities (e.g., via UEI/DUNS-style identifiers when present)
  - tagging events using an ontology/keyword list
  - computing simple “correlation lanes” (e.g., shared keywords, same entity/identifier)

You can drive the workflow via the `ss` CLI and/or the backend API.

## Key concepts

- **Source**: A feed you ingest from (e.g., SAM.gov or USAspending).
- **Raw snapshot**: The original JSON responses saved under `data/raw/<source>/<YYYYMMDD>/...` (useful for audits/debugging).
- **Event**: A normalized record in the DB (an opportunity, an award, etc.).
- **Entity**: A real-world org/vendor/agency that events can link to.
- **Correlation lane**: A lightweight way of saying “these things are related” (shared keywords, same UEI, etc.).
- **Lead snapshot**: A ranked/reviewable output built from events + correlations.

## Hypothetical example

Imagine you’re doing business development for a small IT services firm and you care about “zero trust” work.

Every morning you want to answer:

- “Did any new solicitations drop that match our focus areas?”
- “Has this agency funded similar work recently?”
- “Is the likely incumbent vendor identifiable from prior awards?”

With ShadowScope you could:

1. Ingest recent SAM.gov opportunities (solicitations) and recent USAspending awards.
2. Apply your ontology so events get tagged with terms like `zero trust`, `MFA`, `SIEM`, `cloud migration`.
3. Build correlations so a new solicitation automatically connects to:
   - recent awards from the same agency,
   - the same vendor/entity (when identifiers are available),
   - similar notices that share key phrases.
4. Export a lead snapshot to share internally (CSV/JSON) with a clear “why this looks relevant” trail
   (shared keywords/entities/correlations).

Instead of manual searching across multiple sites, you get a reproducible dataset and explainable links.

## Data sources

ShadowScope currently focuses on public U.S. government datasets such as:

- **SAM.gov Contract Opportunities** (solicitations/opportunities)
- **USAspending** (awards/spending)

(Connectors live under `backend/connectors/`.)

## Quickstart (local)

> Commands can evolve — use `ss --help` and `ss doctor status` for “what to run next” in your current version.

1. Configure your database connection:
   - `DATABASE_URL` in your environment or a local `.env` (gitignored)

2. Verify the system can talk to your DB:
   - `ss doctor status`

3. Run a small ingest:
   - `ss ingest usaspending --pages 1 --limit 25`
   - `ss ingest samgov --days 7 --pages 1 --limit 25` (requires `SAM_API_KEY`)

4. If you’re using ontology/correlation features, follow the hints from:
   - `ss doctor status --source "SAM.gov" --days 7`

### Configuration notes

Common environment variables:

- `DATABASE_URL` — database connection string
- `SAM_API_KEY` — SAM.gov public API key (required for SAM.gov ingestion)
- `SAM_API_BASE_URL` — optional SAM.gov endpoint override  
  If blank/whitespace, ShadowScope falls back to the default `/prod` URL.

PowerShell reminder:

- `SAM_API_KEY` is session-scoped if you set it via `$env:SAM_API_KEY = "..."`.
  If you open a new terminal, you’ll need to set it again (or use a local `.env`).

<!-- SHADOWSCOPE:OVERVIEW:END -->

<!-- SHADOWSCOPE:SPRINT:START -->

## Sprint roadmap

_Last updated: 2026-03-05_

### Sprint goal

Make **SAM.gov ingestion reliable end-to-end** (local Windows dev + CI) so we can consistently:
ingest -> normalize -> tag -> correlate -> produce reviewable lead snapshots.

### Quick status summary (plain English)

- **Done:** SAM.gov base URL behavior is safe (defaults to `/prod`, env override supported, blank override will not break it).
- **Done:** Ingest runs no longer get stuck as `running` if you Ctrl+C — they finalize as `aborted` (with tests).
- **Done:** SAM.gov HTTP retry/backoff now honors `Retry-After` on HTTP 429 (with tests).
- **Done:** We proved the local SAM.gov happy path works with a tracked demo ontology:
  ingest -> ontology tags -> same-keyword correlations -> keyword-pair correlations -> lead snapshot.
- **Still an issue:** SAM.gov entity linking coverage is still 0 for the recent demo slice.
- **Still an issue:** The demo ontology is intentionally broad for smoke-testing; it is not production-quality tagging logic.
- **Next:** Tighten default SAM.gov query behavior, improve entity coverage, and document the PowerShell workflow more clearly.

### Checklist

#### ✅ Completed (this sprint)

- [x] **SAM.gov opportunities base URL** defaults to `/prod` and supports `SAM_API_BASE_URL` override
- [x] Treat blank/whitespace `SAM_API_BASE_URL` as unset (prevents accidental outage mode)
- [x] Add tests for default / override / blank override behavior
- [x] Ensure ingest runs finalize on Ctrl+C (`KeyboardInterrupt`) instead of staying `running`
- [x] Add regression test verifying Ctrl+C marks the run `aborted`
- [x] Fix CI lint failure caused by unused exception variable in KeyboardInterrupt handler
- [x] Honor `Retry-After` for SAM.gov HTTP 429 retries (+ regression tests)
- [x] Add tracked SAM.gov demo ontology + PowerShell happy-path walkthrough
- [x] Prove local SAM.gov demo flow can produce:
  - ingest success
  - ontology tags
  - same-keyword correlations
  - keyword-pair correlations
  - lead snapshot output

#### 🔜 Next up

- [ ] Make SAM.gov ingest return representative non-zero rows more reliably for common/default date windows
- [ ] Improve SAM.gov entity linking coverage
- [ ] Document PowerShell-friendly `SAM_API_KEY` setup more clearly (session vs `.env`) + common failure modes
- [ ] Decide whether to promote the demo ontology into a more realistic procurement ontology, or keep it explicitly demo-only

#### ⚠️ Known issues / risks (still true right now)

- **Rate limiting (HTTP 429):** mitigated via backoff + `Retry-After`, but still possible under heavier usage.
- **Key scope:** `SAM_API_KEY` set via `$env:SAM_API_KEY = ...` is **per-terminal-session**. New terminal = ingest skipped.
- **Entity linking:** recent SAM.gov demo runs still show `entities_total=0`.
- **Demo ontology:** current correlation success is driven by an intentionally broad demo ontology for smoke-testing, not final production semantics.

### How to help (when reporting issues)

Attach:
- last ~200 lines of `logs/ingest.log` (if present)
- the latest raw snapshot JSON under `data/raw/<source>/<YYYYMMDD>/...`

<!-- SHADOWSCOPE:SPRINT:END -->

<!-- SHADOWSCOPE:DEMO:START -->

## PowerShell demo walkthrough

This repo includes a **demo ontology** at `examples/ontology_sam_kwpair_demo.json`.

It is intentionally broad and is meant for **smoke-testing the full pipeline**:

**ingest -> ontology tags -> same-keyword correlations -> keyword-pair correlations -> lead snapshot**

It is **not** a production-quality ontology. Its purpose is to prove the pipeline works end-to-end on local SAM.gov data.

```powershell
# 1) Set your SAM.gov API key for this terminal session
$sec = Read-Host -Prompt "Paste SAM.gov Public API Key (input hidden)" -AsSecureString
$ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
try {
  $env:SAM_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
} finally {
  [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
}

# 2) Ingest a bounded SAM.gov slice
ss ingest samgov --days 30 --pages 2 --limit 50

# 3) Apply the demo ontology
ss ontology apply --path .\examples\ontology_sam_kwpair_demo.json --days 30 --source "SAM.gov"

# 4) Build correlations
ss correlate rebuild-keywords --window-days 30 --source "SAM.gov" --min-events 2
ss correlate rebuild-keyword-pairs --window-days 30 --source "SAM.gov" --min-events 2

# 5) Generate a lead snapshot
ss leads snapshot --source "SAM.gov" --min-score 1 --limit 200

# 6) Inspect pipeline health
ss doctor status --source "SAM.gov" --days 30
```

### What success looks like

A healthy SAM.gov demo run should show success in the **SAM.gov-specific command outputs**, not just a global total.

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

**Do not use `lead_snapshots_total > 0` as a SAM.gov-specific success check**. That count is global across sources.

### Notes

- This demo ontology is intentionally broad so the end-to-end flow is easy to verify locally.
- `SAM_API_KEY` is session-scoped in PowerShell unless you also persist it elsewhere.
- The tracked demo ontology lives under `examples/`; the scratch ontology files created during experimentation are not part of the repo.

<!-- SHADOWSCOPE:DEMO:END -->


Typical workflow:

1) Ingest a bounded slice of USAspending data
- `ss ingest usaspending --days 30 --pages 2 --page-size 100 --keyword "DOE" --keyword "NNSA"`

2) Apply ontology tagging
- `ss ontology apply --path .\ontology.foia.json --days 30 --source USAspending`

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

- Current focus: **USAspending iteration (MVP closeout)** ? stable ingest ? tag ? correlate ? rank ? export.
- Roadmap/checklist: see `ROADMAP.md` (this is the authoritative tracker).
- Future: **SAM.gov** support is planned, but intentionally deferred until USAspending is solid end-to-end.

### Notes
- PowerShell: do not paste placeholders like `<ID>`; use numeric values directly.
- Correlations: use `--window-days` for rebuild commands (not `--days`).
- Raw ingest snapshots: `data/raw/usaspending/YYYYMMDD/page_*.json`.



ShadowScope is a **batch investigative OSINT pipeline** for surfacing “support footprints” of sensitive programs inside **public procurement data** (starting with USAspending; SAM.gov planned).

It is designed for repeatable investigator runs:

1) ingest a time window (seeded searches)  
2) normalize + persist to Postgres (idempotent)  
3) tag with ontology signals (keywords + clause hits)  
4) score/rank leads (**v2** scoring by default)  
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



<!-- BEGIN SHADOWSCOPE-AUDIT-2026-02-24 -->
## Audit Notes & Implementation Checklist (2026-02-24)

This repo now has an audit-derived implementation plan + checklist so we don’t lose detail as we execute M4.2 → M5.

**Top priorities (P0/P1)**
- **Event schema enrichment**: promote high-value USAspending fields (agency/PSC/NAICS/award-id/UEI/etc.) to first-class columns so we can build richer correlation lanes and better investigator filters.
- **Ontology: enable `raw_json` tagging** by safely stringifying `raw_json` and passing it into the tagger (so ontology rules targeting `raw_json` actually fire).
- **Scoring alignment**: make **v2** scoring the default everywhere (API + snapshots) while keeping v1 available explicitly.
- **kw_pair signal upgrade**: promote kw_pair from “count” to “signal” (PMI/log-odds/Fisher/Bayesian shrinkage path) + add explainability exports.
- **API filtering improvements**: add investigator-friendly query params to events/leads/correlations.

Full details + checklists live here:
- `docs/AUDIT_BACKLOG_2026-02-24.md`

<!-- END SHADOWSCOPE-AUDIT-2026-02-24 -->
