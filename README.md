# ShadowScope


## Quickstart

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
