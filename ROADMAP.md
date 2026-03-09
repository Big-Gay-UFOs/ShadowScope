# ShadowScope Roadmap

<!-- ROADMAP:CURRENT_USASPENDING_BEGIN -->

## Iteration 1: USAspending MVP closeout (completed)

Goal achieved: USAspending is runnable end-to-end by a non-developer with reviewable outputs.

### Completed highlights
- Guardrails + CI baseline (PR template, lint/test gates)
- Operator UX hardening (`ss doctor status`)
- Exports + explainability fields
- One-command USAspending workflow (`ss workflow usaspending`)

## Iteration 2: Workflow validation pass (current)

Current theme:
- keep SAM.gov healthy and repeatable with source-scoped smoke artifacts
- close USAspending ontology usefulness gaps on representative windows

### Current status (2026-03-09)
- [x] SAM.gov ingest hardening (base URL defaults, retry/backoff, run finalization)
- [x] SAM-aware doctor hints + entity-coverage diagnostics
- [x] SAM workflow wrapper + smoke artifact bundle (`ss workflow samgov`, `ss workflow samgov-smoke`)
- [x] Live validation confirmed SAM ingest, entity linking, keyword coverage, correlation lanes, and lead snapshot health
- [x] SAM baseline captured for 30-day operator slice (`events_window=50`, `with_keywords=50`, `same_keyword=7`, `kw_pair=14`, entity coverage 100%)`
- [x] USAspending starter ontology tuned (v2 pass) for recurring untagged service/support/maintenance/training/security/license/cloud/software patterns
- [x] USA workflow default keyword threshold calibrated to practical slices (`--min-events-keywords 2`)
- [x] Added deterministic fixture coverage for USAspending keyword tagging, `same_keyword`, `kw_pair`, and over-tagging guardrails
- [x] Expanded schema-safe untagged diagnostics with recurring-term prevalence columns (`tools/diagnose_untagged_usaspending.sql`)
- [x] README/RUNBOOK/QUICKSTART/STATE refreshed with the USA tuning loop and exact operator commands

### Next sprint priorities
- [ ] Re-run live USAspending 30-day slice and measure post-tuning coverage delta vs 14.5% baseline
- [ ] Archive two clean SAM live-key smoke bundles and tighten threshold gates from observed baselines
- [ ] Continue precision tuning for remaining untagged clusters without broad single-word rules
- [ ] Evaluate `kw_pair` signal scoring upgrade path after lane volume stabilizes

### Definition of done for this pass
From a clean Windows PowerShell session, operators can run bounded workflows and reliably produce:
- SAM smoke bundles with required source-scoped non-zero checks
- USAspending runs with non-zero keyword tagging and non-zero keyword lanes for both `same_keyword` and `kw_pair` under documented workflow settings
- a repeatable diagnostic path for inspecting remaining untagged USAspending events

<!-- ROADMAP:CURRENT_USASPENDING_END -->


Last updated: 2026-03-09

## Done
- M0 plumbing: Compose + migrations + idempotent ingest
- M3 investigator signal: ontology tagging + scoring + lead snapshots
- M4 baseline: entity/UEI/keyword correlations + API filtering + exports baseline
- M4.1 FOIA extension: seeded ingest + FOIA ontology + kw_pair + v2 default scoring + runbook

## Next
### M4.2 Workflow polish
- Export helpers (top leads / kw_pair to JSON/CSV)
- Improve entity linking coverage/fallbacks
- Document runbook parameters (Days/MaxRecords/MinScore/ScanLimit)

### M5 Multi-source
- Expand SAM.gov repeatability from smoke checks to stronger thresholds
- Add cross-source joins and correlations (USAspending <-> SAM.gov)



<!-- BEGIN SHADOWSCOPE-AUDIT-ROADMAP-2026-02-24 -->

## Audit-derived expanded checklist (2026-02-24)

This section expands ROADMAP items with detailed, checklist-driven tasks discovered during a repo audit.
See `docs/AUDIT_BACKLOG_2026-02-24.md` for full reasoning + implementation notes.

### M4.2 Workflow polish (expanded)
- [ ] Export helpers:
  - [ ] top leads snapshot export (JSON/CSV)
  - [ ] top kw_pair clusters export (JSON/CSV)
  - [ ] lead deltas export (JSON/CSV)
- [ ] Ontology application quality:
  - [ ] include `raw_json` (safely stringified) in tagger input so rules can match it
  - [ ] fix ontology validation fidelity (default_fields handling)
  - [ ] optionally add ontology lint tool
- [ ] Scoring alignment:
  - [ ] make `/api/leads` default to v2
  - [ ] preserve v1 via explicit `scoring_version` param or legacy route
- [ ] API filtering usability:
  - [ ] add core filters to `/api/events` (source, date range, entity_id, keyword)
  - [ ] add filters to `/api/leads` (min_score, scoring_version, include_explanations)
  - [ ] add correlation filters (min_event_count, min_score_signal)

### M4.3 Relationship matrix: kw_pair count ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ signal
- [ ] Replace kw_pair `score=count` with a signal score:
  - [ ] Phase 1: PMI/NPMI or lift w/ smoothing + min counts + max df filters
  - [ ] Phase 2: log-odds / Fisher exact / Bayesian shrinkage for investigator-grade ranking
- [ ] Add ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œcluster explainÃƒÂ¢Ã¢â€šÂ¬Ã‚Â exports:
  - [ ] member events + co-terms + top clauses (aggregated)
  - [ ] (after schema enrichment) top agencies/PSC/NAICS breakdown per cluster
- [ ] Add new correlation lanes once schema supports it:
  - [ ] same_award_id / same_contract_id / same_doc_id
  - [ ] same_agency
  - [ ] same_PSC / same_NAICS
  - [ ] same_place_region (state/country)

### M4.4 Schema enrichment for richer lanes + filters
- [ ] Promote high-value USAspending fields to first-class columns (award id, agencies, PSC/NAICS, UEI, place region, amounts, action_date)
- [ ] Update OpenSearch mapping + reindex tool to index new fields
- [ ] Update API filters to use these fields

### M5 Multi-source (unchanged, but now with prerequisites)
- [ ] SAM.gov ingestion/enrichment
- [ ] Cross-source joins and correlations
- [ ] Leverage schema-enriched lanes for cross-source linkage

<!-- END SHADOWSCOPE-AUDIT-ROADMAP-2026-02-24 -->

