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

### Current status (2026-03-08)
- [x] SAM.gov ingest hardening (base URL defaults, retry/backoff, run finalization)
- [x] SAM-aware doctor hints + entity-coverage diagnostics
- [x] SAM workflow wrapper + smoke artifact bundle (`ss workflow samgov`, `ss workflow samgov-smoke`)
- [x] Live validation confirmed SAM ingest, entity linking, keyword coverage, correlation lanes, and lead snapshot health
- [x] Added conservative USAspending starter ontology (`examples/ontology_usaspending_starter.json`)
- [x] Added fixture regression coverage for non-zero USAspending keyword tagging + keyword-correlation lanes
- [x] Added schema-safe diagnostic helper for recent untagged USAspending rows (`tools/diagnose_untagged_usaspending.sql`)
- [x] Added SAM workflow `--days` alias ergonomics and aligned PowerShell guidance

### Next sprint priorities
- [ ] Run two clean SAM live-key smoke bundles and tighten non-zero thresholds from observed baselines
- [ ] Tune USAspending starter ontology using sampled untagged rows (quality over keyword-count inflation)
- [ ] Expand deterministic fixture coverage around USAspending ontology edge cases
- [ ] Keep README/RUNBOOK/QUICKSTART/STATE aligned with validated operator flow

### Definition of done for this pass
From a clean Windows PowerShell session, operators can run bounded workflows and reliably produce:
- SAM smoke bundles with required source-scoped non-zero checks
- USAspending runs with non-zero keyword tagging and at least one keyword correlation lane (`same_keyword` and/or `kw_pair`)
- a repeatable diagnostic path for inspecting remaining untagged USAspending events

<!-- ROADMAP:CURRENT_USASPENDING_END -->


Last updated: 2026-03-08

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

