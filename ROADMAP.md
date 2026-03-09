# ShadowScope Roadmap

<!-- ROADMAP:CURRENT_USASPENDING_BEGIN -->

## Iteration 1: USAspending MVP closeout (completed)

Goal achieved: USAspending is runnable end-to-end by a non-developer with reviewable outputs.

### Completed highlights
- Guardrails + CI baseline (PR template, lint/test gates)
- Operator UX hardening (`ss doctor status`)
- Exports + explainability fields
- One-command USAspending workflow (`ss workflow usaspending`)

## Iteration 2: SAM.gov research context hardening (current)

Current theme:
- SAM-first reliability and research context depth on bounded operator windows
- fixture-based/offline-safe validation for SAM signal quality and diagnostics
- USAspending in maintenance mode for this sprint

### Current status (2026-03-09)
- [x] SAM context field contract implemented in normalization (`sam_*` canonical fields in `raw_json`)
- [x] SAM workflow now rebuilds a new correlation lane: `same_sam_naics`
- [x] SAM doctor diagnostics expanded with context-depth metrics (`events_with_research_context`, coverage-by-field, top notice/NAICS/set-aside)
- [x] SAM smoke workflow now enforces non-zero research context checks and records SAM context baseline in bundle JSON
- [x] SAM starter ontology tuned for high-precision procurement indicators (context-scoped, low-noise)
- [x] Added fixture-based regression tests for SAM context extraction/persistence, non-zero correlation utility, and anti-noise guardrails
- [x] Full local pytest suite passes with SAM-first changes
- [x] USAspending left stable in maintenance mode (no new primary-sprint expansion)

### Next sprint priorities
- [ ] Capture additional live SAM smoke bundles and calibrate thresholds for `same_sam_naics`
- [ ] Add cross-source join candidates (SAM opportunities to USAspending downstream signals)
- [ ] Evaluate signal-strength ranking upgrades for keyword/kw-pair lanes
- [ ] Resume USAspending ontology expansion only after SAM context thresholds stabilize

### Definition of done for this pass
From a clean Windows PowerShell session, operators can run bounded SAM workflows and reliably produce:
- non-zero SAM keyword/correlation signal under documented defaults
- non-zero SAM research-context diagnostics in `doctor` and smoke bundles
- repeatable fixture-based regression coverage with no live API dependency in CI-facing tests

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

### M4.3 Relationship matrix: kw_pair count ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ signal
- [ ] Replace kw_pair `score=count` with a signal score:
  - [ ] Phase 1: PMI/NPMI or lift w/ smoothing + min counts + max df filters
  - [ ] Phase 2: log-odds / Fisher exact / Bayesian shrinkage for investigator-grade ranking
- [ ] Add ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“cluster explainÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â exports:
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


