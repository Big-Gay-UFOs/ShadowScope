# ShadowScope Roadmap

<!-- ROADMAP:CURRENT_USASPENDING_BEGIN -->

## Iteration 1: USAspending MVP closeout (current)

Goal: make the USAspending pipeline runnable end-to-end by a non-developer, with outputs that are reviewable.

### Definition of MVP-done (Iteration 1)
MVP is done when a non-developer can run:
1) ingest a bounded slice of USAspending data
2) apply ontology tagging
3) rebuild correlations (including keyword-pairs)
4) link entities
5) generate a lead snapshot
6) export artifacts (CSV/JSON) that explain why a lead scored

### Current status (2026-02-26)
- Done: Guardrails + CI (PR template, ruff gate, full pytest in CI) - PR #58
- Done: Operator UX (`ss doctor status` + semantics hardening) - PRs #59, #60
- Done: Exports + explainability (why_summary + entity exports) - PR #61

### Iteration 1 checklist (authoritative)

#### Repo guardrails / workflow
- [x] Enable branch protection on `main` (applied)
  - [ ] Require PRs (no direct pushes)
  - [ ] Require CI checks to pass
  - [ ] Block force-pushes
  - [ ] (Optional) Require 1 review
- [x] PR template: `.github/PULL_REQUEST_TEMPLATE.md` (PR #58)
- [x] CONTRIBUTING + line-ending guardrails: `.gitattributes`, `CONTRIBUTING.md` (PR #58)

#### CI / quality gates
- [x] CI runs full pytest suite (`python -m pytest`) (PR #58)
- [x] CI runs ruff lint gate (`ruff check .`) (PR #58)

#### Operator UX
- [x] `ss doctor status` (counts, keyword coverage, lane presence, last runs, actionable hints) (PR #59)
- [x] Doctor window semantics aligned with correlation rebuild + lane counts scoped to `--days` (PR #60)
- [x] Optional: one-command workflow wrapper (`ss workflow usaspending`)

#### Exports + explainability
- [x] Lead snapshot export includes explainability fields (why_summary, score components, top kw-pair contributors) (PR #61)
- [x] Entity exports: entity list + event->entity mapping (`ss export entities`) (PR #61)

### Remaining to close Iteration 1
- [x] Apply branch protection settings on GitHub (done)
- [x] Decide on optional workflow wrapper (implemented)

## Iteration 2: SAM.gov (deferred; NOT this sprint)

Start only after Iteration 1 is stable and repeatable.

- [ ] Add SAM.gov connector skeleton (`backend/connectors/samgov.py`)
- [ ] Define normalized fields -> `Event` mapping (doc_id, occurred_at, place_text, snippet, raw_json)
- [ ] Ingest command: `ss ingest samgov ...` (bounded window + filters)
- [ ] Ontology coverage updates for SAM.gov-specific fields
- [ ] Cross-source entity linking strategy (UEI-first)
- [ ] Cross-source correlations (`USAspending <-> SAM.gov`)

<!-- ROADMAP:CURRENT_USASPENDING_END -->


Last updated: 2026-02-26

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
- SAM.gov ingestion/enrichment
- Cross-source joins and correlations



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

### M4.3 Relationship matrix: kw_pair count → signal
- [ ] Replace kw_pair `score=count` with a signal score:
  - [ ] Phase 1: PMI/NPMI or lift w/ smoothing + min counts + max df filters
  - [ ] Phase 2: log-odds / Fisher exact / Bayesian shrinkage for investigator-grade ranking
- [ ] Add “cluster explain” exports:
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
