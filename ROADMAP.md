# ShadowScope Roadmap

<!-- ROADMAP:CURRENT_USASPENDING_BEGIN -->
## Current focus: USAspending MVP closeout (Iteration 1)

This section is the **authoritative checklist** for the current iteration (USAspending).  
SAM.gov is planned, but intentionally **deferred** until USAspending is stable end-to-end.

### Definition of ?MVP done? for Iteration 1 (USAspending)
MVP is ?done? when a non-developer can run:
1) ingest a bounded slice of USAspending data  
2) apply ontology tagging  
3) rebuild correlations (including keyword-pairs)  
4) link entities  
5) generate a lead snapshot  
6) export artifacts (CSV/JSON)  
?and the outputs are understandable enough to review manually.

### ? Completed (already on `main`)
- [x] CLI workflow exists (`ss ingest`, `ss ontology`, `ss correlate`, `ss leads`, `ss export`).
- [x] kw-pair correlations export to CSV+JSON (`ss export kw-pairs`).
- [x] kw-pair exporter supports the actual `lanes_hit` shape (`lane == "kw_pair"`) and legacy nested form.
- [x] Entity-linking supports UEI and CAGE, and stores extra identifiers in `Entity.sites_json`.
- [x] Entity-linking adds DUNS fallback logic (prevents fragmentation when payloads vary).
- [x] Docs: Quickstart + Runbook exist; PowerShell notes and workflow guardrails documented.

### ?? Next sprint backlog (MVP hardening + guardrails)
#### 1) Repo workflow guardrails (GitHub settings)
These are not code changes, but they are MVP-critical because they prevent accidental bypass of the intended workflow.
- [ ] Enable branch protection on `main`:
  - Require PRs (no direct pushes)
  - Require CI checks to pass
  - Block force-pushes
  - Optional: require 1 review
- [ ] Add a PR template (`.github/PULL_REQUEST_TEMPLATE.md`) to standardize change descriptions and testing notes.

#### 2) CI correctness (make the CI reflect reality)
- [ ] Update CI to run the **full** test suite (not a narrow subset).
- [ ] Add a lightweight lint gate:
  - Option A: `ruff` (fast lint + import sorting)
  - Option B: `black` + `ruff` (format + lint)
- [ ] Add a short ?CI expectations? note in the runbook (what must pass before merge).

#### 3) Operator UX (reduce ?hand-holding? needed)
- [ ] Add `ss doctor` (or `ss status`) command that prints:
  - DB connectivity
  - event counts in the selected window
  - keyword coverage (kw>=1, kw>=2)
  - why kw-pairs may be zero (common causes)
- [ ] Add a one-command workflow wrapper (optional but high value):
  - `ss workflow foia --days N --pages P --page-size S --keyword ...`
  - runs ingest ? ontology ? correlations ? entities ? snapshot ? exports

#### 4) Data quality + explainability (still Iteration 1)
- [ ] Improve lead output explainability:
  - include ?why this lead scored? in JSON export (top contributing correlations)
- [ ] Add an ?analyst-friendly? export for entities:
  - entity list (UEI/CAGE/DUNS + name)
  - event?entity mapping export

### Future milestone (deferred): SAM.gov connector (Iteration 2)
Start only after Iteration 1 is stable and repeatable.
- [ ] Add SAM.gov connector skeleton (`backend/connectors/samgov.py`)
- [ ] Define normalized fields ? `Event` mapping (doc_id, occurred_at, place_text, snippet, raw_json)
- [ ] Ingest command: `ss ingest samgov ...` (bounded window + filters)
- [ ] Ontology coverage updates for SAM.gov-specific fields
- [ ] Cross-source entity linking strategy (UEI-first)
- [ ] Cross-source correlations (USAspending ? SAM.gov)

<!-- ROADMAP:CURRENT_USASPENDING_END -->


Last updated: 2026-02-24

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
