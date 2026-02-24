# ShadowScope Audit Backlog & Implementation Plan (2026-02-24)

This document is a **checklist-driven** capture of audit findings + concrete next steps, aligned to ROADMAP milestones.
It’s meant to preserve the “why” behind work items so nothing gets lost as we execute M4.2 → M5.

---

## Priority legend
- **P0** = correctness / alignment / prevents silent failure or investigator confusion
- **P1** = workflow usability / exports / investigator throughput
- **P2** = signal quality upgrades (relationship matrix / kw_pair scoring)
- **P3** = expansion (multi-source, enrichment)

---

## P0 — Schema signal gaps (first-class Event fields) → richer lanes and better explainability

### Problem
Today the Event schema is “good enough to store and dedupe,” but it is **missing first-class, queryable fields** that will make correlations (and investigator workflows) much stronger. Storing everything in `raw_json` is not enough if we want:
- stable correlation lanes (agency/PSC/NAICS/award-id)
- stable API filters
- OpenSearch faceting/aggregation
- consistent explainability exports

### High-value fields to promote to first-class columns (starting with USAspending)
**Identifiers**
- `award_id` / `generated_unique_award_id`
- `transaction_id`
- `piid` / `fain` / `uri` (depending on award type)
- `modification_number` (if present)
- (optional) `source_record_id` (connector-native)

**Parties (recipient)**
- `recipient_name` (normalized)
- `recipient_uei`
- `recipient_parent_uei` (if present)
- `recipient_duns` (legacy, if present)
- `recipient_cage_code` (if present)

**Agencies**
- `awarding_agency_code` + `awarding_agency_name`
- `funding_agency_code` + `funding_agency_name`
- (optional) subtier/bureau codes/names if available

**Classification**
- `award_type` / `award_type_description`
- `psc` (+ description if present)
- `naics` (+ description if present)
- (optional) `cfda`, `program_activity`, `object_class` if surfaced in source data

**Time**
- `action_date` (the transactional “event” date)
- `period_of_performance_start` / `period_of_performance_end` (if available)
- (optional) `ingested_at` already captured via ingest_run, but can also be a column if needed for fast filtering

**Money**
- `action_amount` / `obligation_amount`
- (optional) `total_obligation` / `base_and_all_options_value` when available

**Geography**
- `place_of_performance_city` / `state` / `country` / `zip`
- (optional) recipient location fields

### How to facilitate this (implementation approach)
**Checklist**
- [ ] Decide: first-class columns vs `events.attrs` JSONB for “long tail” fields.
      - Recommended hybrid: **columns for lane + filter fields**, JSONB for long tail.
- [ ] Update connector normalization to extract fields into canonical normalized keys.
- [ ] Add columns to `events` table via Alembic migration.
- [ ] Update SQLAlchemy models accordingly.
- [ ] Update OpenSearch mapping + reindex tool to index these fields (so you can facet/filter quickly).
- [ ] Add/extend correlation lanes that depend on these fields:
      - same_award_id / same_contract_id / same_doc_id
      - same_agency (awarding/funding)
      - same_psc
      - same_naics
      - same_place_region (state/country and optionally city)
- [ ] Add API query params for these fields (events + leads + correlations).

**Design note**
Keep `raw_json` as the immutable “source truth,” but don’t rely on it for workflows that need stable joins, filters, and explainability.

---

## P0 — Low-effort DB migration heuristic cleanup (optional, but easy)
### Problem
The migration helper includes a heuristic list of “core tables.” It’s not critical, but it can become inaccurate as more tables are added.

**Checklist**
- [ ] Update the `core_tables` list to include newer tables (analysis_runs, lead_snapshots, lead_snapshot_items, correlations, correlation_links).
- [ ] Or simplify/replace the heuristic:
      - if `alembic_version` missing, run `alembic upgrade head` directly; only stamp head when you can prove schema already matches.

---

## P0 — Ontology application: include `raw_json` safely (Option 1) + improve ontology fidelity

### Problem
Ontology rules reference `raw_json`, but tagging only works on fields that are actually passed into the tagger.
If `raw_json` isn’t present as a **string field**, those rules become silent no-ops.

### Recommendation: safely stringify `raw_json` and tag against it
**Implementation notes**
- Add a helper like `safe_json_text(obj, max_len=65536)`:
  - `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`
  - truncate to a safe maximum length
  - optionally strip or summarize extremely large arrays (e.g., replace long lists with “<list len=1234>”)
- When building taggable documents, pass:
  - `raw_json = safe_json_text(event.raw_json)`
  - `source_url`, `snippet`, `place_text`, etc.

**Checklist**
- [ ] Implement safe JSON stringify helper (with size control + stable ordering).
- [ ] Ensure tagger field-map includes `"raw_json"` and that events supply it.
- [ ] Add a unit test that a rule targeting raw_json matches expected values.
- [ ] Add a regression test to ensure the noise pack can actually fire on raw_json when intended.

### Ontology fidelity improvements (high ROI)
**Checklist**
- [ ] Fix ontology validation behavior so `default_fields` aren’t overwritten internally.
- [ ] Implement pack-level `default_weight` fallback if rule weight is omitted (optional, but cleans authoring).
- [ ] Add an ontology “lint” command/tool:
      - detects unused fields
      - detects rules that can never match due to field config
      - warns on overly broad regex (catastrophic backtracking risk)
- [ ] Decide whether to support **concept normalization** (recommended, see later).

---

## P0 — Scoring alignment (v2 across the board or keep v1?)
### Decision
Use **v2 as the default** everywhere (CLI snapshots + API), but keep v1 available for backward compatibility.

### Why
- v2 includes co-term evidence (kw_pair) + noise penalties, which better matches the investigator workflow.
- Having `/api/leads` return v1 while snapshots are v2 causes confusing mismatches.

### Checklist (recommended approach)
- [ ] Make `/api/leads` default to v2.
- [ ] Keep v1 via one of:
      - `?scoring_version=v1|v2`
      - or `/api/leads-v1` as an explicit legacy endpoint
- [ ] Return scoring metadata in responses:
      - `scoring_version`, `noise_penalty_applied`, `pair_bonus_applied`
- [ ] Store scoring_version on lead_snapshots (so deltas remain comparable).
- [ ] Add tests ensuring API leads match snapshot scoring given the same inputs.

---

## P2 — Relationship matrix upgrade (kw_pair: from count → signal)

### Problem
kw_pair `score = event_count` is a useful baseline, but it causes frequent-but-uninformative pairs to dominate:
- generic procurement language
- generic “noise” terms
- high-frequency terms that co-occur everywhere

We want kw_pair to surface **surprising co-occurrences**, not just popular ones.

### Core counts (foundation)
For a given window:
- `N` = number of events
- `c1` = count(events containing keyword k1)
- `c2` = count(events containing keyword k2)
- `c12` = count(events containing both k1 and k2)

Build the 2x2 contingency table:
- `a = c12`
- `b = c1 - c12`
- `c = c2 - c12`
- `d = N - c1 - c2 + c12`

### Candidate significance metrics (practical guidance)

#### 1) Lift / PMI (fast, easy, good first step)
- **Lift** = `c12 * N / (c1 * c2)`
- **PMI** = `log(Lift)`
- **NPMI** (normalized PMI) = `PMI / -log(c12/N)` (helps reduce rare-pair bias)

Pros:
- fast to compute
- works well with thresholds

Cons:
- PMI can overvalue very rare pairs unless you enforce `min_pair_count`

Recommended guardrails:
- enforce `min_pair_count >= 2` or `>= 3`
- enforce `max_keyword_df` (drop keywords that appear in too many events)
- optionally exclude noise-pack keywords from pair-building entirely

#### 2) Log-odds / log odds ratio (interpretable association strength)
- **Odds Ratio** OR = `(a*d)/(b*c)` (with smoothing like +0.5 in each cell)
- Score = `log(OR)`

Pros:
- better association interpretation than raw counts

Cons:
- still benefits from shrinkage when counts are small

#### 3) Fisher exact test (statistical significance)
- Compute p-value from the 2x2 table; use `-log10(p)` as score

Pros:
- strong statistical grounding

Cons:
- can be heavier computationally across many pairs
- still needs min-count constraints to stay meaningful

#### 4) Bayesian shrinkage (best quality, but more design choices)
Goal: keep rare pairs from spiking unrealistically while still rewarding true associations.

Options:
- Apply Bayesian smoothing to cell counts (`a,b,c,d`) with a Dirichlet prior.
- Treat `a` as a Binomial with a Beta prior and compare posterior to expectation under independence.
- Use credible intervals to rank by “lower-bound lift” instead of mean lift.

Pros:
- stable ranking
- reduces “rare pair jackpot” issues

Cons:
- requires choosing priors + careful implementation/testing

### Recommended implementation path (incremental)
**Phase 1 (fast win)**
- Switch kw_pair correlation `score` from event_count → `PMI` or `NPMI` (with smoothing).
- Keep `event_count` stored as metadata (lanes_hit or a new column).
- Add filters:
  - ignore pairs where either keyword df > threshold (e.g., > 10% of N)
  - ignore noise-pack keywords
  - enforce min_pair_count

**Phase 2 (quality)**
- Add log-odds ratio and/or Fisher exact for top candidates or as a secondary score.
- Consider Bayesian shrinkage for stable investigator-grade rankings.

**Checklist**
- [ ] Add per-window keyword df counts (c1).
- [ ] Compute pair counts (c12) as today.
- [ ] Compute `score_signal` (PMI/NPMI or log-odds) and store it as `correlations.score`.
- [ ] Preserve `event_count` in metadata (do not lose it).
- [ ] Update v2 pair_bonus to use `score_signal` + `event_count` thresholds.
- [ ] Add tests on a toy dataset to validate that:
      - extremely common terms don’t dominate
      - rare pairs require min counts
      - noise terms are suppressed

---

## P1/P2 — Explainability exports (cluster explain)

### Goal
Make it fast for an investigator to answer: “why is this lead / cluster here?”

### Export: “Top kw_pair clusters explain”
Include:
- correlation id/key, lane
- score_signal + event_count
- member event ids/hashes
- human-readable pair label (k1, k2)
- top entities in cluster
- (when available) top agencies / psc / naics distributions
- top clauses aggregated across member events:
  - clause frequency
  - average weight contribution

**Checklist**
- [ ] Add `ss export kwpair --top N --format json|csv --include-events --include-top-clauses`.
- [ ] Add `ss export leads --snapshot-id X --format json|csv --include-score-details`.
- [ ] Add “deltas export” helper too (new/removed/changed leads).

---

## P2 — Additional correlation lanes (when schema supports it)
Add only when required fields are reliably extracted:

- **same_award_id / same_contract_id / same_doc_id**
  - depends on `award_id` / `piid` / `fain` / `uri` / doc ids
- **same_agency**
  - awarding/funding agency codes
- **same_psc**
- **same_naics**
- **same_place_region**
  - state/country; later: city or geohash

Guardrails:
- ignore keys with extreme cardinality (too common) to avoid giant clusters
- store key counts and enforce caps

---

## P1 — API alignment & filtering (v2 + investigator-friendly queries)

### Checklist
- [ ] Update `/api/leads` default to v2 with `scoring_version` override.
- [ ] Add query params to `/api/events` (minimum set):
  - source, date range, entity_id, keyword, min_score
  - (after schema enrichment) agency, psc, naics, award_id, recipient_uei, place_region
- [ ] Add query params to `/api/correlations`:
  - min_event_count
  - min_score_signal
  - lane filters remain

---

## P1 — Remaining “Workflow polish” items already in ROADMAP (expanded)
- Export helpers (top leads / kw_pair / deltas to JSON/CSV)
- Improve entity linking coverage/fallbacks
- Document runbook parameters (Days/MaxRecords/MinScore/ScanLimit)

---

## P3 — Structural ontology improvements (optional, but powerful)
### Concept normalization (recommended direction)
Currently rules emit `pack:rule`. This is workable, but concept normalization unlocks:
- stable co-term graphs (synonyms collapse to same concept)
- cleaner correlation lanes (“concept” level)
- easier term expansion without graph explosion

Implementation idea:
- allow a rule to specify an explicit `concept_id`
- allow multiple patterns/synonyms to map to the same concept_id
- keep pack/rule naming for explainability, but concept_id drives clustering

### Term expansion suggestions (curated additions)
Acquisition mechanics:
- IDIQ, task order, BPA/BOA, OTA/Other Transaction Authority, CRADA
- SBIR/STTR
- proprietary data, limited rights, distribution statements
- export controlled, ITAR, EAR
- SCIF, secure area, SAP (as applicable/appropriate)

DOE/NNSA ops language (if relevant to your scope):
- MC&A, nuclear explosive safety, protective force
- vault-type room, surety
- Category I/II SNM
- pit production, tritium
- weapon component (careful with broadness)

Noise suppressors (to reduce false positives):
- janitorial, grounds, HVAC, office furniture, generic IT services
- routine training and basic compliance chatter

---

## P3 — Multi-source (M5)
- SAM.gov ingestion/enrichment
- cross-source joins + correlation lanes

---

## Optional hygiene (not required, but improves maintainability)
- Add CI to run tests + lint + type checks
- Add unit tests for ontology validation and tagging field coverage
- Add integration tests: run ingest → tag → correlate → snapshot on a tiny fixture dataset
- Consider reformatting single-line markdown files to standard multi-line markdown for easier future diffs