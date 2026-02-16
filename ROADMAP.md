# ShadowScope Roadmap

Last updated: 2026-02-16

## Current status
- M0 (Plumbing baseline): DONE
- M3 (Investigator signal): DONE (ontology -> tagging -> scoring -> analysis_runs -> lead snapshots -> deltas)
- M4 (Entity enrichment + correlations): NEXT

## M0 - Plumbing baseline (DONE)
- [x] Compose stack reproducible (backend + Postgres + OpenSearch)
- [x] Deterministic migrations (advisory lock + alembic_version TEXT)
- [x] Ingest semantics clarified (pages + page-size + max-records)
- [x] Idempotent ingest (stable hash + uq_events_hash)
- [x] ingest_runs tracking
- [x] OpenSearch reindex: recreate + incremental + --json + wait/retry
- [x] /health includes OpenSearch
- [x] /api/search backed by OpenSearch
- [x] Ops runbook
- [x] CI (pytest)

## M3 - Investigator signal (DONE)
Goal: produce rankable, explainable leads from public procurement text via ontology hits.

- [x] M3-01 Ontology spec + validator (ontology.json + ss ontology validate)
- [x] M3-02 Tagger writes events.keywords + events.clauses (idempotent) (ss ontology apply)
- [x] M3-03 analysis_runs persisted for ontology apply (success/failed + counters + dry_run)
- [x] M3-04 Scoring + /api/leads (score breakdown included)
- [x] M3-05 Lead snapshots + deltas
  - [x] lead_snapshots + lead_snapshot_items tables
  - [x] ss leads snapshot
  - [x] ss leads delta (compare two snapshots)
  - [x] API endpoints: /api/lead-snapshots, /api/lead-snapshots/{id}/items, /api/lead-deltas
  - [x] Schema cleanup: renamed lead_snapshots.limit -> max_items (avoid SQL keyword quoting issues)

## M4 - Entity enrichment + correlations (NEXT)
Goal: link events into explainable investigative graphs (who/what/where patterns across time).

### M4-01 Entity extraction + normalization
- [ ] Normalize vendor/entity identifiers where available (CAGE/UEI/name variants)
- [ ] Link events -> entities more reliably

### M4-02 Correlation / relationship layer
- [ ] Define correlation heuristics (same entity, shared addresses, repeated doc_id patterns, etc.)
- [ ] Persist correlations and correlation links

### M4-03 API + exports
- [ ] /api/entities/{id}/events and /api/correlations endpoints
- [ ] Export correlation views for investigation
