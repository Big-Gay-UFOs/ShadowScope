# ShadowScope Roadmap

Last updated: 2026-02-15

## Current status
- Plumbing baseline: COMPLETE (Compose + DB + OpenSearch + CI).
- M3 (Tagging + Scoring): STARTED. Ontology spec + validator in progress.

## M0 - Plumbing baseline (DONE)
- [x] Compose stack reproducible (backend + Postgres + OpenSearch)
- [x] Deterministic migrations (advisory lock + alembic_version TEXT)
- [x] Ingest semantics clarified (pages + page-size + max-records)
- [x] Idempotent ingest (stable hash + uq_events_hash)
- [x] ingest_runs tracking
- [x] OpenSearch indexing tool: recreate + incremental + --json
- [x] /health includes OpenSearch
- [x] /api/search backed by OpenSearch
- [x] Ops runbook
- [x] CI (pytest)

## M3 - Investigator signal (NEXT)
Goal: make results rankable and filterable by "signals" (ontology hits), then score and later cluster.

### M3-01 Ontology spec + default file + validator (NOW)
- [ ] ontology.json (default packs/rules/weights/fields)
- [ ] ss ontology validate (fails fast, prints hash + counts)
- [ ] tests: default ontology validates

### M3-02 Tagger (populate events.keywords / events.clauses) (NEXT)
- [ ] Apply ontology rules to event fields deterministically
- [ ] Persist keyword hits to Postgres (idempotent updates)
- [ ] CLI: ss analyze tag --days N (or ss ontology apply)
- [ ] OpenSearch refresh strategy after tagging (use reindex --full)

### M3-03 analysis_runs persistence (NEXT)
- [ ] analysis_runs table: ontology hash, window, counts, status
- [ ] CLI records analysis run id and summary

### M3-04 scoring + leads upgrade (NEXT)
- [ ] Score from clause weights
- [ ] /api/leads ranks by score and supports filters

### M3-05 deltas and cluster persistence (LATER)
- [ ] anomaly_clusters + membership tables
- [ ] delta report between runs