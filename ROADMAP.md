# ShadowScope Roadmap

Last updated: 2026-02-15

## Current status
- M0 (Plumbing baseline): DONE
- M3 (Tagging + Scoring): IN PROGRESS (M3-01 DONE, starting M3-02)

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

## M3 - Investigator signal (IN PROGRESS)
Goal: make results rankable/filterable by 'signals' (ontology hits), then score and later cluster.

### M3-01 Ontology spec + default file + validator (DONE)
- [x] ontology.json (default packs/rules/weights/fields)
- [x] ss ontology validate (prints hash + counts; fails fast if invalid)
- [x] tests validate default ontology

### M3-02 Tagger (populate events.keywords / events.clauses) (NEXT)
- [ ] Implement tagger engine (phrase + regex; per-field; case-insensitive default)
- [ ] Persist keyword hits to Postgres (idempotent updates)
- [ ] Persist structured clause hits to Postgres (events.clauses)
- [ ] CLI: ss ontology apply --days N [--source] [--dry-run] (idempotent)
- [ ] OpenSearch refresh strategy after tagging (tools/opensearch_reindex.py --full)
- [ ] Tests: matcher correctness + idempotent DB update

### M3-03 analysis_runs persistence (NEXT)
- [ ] analysis_runs table: ontology hash, window, counts, status
- [ ] CLI records analysis run id and summary

### M3-04 scoring + leads upgrade (NEXT)
- [ ] Score from clause weights
- [ ] /api/leads ranks by score and supports filters

### M3-05 deltas and cluster persistence (LATER)
- [ ] anomaly_clusters + membership tables
- [ ] delta report between runs
