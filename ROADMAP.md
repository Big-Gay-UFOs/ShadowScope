# ShadowScope Roadmap

Last updated: 2026-02-16

## Current status
- M0 (Plumbing baseline): DONE
- M3 (Tagging + Scoring): IN PROGRESS (M3-01/02/03 DONE, starting M3-04)

## M3 - Investigator signal (IN PROGRESS)

### M3-01 Ontology spec + default file + validator (DONE)
- [x] ontology.json (default packs/rules/weights/fields)
- [x] ss ontology validate
- [x] tests validate default ontology

### M3-02 Tagger (populate events.keywords / events.clauses) (DONE)
- [x] Tagger engine (phrase + regex; per-field; case-insensitive default)
- [x] Persist keyword hits to Postgres (idempotent updates)
- [x] Persist structured clause hits to Postgres (events.clauses)
- [x] CLI: ss ontology apply --days N [--source] [--dry-run]
- [x] OpenSearch refresh after tagging (tools/opensearch_reindex.py --full)
- [x] Tests: matcher correctness + idempotent DB update

### M3-03 analysis_runs persistence (DONE)
- [x] analysis_runs table + migrations
- [x] ss ontology apply creates analysis_runs rows (success/failed, counters, dry_run)

### M3-04 scoring + leads upgrade (NEXT)
- [ ] Implement scoring function (sum clause weights, with keyword fallback)
- [ ] Upgrade /api/leads to rank by clause-weight score
- [ ] Include score breakdown in response (top clauses, pack/rule counts)
- [ ] Tests for scoring

### M3-05 deltas + clustering (LATER)
- [ ] Persist clusters and run-to-run deltas
