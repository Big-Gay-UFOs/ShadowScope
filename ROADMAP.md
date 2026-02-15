# ShadowScope Roadmap

Last updated: 2026-02-15

## Mission
ShadowScope is a batch OSINT pipeline designed to surface "support-footprint signals" for sensitive programs using public procurement data.
It is intentionally not an always-on alerting system. It is operated as an investigative batch tool: ingest a window, normalize + persist, index, investigate, then re-run later and compare deltas.

## Operating model (batch)
A typical run should look like:
1) Ingest a defined time window (e.g., last 30/90 days) from one or more sources.
2) Normalize records into a common Event schema and store in Postgres (source of truth).
3) Index events into OpenSearch for fast text + filter queries.
4) Investigate via search + ranked views.
5) Re-run later; compare what changed (new high-signal events, new/changed clusters).

## Architecture choices
- Postgres = source of truth (events/entities/correlations/ingest_runs, etc.)
- OpenSearch = derived search layer (fast full-text and filtering)
- Docker Compose = reproducible local stack (backend + db + opensearch)

## Current status (2026-02-15)
Plumbing baseline is complete and verified:
- Docker Compose stack runs (backend + Postgres + OpenSearch).
- Deterministic migrations:
  - advisory lock prevents concurrent Alembic upgrades
  - alembic_version.version_num widened to TEXT (no revision-length failure)
- Ingest (USAspending):
  - predictable semantics (pages + page-size + max-records)
  - idempotent (stable hash + uq_events_hash)
  - ingest run tracking persisted (ingest_runs)
- OpenSearch indexing:
  - reindex tool supports --recreate, --full, incremental default, and --json summary
  - mapping documented (docs/opensearch.md)
- API:
  - /health includes db + opensearch status
  - /api/search backed by OpenSearch
- Ops runbook added (docs/runbook_ops.md)
- CI added (GitHub Actions pytest)

Known caveats:
- Windows host port 8000 may behave inconsistently (Docker Desktop port proxy). Use local docker-compose.override.yml mapping 8001:8000 (documented in ops runbook).
- OpenSearch incremental indexing uses max(event_id) in index. If Postgres is reset but OpenSearch is not, run --recreate to resync.

## Milestones

### M0 - Plumbing baseline (DONE)
- [x] Reproducible Compose stack
- [x] Deterministic Alembic migrations (lock + widened version column)
- [x] Ingest semantics clarified + idempotent inserts
- [x] ingest_runs persisted
- [x] OpenSearch reindex hardened + incremental + JSON summary
- [x] /health includes OpenSearch
- [x] /api/search backed by OpenSearch
- [x] CI workflow

### M1 - Ingestion at scale (PARTIAL)
Goal: ingest real windows repeatedly without duplication; keep runs auditable.
- [x] USAspending ingest works + dedupe verified
- [x] ingest run tracking exists
- [ ] Run larger windows (e.g., 90/180/365 days) and validate:
      - stable idempotency across reruns
      - runtime performance (pages * page-size)
      - export size and stability

### M2 - Search indexing (DONE)
Goal: events searchable by text and filters.
- [x] OpenSearch index mapping + documented versioning
- [x] Postgres -> OpenSearch indexing tool (recreate/full/incremental)
- [x] /api/search endpoint

### M3 - Investigator signal: Tagging + Scoring (NEXT)
Goal: make results meaningfully rankable and filterable by "signals" (not just raw text).
- [ ] Define ontology format (packs/categories, weights, phrases/regex rules, enable/disable)
- [ ] Implement tagger that populates events.keywords (and optionally events.clauses)
- [ ] Add analysis_runs table (analysis metadata: ontology version/hash, window, timestamps)
- [ ] Add CLI: ss analyze (tag + score + record analysis run)
- [ ] Upgrade /api/leads to use the ontology/scoring model (not placeholder)
- [ ] Ensure OpenSearch refresh after analysis (run --full or reindex strategy)

### M4 - Persisted anomaly clusters + deltas (LATER)
Goal: generate and track multi-signal clusters across runs.
- [ ] anomaly_clusters table (fingerprint, score, last_seen)
- [ ] cluster_members table (event <-> cluster membership)
- [ ] delta reporting (new clusters, growing clusters, new high-signal events)
- [ ] API endpoints for clusters + deltas

### M5 - Better correlation (LATER)
Goal: stronger linkage for higher confidence clusters.
- [ ] Contractor normalization + linking
- [ ] Place normalization (optional geocoding)
- [ ] Entity resolution heuristics (FFRDC/UARC/site)

### M6 - Investigator outputs (LATER)
Goal: export bundles usable for review/sharing.
- [ ] Run summary exports
- [ ] Cluster dossier exports (events + rationale + trends)
- [ ] Optional report generator