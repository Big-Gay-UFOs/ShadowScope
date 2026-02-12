# ShadowScope Roadmap

## Mission

ShadowScope is a batch OSINT pipeline designed to surface **support-footprint signals** for sensitive programs (e.g., UAP/UFO reverse engineering, crash retrieval support, exotic materials handling) using **public procurement data**.

ShadowScope is intentionally **not** an always-on alerting system. It is operated as an **investigative batch analysis tool**: you run a windowed ingest + analysis pass, review ranked results, then re-run later to see what changed.

## Operating model (batch)

A typical run should look like:

1. **Ingest** a defined time window (e.g., last 30/90 days) from one or more sources.
2. **Tag** events with a configurable ontology (terms/categories, weights, enabled/disabled).
3. **Score** events and produce a ranked leads view.
4. **Cluster** events into anomaly clusters using co-occurrence + proximity signals.
5. **Persist** clusters so future runs can attach new events to existing clusters.
6. **Report deltas** (new clusters, clusters with increased activity, new high-signal events).

## Key concepts

- **Event**: a normalized record (award/notice/etc.) stored in Postgres with a stable hash.
- **Ontology**: the editable term/category definitions used for tagging and scoring.
- **Lead**: a ranked view of events based on scoring.
- **Anomaly cluster**: a persisted "case file" representing an area/signature where multiple signals co-occur.
- **Fingerprint**: the identity of a cluster across runs (geographic anchor + signature vector).

## Architecture choices

- **Postgres = source of truth** for structured tables (events/entities/correlations).
- **OpenSearch = search layer** for fast full-text/filtered queries once events are indexed.
- **Docker Compose = reproducible local stack** for backend + Postgres + OpenSearch.

## Current status (2026-02-12)

Infrastructure and baseline app are stable:

- Docker Compose stack runs reliably (Postgres healthcheck + backend dependency).
- Postgres migrations apply on startup.
- OpenSearch is up and reachable.
- FastAPI endpoints work: `/api/events`, `/api/entities`, `/api/leads`.
- CLI exists: `python -m shadowscope.cli ...` with `ingest usaspending` and `ingest sam`.
- Entities can be seeded into Postgres via `seed_entities.py`.

Gaps:

- OpenSearch indexing of ShadowScope events is not yet implemented.
- Anomaly clusters are not yet persisted as first-class records.
- Entity/contractor/location correlation is minimal (events typically have `entity_id` unset).

## Milestones

### M1 — Ingestion at scale (Postgres)

Goal: reliably ingest real records into Postgres with idempotency.

- Run USAspending ingest for larger windows (days/pages/limit).
- Validate dedupe (stable hash) and repeatable re-runs.
- Add SAM ingest once `SAM_API_KEY` is configured.

Deliverable: Postgres has thousands of real events; reruns do not duplicate.

### M2 — Search indexing (OpenSearch)

Goal: make events searchable by text and filters.

- Define an `events` index mapping.
- Add an index bootstrap step.
- Bulk index events from Postgres.
- Add `/api/search?q=...` endpoint.

Deliverable: OpenSearch contains a ShadowScope index; search endpoint returns ranked hits.

### M3 — Anomaly clusters (persisted)

Goal: create and track multi-signal clusters across runs.

- Add `analysis_runs` table (run metadata: time window, ontology version).
- Add `anomaly_clusters` table (geo anchor + signature + score + last_seen).
- Add `cluster_members` table (event ↔ cluster membership).
- Implement cluster fingerprinting and delta reporting.

Deliverable: each batch run produces clusters; subsequent runs update existing clusters and show deltas.

### M4 — Better correlation (entity/location/contractor)

Goal: stronger linkage to enable higher-confidence clusters.

- Improve place normalization + optional geocoding.
- Normalize contractors into a table and link events.
- Resolve events to entities (FFRDC/UARC/site) using place + agency + contractor heuristics.

Deliverable: clusters and leads can be filtered by entity/site/contractor.

### M5 — Investigator outputs

Goal: make results easy to review and store.

- Export run summaries (JSON/CSV).
- Export cluster dossiers (events + rationale + trend info).
- Optional: markdown report generator for sharing.

Deliverable: one command produces a usable report bundle for a run.
