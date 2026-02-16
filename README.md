# ShadowScope

ShadowScope is a batch investigative OSINT pipeline for surfacing "support footprints" of sensitive programs inside public procurement data.

It is designed for batch workflows (not real-time alerting):
1) ingest a time window
2) normalize + persist to Postgres
3) tag with ontology signals (keywords + clause hits)
4) score/rank leads
5) snapshot leads and compute deltas across runs
6) investigate via search and ranked views

Last updated: 2026-02-16

---

## Current status

Plumbing is stable and M3 "investigator signal" is complete:
- Compose stack: backend + Postgres + OpenSearch
- Alembic migrations are deterministic (startup + CLI) with an advisory lock
- Ingest is idempotent (stable hash + uq_events_hash)
- ingest_runs and analysis_runs are persisted for auditability
- Ontology tagging populates events.keywords and events.clauses (idempotent)
- Scoring + /api/leads provides explainable lead ranking
- Lead snapshots and deltas are persisted (compare two snapshots)
- CI runs pytest on push/PR

Next focus is M4: entity enrichment + correlation/relationship layer.

---

## Core concepts

- Postgres is the source of truth for normalized events and analysis artifacts.
- OpenSearch is a derived index built from Postgres events for fast text search.
- Stable hash is the dedupe key for events (uq_events_hash).
- Ontology hits are persisted (keywords and clause-level matches with weights).
- Runs are first-class:
  - ingest_runs: source ingest executions
  - analysis_runs: ontology apply executions
  - lead_snapshots: persisted ranked lead lists for delta comparisons

---

## Architecture

Services (Docker Compose):
- backend: FastAPI API + ingestion + analysis utilities
- db: Postgres 15
- opensearch: OpenSearch 2.x

Key repo paths:
- backend/app.py: FastAPI app + startup lifecycle
- backend/api/routes.py: /api endpoints (events/entities/search/leads/analysis-runs/lead-snapshots/lead-deltas)
- backend/db/: models + migrations + ops
- backend/analysis/: ontology validation, tagger, scoring
- backend/services/: ingest, tagging, leads, deltas
- tools/opensearch_reindex.py: Postgres -> OpenSearch indexing
- shadowscope/cli.py: CLI control plane
- docs/: runbook + opensearch notes

---

## Data flow

1) Ingest -> Postgres
- USAspending ingest writes raw snapshots under data/raw/usaspending/YYYYMMDD/
- Normalized rows are inserted into events with a stable hash; duplicates are ignored.

2) Ontology apply -> Postgres
- ss ontology apply reads ontology.json and writes:
  - events.keywords: stable pack:rule identifiers
  - events.clauses: structured match objects with weights
- analysis_runs records parameters and counters for each apply.

3) Scoring -> Leads
- Score is derived from clauses (sum of weights), with a keyword fallback.
- /api/leads returns ranked events with a score breakdown.

4) Lead snapshots + deltas
- ss leads snapshot persists a ranked list into lead_snapshots + lead_snapshot_items.
- ss leads delta compares two snapshots and reports:
  - new leads, removed leads, and changed rank/score.

5) OpenSearch
- tools/opensearch_reindex.py builds/refreshes the index from Postgres.
- Use --recreate after mapping changes or DB resets; use --full to refresh docs.

---

## Quick start (Docker-first)

Start services:
- docker compose up -d --build

Run migrations:
- python -m shadowscope.cli db init

Ingest a small batch:
- python -m shadowscope.cli ingest usaspending --days 7 --pages 1 --page-size 25

Tag events:
- python -m shadowscope.cli ontology validate
- python -m shadowscope.cli ontology apply --days 30 --source USAspending

Refresh OpenSearch index (optional but recommended after tagging):
- python tools/opensearch_reindex.py --opensearch-url http://127.0.0.1:9200 --database-url "postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope" --index shadowscope-events --full --json

Leads + snapshots:
- python -m shadowscope.cli leads snapshot --source USAspending --min-score 1 --limit 50 --notes "baseline"
- python -m shadowscope.cli leads snapshot --source USAspending --min-score 1 --limit 50 --notes "after_change"
- python -m shadowscope.cli leads delta --from-snapshot-id <OLD> --to-snapshot-id <NEW> --json

---

## API endpoints (selected)

Base: http://localhost:8000 (or use 8001 override below)

- GET /health
- GET /api/search?q=...&limit=...
- GET /api/leads?limit=...&min_score=...&include_details=true
- GET /api/analysis-runs?limit=...
- GET /api/lead-snapshots?limit=...
- GET /api/lead-snapshots/{snapshot_id}/items?limit=...
- GET /api/lead-deltas?from_snapshot_id=...&to_snapshot_id=...

---

## Windows note (Docker Desktop port proxy)

If port 8000 is flaky on Windows, use a local override (do not commit):

docker-compose.override.yml:
services:
  backend:
    ports:
      - "8001:8000"

Then:
- docker compose up -d --build --force-recreate
- use http://127.0.0.1:8001/...

---

## Ops safety notes

- Do not run a second Postgres container against the same Docker volume.
- Prefer inspection via:
  docker compose exec -T db psql -U postgres -d shadowscope -c "select count(*) from events;"