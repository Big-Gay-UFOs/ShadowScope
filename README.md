# ShadowScope

ShadowScope is a batch investigative OSINT pipeline for surfacing "support footprints" of sensitive programs inside public procurement data. It ingests procurement records, normalizes them into a consistent schema, indexes them for fast search, and (next) will compute higher-level investigative outputs (tagging/ontology hits, improved scoring, and anomaly clustering across repeated batch runs).

Primary workflow (not real-time alerting):
- run a batch ingest for a time window
- normalize + persist to Postgres
- index into OpenSearch
- investigate via search and ranked views
- compare results across future runs

Last updated: 2026-02-15

---

## Current status

### Plumbing milestone: complete
The project is now in a stable, reproducible baseline state:

- Docker Compose runtime (backend + Postgres + OpenSearch) is reproducible.
- Deterministic migrations:
  - Alembic runs on startup and via CLI.
  - Postgres advisory lock prevents concurrent upgrades.
  - alembic_version.version_num widened to TEXT to avoid revision-length failures.
- Ingest is idempotent (stable event hashing + uq_events_hash unique constraint).
- Ingest run tracking is persisted (ingest_runs table).
- OpenSearch indexing supports:
  - full rebuild (--recreate)
  - incremental indexing (default)
  - structured one-line JSON summary (--json)
- API includes:
  - /health (db + opensearch health)
  - /api/search (OpenSearch-backed)
- CI exists (pytest in GitHub Actions).

### Implemented today
- Ingest: USAspending (active). SAM.gov is intentionally deferred.
- Normalize/store: USAspending rows become events in Postgres with a stable hash.
- Search: events are indexed into OpenSearch and queried via /api/search.
- Leads: /api/leads exists but scoring is currently simplistic/placeholder.

### Planned next
- Ontology tagging (populate events.keywords with real signal via term packs).
- Better scoring and ranking (use tags + heuristics instead of placeholder scoring).
- Persisted anomaly clusters and run-to-run deltas.

---

## Core concepts

- Postgres is the source of truth.
- OpenSearch is a derived index built from Postgres events.
- Stable hash is the dedupe key (uq_events_hash).
- Batch runs are first-class: ingest_runs captures parameters and outcomes for auditing and future run comparisons.

---

## Architecture

### Services (Docker Compose)
- backend: FastAPI API + ingest services + db ops
- db: Postgres 15
- opensearch: OpenSearch 2.x

### Repo layout (high level)
- backend/
  - app.py: FastAPI app, startup lifecycle (migrations), /health
  - api/routes.py: /api endpoints (events, entities, leads, search)
  - connectors/: source connectors (USAspending)
  - services/: ingest + export routines
  - db/: models + Alembic migrations + ops (sync/reset/stamp)
  - search/: OpenSearch adapter utilities
- shadowscope/cli.py: CLI control plane (db, ingest, export, test, serve)
- tools/opensearch_reindex.py: Postgres -> OpenSearch indexing tool (recreate/incremental/json)
- docs/
  - opensearch.md: mapping + versioning strategy
  - runbook_ops.md: backup/restore + indexing + Windows port workaround
- .github/workflows/ci.yml: CI (pytest)

---

## Data flow (how it works)

### 1) Ingest (USAspending -> raw snapshots + normalized rows)
- Fetches USAspending pages with retry/backoff.
- Writes raw snapshots to data/raw/usaspending/YYYYMMDD/page_<n>.json for traceability.
- Normalizes into events:
  - category, doc_id, snippet, raw_json
  - hash: stable SHA-256 digest (dedupe key)

### 2) Persist (Postgres)
- Inserts use "do nothing on conflict" semantics on hash (uq_events_hash).
- Re-running the same ingest should not duplicate rows.

### 3) Track ingest runs (Postgres)
Each ingest writes an ingest_runs row with:
- params: days, pages, page_size, max_records, start_page
- counts: fetched, normalized, inserted
- timestamps, status, and error (if any)

### 4) Index (Postgres -> OpenSearch)
tools/opensearch_reindex.py loads docs from Postgres into OpenSearch:
- document _id = hash
- includes event_id (Postgres events.id) used for incremental indexing

Modes:
- --recreate: drop + recreate index, then full load (use after DB resets or mapping changes)
- default: incremental by max(event_id) in the existing index

Important: incremental indexing will not "go backwards". If Postgres is reset but the index is not, run --recreate to resync.

### 5) Search (API -> OpenSearch)
- /api/search performs a multi-field query over snippet/place_text/doc_id/keywords
- optional filters: source, category

---

## API endpoints

Base: http://localhost:8000 (see Windows note below)

- GET /health
- GET /api/ping
- GET /api/entities
- GET /api/events?limit=50
- GET /api/leads?limit=50&min_score=1&source=...&exclude_source=...
- GET /api/search?q=...&limit=50&source=...&category=...

---

## CLI

Preferred:
- python -m shadowscope.cli ...

Key commands:
- python -m shadowscope.cli db init
- python -m shadowscope.cli ingest usaspending --days 7 --pages 2 --page-size 100 --max-records 200
- python -m shadowscope.cli export events --out data/exports
- python -m shadowscope.cli test

Ingest semantics:
- --pages: how many pages to request
- --page-size: records per page (USAspending max is 100)
- --max-records: total cap across all pages (defaults to pages * page-size)

The ingest CLI prints:
- Run ID
- Summary line (copy/paste friendly)

---

## Quick start (Docker-first)

1) Start:
- docker compose up -d --build

2) Health:
- http://localhost:8000/health

3) Ingest a small batch:
- docker compose exec -T backend python -m shadowscope.cli ingest usaspending --days 7 --pages 1 --page-size 25

4) Build OpenSearch index:
- full rebuild:
  python tools/opensearch_reindex.py --opensearch-url http://127.0.0.1:9200 --database-url "postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope" --index shadowscope-events --recreate
- incremental:
  python tools/opensearch_reindex.py --opensearch-url http://127.0.0.1:9200 --database-url "postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope" --index shadowscope-events

5) Search:
- http://localhost:8000/api/search?q=nasa&limit=10

---

## Windows note (Docker Desktop port proxy)
If port 8000 behaves inconsistently on Windows, use a local override (do not commit):

docker-compose.override.yml:
services:
  backend:
    ports:
      - "8001:8000"

Then:
- docker compose up -d --build --force-recreate
- use:
  http://127.0.0.1:8001/health
  http://127.0.0.1:8001/api/search?q=nasa&limit=5

---

## Operations
See docs/runbook_ops.md and docs/opensearch.md.

Important: do not run a second Postgres instance against the same Docker volume. Inspect DB via:
- docker compose exec -T db psql -U postgres -d shadowscope -c "SELECT COUNT(*) FROM events;"

---

## Limitations (current)
- Tagging/ontology is not implemented yet; keywords is generally empty.
- Lead scoring is currently simplistic/placeholder until tagging/correlation improves.
- Persisted clusters and run-to-run deltas are planned but not implemented.

---

## Testing and CI
Local:
- python -m shadowscope.cli test

CI:
- GitHub Actions runs pytest on push and pull requests.

---

## License
TBD (select an OSI-approved license before broader public use).