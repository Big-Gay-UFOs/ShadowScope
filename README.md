# ShadowScope

ShadowScope is a batch investigative OSINT pipeline for surfacing “support footprints” of sensitive programs inside public procurement data. It is designed to ingest procurement records, normalize them into a consistent schema, index them for fast search, and (next) compute higher-level investigative outputs like tagging/ontology hits, improved scoring, and anomaly clustering across repeated batch runs.

The intended workflow is not “real-time alerting.” It’s: run a batch ingest for a time window → normalize + persist → index → investigate/search → compare runs over time.

Last updated: 2026-02-15

---

## Current status

### Plumbing milestone: complete
The project is now in a stable, reproducible “investigation-ready” baseline state:

- Docker Compose runtime (backend + Postgres + OpenSearch) is reproducible.
- Deterministic migrations:
  - Alembic runs on startup and via CLI.
  - Postgres advisory lock prevents concurrent upgrades.
  - `alembic_version.version_num` widened to `text` to avoid 32-char revision failures.
- Ingest is idempotent (stable event hashing + `uq_events_hash` unique constraint).
- Ingest run tracking is persisted (`ingest_runs` table).
- OpenSearch indexing is reliable and supports:
  - full rebuild (`--recreate`)
  - incremental indexing (default)
  - structured one-line JSON summaries (`--json`)
- API includes:
  - `/health` (db + opensearch health)
  - `/api/search` (OpenSearch-backed)
- CI exists (pytest in GitHub Actions).

### What is implemented today
- Ingest: USAspending (production path). SAM.gov is present as a placeholder but is intentionally deferred.
- Normalize → store: USAspending records become `events` in Postgres with a stable `hash`.
- Search: events are indexed into OpenSearch and queried via `/api/search`.
- Leads: `/api/leads` provides a basic ranked view (currently simple scoring; see “Limitations”).

### What is planned next
- Ontology tagging (populate `events.keywords` with real signal based on configurable term packs).
- Better scoring and ranking (use tags + heuristics instead of placeholder scoring).
- Persisted anomaly clusters and run-to-run deltas.

---

## Core concepts

- **Postgres is the source of truth.**
  Normalized `events` and supporting tables live in Postgres.
- **OpenSearch is a derived index.**
  It’s rebuilt from Postgres and used for fast full-text search and filtering.
- **Stable hash = dedupe key.**
  Every event has a deterministic `hash` and Postgres enforces uniqueness (`uq_events_hash`). Re-running ingest should not duplicate events.
- **Batch runs are first-class.**
  `ingest_runs` captures parameters and outcomes for each ingest execution to support auditing and (later) run comparisons.

---

## Architecture

### Services (Docker Compose)
- `backend`: FastAPI application (API + ingestion services + DB ops)
- `db`: Postgres 15 (primary data store)
- `opensearch`: OpenSearch 2.x (search index)

### Repository layout (high level)
- `backend/`
  - `app.py`: FastAPI app, startup lifecycle (migrations), `/health`
  - `api/routes.py`: `/api/*` endpoints (`events`, `entities`, `leads`, `search`)
  - `connectors/`: source-specific connectors (USAspending)
  - `services/`: ingestion + export routines
  - `db/`: models + Alembic migrations + ops (sync/reset/stamp)
  - `search/`: OpenSearch adapter utilities
- `shadowscope/cli.py`: CLI control plane (`db`, `ingest`, `export`, `test`, `serve`)
- `tools/opensearch_reindex.py`: Postgres → OpenSearch indexing tool (recreate/incremental/json)
- `docs/`
  - `opensearch.md`: mapping + versioning strategy
  - `runbook_ops.md`: backup/restore + indexing + Windows port workaround
- `.github/workflows/ci.yml`: GitHub Actions CI (pytest)

---

## Data flow (how it works)

### 1) Ingest (USAspending → raw snapshots + normalized rows)
- The ingest service calls the USAspending “spending_by_award” endpoint with retry/backoff.
- Raw pages are written to `data/raw/usaspending/YYYYMMDD/page_<n>.json` for traceability.
- The connector normalizes raw rows into ShadowScope `events`:
  - `category`: e.g., `procurement`
  - `doc_id`: award identifier
  - `snippet`: description text
  - `raw_json`: full original row payload
  - `hash`: stable SHA-256 digest (dedupe key)

### 2) Persist (Postgres)
- Events are inserted with “do nothing on conflict” semantics:
  - Unique key is `hash` (enforced by `uq_events_hash`).
- Re-running the same ingest should produce `inserted=0` for duplicates.

### 3) Track ingest runs (Postgres)
Every ingest execution creates a row in `ingest_runs` capturing:
- parameters: `days`, `pages`, `page_size`, `max_records`, `start_page`
- outputs: `fetched`, `normalized`, `inserted`, timestamps, and status/error
This supports auditing and later “compare runs” features.

### 4) Index (Postgres → OpenSearch)
`tools/opensearch_reindex.py` creates/maintains the OpenSearch index and loads docs from Postgres:

- Document `_id` is the event `hash` (stable).
- Each document includes:
  - `event_id` (Postgres `events.id`) used for incremental indexing
  - text fields (`snippet`, `place_text`)
  - metadata fields (`category`, `source`, `doc_id`, `source_url`, timestamps)

Indexing modes:
- `--recreate`: drop + recreate index, then full load (use after DB resets or mapping changes).
- default incremental: checks the max `event_id` in the index and loads only `events.id > max`.

Important: because incremental indexing uses `event_id`, if you reset Postgres but keep an old OpenSearch index, incremental mode will not “go backwards.” Run `--recreate` to resync.

### 5) Search (API → OpenSearch)
- `/api/search` issues an OpenSearch query (multi-match over `snippet`, `place_text`, `doc_id`, `keywords`).
- Optional filters: `source`, `category`

---

## API endpoints

Base: `http://localhost:8000` (or see Windows note below)

- `GET /health`
  - returns db status plus OpenSearch health block
- `GET /api/ping`
- `GET /api/entities`
- `GET /api/events?limit=50`
- `GET /api/leads?limit=50&min_score=1&source=...&exclude_source=...`
- `GET /api/search?q=...&limit=50&source=...&category=...`

---

## CLI

Preferred (cross-platform):
- `python -m shadowscope.cli ...`

If you installed the package with a console entrypoint, you may also have:
- `ss ...`

Key commands:
- `python -m shadowscope.cli db init`
- `python -m shadowscope.cli ingest usaspending --days 7 --pages 2 --page-size 100 --max-records 200`
- `python -m shadowscope.cli export events --out data/exports`
- `python -m shadowscope.cli test`

Ingest command notes:
- `--pages` controls how many pages you attempt.
- `--page-size` controls records per page (USAspending max is 100).
- `--max-records` caps total records across all pages (defaults to pages * page-size).

The ingest CLI prints:
- Run ID
- Summary line (copy/paste friendly)

---

## Quick start (Docker-first)

1) Start services:
- `docker compose up -d --build`

2) Health:
- `http://localhost:8000/health`

3) Ingest a small batch:
- `docker compose exec -T backend python -m shadowscope.cli ingest usaspending --days 7 --pages 1 --page-size 25`

4) Build OpenSearch index:
- full rebuild:
  - `python tools/opensearch_reindex.py --opensearch-url http://127.0.0.1:9200 --database-url "postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope" --index shadowscope-events --recreate`
- incremental:
  - `python tools/opensearch_reindex.py --opensearch-url http://127.0.0.1:9200 --database-url "postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope" --index shadowscope-events`

5) Search:
- `http://localhost:8000/api/search?q=nasa&limit=10`

---

## Windows note (Docker Desktop port proxy)
On some Windows setups, port `8000` can behave inconsistently due to the Docker Desktop port proxy layer. If you see inconsistent host responses, use a local override:

Create `docker-compose.override.yml` (do not commit):
```yaml
services:
  backend:
    ports:
      - "8001:8000"