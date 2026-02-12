# ShadowScope

ShadowScope is a **batch investigative OSINT pipeline** that ingests public procurement records, tags them with a configurable **keyword ontology**, scores individual records, and aggregates multi-signal **anomaly clusters** (co-occurring indicators in proximity). The goal is to surface likely *support footprints* for sensitive programs that won’t be labeled directly (e.g., UAP/UFO reverse engineering, crash retrieval support, exotic materials handling, specialized containment/shielding, classified facilities and networks).

It is intentionally **not** a continuous alerting system. The primary workflow is: run a batch ingest for a time window, compute scores and clusters, persist cluster records, and compare results across future runs.

## What it does

1. **Ingest**: pull procurement records (currently: USAspending; optional: SAM.gov with API key).
2. **Normalize**: convert source-specific records into a common Event schema and store in Postgres.
3. **Tag**: apply ontology terms (keyword packs) to produce structured keyword hits.
4. **Score**: compute an event score and a ranked “leads” view.
5. **Cluster**: group events into anomaly clusters using proximity signals (time/geo/contractor/entity) and rank clusters.
6. **Track across runs**: persist clusters and update them as new events appear in later batch runs.

## Key terms (quick definitions)

- **Docker**: runs the stack (backend, Postgres, OpenSearch) as isolated services on your machine.
- **Postgres**: the primary database (“source of truth”) holding structured tables (events, entities, correlations).
- **OpenSearch**: the search engine used for fast full-text queries once events are indexed (think “Google layer”).
- **Ontology**: the configurable set of terms/categories you use to tag records (keyword packs, weights, enable/disable).
- **Event**: one normalized record (e.g., an award/notice) with text, metadata, keywords, and a stable hash.
- **Lead**: a ranked view of events based on scores (currently computed, not persisted as a table).
- **Anomaly cluster**: a persisted “case file” representing a geographic/temporal/contractor pattern of co-occurring signals.

## Current architecture

- `backend`: FastAPI service + connector ingestion + scoring/correlation logic.
- `db`: Postgres 15.
- `opensearch`: OpenSearch 2.11.

## Quick start (local)

1. Start the stack:
   - `docker compose up -d --build`
2. Health check:
   - `http://localhost:8000/health`
3. Seed baseline entities (FFRDCs):
   - `docker exec -it shadowscope-backend python seed_entities.py`
4. Run a small USAspending ingest into Postgres:
   - `docker exec -it shadowscope-backend python -m shadowscope.cli ingest usaspending --days 7 --pages 1 --limit 100`
5. Explore:
   - `http://localhost:8000/api/events`
   - `http://localhost:8000/api/entities`
   - `http://localhost:8000/api/leads`

## Development status

As of 2026-02-12:

- [x] Repo hygiene: ignore runtime artifacts (`data/`, `logs/`, `*.db`, `__pycache__/`).
- [x] Docker stack: Postgres + OpenSearch + backend.
- [x] Postgres migrations: schema created and upgraded on startup.
- [x] Seed entities: `seed_entities.py` loads baseline entities.
- [x] CLI control plane: `db`/`export`/`ingest`/`serve`/`test`.
- [x] Ingest (skeleton): USAspending + SAM CLI commands wired.
- [ ] Ingest (production): run larger real-world pulls into Postgres and validate idempotency at scale.
- [ ] OpenSearch indexing: create an app index and index events; add `/api/search`.
- [ ] Anomaly clusters: persist cluster records + membership; rank clusters; support deltas across runs.
- [ ] Correlation upgrades: attach events to entities/contractors/locations more reliably.

See `ROADMAP.md` for the clarified goals and milestone plan, and `phase0_plan.md` for architecture notes.

## License

TBD – select an OSI-approved license before public release.
