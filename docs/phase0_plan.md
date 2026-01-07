# Phase 0 — Architecture & Scope

## System architecture (high-level)

```mermaid
graph TD
  subgraph Ingestion
    A[Source Schedulers]
    B[Connector Workers]
    C[Parser & Clause Miner]
  end
  subgraph DataPlatform
    D[(Postgres)]
    E[(OpenSearch)]
    F[Blob Storage (S3-compatible)]
    G[Entity Resolver]
  end
  subgraph Analytics
    H[Correlation Engine]
    I[Scoring & Anti-Confirmation]
  end
  subgraph Interfaces
    J[FastAPI Service]
    K[GPT Tool Adapter]
    L[CLI / Cron]
  end

  A --> B --> C
  C --> D
  C --> E
  C --> F
  D --> G --> D
  D --> H
  E --> H
  H --> I --> J
  J --> K
  J --> L
```

## Execution plan (initial roadmap)

1. **Bootstrap & infrastructure** – scaffold repo, define data contracts, ship Docker Compose stack (FastAPI + Postgres + OpenSearch) with health checks.
2. **Data modeling** – design normalized schema, OpenSearch mappings, and seed entity loader to support repeatable dev/test cycles.
3. **Core connectors (Phase 3a)** – implement USAspending, SAM.gov, FPDS ATOM, CBD archive, and FRPP pipelines with pagination, idempotency, and normalization.
4. **Parsing & enrichment** – build PDF/text extraction, clause mining, and keyword packs; configure attachment storage.
5. **Entity resolution & geospatial enrichment** – implement UEI/CAGE lookups, fuzzy matching, and geocoding with caching.
6. **Correlation engine & scoring** – build temporal/spatial clustering, lane coverage scoring, and anti-confirmation logic.
7. **API + tooling** – expose ingestion/correlation endpoints, provide GPT tool manifest, and deliver CLI workflows.
8. **Testing, sample runs, and operations** – unit/integration tests, sample datasets, runbooks, logging, and deployment notes.

## Data sources & coverage notes

| Lane | Source | Realistic coverage window | Notes |
| ---- | ------ | ------------------------- | ----- |
| Procurement | USAspending API | 2008 → present | JSON REST API with pagination by `page`/`limit`; supports filtering by action date. |
| Procurement | SAM.gov Opportunities | 2001 → present (varies); attachments recent 5–7 years | Requires API key; attachments accessible via public URLs; older records may need scraping of archived notices. |
| Procurement | FPDS ATOM | 1996 → present | SOAP/ATOM feed; best accessed in weekly signed-date windows; consider archival throttling. |
| Procurement | CBD Archives | 1990 → 2001 | Text/PDF bulletins; static file hosting; requires relaxed parsers for legacy encodings. |
| Property | FRPP | 2010 → present annual snapshots | CSV/XLSX bulk downloads; compute YoY deltas for ROI filtering. |
| Property | Regents/Trustees agendas | 2005 → present (varies per institution) | Build pluggable scrapers; start with UNM, UNLV, Cal Poly Pomona. |
| Property | Local permits | 2000s → present | City/county portals; design config-driven scraper for NV Clark County & Los Alamos County NM. |
| Nuclear/Reg | NRC ADAMS | 1990s → present | ADAMS ADAMS accession search; use keyword + facility filters; provide rate limiting. |
| Nuclear/Reg | Agreement-State portals | 2000s → present | Track NM, NV, TN states; some require CSV downloads, others HTML scraping. |
| Nuclear/Reg | DOE/NNSA NEPA reading rooms | 1994 → present | Index of PDFs; parse for shielding/radiological terms. |
| Security | Public job boards & clause PDFs | 2005 → present | Focus on cleared job boards + posted DD254/JSIG attachments; store metadata + text. |
| Security | Policy/Compliance docs | 1990s → present | Mine for FAR/DFARS, DoDM 5205.07, ICD-705 references. |
| Transport | PHMSA special permits | 1990 → present | HTML/CSV; emit permit renewals and approvals keyed by entity/state. |
| Transport | FAA TFR/NOTAM | 2000 → present | REST feeds for current; historical via FAA NOTAM archive (CSV). |

## Stack options & selection

1. **Data store:** PostgreSQL vs. SQLite vs. MySQL
   - PostgreSQL chosen for native JSONB, GIS extensions (PostGIS), robust concurrency, and compatibility with OpenSearch sync jobs.
2. **Search index:** OpenSearch vs. PostgreSQL Full-Text Search vs. Meilisearch
   - OpenSearch selected for distributed search, analyzers, and compatibility with vector expansion if needed later. Postgres FTS retained for lightweight text queries.
3. **Orchestration:** Docker Compose vs. local virtualenv + services
   - Docker Compose selected to encapsulate Postgres, OpenSearch, and the FastAPI service, simplifying onboarding and mirroring production containerization.

## MVP scope vs. stretch goals

**MVP scope**
- Entity & event schema defined with migrations.
- Connectors for USAspending, SAM.gov, FPDS ATOM (limited window), CBD archive (sample slice), and FRPP delta logic.
- Parsing pipeline for PDFs/text with clause/keyword extraction.
- Basic entity normalization (UEI/CAGE + fuzzy) and geocoding cache.
- Correlation engine producing lead groups with High/Med/Low scores and anti-confirmation notes.
- FastAPI endpoints (`/ingest/run`, `/events/search`, `/correlate/run`, `/correlate/{id}/dossier`) plus CLI wrappers.
- Automated tests covering connectors, parsing, correlation logic, and sample ingest run.

**Acceptance criteria**
- Running `docker compose up` seeds schema, exposes FastAPI docs, and reports healthy status.
- Invoking the USAspending connector with sample creds inserts normalized events into Postgres and indexes OpenSearch.
- End-to-end sample script produces at least one correlation group from fixtures.
- Documentation includes runbook, API references, and GPT tool manifest.

**Stretch goals**
- Incremental connectors (NEPA, ADAMS, PHMSA, FAA, Regents/permits) with configurable schedulers.
- Automated FOIA drafting endpoint leveraging templated statutes.
- Prometheus metrics exporter and Grafana dashboard definitions.
- Terraform blueprints for single-VM and ECS deployment targets.
- UI enhancements (web dashboard) beyond the minimal CLI.

## Minimal dependency list (Phase 1+)

- Python 3.13 runtime
- FastAPI + Uvicorn
- SQLAlchemy + Alembic (migrations)
- psycopg[binary]
- requests + httpx (for connectors)
- pydantic
- elasticsearch-py (for OpenSearch)
- pdfplumber, python-tika (parsing)
- rapidfuzz (fuzzy matching)
- geopandas/shapely or geopy for geocoding utilities
- pytest + pytest-dotenv + responses (testing)
- docker, docker compose v2

## Immediate next steps

1. Implement the relational schema and OpenSearch mappings (Phase 2).
2. Expand USAspending connector to persist events and cover edge cases; begin SAM.gov connector (Phase 3).
3. Stand up OpenSearch client utilities and index bootstrapping inside the backend service.
