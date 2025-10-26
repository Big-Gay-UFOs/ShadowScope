# ShadowScope

ShadowScope is an open-source intelligence (OSINT) pipeline focused on surfacing potential "program shadow" signals across FFRDCs, UARCs, and associated cut-outs. The system ingests public procurement, property, regulatory, security, and transport datasets; normalizes entities; and correlates multi-lane event clusters to highlight leads for further human review.

## Repository structure

```
shadowscope/
  docs/                # Architecture plans, design notes, and operational checklists
  backend/             # FastAPI service, ETL connectors, correlation engine, and database layer
  ui/                  # Developer-facing CLI utilities and future lightweight interfaces
  docker-compose.yml   # Local orchestration for Postgres, OpenSearch, and the FastAPI backend
  requirements.txt     # Python runtime dependencies
```

## Getting started

1. Read `docs/phase0_plan.md` for the Phase 0 architecture, scope, and dependency notes.
2. Install Docker and Docker Compose v2.
3. Copy `.env.example` (to be added in a later phase) and set required credentials.
4. Build the backend image (installs Python dependencies during the image build so runtime internet access is not required):
   ```bash
   docker compose build backend
   ```
5. Start the stack:
   ```bash
   docker compose up -d
   ```
6. Once the services are healthy, visit `http://localhost:8000/health` for a simple readiness check.

### Offline-friendly local testing

If you need to run unit tests or work behind a restrictive network proxy, create a local virtual environment **before** network access is removed:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

With the services running (see above) and Postgres listening on `localhost:5432`, tests can be executed offline:

```bash
export TEST_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope
pytest -q
```

## Development status

- [x] Phase 0: Scope, architecture outline, and dependency list
- [x] Phase 1 (bootstrap): Repository scaffold, Docker Compose, FastAPI hello endpoint, and database initialization
- [ ] Phase 2: Data model, index mappings, and seed data
- [ ] Phase 3+: Incremental connector, parsing, correlation, and UI development

## License

TBD – select an OSI-approved license before public release.
