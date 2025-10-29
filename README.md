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

### Local testing

Create a virtual environment (once) and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
```

With the environment active you can run the lightweight test suite against SQLite:

```bash
pytest -q tests/test_health.py
```

To prove outbound HTTP connectivity and confirm the hardened USAspending payload still returns data, execute the CLI smoke:

```bash
python -m ui.minimal_cli --since 2025-01-01 --limit 5
```

The script prints how many awards were fetched and surfaces any upstream validation errors if they occur. SAM.gov integration will require authenticated API access and is planned for a later phase, so there is no equivalent smoke test yet.

## Development status

- [x] Phase 0: Scope, architecture outline, and dependency list
- [x] Phase 1 (bootstrap): Repository scaffold, Docker Compose, FastAPI hello endpoint, and database initialization
- [ ] Phase 2: Data model, index mappings, and seed data
- [ ] Phase 3+: Incremental connector, parsing, correlation, and UI development

## License

TBD – select an OSI-approved license before public release.
