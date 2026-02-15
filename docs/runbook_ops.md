# ShadowScope Ops Runbook

## Start/Stop
Start:
  docker compose up -d --build

Stop (keep data):
  docker compose down

Stop + DELETE data (destructive):
  docker compose down -v

## Database backup (Postgres)
Create a SQL dump:
  New-Item -ItemType Directory -Force backups | Out-Null
  docker compose exec -T db pg_dump -U postgres -d shadowscope > ("backups\shadowscope_{0}.sql" -f (Get-Date -Format "yyyyMMdd_HHmmss"))

## Database restore (Postgres)
Restore from a dump file (destructive to current DB contents if dump includes schema):
  Get-Content .\backups\shadowscope_YYYYMMDD_HHMMSS.sql | docker compose exec -T db psql -U postgres -d shadowscope

## IMPORTANT: Prevent Postgres corruption
Do NOT run a second Postgres container against the same Docker volume.
Never run commands like:
  docker run ... -v shadowscope_db_data:/var/lib/postgresql/data postgres:15
while compose 'db' exists/runs.
Use this instead for inspection:
  docker compose exec -T db psql -U postgres -d shadowscope -c "SELECT COUNT(*) FROM events;"

## OpenSearch indexing
Full rebuild (drop + recreate index):
  python tools\opensearch_reindex.py --opensearch-url http://127.0.0.1:9200 --database-url "postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope" --index shadowscope-events --recreate

Incremental (default; indexes only new events):
  python tools\opensearch_reindex.py --opensearch-url http://127.0.0.1:9200 --database-url "postgresql+psycopg://postgres:postgres@localhost:5432/shadowscope" --index shadowscope-events

## Windows host port workaround (Docker Desktop)
If port 8000 behaves inconsistently, create a local override (do not commit):
  docker-compose.override.yml:
    services:
      backend:
        ports:
          - "8001:8000"

Then:
  docker compose up -d --build --force-recreate
Use:
  http://127.0.0.1:8001/health
  http://127.0.0.1:8001/api/search?q=nasa&limit=5