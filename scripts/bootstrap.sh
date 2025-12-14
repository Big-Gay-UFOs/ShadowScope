#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "ShadowScope bootstrap starting..."

if command -v python3.13 >/dev/null 2>&1; then
  PY_CMD="python3.13"
elif command -v python3 >/dev/null 2>&1; then
  PY_CMD="python3"
else
  PY_CMD="python"
fi

if [ ! -d ".venv" ]; then
  echo "Creating virtual environment at $REPO_ROOT/.venv"
  "$PY_CMD" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e .

if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "Created .env from template"
fi

echo "Initializing database..."
ss db init

echo "Running tests..."
ss test

echo "Ingesting USAspending sample..."
ss ingest usaspending --days 3 --limit 25 --pages 1

echo "Exporting events..."
export_output=$(ss export events)
echo "$export_output"

echo "Log files:"
echo " - $REPO_ROOT/logs/app.log"
echo " - $REPO_ROOT/logs/ingest.log"

echo "Starting API server on http://127.0.0.1:8000"
ss serve --host 127.0.0.1 --port 8000
