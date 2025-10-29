#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[maintenance] $1"
}

if [[ ! -d .venv ]]; then
  log "Creating virtual environment (.venv)"
  python3 -m venv .venv
fi

source .venv/bin/activate
log "Ensuring dependencies are installed"
python -m pip install --upgrade pip >/dev/null 2>&1 || true
python -m pip install -r requirements.txt

export DATABASE_URL="${DATABASE_URL:-sqlite:///./dev.db}"
log "Using DATABASE_URL=${DATABASE_URL}"

if [[ -f dev.db ]]; then
  log "Removing existing dev.db"
  rm dev.db
fi

if pkill -f "uvicorn backend.app:app" 2>/dev/null; then
  log "Stopped existing uvicorn processes"
fi

log "Starting uvicorn on http://0.0.0.0:8000"
python -m uvicorn backend.app:app --host 0.0.0.0 --port 8000 > server.log 2>&1 &
UVICORN_PID=$!
trap 'log "Stopping uvicorn (PID ${UVICORN_PID})"; kill ${UVICORN_PID} 2>/dev/null || true' EXIT

python - <<'PY'
import time, requests
ok=False
for _ in range(120):
    try:
        h = requests.get("http://127.0.0.1:8000/health", timeout=1)
        p = requests.get("http://127.0.0.1:8000/api/ping", timeout=1)
        print("HEALTH:", h.status_code, h.text)
        print("PING:",   p.status_code, p.text)
        ok=True
        break
    except Exception:
        time.sleep(0.5)
print("OK" if ok else "FAILED")
PY

echo
echo '--- last 60 lines of server.log ---'
tail -n 60 server.log || true
