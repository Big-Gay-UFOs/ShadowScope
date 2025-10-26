#!/usr/bin/env bash
set -euo pipefail
source .venv/bin/activate
export DATABASE_URL=sqlite:///./dev.db

rm -f dev.db || true
pkill -f "uvicorn backend.app:app" 2>/dev/null || true
nohup python -m uvicorn backend.app:app --host 0.0.0.0 --port 8000 > server.log 2>&1 &

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
