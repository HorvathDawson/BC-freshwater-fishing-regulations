#!/usr/bin/env bash
# One command to run the whole curation-review app: backend (FastAPI :8787) + frontend (Vite :5173).
# Local only, makes NO LLM calls, spends NO credits. Ctrl-C stops both.
#
#   bash curation-review/run.sh      then open http://localhost:5173
#
# NOTE localhost, not 127.0.0.1: Vite binds ::1 by default, so the IPv4 literal
# refuses the connection while the app is running perfectly well.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"

# --- backend (background) ---
echo "starting backend on :8787 …"
( cd "$ROOT/curation-review/backend" && PYTHONPATH="$ROOT" "$ROOT/.venv/bin/uvicorn" app:app --reload --port "${PORT:-8787}" ) &
BACKEND_PID=$!
trap 'echo; echo "stopping…"; kill "$BACKEND_PID" 2>/dev/null || true' EXIT INT TERM

# --- frontend (foreground) ---
cd "$ROOT/curation-review/frontend"
[ -d node_modules ] || { echo "installing frontend deps…"; npm install; }
echo "starting frontend on :5173 …  → open http://localhost:5173"
npm run dev
