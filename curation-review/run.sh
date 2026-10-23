#!/usr/bin/env bash
# One command to run the whole curation-review app: backend (FastAPI :8787) + frontend (Vite :5173).
# Local only, makes NO LLM calls, spends NO credits. Ctrl-C stops both.
#
#   bash curation-review/run.sh          review mode — then open http://127.0.0.1:5173
#   bash curation-review/run.sh --fix    fix mode (the owner): editors, entry save, split add/move/delete/rename
#
# REVIEW MODE (the default) serves reads and one write, a mark (POST /api/marks); the fix routes are
# not registered at all. Both servers bind 127.0.0.1 only (uvicorn `--host 127.0.0.1`; Vite
# `host: "127.0.0.1"`): not reachable from another machine. The backend's CORS admits only the Vite
# origin. PORT / CURATION_VITE_PORT move the two ports (a second copy beside the owner's).
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
if [ "${1:-}" = "--fix" ]; then export CURATION_FIX=1; MODE=fix; else export CURATION_FIX=0; MODE=review; fi
export CURATION_VITE_PORT="${CURATION_VITE_PORT:-5173}"
export CURATION_API="http://127.0.0.1:${PORT:-8787}"

# --- backend (background) ---
echo "starting backend on :${PORT:-8787} ($MODE mode) …"
( cd "$ROOT/curation-review/backend" && PYTHONPATH="$ROOT" "$ROOT/.venv/bin/uvicorn" app:app --reload --host 127.0.0.1 --port "${PORT:-8787}" ) &
BACKEND_PID=$!
trap 'echo; echo "stopping…"; kill "$BACKEND_PID" 2>/dev/null || true' EXIT INT TERM

# --- frontend (foreground) ---
cd "$ROOT/curation-review/frontend"
[ -d node_modules ] || { echo "installing frontend deps…"; npm install; }
echo "starting frontend on :$CURATION_VITE_PORT …  → open http://127.0.0.1:$CURATION_VITE_PORT"
npm run dev
