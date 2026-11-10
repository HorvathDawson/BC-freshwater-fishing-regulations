#!/usr/bin/env bash
# One command to run the whole curation-review app: backend (FastAPI :8787) + frontend (Vite :5173).
# Local only, makes NO LLM calls, spends NO credits. Ctrl-C stops both.
#
#   bash curation-review/run.sh          then open http://127.0.0.1:5173
#
# REVIEW MODE, the only mode (T185): the app serves reads and one write, a mark (POST /api/marks); it
# never writes the catalogue or splits.json. Both servers bind 127.0.0.1 only (uvicorn
# `--host 127.0.0.1`; Vite `host: "127.0.0.1"`): not reachable from another machine. The backend's
# CORS admits only the Vite origin. PORT / CURATION_VITE_PORT move the two ports (a second copy
# beside the owner's).
# (T541) It refuses to start when either port is already listening (naming the PID), and goes on to Vite only once
# GET /api/ready answers from the backend it started; the app header names that backend's start commit, marks store
# and write-through, with a banner when the repo's HEAD has moved since.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
export CURATION_VITE_PORT="${CURATION_VITE_PORT:-5173}"
export CURATION_API="http://127.0.0.1:${PORT:-8787}"
# WARM BEFORE THE REVIEWER SITS DOWN (T136): the backend loads every cache a first entry reads in the
# background; `curl -s $CURATION_API/api/ready` says `"ready": true` when it is done.
export CURATION_WARM="${CURATION_WARM:-1}"
# (T520) a saved input goes to STAGING by deploy/staging/sync-inputs.sh (backend/inputs_sync.py); off unless set
export CURATION_SYNC_INPUTS="${CURATION_SYNC_INPUTS:-1}"

# --- (T541) refuse a taken port: a second backend on one used to die with "address already in use" (scrolled away
# by Vite) while the new frontend talked to the OLD backend — its code, its marks store, its write-through ---
API_PORT="${PORT:-8787}"
free_from() {  # the first port from $1 up that nothing listens on (the suggestion only; never taken here)
  local p="$1"
  while lsof -nP -iTCP:"$p" -sTCP:LISTEN -t >/dev/null 2>&1; do p=$((p + 1)); done
  echo "$p"
}
taken() {  # port name: refuse, naming who holds it and the two ways on
  local pids
  pids="$( { lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null || true; } | sort -u | tr '\n' ' ')"  # rc 1 = none
  [ -n "$pids" ] || return 0
  echo "REFUSED: the $2 port :$1 is already listening (PID ${pids% }: $(ps -o command= -p "${pids%% *}" 2>/dev/null | cut -c1-120))." >&2
  echo "  Either use that copy (open http://127.0.0.1:$CURATION_VITE_PORT — check its header names the commit you expect)," >&2
  echo "  or start this one beside it on other ports:  PORT=$(free_from $((API_PORT + 1))) CURATION_VITE_PORT=$(free_from $((CURATION_VITE_PORT + 1))) bash curation-review/run.sh" >&2
  exit 1
}
taken "$API_PORT" backend
taken "$CURATION_VITE_PORT" frontend

# --- backend (background) ---
# (T434) no --reload unless DEV=1: a reload kills the write-through thread mid-apply (the shutdown hook drains it)
# Its output goes to the terminal AND a log, so a backend that dies at start says why here (T541).
BACKEND_LOG="$(mktemp -t curation-backend)"
echo "starting backend on :$API_PORT …  (log: $BACKEND_LOG)"
( cd "$ROOT/curation-review/backend" && exec > >(tee -a "$BACKEND_LOG") 2>&1 && PYTHONPATH="$ROOT" exec "$ROOT/.venv/bin/uvicorn" app:app ${DEV:+--reload} --host 127.0.0.1 --port "$API_PORT" ) &
BACKEND_PID=$!
trap 'echo; echo "stopping…"; kill "$BACKEND_PID" 2>/dev/null || true' EXIT INT TERM

# --- (T541) wait until GET /api/ready answers FROM THIS CHILD (its pid, or under DEV=1 the reloader's worker) ---
READY_WAIT="${CURATION_READY_WAIT:-180}"
t0=$SECONDS
while :; do
  if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    wait "$BACKEND_PID" 2>/dev/null && rc=0 || rc=$?
    echo "REFUSED: the backend exited (rc=$rc) before answering /api/ready. Its last output:" >&2
    tail -n 30 "$BACKEND_LOG" >&2
    trap - EXIT; exit 1
  fi
  got="$(curl -s --max-time 5 "$CURATION_API/api/ready" 2>/dev/null || true)"
  if [ -n "$got" ]; then
    who="$(printf '%s' "$got" | "$ROOT/.venv/bin/python" -c 'import json,sys; b=json.load(sys.stdin)["backend"]; print(b["pid"], (b["started_commit"] or "?")[:12], "on" if b["write_through"] else "OFF", b["marks_path"])' 2>&1)" || {
      echo "REFUSED: :$API_PORT answered /api/ready with something that is not this backend's identity: $who" >&2; exit 1; }
    set -- $who
    if [ "$1" = "$BACKEND_PID" ] || [ "$(ps -o ppid= -p "$1" 2>/dev/null | tr -d ' ')" = "$BACKEND_PID" ]; then
      echo "backend ready to answer: pid $1, started on commit $2, write-through $3, marks $4"
      break
    fi
    echo "REFUSED: :$API_PORT is answered by PID $1, not the backend this script started ($BACKEND_PID)." >&2; exit 1
  fi
  if [ $((SECONDS - t0)) -ge "$READY_WAIT" ]; then
    echo "REFUSED: the backend (PID $BACKEND_PID) did not answer /api/ready within ${READY_WAIT}s; log: $BACKEND_LOG" >&2
    exit 1
  fi
  sleep 1
done

# --- frontend (foreground) ---
cd "$ROOT/curation-review/frontend"
[ -d node_modules ] || { echo "installing frontend deps…"; npm install; }
echo "starting frontend on :$CURATION_VITE_PORT …  → open http://127.0.0.1:$CURATION_VITE_PORT"
npm run dev
