#!/usr/bin/env bash
# Start the curation-review backend. Local, makes NO LLM calls (no credits).
#   run.sh          review mode, the only mode: reads, and one write — a reviewer's mark
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT/curation-review/backend"          # so `import reuse` resolves (hyphenated dir can't be a package)
export PYTHONPATH="$ROOT"                    # so `import pipeline...` resolves
export CURATION_WARM="${CURATION_WARM:-1}"   # load the caches at startup; GET /api/ready says when (T136)
# (T434) no --reload unless DEV=1: a reload kills the write-through thread mid-apply (the shutdown hook drains it)
exec "$ROOT/.venv/bin/uvicorn" app:app ${DEV:+--reload} --host 127.0.0.1 --port "${PORT:-8787}"
