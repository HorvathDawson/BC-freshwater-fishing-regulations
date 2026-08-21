#!/usr/bin/env bash
# Start the curation-review backend. Local, read/writes files, makes NO LLM calls (no credits).
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT/curation-review/backend"          # so `import reuse` resolves (hyphenated dir can't be a package)
export PYTHONPATH="$ROOT"                    # so `import pipeline...` resolves
exec "$ROOT/.venv/bin/uvicorn" app:app --reload --port "${PORT:-8787}"
