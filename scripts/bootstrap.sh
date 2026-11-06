#!/bin/bash
# bootstrap.sh — a fresh clone to a machine that can run `python -m pipeline build` (T416).
#
#   bash scripts/bootstrap.sh            # tools check, venv, pinned inputs, synopsis extraction backup
#
# Re-runnable: each step skips what is already done (the pinned downloads resume). It never fetches the live sources:
# `data/source/pins.json` names the exact bytes the live release was built from. Refreshing from upstream is a separate
# choice (`python data/fetch_data.py`, then `python -m pipeline.tools.source_pins pin …`). Runbook:
# handoff/PLAN/runbooks/fresh-machine.md.
#
# Secrets are not needed to build. To upload: rclone remote `r2:` (R2 S3 token) and wrangler login (staging deploys).
set -euo pipefail
cd "$(dirname "$0")/.."
t() { echo "[$(date '+%F %T')] $*"; }
t0=$SECONDS

t "tools"
missing=()
for tool in tippecanoe tile-join pmtiles curl git; do command -v "$tool" >/dev/null || missing+=("$tool"); done
if (( ${#missing[@]} )); then
  echo "missing: ${missing[*]} — on a Mac: brew install tippecanoe pmtiles   (tile-join comes with tippecanoe)" >&2
  exit 1
fi
for tool in rclone node; do command -v "$tool" >/dev/null || echo "  (optional, for uploads/the app: $tool not found)"; done

PY=${PYTHON:-python3.13}
if [[ ! -x .venv/bin/python ]]; then
  t "venv ($PY)"
  "$PY" -m venv .venv
  .venv/bin/pip install -q --upgrade pip
fi
.venv/bin/pip install -q -r requirements.txt

t "pinned inputs (data/source/pins.json)"
.venv/bin/python -m pipeline.tools.source_pins fetch

t "synopsis extraction backup"
PYTHONPATH="$PWD" .venv/bin/python data/fetch_data.py --add-parsing-data

# the DFO salmon pages, typed (git-ignored; the bundle reads them): today's pages, ~15 s, no credits, no secrets
t "DFO salmon feed"
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.live

t "done ($((SECONDS - t0)) s). Next: python -m pipeline build --atlas --promote   (the atlas is not in git)"
