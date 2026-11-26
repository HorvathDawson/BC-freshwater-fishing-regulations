#!/bin/bash
# bootstrap.sh — a fresh clone to a machine that can run `python -m pipeline build` (T416).
#
#   bash scripts/bootstrap.sh              # tools check, git hooks, venv, JS install, pinned inputs, basemap, DFO pages
#   bash scripts/bootstrap.sh --setup-only # the same up to and including the installs; fetches NOTHING (T1120)
#
# Re-runnable: each step skips what is already done (the pinned downloads resume). It never fetches the live sources:
# `data/source/pins.json` names the exact bytes the live release was built from. Refreshing from upstream is a separate
# choice (`python data/fetch_data.py`, then `python -m pipeline.tools.source_pins pin …`). Runbook:
# docs/runbooks/fresh-machine.md.
#
# Secrets are not needed to build. To upload: rclone remote `r2:` (R2 S3 token) and wrangler login (staging deploys).
set -euo pipefail
cd "$(dirname "$0")/.."
setup_only=0
case "${1:-}" in
  --setup-only) setup_only=1 ;;
  "") ;;
  *) echo "usage: bash scripts/bootstrap.sh [--setup-only]" >&2; exit 2 ;;
esac
t() { echo "[$(date '+%F %T')] $*"; }
t0=$SECONDS

t "tools"
missing=()
for tool in tippecanoe tile-join pmtiles curl git; do command -v "$tool" >/dev/null || missing+=("$tool"); done
if (( ${#missing[@]} )); then
  echo "missing: ${missing[*]} — on a Mac: brew install tippecanoe pmtiles   (tile-join comes with tippecanoe)" >&2
  exit 1
fi
for tool in rclone node pnpm gh jq; do command -v "$tool" >/dev/null || echo "  (optional, for uploads/the app/CI status: $tool not found)"; done

# (T1120) the versioned commit-msg / pre-push hooks: once per clone (worktrees share it)
t "git hooks (core.hooksPath .githooks)"
git config core.hooksPath .githooks

PY=${PYTHON:-python3.13}
if [[ ! -x .venv/bin/python ]]; then
  t "venv ($PY)"
  "$PY" -m venv .venv
  .venv/bin/pip install -q --upgrade pip
fi
.venv/bin/pip install -q -r requirements.txt -c constraints.txt     # pinned: the live release's exact versions

# (T1120) the JS install: the app workspace, which also holds the commit-msg hook's commitlint
if command -v pnpm >/dev/null; then
  t "app install (pnpm -C app install --frozen-lockfile)"
  pnpm -C app install --frozen-lockfile
else
  echo "  (pnpm not found: skipped the app install; the commit-msg hook needs it. See app/package.json packageManager)"
fi

if (( setup_only )); then
  t "setup only ($((SECONDS - t0)) s): tools, hooks, .venv, app install. Nothing was fetched. Next: bash scripts/bootstrap.sh"
  exit 0
fi

t "pinned inputs (data/source/pins.json)"
.venv/bin/python -m pipeline.tools.source_pins fetch

# (T416) the released basemap: the pinned bytes are the live release's ground; releasing them writes
# data/release/basemap/<id>/ and the sidecar beside the tiles, which the release refuses to go without
t "basemap (pinned, released)"
mkdir -p data/generated/tiles
cmp -s data/source/basemap.pmtiles data/generated/tiles/basemap.pmtiles || cp data/source/basemap.pmtiles data/generated/tiles/basemap.pmtiles
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.deliver.tiles.basemap --release-only --out data/generated/tiles/basemap.pmtiles

# (the synopsis extraction + its row boxes are pins too, `generated/…`: the old `fetch_data.py --add-parsing-data`
#  backup was a stale copy, and without the row boxes every rule shipped without its highlight — T416)

# the DFO salmon pages, typed (git-ignored; the bundle reads them): today's pages, ~15 s, no credits, no secrets
t "DFO salmon feed"
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.live

t "done ($((SECONDS - t0)) s). Next: python -m pipeline build --atlas --promote   (the atlas is not in git)"
