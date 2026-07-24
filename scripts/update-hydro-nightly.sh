#!/usr/bin/env bash
# update-hydro-nightly.sh — nightly hydro history + climatology cron (~01:00 PST).
#
# Thin wrapper over pipeline/recurring/hydro/jobs.py (see update-hydro-realtime.sh
# for the portability rationale). STATEFUL: keeps the full hydro.db in R2, refreshes
# daily means + HYDAT climatology, exports history/climatology/realtime, rebuilds the
# small seed DB the realtime cron consumes, and pushes the full DB back for next night.
#
# Usage:
#   ./scripts/update-hydro-nightly.sh             # run against R2 (needs creds)
#   ./scripts/update-hydro-nightly.sh --local DIR # dry run against a local dir
#
# Env: DEPLOY_ENV (staging|production); R2 S3 creds — see jobs.py.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

exec python -m pipeline.recurring.hydro.jobs nightly "$@"
