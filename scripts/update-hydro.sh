#!/usr/bin/env bash
# update-hydro.sh — the single unified hydro cron.
#
# Thin wrapper over the portable Python entrypoint. ALL orchestration + R2 I/O
# lives in pipeline/recurring/hydro/jobs.py (boto3, S3-compatible). One job does
# everything, gating internally by cheap change-detection:
#   • every run — fetch latest + (re)write stations.json, gauges.geojson, recent/
#   • daily     — merge fresh daily means into history/<id>.json (idempotent)
#   • HYDAT     — rebuild climatology/<id>.json when a new HYDAT release lands
# STATE lives in the JSON artifacts (no persisted DB in R2); the run uses an
# ephemeral /tmp sqlite as scratch.
#
# Usage:
#   ./scripts/update-hydro.sh              # run against R2 (needs creds)
#   ./scripts/update-hydro.sh --local DIR  # dry run against a local dir
#   ./scripts/update-hydro.sh --force      # force the history + climatology tiers
#
# Env: DEPLOY_ENV (staging|production) selects the bucket; R2 S3 creds — see jobs.py.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

exec python -m pipeline.recurring.hydro.jobs run "$@"
