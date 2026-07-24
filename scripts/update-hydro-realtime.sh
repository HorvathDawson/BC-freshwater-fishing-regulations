#!/usr/bin/env bash
# update-hydro-realtime.sh — frequent (~15 min) hydro realtime/forecast cron.
#
# Thin wrapper over the portable Python entrypoint. ALL orchestration + R2 I/O
# lives in pipeline/recurring/hydro/jobs.py (boto3, S3-compatible), so this shell
# is throwaway: the same `python -m ... jobs realtime` runs unchanged in a
# Cloudflare Container triggered by a Cron Trigger. See jobs.py for the design.
#
# Stateless for the heavy readings: pulls only the small seed DB + the fid shards
# its gauges need, re-fetches the last 14 days fresh, exports realtime, uploads.
#
# Usage:
#   ./scripts/update-hydro-realtime.sh            # run against R2 (needs creds)
#   ./scripts/update-hydro-realtime.sh --local DIR  # dry run against a local dir
#
# Env: DEPLOY_ENV (staging|production) selects the bucket; R2 S3 creds — see jobs.py.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

exec python -m pipeline.recurring.hydro.jobs realtime "$@"
