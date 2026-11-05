#!/bin/bash
# STAGING ONLY: deploy the feeds Worker (bc-fishing-feeds-staging; hardcoded), then RUN EVERY FEED JOB at once, so a
# release's new output is in the feeds now, not at the next cron (owner 2026-11-05: the forecast detail stayed old after a
# worker release). The deployed version is in every job's reuse key (src/io.ts `rev`), so these runs recompute everything.
#   deploy/staging/feeds.sh
# Exit 1 when a job fails (its JSON is printed). Production: the same with its own worker, by the owner (T240).
set -euo pipefail
cd "$(dirname "$0")/../.."
URL=https://bc-fishing-feeds-staging.horvath-dawson.workers.dev
npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.toml 2>&1 | grep -v "npm warn" | tail -4
T=$(tr -d '\n' < app/workers/feeds/.run-token)
sleep 10   # the new version reaches every location
fail=0
for job in gauges dfo inseason; do
  out=$(curl -s -w '\n%{http_code}' -X POST -H "Authorization: Bearer $T" "$URL/run/$job")
  code=${out##*$'\n'}; body=${out%$'\n'*}
  echo "$job: HTTP $code $(printf '%s' "$body" | head -c 300)"
  [[ "$code" == 200 ]] || fail=1
done
exit $fail
